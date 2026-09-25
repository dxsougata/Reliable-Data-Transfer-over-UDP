"""
protocols/go_back_n.py

Go-Back-N ARQ over UDP: the sender may have up to `window_size` packets
in flight at once instead of waiting for each ACK individually.

    base                                  next_seq_num
     |                                          |
    [sent+acked][ sent, waiting for ACK  ][ not yet sent ]
                 <----- window_size ----->

  - Receiver only accepts packets that arrive strictly in order. Anything
    out of order (or corrupted) is dropped, and the receiver re-sends the
    ACK for the last packet it *did* accept in order (a cumulative ACK).
  - Sender keeps ONE timer, for the oldest unacked packet (`base`). If it
    fires, the sender resends every packet currently in flight
    (base .. next_seq_num-1), not just the oldest one -- that's the
    defining "go back N" behaviour.
  - An ACK for seq N is cumulative: it means "everything up to and
    including N has been received correctly", so receiving it slides
    `base` forward past N.

Sequence numbers here are just "packet index in this transfer" taken
modulo SEQ_MODULO (see common/config.py). Because SEQ_MODULO (65536) is
always far larger than any reasonable window_size, converting an
incoming ACK's sequence number back into a window position is done with
modular offset arithmetic (see `_ack_offset`).
"""

import socket
import time

from common.packet import (
    Packet,
    PacketType,
    PacketError,
    make_start_payload,
    parse_start_payload,
)
from common.checksum import file_hash
from common.config import (
    MAX_PAYLOAD_SIZE,
    DEFAULT_TIMEOUT,
    DEFAULT_WINDOW_SIZE,
    MAX_RETRIES,
    UDP_RECV_BUFFER_SIZE,
    SEQ_MODULO,
)


class TransferFailed(Exception):
    """Raised when the window's base packet could not be delivered within
    max_retries full-window retransmissions."""


def _ack_offset(ack_seq: int, base_seq: int) -> int:
    """How many positions past base_seq does ack_seq sit, under modular
    sequence-number arithmetic? (0 means "ack_seq == base_seq".)"""
    return (ack_seq - base_seq) % SEQ_MODULO


class GoBackNSender:
    def __init__(self, sock: socket.socket, dest_addr, timeout=DEFAULT_TIMEOUT,
                 window_size=DEFAULT_WINDOW_SIZE, max_retries=MAX_RETRIES, on_event=None):
        self.sock = sock
        self.dest_addr = dest_addr
        self.timeout = timeout
        self.window_size = window_size
        self.max_retries = max_retries
        self.on_event = on_event or (lambda *a, **k: None)

    def _send_window(self, packets: list, stats: dict) -> dict:
        """
        Reliably deliver every packet in `packets` (already in final
        transfer order -- START, DATA..., END) using a sliding window.
        Returns the stats dict, updated in place.
        """
        n = len(packets)
        base = 0            # index of oldest unacked packet
        next_seq_num = 0     # index of next packet to send
        timer_deadline = None
        base_retry_count = 0

        self.sock.settimeout(self.timeout)

        while base < n:
            # Send everything the window currently allows.
            while next_seq_num < min(base + self.window_size, n):
                self.sock.sendto(packets[next_seq_num].pack(), self.dest_addr)
                stats["packets_sent"] += 1
                if timer_deadline is None:
                    timer_deadline = time.monotonic() + self.timeout
                next_seq_num += 1

            remaining = timer_deadline - time.monotonic() if timer_deadline else self.timeout
            if remaining <= 0:
                # Timeout: go back N -- resend the whole in-flight window.
                base_retry_count += 1
                if base_retry_count > self.max_retries:
                    raise TransferFailed(
                        f"gave up on seq={packets[base].seq_num} after {self.max_retries} retries"
                    )
                self.on_event("timeout_retransmit_window", base=packets[base].seq_num,
                              count=next_seq_num - base)
                for i in range(base, next_seq_num):
                    self.sock.sendto(packets[i].pack(), self.dest_addr)
                    stats["packets_sent"] += 1
                    stats["retransmissions"] += 1
                timer_deadline = time.monotonic() + self.timeout
                continue

            self.sock.settimeout(remaining)
            try:
                data, _ = self.sock.recvfrom(UDP_RECV_BUFFER_SIZE)
            except socket.timeout:
                continue  # loop back around, the remaining<=0 branch fires next pass

            try:
                ack = Packet.unpack(data)
            except PacketError:
                continue
            if ack.type != PacketType.ACK or not ack.is_valid():
                continue

            offset = _ack_offset(ack.seq_num, packets[base].seq_num)
            in_flight = next_seq_num - base
            if offset >= in_flight:
                continue  # duplicate / stale / not-yet-sent ack, ignore

            # Cumulative ACK: everything through this offset is confirmed.
            base += offset + 1
            base_retry_count = 0
            self.on_event("ack_received", seq=ack.seq_num, new_base=base)
            timer_deadline = time.monotonic() + self.timeout if base < next_seq_num else None

        return stats

    def send_file(self, filepath: str) -> dict:
        with open(filepath, "rb") as f:
            file_data = f.read()

        chunks = [
            file_data[i:i + MAX_PAYLOAD_SIZE]
            for i in range(0, len(file_data), MAX_PAYLOAD_SIZE)
        ] or [b""]

        stats = {
            "packets_sent": 0,
            "retransmissions": 0,
            "bytes_sent": len(file_data),
            "chunks_sent": len(chunks),
        }

        start_time = time.monotonic()

        start_payload = make_start_payload(len(chunks), bytes.fromhex(file_hash(file_data)))
        packets = [Packet(PacketType.START, 0, start_payload)]
        packets += [Packet(PacketType.DATA, i + 1, chunk) for i, chunk in enumerate(chunks)]
        packets.append(Packet(PacketType.END, len(chunks) + 1, b""))

        self._send_window(packets, stats)

        stats["elapsed_sec"] = time.monotonic() - start_time
        stats["goodput_bps"] = (
            (stats["bytes_sent"] * 8) / stats["elapsed_sec"] if stats["elapsed_sec"] > 0 else 0
        )
        return stats


class GoBackNReceiver:
    def __init__(self, sock: socket.socket, on_event=None):
        self.sock = sock
        self.on_event = on_event or (lambda *a, **k: None)

    def receive_file(self, save_path: str) -> dict:
        expected_seq = 0
        received_chunks = []
        total_chunks = None
        expected_hash_bytes = None
        sender_addr = None
        last_acked_seq = None  # None until we've accepted at least one packet

        stats = {"packets_received": 0, "out_of_order_discarded": 0, "corrupted_discarded": 0}

        self.sock.settimeout(None)
        while True:
            data, addr = self.sock.recvfrom(UDP_RECV_BUFFER_SIZE)
            if sender_addr is None:
                sender_addr = addr

            try:
                pkt = Packet.unpack(data)
            except PacketError:
                continue

            if not pkt.is_valid():
                stats["corrupted_discarded"] += 1
                self.on_event("corrupted", seq=pkt.seq_num)
                continue  # no ACK at all; sender's timer will recover it

            stats["packets_received"] += 1

            if pkt.seq_num != expected_seq:
                # Out of order (a gap exists) -- go-back-N receivers do not
                # buffer these. Re-ACK the last in-order packet we accepted
                # so the sender's cumulative-ACK based window can recover.
                stats["out_of_order_discarded"] += 1
                self.on_event("out_of_order", got=pkt.seq_num, expected=expected_seq)
                if last_acked_seq is not None:
                    self.sock.sendto(Packet(PacketType.ACK, last_acked_seq).pack(), sender_addr)
                continue

            # In-order, valid: accept and cumulatively ACK it.
            self.sock.sendto(Packet(PacketType.ACK, pkt.seq_num).pack(), sender_addr)
            last_acked_seq = pkt.seq_num
            expected_seq = (expected_seq + 1) % SEQ_MODULO

            if pkt.type == PacketType.START:
                total_chunks, expected_hash_bytes = parse_start_payload(pkt.payload)
            elif pkt.type == PacketType.DATA:
                received_chunks.append(pkt.payload)
            elif pkt.type == PacketType.END:
                break

        file_data = b"".join(received_chunks)
        with open(save_path, "wb") as f:
            f.write(file_data)

        stats["chunks_received"] = len(received_chunks)
        stats["expected_chunks"] = total_chunks
        stats["bytes_received"] = len(file_data)
        stats["hash_match"] = (
            expected_hash_bytes is not None
            and bytes.fromhex(file_hash(file_data)) == expected_hash_bytes
        )
        return stats