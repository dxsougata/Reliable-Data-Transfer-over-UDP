"""Selective Repeat ARQ over UDP with per-packet timers and ACKs."""

import socket
import time

from common.checksum import file_hash
from common.config import (
    DEFAULT_TIMEOUT,
    DEFAULT_WINDOW_SIZE,
    MAX_PAYLOAD_SIZE,
    MAX_RETRIES,
    MAX_RTO,
    MIN_RTO,
    RTO_CLOCK_GRANULARITY,
    SEQ_MODULO,
    UDP_RECV_BUFFER_SIZE,
)
from common.packet import (
    Packet,
    PacketError,
    PacketType,
    make_start_payload,
    parse_start_payload,
)
from rto import RtoEstimator


class TransferFailed(Exception):
    """Raised when a packet exceeds the configured retransmission limit."""


class SelectiveRepeatSender:
    def __init__(self, sock: socket.socket, dest_addr, timeout=DEFAULT_TIMEOUT,
                 window_size=DEFAULT_WINDOW_SIZE, max_retries=MAX_RETRIES,
                 on_event=None):
        if not 0 < window_size <= SEQ_MODULO // 2:
            raise ValueError("window_size must be between 1 and SEQ_MODULO / 2")
        self.sock = sock
        self.dest_addr = dest_addr
        self.window_size = window_size
        self.max_retries = max_retries
        self.on_event = on_event or (lambda *a, **k: None)
        self.rto_estimator = RtoEstimator(
            initial_rto=timeout,
            min_rto=MIN_RTO,
            max_rto=MAX_RTO,
            clock_granularity=RTO_CLOCK_GRANULARITY,
        )

    def _send_packets(self, packets, stats):
        base = 0
        next_index = 0
        outstanding = {}
        seq_to_index = {}

        while base < len(packets):
            while next_index < min(base + self.window_size, len(packets)):
                packet = packets[next_index]
                sent_at = time.monotonic()
                self.sock.sendto(packet.pack(), self.dest_addr)
                outstanding[next_index] = {
                    "packet": packet,
                    "sent_at": sent_at,
                    "deadline": sent_at + self.rto_estimator.rto,
                    "retries": 0,
                    "retransmitted": False,
                    "acked": False,
                }
                seq_to_index[packet.seq_num] = next_index
                stats["packets_sent"] += 1
                next_index += 1

            unacked = [item for item in outstanding.values() if not item["acked"]]
            if not unacked:
                continue

            now = time.monotonic()
            nearest_deadline = min(item["deadline"] for item in unacked)
            remaining = nearest_deadline - now
            if remaining <= 0:
                expired = [
                    (index, item)
                    for index, item in outstanding.items()
                    if not item["acked"] and item["deadline"] <= now
                ]
                self.rto_estimator.backoff()
                self.on_event("rto_backoff", rto=self.rto_estimator.rto)
                for index, item in expired:
                    item["retries"] += 1
                    if item["retries"] > self.max_retries:
                        raise TransferFailed(
                            f"gave up on seq={item['packet'].seq_num} "
                            f"after {self.max_retries} retries"
                        )
                    item["retransmitted"] = True
                    self.sock.sendto(item["packet"].pack(), self.dest_addr)
                    stats["packets_sent"] += 1
                    stats["retransmissions"] += 1
                    item["deadline"] = time.monotonic() + self.rto_estimator.rto
                    self.on_event(
                        "retransmit", seq=item["packet"].seq_num,
                        attempt=item["retries"],
                    )
                continue

            self.sock.settimeout(remaining)
            try:
                data, _ = self.sock.recvfrom(UDP_RECV_BUFFER_SIZE)
            except socket.timeout:
                continue

            try:
                ack = Packet.unpack(data)
            except PacketError:
                continue
            if ack.type != PacketType.ACK or not ack.is_valid():
                continue

            index = seq_to_index.get(ack.seq_num)
            if index is None or index not in outstanding:
                continue
            item = outstanding[index]
            if item["acked"]:
                continue

            if not item["retransmitted"]:
                self.rto_estimator.update(time.monotonic() - item["sent_at"])
            item["acked"] = True
            self.on_event("ack_received", seq=ack.seq_num)

            while base in outstanding and outstanding[base]["acked"]:
                old_packet = outstanding[base]["packet"]
                del seq_to_index[old_packet.seq_num]
                del outstanding[base]
                base += 1

        return stats

    def send_file(self, filepath: str) -> dict:
        with open(filepath, "rb") as file:
            file_data = file.read()

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
        start_payload = make_start_payload(
            len(chunks), bytes.fromhex(file_hash(file_data))
        )
        packets = [Packet(PacketType.START, 0, start_payload)]
        packets.extend(
            Packet(PacketType.DATA, (index + 1) % SEQ_MODULO, chunk)
            for index, chunk in enumerate(chunks)
        )
        packets.append(
            Packet(PacketType.END, (len(chunks) + 1) % SEQ_MODULO, b"")
        )

        self._send_packets(packets, stats)
        stats["elapsed_sec"] = time.monotonic() - start_time
        stats["goodput_bps"] = (
            stats["bytes_sent"] * 8 / stats["elapsed_sec"]
            if stats["elapsed_sec"] > 0 else 0
        )
        stats["rto_final_sec"] = self.rto_estimator.rto
        stats["rtt_samples"] = self.rto_estimator.samples
        stats["srtt_sec"] = self.rto_estimator.srtt
        return stats

class SelectiveRepeatReceiver:
    def __init__(self, sock: socket.socket, window_size=DEFAULT_WINDOW_SIZE,
                 on_event=None):
        if not 0 < window_size <= SEQ_MODULO // 2:
            raise ValueError("window_size must be between 1 and SEQ_MODULO / 2")
        self.sock = sock
        self.window_size = window_size
        self.on_event = on_event or (lambda *a, **k: None)

    def receive_file(self, save_path: str) -> dict:
        base_seq = 0
        buffer = {}
        received_chunks = []
        total_chunks = None
        expected_hash_bytes = None
        sender_addr = None
        stats = {
            "packets_received": 0,
            "out_of_window_discarded": 0,
            "corrupted_discarded": 0,
        }

        self.sock.settimeout(None)
        while True:
            data, addr = self.sock.recvfrom(UDP_RECV_BUFFER_SIZE)
            if sender_addr is None:
                sender_addr = addr

            try:
                packet = Packet.unpack(data)
            except PacketError:
                continue
            if not packet.is_valid():
                stats["corrupted_discarded"] += 1
                self.on_event("corrupted", seq=packet.seq_num)
                continue

            stats["packets_received"] += 1
            offset = (packet.seq_num - base_seq) % SEQ_MODULO
            if offset < self.window_size:
                # ACK every in-window packet, including duplicates, so a lost
                # ACK can be recovered without retransmitting other packets.
                self.sock.sendto(Packet(PacketType.ACK, packet.seq_num).pack(), sender_addr)
                if packet.seq_num not in buffer:
                    buffer[packet.seq_num] = packet
            elif 0 < (base_seq - packet.seq_num) % SEQ_MODULO <= self.window_size:
                # It is a duplicate from the previous receive window.
                self.sock.sendto(Packet(PacketType.ACK, packet.seq_num).pack(), sender_addr)
                continue
            else:
                stats["out_of_window_discarded"] += 1
                self.on_event("out_of_window", seq=packet.seq_num, base=base_seq)
                continue

            while base_seq in buffer:
                in_order = buffer.pop(base_seq)
                base_seq = (base_seq + 1) % SEQ_MODULO
                if in_order.type == PacketType.START:
                    total_chunks, expected_hash_bytes = parse_start_payload(in_order.payload)
                elif in_order.type == PacketType.DATA:
                    received_chunks.append(in_order.payload)
                elif in_order.type == PacketType.END:
                    file_data = b"".join(received_chunks)
                    with open(save_path, "wb") as file:
                        file.write(file_data)
                    stats["chunks_received"] = len(received_chunks)
                    stats["expected_chunks"] = total_chunks
                    stats["bytes_received"] = len(file_data)
                    stats["hash_match"] = (
                        expected_hash_bytes is not None
                        and bytes.fromhex(file_hash(file_data)) == expected_hash_bytes
                    )
                    return stats
