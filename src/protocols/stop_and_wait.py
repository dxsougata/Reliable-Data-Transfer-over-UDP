import socket
import time

from common.packet import Packet, PacketType, PacketError, make_start_payload, parse_start_payload
from common.checksum import file_hash
from common.config import (
    MAX_PAYLOAD_SIZE,
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
    UDP_RECV_BUFFER_SIZE,
    SEQ_MODULO,
    MIN_RTO,
    MAX_RTO,
    RTO_CLOCK_GRANULARITY,
)
from rto import RtoEstimator


class TransferFailed(Exception):
    """Raised when a packet could not be delivered within MAX_RETRIES."""


class StopAndWaitSender:
    def __init__(self, sock: socket.socket, dest_addr, timeout=DEFAULT_TIMEOUT,
                 max_retries=MAX_RETRIES, on_event=None):
        """
        sock:        an already-created UDP socket (not bound to anything
                     in particular). This class will call settimeout() on it.
        dest_addr:   (host, port) tuple of the receiver.
        on_event:    optional callback on_event(name: str, **info) used for
                     logging / experiment metrics (e.g. "retransmit", "ack").
                     Safe to leave as None.
        """
        self.sock = sock
        self.dest_addr = dest_addr
        self.rto_estimator = RtoEstimator(
            initial_rto=timeout,
            min_rto=MIN_RTO,
            max_rto=MAX_RTO,
            clock_granularity=RTO_CLOCK_GRANULARITY,
        )
        self.max_retries = max_retries
        self.on_event = on_event or (lambda *a, **k: None)


    def _send_reliable(self, packet: Packet, stats: dict) -> None:
        """Send `packet` and block until the matching ACK arrives, resending
        on timeout. Raises TransferFailed if max_retries is exceeded."""
        attempts = 0

        while attempts <= self.max_retries:
            current_rto = self.rto_estimator.rto
            sent_at = time.monotonic()
            self.sock.sendto(packet.pack(), self.dest_addr)
            stats["packets_sent"] += 1
            if attempts > 0:
                stats["retransmissions"] += 1
                self.on_event("retransmit", seq=packet.seq_num, attempt=attempts)

            deadline = sent_at + current_rto
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break  # fall through to outer retransmit
                self.sock.settimeout(remaining)
                try:
                    data, _ = self.sock.recvfrom(UDP_RECV_BUFFER_SIZE)
                except socket.timeout:
                    break

                try:
                    ack = Packet.unpack(data)
                except PacketError:
                    continue  # garbage on the wire, keep waiting

                if ack.type == PacketType.ACK and ack.is_valid() and ack.seq_num == packet.seq_num:
                    if attempts == 0:
                        self.rto_estimator.update(time.monotonic() - sent_at)
                    self.on_event("ack_received", seq=ack.seq_num)
                    return  # success


            self.rto_estimator.backoff()
            self.on_event("rto_backoff", rto=self.rto_estimator.rto)
            attempts += 1

        raise TransferFailed(f"gave up on seq={packet.seq_num} after {self.max_retries} retries")



    def send_file(self, filepath: str) -> dict:
        """Reliably transfer filepath to dest_addr. Returns a stats dict with
        packets_sent, retransmissions, bytes_sent, chunks_sent, elapsed_sec."""
        with open(filepath, "rb") as f:
            file_data = f.read()

        chunks = [
            file_data[i:i + MAX_PAYLOAD_SIZE]
            for i in range(0, len(file_data), MAX_PAYLOAD_SIZE)
        ] or [b""]  # handle empty file: still send one (empty) chunk

        stats = {
            "packets_sent": 0,
            "retransmissions": 0,
            "bytes_sent": len(file_data),
            "chunks_sent": len(chunks),
        }

        start_time = time.monotonic()
        seq = 0

        # 1. START: tell the receiver how many chunks are coming and the
        #    expected final hash, so it can verify the reassembled file.
        start_payload = make_start_payload(len(chunks), bytes.fromhex(file_hash(file_data)))
        self._send_reliable(Packet(PacketType.START, seq, start_payload), stats)
        seq = (seq + 1) % SEQ_MODULO

        # 2. DATA packets, one at a time, each fully acked before the next.
        for chunk in chunks:
            self._send_reliable(Packet(PacketType.DATA, seq, chunk), stats)
            seq = (seq + 1) % SEQ_MODULO

        # 3. END: signals the receiver the transfer is complete.
        self._send_reliable(Packet(PacketType.END, seq, b""), stats)

        stats["elapsed_sec"] = time.monotonic() - start_time
        stats["rto_final_sec"] = self.rto_estimator.rto
        stats["rtt_samples"] = self.rto_estimator.samples
        stats["srtt_sec"] = self.rto_estimator.srtt
        stats["goodput_bps"] = (
            (stats["bytes_sent"] * 8) / stats["elapsed_sec"] if stats["elapsed_sec"] > 0 else 0
        )
        return stats


class StopAndWaitReceiver:
    def __init__(self, sock: socket.socket, on_event=None):
        """sock must already be bound (e.g. sock.bind((host, port)))."""
        self.sock = sock
        self.on_event = on_event or (lambda *a, **k: None)

    def receive_file(self, save_path: str) -> dict:
        """
        Block until a full file has been reliably received, write it to
        save_path, and return a stats dict including whether the SHA-256
        hash sent by the sender matched the reassembled file.
        """
        expected_seq = 0
        received_chunks = []
        total_chunks = None
        expected_hash_bytes = None
        sender_addr = None

        stats = {"packets_received": 0, "duplicates_discarded": 0, "corrupted_discarded": 0}

        self.sock.settimeout(None)  # receiver blocks indefinitely between packets
        while True:
            data, addr = self.sock.recvfrom(UDP_RECV_BUFFER_SIZE)
            if sender_addr is None:
                sender_addr = addr

            try:
                pkt = Packet.unpack(data)
            except PacketError:
                continue  # not even a parseable packet, drop silently

            if not pkt.is_valid():
                stats["corrupted_discarded"] += 1
                self.on_event("corrupted", seq=pkt.seq_num)
                # Can't trust pkt.seq_num either (header may be corrupt), so
                # we simply don't ACK; the sender's timer will retransmit.
                continue

            stats["packets_received"] += 1

            if pkt.seq_num != expected_seq:
                # Either a duplicate of something we already have (sender's
                # ACK to us was lost) or, in theory, out of order -- either
                # way in Stop-and-Wait the fix is the same: re-ACK the last
                # packet we *did* accept so the sender's timer clears.
                stats["duplicates_discarded"] += 1
                last_good = (expected_seq - 1) % SEQ_MODULO
                self.sock.sendto(Packet(PacketType.ACK, last_good).pack(), sender_addr)
                continue

            # In-order, valid packet -> accept it.
            self.sock.sendto(Packet(PacketType.ACK, pkt.seq_num).pack(), sender_addr)
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
