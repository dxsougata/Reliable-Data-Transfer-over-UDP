"""
src/channel.py - Bidirectional UDP Channel Emulator

Simulates packet loss, corruption, duplication, delay, and reordering
between Sender and Receiver over UDP.
"""

import argparse
from dataclasses import dataclass
import heapq
import random
import socket
import threading
import time
from typing import Optional

try:
    from common.config import (
        BUFFER_SIZE,
        CHANNEL_HOST,
        CHANNEL_PORT,
        RECEIVER_HOST,
        RECEIVER_PORT,
    )
except ModuleNotFoundError:
    from src.common.config import (
        BUFFER_SIZE,
        CHANNEL_HOST,
        CHANNEL_PORT,
        RECEIVER_HOST,
        RECEIVER_PORT,
    )


@dataclass
class ChannelConfig:
    loss_probability: float = 0.0
    duplicate_probability: float = 0.0
    reorder_probability: float = 0.0
    corruption_probability: float = 0.0
    delay_probability: float = 0.0
    min_delay_ms: float = 0.0
    max_delay_ms: float = 100.0
    reorder_min_delay_ms: float = 50.0
    reorder_max_delay_ms: float = 150.0
    seed: Optional[int] = 42


class ChannelEmulator:
    def __init__(
        self,
        listen_host: str = CHANNEL_HOST,
        listen_port: int = CHANNEL_PORT,
        receiver_host: str = RECEIVER_HOST,
        receiver_port: int = RECEIVER_PORT,
        config: Optional[ChannelConfig] = None,
        **kwargs,
    ):
        self.config = config or ChannelConfig()
        for k, v in kwargs.items():
            if hasattr(self.config, k):
                setattr(self.config, k, v)

        self.listen_addr = (listen_host, listen_port)
        self.receiver_addr = (receiver_host, receiver_port)
        self.sender_addr = None
        self.rng = random.Random(self.config.seed)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.listen_addr)
        self.sock.settimeout(0.5)

        self.queue = []
        self.counter = 0
        self.cv = threading.Condition()
        self.running = False
        self.stats = {
            "received": 0, "forwarded": 0, "dropped": 0,
            "corrupted": 0, "duplicated": 0, "delayed": 0, "reordered": 0,
        }

    def _corrupt_packet(self, data: bytes) -> bytes:
        if not data:
            return data
        b = bytearray(data)
        idx = self.rng.randrange(len(b))
        b[idx] ^= 0xFF
        return bytes(b)

    def _schedule_packet(self, data: bytes, dest: tuple, extra_delay: float = 0.0):
        delay = extra_delay
        if self.rng.random() < self.config.delay_probability:
            d = self.rng.uniform(self.config.min_delay_ms, self.config.max_delay_ms) / 1000.0
            delay += d
            self.stats["delayed"] += 1
            print(f"[CHANNEL] DELAY {d * 1000:.1f}ms")

        with self.cv:
            self.counter += 1
            heapq.heappush(self.queue, (time.monotonic() + delay, self.counter, data, dest))
            self.cv.notify()

    def _forward_worker(self):
        while self.running:
            with self.cv:
                while self.running and not self.queue:
                    self.cv.wait(timeout=0.1)
                if not self.running:
                    break
                delivery_time, _, data, dest = self.queue[0]
                rem = delivery_time - time.monotonic()
                if rem > 0:
                    self.cv.wait(timeout=rem)
                    continue
                heapq.heappop(self.queue)
            try:
                self.sock.sendto(data, dest)
                self.stats["forwarded"] += 1
            except OSError:
                pass

    def _handle_packet(self, data: bytes, source: tuple):
        if source == self.receiver_addr:
            if not self.sender_addr:
                return
            dest = self.sender_addr
        else:
            self.sender_addr = source
            dest = self.receiver_addr

        self.stats["received"] += 1

        # 1. Packet Loss
        if self.rng.random() < self.config.loss_probability:
            self.stats["dropped"] += 1
            print(f"[CHANNEL] DROP ({len(data)}B)")
            return

        # 2. Packet Corruption
        if self.rng.random() < self.config.corruption_probability:
            data = self._corrupt_packet(data)
            self.stats["corrupted"] += 1
            print(f"[CHANNEL] CORRUPT ({len(data)}B)")

        # 3. Packet Reordering (extra delay)
        extra = 0.0
        if self.rng.random() < self.config.reorder_probability:
            extra = self.rng.uniform(self.config.reorder_min_delay_ms, self.config.reorder_max_delay_ms) / 1000.0
            self.stats["reordered"] += 1
            print(f"[CHANNEL] REORDER (+{extra * 1000:.1f}ms)")

        # 4. Schedule Forwarding (+ normal delay)
        self._schedule_packet(data, dest, extra)

        # 5. Packet Duplication
        if self.rng.random() < self.config.duplicate_probability:
            self.stats["duplicated"] += 1
            print("[CHANNEL] DUPLICATE")
            self._schedule_packet(data, dest)

    def _listen_worker(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
            except (socket.timeout, BlockingIOError):
                continue
            except OSError:
                break
            self._handle_packet(data, addr)

    def start(self):
        if self.running:
            return
        self.running = True
        threading.Thread(target=self._forward_worker, daemon=True).start()
        threading.Thread(target=self._listen_worker, daemon=True).start()
        print(f"[CHANNEL] Listening on {self.listen_addr} -> Receiver: {self.receiver_addr}")

    def stop(self):
        self.running = False
        with self.cv:
            self.cv.notify_all()
        try:
            self.sock.close()
        except OSError:
            pass
        print("\n========== CHANNEL STATISTICS ==========")
        for k, v in self.stats.items():
            print(f"  {k:<12}: {v}")
        print("========================================")

    def run(self):
        self.start()
        try:
            while self.running:
                time.sleep(0.2)
        except KeyboardInterrupt:
            print("\n[CHANNEL] Shutdown requested.")
        finally:
            self.stop()


def main():
    p = argparse.ArgumentParser(description="UDP Channel Emulator")
    p.add_argument("--listen-host", default=CHANNEL_HOST, help="Channel IP (default: %(default)s)")
    p.add_argument("--listen-port", type=int, default=CHANNEL_PORT, help="Channel Port (default: %(default)s)")
    p.add_argument("--receiver-host", default=RECEIVER_HOST, help="Receiver IP (default: %(default)s)")
    p.add_argument("--receiver-port", type=int, default=RECEIVER_PORT, help="Receiver Port (default: %(default)s)")
    p.add_argument("--loss", type=float, default=0.0, help="Packet loss probability [0.0-1.0]")
    p.add_argument("--duplicate", "--dup", dest="duplicate", type=float, default=0.0, help="Duplication probability")
    p.add_argument("--corrupt", type=float, default=0.0, help="Corruption probability")
    p.add_argument("--delay", type=float, default=0.0, help="Packet delay probability")
    p.add_argument("--reorder", type=float, default=0.0, help="Packet reorder probability")
    p.add_argument("--min-delay", type=float, default=0.0, help="Min delay in ms (default: 0)")
    p.add_argument("--max-delay", type=float, default=100.0, help="Max delay in ms (default: 100)")
    p.add_argument("--reorder-min-delay", type=float, default=50.0, help="Min reorder delay in ms (default: 50)")
    p.add_argument("--reorder-max-delay", type=float, default=150.0, help="Max reorder delay in ms (default: 150)")
    p.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = p.parse_args()

    config = ChannelConfig(
        loss_probability=args.loss,
        duplicate_probability=args.duplicate,
        reorder_probability=args.reorder,
        corruption_probability=args.corrupt,
        delay_probability=args.delay,
        min_delay_ms=args.min_delay,
        max_delay_ms=args.max_delay,
        reorder_min_delay_ms=args.reorder_min_delay,
        reorder_max_delay_ms=args.reorder_max_delay,
        seed=args.seed,
    )
    ChannelEmulator(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        receiver_host=args.receiver_host,
        receiver_port=args.receiver_port,
        config=config,
    ).run()


if __name__ == "__main__":
    main()
