"""
src/channel.py

Bidirectional UDP Channel Emulator.

Topology:

    Sender  --->  Channel  --->  Receiver
       ^            |               |
       |            |               |
       +------------+---------------+
                    ACK

The channel simulates:
    - packet loss
    - packet duplication
    - packet corruption
    - packet delay
    - packet reordering

The channel works with raw bytes and therefore does not depend on any
particular ARQ protocol.

The sender's UDP source port is learned automatically because the sender
may use an OS-assigned ephemeral port.

Default topology:

    Sender destination : 127.0.0.1:9001
    Channel             : 127.0.0.1:9001
    Receiver            : 127.0.0.1:9002
"""

import argparse
import heapq
import random
import socket
import threading
import time
from dataclasses import dataclass
from typing import Optional


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ChannelConfig:
    """Configuration for channel impairments."""

    loss_probability: float = 0.0
    duplicate_probability: float = 0.0
    reorder_probability: float = 0.0
    corruption_probability: float = 0.0
    delay_probability: float = 0.0

    min_delay_ms: float = 0.0
    max_delay_ms: float = 100.0

    # Additional delay used when a packet is selected for reordering.
    reorder_min_delay_ms: float = 50.0
    reorder_max_delay_ms: float = 150.0

    # Deterministic experiments.
    seed: Optional[int] = 42

    def __post_init__(self):
        probabilities = {
            "loss_probability": self.loss_probability,
            "duplicate_probability": self.duplicate_probability,
            "reorder_probability": self.reorder_probability,
            "corruption_probability": self.corruption_probability,
            "delay_probability": self.delay_probability,
        }

        for name, value in probabilities.items():
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"{name} must be between 0.0 and 1.0"
                )

        if self.min_delay_ms < 0:
            raise ValueError(
                "min_delay_ms cannot be negative"
            )

        if self.max_delay_ms < self.min_delay_ms:
            raise ValueError(
                "max_delay_ms must be >= min_delay_ms"
            )

        if self.reorder_min_delay_ms < 0:
            raise ValueError(
                "reorder_min_delay_ms cannot be negative"
            )

        if self.reorder_max_delay_ms < self.reorder_min_delay_ms:
            raise ValueError(
                "reorder_max_delay_ms must be >= reorder_min_delay_ms"
            )


# ============================================================================
# Statistics
# ============================================================================

@dataclass
class ChannelStats:
    """Statistics collected by the channel."""

    packets_received: int = 0
    packets_forwarded: int = 0

    packets_dropped: int = 0
    packets_duplicated: int = 0
    packets_corrupted: int = 0
    packets_delayed: int = 0
    packets_reordered: int = 0

    bytes_received: int = 0
    bytes_forwarded: int = 0

    def print_summary(self):
        print("\n========== CHANNEL STATISTICS ==========")
        print(f"Packets received   : {self.packets_received}")
        print(f"Packets forwarded  : {self.packets_forwarded}")
        print(f"Packets dropped    : {self.packets_dropped}")
        print(f"Packets duplicated : {self.packets_duplicated}")
        print(f"Packets corrupted  : {self.packets_corrupted}")
        print(f"Packets delayed    : {self.packets_delayed}")
        print(f"Packets reordered  : {self.packets_reordered}")
        print(f"Bytes received     : {self.bytes_received}")
        print(f"Bytes forwarded    : {self.bytes_forwarded}")
        print("========================================")


# ============================================================================
# Scheduled packet
# ============================================================================

@dataclass(order=True)
class ScheduledPacket:
    """
    Packet waiting to be forwarded.

    Packets are ordered by delivery_time and then by insertion order.
    """

    delivery_time: float
    order: int
    data: bytes
    destination: tuple
    direction: str


# ============================================================================
# Channel Emulator
# ============================================================================

class ChannelEmulator:
    """
    Bidirectional UDP channel emulator.

    Sender -> Channel -> Receiver
    Receiver -> Channel -> Sender
    """

    def __init__(
        self,
        listen_host: str = "127.0.0.1",
        listen_port: int = 9001,
        receiver_host: str = "127.0.0.1",
        receiver_port: int = 9002,
        config: Optional[ChannelConfig] = None,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port

        self.receiver_address = (
            receiver_host,
            receiver_port,
        )

        self.config = config or ChannelConfig()

        # Dedicated RNG so experiments are reproducible.
        self.rng = random.Random(self.config.seed)

        # UDP socket used both to receive packets from the sender/receiver
        # and to forward packets onward.
        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )

        self.sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        )

        self.sock.bind(
            (self.listen_host, self.listen_port)
        )

        self.running = False

        # Learned from the first packet arriving from the sender.
        self.sender_address = None

        self.stats = ChannelStats()

        # Priority queue containing packets waiting for delivery.
        self._queue = []

        self._queue_counter = 0

        self._queue_condition = threading.Condition()

        self._forward_thread = None

    # ========================================================================
    # Lifecycle
    # ========================================================================

    def start(self):
        """Start the channel's forwarding worker."""

        if self.running:
            return

        self.running = True

        self._forward_thread = threading.Thread(
            target=self._forward_worker,
            daemon=True,
        )

        self._forward_thread.start()

        print(
            f"[CHANNEL] Listening on "
            f"{self.listen_host}:{self.listen_port}"
        )

        print(
            f"[CHANNEL] Receiver: "
            f"{self.receiver_address[0]}:"
            f"{self.receiver_address[1]}"
        )

        print(
            f"[CHANNEL] Random seed: "
            f"{self.config.seed}"
        )

    def stop(self):
        """Stop the channel and close its socket."""

        if not self.running:
            return

        self.running = False

        with self._queue_condition:
            self._queue_condition.notify_all()

        try:
            self.sock.close()
        except OSError:
            pass

        if self._forward_thread is not None:
            self._forward_thread.join(timeout=1.0)

        self.stats.print_summary()

    def run(self):
        """Run until Ctrl+C or shutdown."""

        self.start()

        try:
            while self.running:

                try:
                    self.sock.settimeout(0.5)

                    data, source = self.sock.recvfrom(65535)

                except socket.timeout:
                    continue

                except OSError:
                    if self.running:
                        raise
                    break

                self._handle_packet(
                    data,
                    source,
                )

        except KeyboardInterrupt:
            print("\n[CHANNEL] Shutdown requested.")

        finally:
            self.stop()

    # ========================================================================
    # Direction handling
    # ========================================================================

    def _identify_direction(self, source: tuple):
        """
        Identify the direction of a packet.

        Receiver -> Channel is identified using the configured receiver
        address.

        Sender -> Channel is identified using the sender address learned
        from the first sender packet.
        """

        # --------------------------------------------------------------------
        # Receiver -> Channel
        # --------------------------------------------------------------------

        if (
            source[0] == self.receiver_address[0]
            and source[1] == self.receiver_address[1]
        ):
            if self.sender_address is None:
                print(
                    "[CHANNEL] Ignoring receiver packet because "
                    "sender address is not known yet."
                )

                return None, None

            return (
                "receiver_to_sender",
                self.sender_address,
            )

        # --------------------------------------------------------------------
        # Sender -> Channel
        # --------------------------------------------------------------------

        if self.sender_address is None:

            self.sender_address = source

            print(
                f"[CHANNEL] Learned sender address: "
                f"{self.sender_address}"
            )

            return (
                "sender_to_receiver",
                self.receiver_address,
            )

        # Known sender.
        if source == self.sender_address:
            return (
                "sender_to_receiver",
                self.receiver_address,
            )

        # Unknown endpoint.
        print(
            f"[CHANNEL] Ignoring packet from unknown endpoint: "
            f"{source}"
        )

        return None, None

    # ========================================================================
    # Packet processing
    # ========================================================================

    def _handle_packet(
        self,
        data: bytes,
        source: tuple,
    ):
        """Apply channel impairments to one received UDP packet."""

        direction, destination = self._identify_direction(
            source
        )

        if direction is None:
            return

        self.stats.packets_received += 1
        self.stats.bytes_received += len(data)

        print(
            f"[CHANNEL] RX {len(data)} bytes "
            f"{source} -> {destination}"
        )

        # ====================================================================
        # 1. PACKET LOSS
        # ====================================================================

        if (
            self.rng.random()
            < self.config.loss_probability
        ):
            self.stats.packets_dropped += 1

            print("[CHANNEL] DROP")

            return

        # ====================================================================
        # 2. PACKET CORRUPTION
        # ====================================================================

        packet = data

        if (
            len(packet) > 0
            and self.rng.random()
            < self.config.corruption_probability
        ):
            packet = self._corrupt_packet(packet)

            self.stats.packets_corrupted += 1

            print("[CHANNEL] CORRUPT")

        # ====================================================================
        # 3. PACKET REORDERING
        # ====================================================================

        extra_delay = 0.0

        if (
            self.rng.random()
            < self.config.reorder_probability
        ):
            extra_delay = self.rng.uniform(
                self.config.reorder_min_delay_ms,
                self.config.reorder_max_delay_ms,
            ) / 1000.0

            self.stats.packets_reordered += 1

            print(
                f"[CHANNEL] REORDER "
                f"(extra delay "
                f"{extra_delay * 1000:.2f} ms)"
            )

        # ====================================================================
        # 4. NORMAL PACKET DELAY
        # ====================================================================

        self._schedule_packet(
            data=packet,
            destination=destination,
            direction=direction,
            extra_delay=extra_delay,
        )

        # ====================================================================
        # 5. PACKET DUPLICATION
        # ====================================================================

        if (
            self.rng.random()
            < self.config.duplicate_probability
        ):
            self.stats.packets_duplicated += 1

            print("[CHANNEL] DUPLICATE")

            # Duplicate gets its own delay decision.
            self._schedule_packet(
                data=packet,
                destination=destination,
                direction=direction,
            )

    # ========================================================================
    # Corruption
    # ========================================================================

    def _corrupt_packet(self, data: bytes) -> bytes:
        """
        Corrupt exactly one byte.

        The channel intentionally knows nothing about the packet format.

        Packet checksum validation is handled by the receiver.
        """

        corrupted = bytearray(data)

        index = self.rng.randrange(
            len(corrupted)
        )

        old_value = corrupted[index]

        new_value = self.rng.randrange(256)

        # Ensure the byte actually changes.
        while new_value == old_value:
            new_value = self.rng.randrange(256)

        corrupted[index] = new_value

        return bytes(corrupted)

    # ========================================================================
    # Packet scheduling
    # ========================================================================

    def _schedule_packet(
        self,
        data: bytes,
        destination: tuple,
        direction: str,
        extra_delay: float = 0.0,
    ):
        """
        Put a packet into the forwarding queue.

        extra_delay is primarily used for reordering.
        """

        delay = extra_delay

        # ------------------------------------------------------------
        # Normal delay
        # ------------------------------------------------------------

        if (
            self.rng.random()
            < self.config.delay_probability
        ):
            normal_delay_ms = self.rng.uniform(
                self.config.min_delay_ms,
                self.config.max_delay_ms,
            )

            delay += normal_delay_ms / 1000.0

            self.stats.packets_delayed += 1

            print(
                f"[CHANNEL] DELAY "
                f"{normal_delay_ms:.2f} ms"
            )

        delivery_time = (
            time.monotonic()
            + delay
        )

        with self._queue_condition:

            self._queue_counter += 1

            scheduled = ScheduledPacket(
                delivery_time=delivery_time,
                order=self._queue_counter,
                data=data,
                destination=destination,
                direction=direction,
            )

            heapq.heappush(
                self._queue,
                scheduled,
            )

            self._queue_condition.notify()

    # ========================================================================
    # Forwarding worker
    # ========================================================================

    def _forward_worker(self):
        """Forward queued packets when their delivery time arrives."""

        while self.running:

            with self._queue_condition:

                # Wait until at least one packet exists.
                while (
                    self.running
                    and not self._queue
                ):
                    self._queue_condition.wait()

                if not self.running:
                    break

                packet = self._queue[0]

                now = time.monotonic()

                remaining = (
                    packet.delivery_time
                    - now
                )

                # Packet is not ready yet.
                if remaining > 0:

                    self._queue_condition.wait(
                        timeout=remaining
                    )

                    continue

                # Packet is ready.
                heapq.heappop(
                    self._queue
                )

            # --------------------------------------------------------
            # Do not hold the queue lock while doing network I/O.
            # --------------------------------------------------------

            try:

                self.sock.sendto(
                    packet.data,
                    packet.destination,
                )

                self.stats.packets_forwarded += 1
                self.stats.bytes_forwarded += len(
                    packet.data
                )

                print(
                    f"[CHANNEL] TX "
                    f"{len(packet.data)} bytes "
                    f"-> {packet.destination}"
                )

            except OSError:

                if self.running:
                    print(
                        "[CHANNEL] ERROR: "
                        "failed to forward packet"
                    )


# ============================================================================
# Command-line interface
# ============================================================================

def build_argument_parser():
    """Create the command-line argument parser."""

    parser = argparse.ArgumentParser(
        description=(
            "Reliable Data Transfer UDP "
            "Channel Emulator"
        )
    )

    # ------------------------------------------------------------------------
    # Network configuration
    # ------------------------------------------------------------------------

    parser.add_argument(
        "--listen-host",
        default="127.0.0.1",
        help=(
            "Host/IP on which the channel listens "
            "(default: 127.0.0.1)"
        ),
    )

    parser.add_argument(
        "--listen-port",
        type=int,
        default=9001,
        help=(
            "UDP port on which the channel listens "
            "(default: 9001)"
        ),
    )

    parser.add_argument(
        "--receiver-host",
        default="127.0.0.1",
        help=(
            "Receiver host/IP "
            "(default: 127.0.0.1)"
        ),
    )

    parser.add_argument(
        "--receiver-port",
        type=int,
        default=9002,
        help=(
            "Receiver UDP port "
            "(default: 9002)"
        ),
    )

    # ------------------------------------------------------------------------
    # Impairments
    # ------------------------------------------------------------------------

    parser.add_argument(
        "--loss",
        type=float,
        default=0.0,
        help=(
            "Packet loss probability "
            "[0.0 - 1.0]"
        ),
    )

    parser.add_argument(
        "--duplicate",
        type=float,
        default=0.0,
        help=(
            "Packet duplication probability "
            "[0.0 - 1.0]"
        ),
    )

    parser.add_argument(
        "--reorder",
        type=float,
        default=0.0,
        help=(
            "Packet reordering probability "
            "[0.0 - 1.0]"
        ),
    )

    parser.add_argument(
        "--corrupt",
        type=float,
        default=0.0,
        help=(
            "Packet corruption probability "
            "[0.0 - 1.0]"
        ),
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help=(
            "Packet delay probability "
            "[0.0 - 1.0]"
        ),
    )

    # ------------------------------------------------------------------------
    # Delay configuration
    # ------------------------------------------------------------------------

    parser.add_argument(
        "--min-delay",
        type=float,
        default=0.0,
        help=(
            "Minimum normal delay in milliseconds "
            "(default: 0)"
        ),
    )

    parser.add_argument(
        "--max-delay",
        type=float,
        default=100.0,
        help=(
            "Maximum normal delay in milliseconds "
            "(default: 100)"
        ),
    )

    parser.add_argument(
        "--reorder-min-delay",
        type=float,
        default=50.0,
        help=(
            "Minimum additional delay for "
            "reordering in milliseconds "
            "(default: 50)"
        ),
    )

    parser.add_argument(
        "--reorder-max-delay",
        type=float,
        default=150.0,
        help=(
            "Maximum additional delay for "
            "reordering in milliseconds "
            "(default: 150)"
        ),
    )

    # ------------------------------------------------------------------------
    # Experiment reproducibility
    # ------------------------------------------------------------------------

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help=(
            "Random seed for reproducible "
            "experiments (default: 42)"
        ),
    )

    return parser


# ============================================================================
# Main
# ============================================================================

def main():
    """Program entry point."""

    parser = build_argument_parser()

    args = parser.parse_args()

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

    channel = ChannelEmulator(
        listen_host=args.listen_host,
        listen_port=args.listen_port,

        receiver_host=args.receiver_host,
        receiver_port=args.receiver_port,

        config=config,
    )

    channel.run()


if __name__ == "__main__":
    main()