import time
import socket

from src.common.packet import create_data_packet, create_ack_packet, Packet


class SelectiveRepeatSender:
    def __init__(self, window_size=4, timeout=1.0):
        self.window_size = window_size
        self.timeout = timeout

        self.base = 0
        self.next_seq_num = 0

        self.unacked_packets = {}
        self.timers = {}

    def create_packet(self, data):
        packet = create_data_packet(
            self.next_seq_num,
            data
        )

        self.unacked_packets[self.next_seq_num] = packet
        self.timers[self.next_seq_num] = time.time()

        self.next_seq_num += 1

        return packet

    def send(self, data, sock, address):
  
        if self.next_seq_num < self.base + self.window_size:

            packet = self.create_packet(data)

            sock.sendto(
                packet.serialize(),
                address
            )

            print(
                f"Sending packet {packet.seq_num}: "
                f"{packet.data.decode()}"
            )

            return packet

        print("Window is full. Cannot send new packet.")

        return None

    def receive_ack(self, ack_num):
        if ack_num in self.unacked_packets:

            del self.unacked_packets[ack_num]
            del self.timers[ack_num]

            print(
                f"ACK received for packet {ack_num}"
            )

            while (
                self.base not in self.unacked_packets
                and self.base < self.next_seq_num
            ):
                self.base += 1

    def retransmit(self, sock, address):
        current_time = time.time()

        for seq_num, packet in list(
            self.unacked_packets.items()
        ):

            elapsed_time = (
                current_time - self.timers[seq_num]
            )

            if elapsed_time >= self.timeout:

                sock.sendto(
                    packet.serialize(),
                    address
                )

                print(
                    f"Timeout for packet {seq_num}. "
                    f"Retransmitting: "
                    f"{packet.data.decode()}"
                )

                self.timers[seq_num] = time.time()


class SelectiveRepeatReceiver:
    def __init__(self, window_size=4):

        self.window_size = window_size

        self.base = 0

        self.buffer = {}

    def receive_packet(self, packet, sock, address):

        seq_num = packet.seq_num

        if (
            self.base
            <= seq_num
            < self.base + self.window_size
        ):

            if seq_num not in self.buffer:

                self.buffer[seq_num] = packet

                print(
                    f"Received packet {seq_num}: "
                    f"{packet.data.decode()}"
                )

            else:

                print(
                    f"Duplicate packet {seq_num} received"
                )

            ack_packet = create_ack_packet(seq_num)

            sock.sendto(
                ack_packet.serialize(),
                address
            )

            print(
                f"ACK sent for packet {seq_num}"
            )

            self.deliver_packets()

            return seq_num

        elif seq_num < self.base:

            ack_packet = create_ack_packet(seq_num)

            sock.sendto(
                ack_packet.serialize(),
                address
            )

            print(
                f"Duplicate packet {seq_num}. "
                f"ACK resent."
            )

            return seq_num

        else:

            print(
                f"Packet {seq_num} is outside "
                f"the receiver window"
            )

            return None

    def deliver_packets(self):

        while self.base in self.buffer:

            packet = self.buffer.pop(self.base)

            print(
                f"Delivered packet "
                f"{packet.seq_num}: "
                f"{packet.data.decode()}"
            )

            self.base += 1