class SelectiveRepeat:
    def __init__(self, window_size=4, timeout=1.0):
        self.window_size = window_size
        self.timeout = timeout

        self.base = 0
        self.next_seq_num = 0

        self.unacked_packets = {}
        self.timers = {}

    def create_packet(self, data):
        packet = {
            "seq_num": self.next_seq_num,
            "data": data
        }

        self.unacked_packets[self.next_seq_num] = packet
        self.timers[self.next_seq_num] = None

        self.next_seq_num += 1

        return packet

    def send(self, data):
        if self.next_seq_num < self.base + self.window_size:
            packet = self.create_packet(data)

            print(
                f"Sending packet {packet['seq_num']}: "
                f"{packet['data']}"
            )

            return packet

        print("Window is full. Cannot send new packet.")
        return None

    def receive_ack(self, ack_num):
        if ack_num in self.unacked_packets:
            del self.unacked_packets[ack_num]
            del self.timers[ack_num]

            print(f"ACK received for packet {ack_num}")

            while (
                self.base not in self.unacked_packets
                and self.base < self.next_seq_num
            ):
                self.base += 1

    def retransmit(self):
        for seq_num, packet in self.unacked_packets.items():
            print(
                f"Retransmitting packet {seq_num}: "
                f"{packet['data']}"
            )


class SelectiveRepeatReceiver:
    def __init__(self, window_size=4):
        self.window_size = window_size
        self.base = 0
        self.buffer = {}

    def receive_packet(self, packet):
        seq_num = packet["seq_num"]

        if self.base <= seq_num < self.base + self.window_size:

            if seq_num not in self.buffer:
                self.buffer[seq_num] = packet
                print(
                    f"Received packet {seq_num}: "
                    f"{packet['data']}"
                )

            print(f"ACK sent for packet {seq_num}")

            self.deliver_packets()

            return seq_num

        print(f"Packet {seq_num} is outside the receiver window")
        return None

    def deliver_packets(self):
        while self.base in self.buffer:
            packet = self.buffer.pop(self.base)

            print(
                f"Delivered packet {packet['seq_num']}: "
                f"{packet['data']}"
            )

            self.base += 1


if __name__ == "__main__":
    sender = SelectiveRepeat(window_size=4)

    sender.send("Packet A")
    sender.send("Packet B")
    sender.send("Packet C")

    sender.receive_ack(1)

    sender.retransmit()

    receiver = SelectiveRepeatReceiver(window_size=4)

    receiver.receive_packet({
        "seq_num": 0,
        "data": "Packet A"
    })

    receiver.receive_packet({
        "seq_num": 2,
        "data": "Packet C"
    })

    receiver.receive_packet({
        "seq_num": 1,
        "data": "Packet B"
    })