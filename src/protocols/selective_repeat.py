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


if __name__ == "__main__":
    sr = SelectiveRepeat(window_size=4)

    sr.send("Packet A")
    sr.send("Packet B")
    sr.send("Packet C")

    sr.receive_ack(1)

    sr.retransmit()