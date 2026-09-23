class SelectiveRepeat:
    def __init__(self, window_size=4, timeout=1.0):
        self.window_size = window_size
        self.timeout = timeout

        self.base = 0
        self.next_seq_num = 0

        self.unacked_packets = {}
        self.timers = {}

    def send(self, data):
        pass

    def receive_ack(self, ack_num):
        pass

    def retransmit(self):
        pass