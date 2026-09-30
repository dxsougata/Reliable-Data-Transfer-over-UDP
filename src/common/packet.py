import struct
from src.common.checksum import calculate_checksum, verify_checksum

DATA = 0
ACK = 1

class Packet:
    def __init__(self, packet_type, seq_num, data=b""):
        self.packet_type = packet_type
        self.seq_num = seq_num

        if isinstance(data, str):
            data = data.encode()

        self.data = data

    def serialize(self):
        """
        Convert Packet object into bytes for UDP transmission.
        """

        # Header:
        # 1 byte  -> packet type
        # 4 bytes -> sequence number
        # 4 bytes -> data length
        header = struct.pack(
            "!BII",
            self.packet_type,
            self.seq_num,
            len(self.data)
        )

        checksum = calculate_checksum(header + self.data)

        packet = struct.pack("!I", checksum)
        packet += header
        packet += self.data

        return packet

    @staticmethod
    def deserialize(packet_bytes):
        """
        Convert received bytes back into a Packet object.
        """

        # Minimum packet size:
        # 4 bytes checksum
        # 1 byte packet type
        # 4 bytes sequence number
        # 4 bytes data length
        if len(packet_bytes) < 13:
            raise ValueError("Invalid packet")

        received_checksum = struct.unpack(
            "!I",
            packet_bytes[:4]
        )[0]

        packet_type, seq_num, data_length = struct.unpack(
            "!BII",
            packet_bytes[4:13]
        )

        data = packet_bytes[13:13 + data_length]

        if not verify_checksum(
            packet_bytes[4:13] + data,
            received_checksum
        ):
            raise ValueError("Checksum verification failed")

        return Packet(
            packet_type,
            seq_num,
            data
        )

    def is_data(self):
        return self.packet_type == DATA

    def is_ack(self):
        return self.packet_type == ACK


def create_data_packet(seq_num, data):
    """
    Create a DATA packet.
    """
    return Packet(DATA, seq_num, data)


def create_ack_packet(seq_num):
    """
    Create an ACK packet.
    """
    return Packet(ACK, seq_num)