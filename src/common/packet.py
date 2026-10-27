"""Packet encoding shared by the Stop-and-Wait and Go-Back-N protocols."""

import struct
import zlib
from dataclasses import dataclass, field
from enum import IntEnum


class PacketError(ValueError):
    """Raised when a datagram cannot be decoded as a protocol packet."""


class PacketType(IntEnum):
    START = 1
    DATA = 2
    END = 3
    ACK = 4


# Wire header: type (1 byte), sequence number (4), payload length (4),
# CRC32 (4), followed by payload bytes.
_HEADER_FORMAT = "!BIII"
_HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)
_CHECKSUM_HEADER_FORMAT = "!BII"


@dataclass
class Packet:
    type: PacketType
    seq_num: int
    payload: bytes = b""
    _checksum: int | None = field(default=None, repr=False, compare=False)

    def _checksum_bytes(self) -> bytes:
        return struct.pack(
            _CHECKSUM_HEADER_FORMAT,
            int(self.type),
            self.seq_num,
            len(self.payload),
        ) + self.payload

    def pack(self) -> bytes:
        checksum = zlib.crc32(self._checksum_bytes()) & 0xFFFFFFFF
        return struct.pack(
            _HEADER_FORMAT,
            int(self.type),
            self.seq_num,
            len(self.payload),
            checksum,
        ) + self.payload

    @classmethod
    def unpack(cls, data: bytes) -> "Packet":
        if len(data) < _HEADER_SIZE:
            raise PacketError("Packet is shorter than its header")

        type_value, seq_num, payload_length, checksum = struct.unpack(
            _HEADER_FORMAT, data[:_HEADER_SIZE]
        )
        payload = data[_HEADER_SIZE:]
        if len(payload) != payload_length:
            raise PacketError("Payload length does not match the header")

        try:
            packet_type = PacketType(type_value)
        except ValueError as exc:
            raise PacketError(f"Unknown packet type: {type_value}") from exc

        return cls(packet_type, seq_num, payload, checksum)

    def is_valid(self) -> bool:
        expected = zlib.crc32(self._checksum_bytes()) & 0xFFFFFFFF
        return self._checksum is not None and self._checksum == expected

    @property
    def data(self) -> bytes:
        return self.payload

    @property
    def packet_type(self) -> int:
        return int(self.type)

    def serialize(self) -> bytes:
        return self.pack()

    @staticmethod
    def deserialize(packet_bytes: bytes) -> "Packet":
        return Packet.unpack(packet_bytes)

    def is_data(self) -> bool:
        return self.type == PacketType.DATA

    def is_ack(self) -> bool:
        return self.type == PacketType.ACK


def create_data_packet(seq_num: int, data: bytes | str) -> Packet:
    if isinstance(data, str):
        data = data.encode()
    return Packet(PacketType.DATA, seq_num, data)


def create_ack_packet(seq_num: int) -> Packet:
    return Packet(PacketType.ACK, seq_num)


def make_start_payload(total_chunks: int, file_hash: bytes) -> bytes:
    """Encode a 32-bit chunk count followed by a SHA-256 digest."""
    if not 0 <= total_chunks <= 0xFFFFFFFF:
        raise ValueError("total_chunks must fit in an unsigned 32-bit integer")
    if len(file_hash) != 32:
        raise ValueError("file_hash must be a 32-byte SHA-256 digest")
    return struct.pack("!I", total_chunks) + file_hash


def parse_start_payload(payload: bytes) -> tuple[int, bytes]:
    if len(payload) != 36:
        raise PacketError("Invalid START payload")
    return struct.unpack("!I", payload[:4])[0], payload[4:]
