import zlib
import hashlib


def calculate_checksum(data):
    return zlib.crc32(data) & 0xffffffff


def verify_checksum(data, received_checksum):
    calculated_checksum = calculate_checksum(data)
    return calculated_checksum == received_checksum


def file_hash(data):
    return hashlib.sha256(data).hexdigest()
