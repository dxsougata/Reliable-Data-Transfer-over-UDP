import zlib


def calculate_checksum(data):
    return zlib.crc32(data) & 0xffffffff


def verify_checksum(data, checksum):
    return calculate_checksum(data) == checksum