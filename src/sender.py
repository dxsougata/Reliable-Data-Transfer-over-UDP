import socket
import time
import struct

# ============================================================
# Configuration
# ============================================================

CHANNEL_HOST = "127.0.0.1"
CHANNEL_PORT = 9001

TIMEOUT = 1.0

DATA_SIZE = 1024


# ============================================================
# Packet creation
# ============================================================

def create_packet(seq_num, data):
    """
    Packet format:

    4 bytes  -> sequence number
    4 bytes  -> data length
    N bytes  -> data
    """

    header = struct.pack("!II", seq_num, len(data))

    return header + data


# ============================================================
# ACK parsing
# ============================================================

def parse_ack(packet):
    """
    ACK format:

    4 bytes -> ACK sequence number
    """

    if len(packet) != 4:
        return None

    ack_num = struct.unpack("!I", packet)[0]

    return ack_num


# ============================================================
# Send one packet reliably
# ============================================================

def send_packet(sock, packet, seq_num):

    while True:

        print(f"[SENDER] Sending packet SEQ={seq_num}")

        sock.sendto(
            packet,
            (CHANNEL_HOST, CHANNEL_PORT)
        )

        start_time = time.monotonic()

        while True:

            remaining = TIMEOUT - (
                time.monotonic() - start_time
            )

            if remaining <= 0:

                print(
                    f"[SENDER] Timeout SEQ={seq_num}"
                )

                break

            sock.settimeout(remaining)

            try:

                ack_packet, address = sock.recvfrom(1024)

                ack_num = parse_ack(ack_packet)

                if ack_num is None:
                    continue

                print(
                    f"[SENDER] Received ACK={ack_num}"
                )

                if ack_num == seq_num:

                    print(
                        f"[SENDER] Packet SEQ={seq_num} "
                        f"successfully delivered"
                    )

                    return

            except socket.timeout:

                print(
                    f"[SENDER] Timeout SEQ={seq_num}"
                )

                break


# ============================================================
# Main
# ============================================================

def main():

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    sequence_number = 0

    try:

        message = input(
            "Enter message to send: "
        )

        data = message.encode()

        # Split data into chunks
        chunks = [
            data[i:i + DATA_SIZE]
            for i in range(0, len(data), DATA_SIZE)
        ]

        for chunk in chunks:

            packet = create_packet(
                sequence_number,
                chunk
            )

            send_packet(
                sock,
                packet,
                sequence_number
            )

            sequence_number += 1

        print("[SENDER] Transmission complete")

    finally:

        sock.close()


if __name__ == "__main__":
    main()