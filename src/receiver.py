import socket
import struct

# ============================================================
# Configuration
# ============================================================

RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 9002

BUFFER_SIZE = 65535


# ============================================================
# Parse data packet
# ============================================================

def parse_packet(packet):

    # Minimum packet size:
    # 4 bytes sequence number
    # 4 bytes data length

    if len(packet) < 8:
        return None, None

    seq_num, data_length = struct.unpack(
        "!II",
        packet[:8]
    )

    data = packet[8:]

    # Check data length

    if len(data) != data_length:
        print("[RECEIVER] Invalid packet length")
        return None, None

    return seq_num, data


# ============================================================
# Create ACK
# ============================================================

def create_ack(seq_num):

    return struct.pack(
        "!I",
        seq_num
    )


# ============================================================
# Main receiver
# ============================================================

def main():

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    sock.bind(
        (
            RECEIVER_HOST,
            RECEIVER_PORT
        )
    )

    print(
        f"[RECEIVER] Listening on "
        f"{RECEIVER_HOST}:{RECEIVER_PORT}"
    )

    expected_sequence = 0

    received_data = bytearray()

    try:

        while True:

            packet, sender_address = sock.recvfrom(
                BUFFER_SIZE
            )

            seq_num, data = parse_packet(packet)

            if seq_num is None:
                continue

            print(
                f"[RECEIVER] Received "
                f"SEQ={seq_num}"
            )

            # ==================================================
            # Correct packet
            # ==================================================

            if seq_num == expected_sequence:

                received_data.extend(data)

                print(
                    f"[RECEIVER] Accepted "
                    f"SEQ={seq_num}"
                )

                # Send ACK
                ack = create_ack(seq_num)

                sock.sendto(
                    ack,
                    sender_address
                )

                print(
                    f"[RECEIVER] Sent ACK={seq_num}"
                )

                expected_sequence += 1

            # ==================================================
            # Duplicate packet
            # ==================================================

            elif seq_num < expected_sequence:

                print(
                    f"[RECEIVER] Duplicate "
                    f"SEQ={seq_num}"
                )

                # Send ACK again
                ack = create_ack(seq_num)

                sock.sendto(
                    ack,
                    sender_address
                )

                print(
                    f"[RECEIVER] Re-sent ACK={seq_num}"
                )

            # ==================================================
            # Out-of-order packet
            # ==================================================

            else:

                print(
                    f"[RECEIVER] Out-of-order packet "
                    f"SEQ={seq_num}"
                )

                # ACK the last correctly received packet

                if expected_sequence > 0:

                    ack = create_ack(
                        expected_sequence - 1
                    )

                    sock.sendto(
                        ack,
                        sender_address
                    )

    except KeyboardInterrupt:

        print("\n[RECEIVER] Shutdown")

        print(
            "[RECEIVER] Final data:",
            received_data.decode(
                errors="replace"
            )
        )

    finally:

        sock.close()


if __name__ == "__main__":
    main()