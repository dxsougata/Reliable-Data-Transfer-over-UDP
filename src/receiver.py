import os
import socket

from common.config import RECEIVER_HOST, RECEIVER_PORT
from protocols.go_back_n import GoBackNReceiver
from protocols.selective_repeat import SelectiveRepeatReceiver
from protocols.stop_and_wait import StopAndWaitReceiver


def main():
    print("Select protocol (must match sender.py):")
    print("1. Stop-and-Wait")
    print("2. Go-Back-N")
    print("3. Selective Repeat")
    choice = input("Enter 1, 2, or 3: ").strip()
    if choice not in ("1", "2", "3"):
        raise SystemExit("Invalid choice. Enter 1, 2, or 3.")

    receiver_class = {
        "1": StopAndWaitReceiver,
        "2": GoBackNReceiver,
        "3": SelectiveRepeatReceiver,
    }[choice]
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(project_root, "data", "output", "received_file.txt")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((RECEIVER_HOST, RECEIVER_PORT))
        protocol_name = {
            "1": "Stop-and-Wait",
            "2": "Go-Back-N",
            "3": "Selective Repeat",
        }[choice]
        print(f"[{protocol_name}] Listening on {RECEIVER_HOST}:{RECEIVER_PORT}")
        print(f"Output file: {output_path}")
        receiver = receiver_class(sock)
        try:
            stats = receiver.receive_file(output_path)
        except KeyboardInterrupt:
            print("\nReceiver stopped.")
            return

    print(f"[{protocol_name}] Transfer complete: {stats}")


if __name__ == "__main__":
    main()
