"""Command-line sender with selectable Stop-and-Wait and Go-Back-N ARQ."""

import socket

from common.config import CHANNEL_HOST, CHANNEL_PORT
from protocols.go_back_n import GoBackNSender, TransferFailed as GoBackNTransferFailed
from protocols.selective_repeat import SelectiveRepeatSender, TransferFailed as SelectiveRepeatTransferFailed
from protocols.stop_and_wait import StopAndWaitSender, TransferFailed as StopAndWaitTransferFailed


def main():
    print("Select protocol:")
    print("1. Stop-and-Wait")
    print("2. Go-Back-N")
    print("3. Selective Repeat")
    choice = input("Enter 1, 2, or 3: ").strip()
    if choice not in ("1", "2", "3"):
        raise SystemExit("Invalid choice. Enter 1, 2, or 3.")

    filepath = input("Enter path of file: ").strip().strip('"')
    sender_class = {
        "1": StopAndWaitSender,
        "2": GoBackNSender,
        "3": SelectiveRepeatSender,
    }[choice]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sender = sender_class(sock, (CHANNEL_HOST, CHANNEL_PORT))
        try:
            stats = sender.send_file(filepath)
        except (
            OSError,
            GoBackNTransferFailed,
            StopAndWaitTransferFailed,
            SelectiveRepeatTransferFailed,
        ) as exc:
            raise SystemExit(f"Transfer failed: {exc}") from exc

    protocol_name = {
        "1": "Stop-and-Wait",
        "2": "Go-Back-N",
        "3": "Selective Repeat",
    }[choice]
    print(f"[{protocol_name}] Transfer complete: {stats}")


if __name__ == "__main__":
    main()
