"""Administrative command-line interface (CLI) for the Aquaponics Engine.

Implements the 'ape' command, including 'ape -x' for requesting a controlled
administrative stop of the existing running Engine process.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import socket
import sys
from typing import Final, Sequence

from aquaponics_controller.ipc import (
    COMMAND_STOP,
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    STATUS_ACCEPTED,
    STATUS_REJECTED,
    deserialize_message,
    resolve_socket_path,
    serialize_message,
)

# Standard administrative CLI exit codes
EXIT_SUCCESS: Final[int] = 0
EXIT_ENGINE_NOT_RUNNING: Final[int] = 1
EXIT_PERMISSION_DENIED: Final[int] = 2
EXIT_TIMEOUT: Final[int] = 3
EXIT_ERROR: Final[int] = 4


def send_stop_request(
    socket_path: Path,
    timeout_seconds: float = DEFAULT_CLIENT_TIMEOUT_SECONDS,
) -> int:
    """Connect to the running Engine and issue a controlled stop request.

    Args:
        socket_path: Resolved filesystem path to the Engine IPC socket.
        timeout_seconds: Timeout in seconds for socket connection and I/O.

    Returns:
        Exit code representing the operation result.
    """
    if not socket_path.exists():
        sys.stderr.write(
            f"Error: Aquaponics Engine is not running (socket file not found at {socket_path})\n"
        )
        return EXIT_ENGINE_NOT_RUNNING

    client_sock: socket.socket | None = None
    try:
        if hasattr(socket, "AF_UNIX"):
            client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client_sock.settimeout(timeout_seconds)
            client_sock.connect(str(socket_path))
        else:
            # Fallback for environments lacking AF_UNIX support
            content = socket_path.read_text(encoding="utf-8").strip()
            if not content.isdigit():
                sys.stderr.write(
                    f"Error: Malformed socket pointer at {socket_path}\n"
                )
                return EXIT_ENGINE_NOT_RUNNING
            port = int(content)
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.settimeout(timeout_seconds)
            client_sock.connect(("127.0.0.1", port))

        # Transmit STOP command
        request_data = serialize_message({"command": COMMAND_STOP})
        client_sock.sendall(request_data)

        # Receive response from Engine
        response_bytes = client_sock.recv(4096)
        if not response_bytes:
            sys.stderr.write(
                f"Error: Connection closed by Engine before response was received ({socket_path})\n"
            )
            return EXIT_ENGINE_NOT_RUNNING

        response = deserialize_message(response_bytes)

    except PermissionError as exc:
        sys.stderr.write(
            f"Error: Permission denied accessing socket at {socket_path}: {exc}\n"
        )
        return EXIT_PERMISSION_DENIED

    except (ConnectionRefusedError, FileNotFoundError) as exc:
        sys.stderr.write(
            f"Error: Aquaponics Engine is not running (connection refused on {socket_path}): {exc}\n"
        )
        return EXIT_ENGINE_NOT_RUNNING

    except (socket.timeout, TimeoutError):
        sys.stderr.write(
            f"Error: Communication timed out after {timeout_seconds:.1f}s connecting to {socket_path}\n"
        )
        return EXIT_TIMEOUT

    except OSError as exc:
        # Detect permission errors reported through OSError errno (e.g. EACCES = 13)
        if getattr(exc, "errno", None) == 13:
            sys.stderr.write(
                f"Error: Permission denied accessing socket at {socket_path}: {exc}\n"
            )
            return EXIT_PERMISSION_DENIED

        sys.stderr.write(
            f"Error: Socket communication error with Engine: {exc}\n"
        )
        return EXIT_ERROR

    finally:
        if client_sock is not None:
            try:
                client_sock.close()
            except OSError:
                pass

    # Evaluate Engine response
    status = response.get("status")
    if status == STATUS_ACCEPTED:
        sys.stdout.write("Aquaponics Engine stop request accepted. Engine is shutting down.\n")
        return EXIT_SUCCESS

    if status == STATUS_REJECTED:
        reason = response.get("reason", "No reason provided")
        if "permission" in reason.lower() or "unauthorized" in reason.lower():
            sys.stderr.write(f"Error: Permission denied by Engine: {reason}\n")
            return EXIT_PERMISSION_DENIED

        sys.stderr.write(f"Error: Engine rejected stop request: {reason}\n")
        return EXIT_ERROR

    sys.stderr.write(f"Error: Unexpected response received from Engine: {response}\n")
    return EXIT_ERROR


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser for ape."""
    parser = argparse.ArgumentParser(
        prog="ape",
        description="Administrative CLI for the Aquaponics Engine.",
    )
    parser.add_argument(
        "-x",
        "--stop",
        action="store_true",
        help="Request controlled shutdown of the existing running Engine.",
    )
    parser.add_argument(
        "--socket-path",
        type=str,
        default=None,
        help="Custom filesystem path to the Engine Unix domain socket.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_CLIENT_TIMEOUT_SECONDS,
        help=f"Socket communication timeout in seconds (default: {DEFAULT_CLIENT_TIMEOUT_SECONDS}s).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Main execution entry point for the ape CLI tool.

    Args:
        argv: Command-line arguments sequence; defaults to sys.argv[1:].

    Returns:
        Process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.stop:
        parser.print_help(sys.stderr)
        return EXIT_ERROR

    socket_path = resolve_socket_path(args.socket_path)
    return send_stop_request(socket_path=socket_path, timeout_seconds=args.timeout)


if __name__ == "__main__":
    sys.exit(main())

