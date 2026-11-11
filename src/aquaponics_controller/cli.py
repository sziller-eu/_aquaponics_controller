"""``ape``: the local administrative command-line client.

    ape -x [--socket PATH] [--timeout SECONDS]

``-x`` / ``--stop`` asks the already-running Engine, through the local IPC
endpoint, to shut itself down. The CLI never starts an Engine and never signals
the Engine process. A successful answer means the Engine has *initiated* shutdown;
it does not mean the Engine process has exited.

The client never retries: a repeated request must be an explicit decision.

Exit status:
    0  the Engine confirmed that shutdown was initiated
    1  the Engine refused the request
    2  usage error
    3  the Engine is not reachable (no endpoint, or nothing listening)
    4  permission denied by the operating system
    5  timed out
    6  the connection failed or the answer was not understood
"""

import argparse
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .ipc_protocol import (
    ERROR_MALFORMED_REQUEST,
    MAX_MESSAGE_BYTES,
    OPERATION_STOP,
    STATUS_SHUTDOWN_ALREADY_INITIATED,
    AdministrativeResponse,
    ProtocolError,
    decode_response,
    encode_request,
    resolve_endpoint_path,
)

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_USAGE = 2
EXIT_UNREACHABLE = 3
EXIT_PERMISSION_DENIED = 4
EXIT_TIMEOUT = 5
EXIT_COMMUNICATION_FAILURE = 6

DEFAULT_TIMEOUT_SECONDS = 5.0


class ClientError(Exception):
    """A failure to complete the exchange with the Engine, with the exit status to report."""

    def __init__(self, exit_status: int, message: str):
        super().__init__(message)
        self.exit_status = exit_status


def send_stop_request(endpoint: Path, timeout_seconds: float) -> AdministrativeResponse:
    """Submit a STOP request and return the Engine's response. Raise ClientError on failure.

    ``timeout_seconds`` bounds the whole exchange, so the call cannot hang.
    """
    request = encode_request(OPERATION_STOP, datetime.now(timezone.utc))
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(timeout_seconds)
            connection.connect(str(endpoint))
            connection.sendall(request)
            return decode_response(_read_response_line(connection, timeout_seconds))
    except TimeoutError:
        raise ClientError(EXIT_TIMEOUT, f"no answer from the Engine within {timeout_seconds:g} s") from None
    except PermissionError:
        raise ClientError(EXIT_PERMISSION_DENIED, f"permission denied for {endpoint}") from None
    except (FileNotFoundError, ConnectionRefusedError):
        raise ClientError(EXIT_UNREACHABLE, f"no Engine is listening at {endpoint}") from None
    except ProtocolError as exc:
        raise ClientError(EXIT_COMMUNICATION_FAILURE, f"unintelligible answer from the Engine: {exc.message}") from None
    except OSError as exc:
        raise ClientError(EXIT_COMMUNICATION_FAILURE, f"communication with the Engine failed: {exc}") from None


def _read_response_line(connection: socket.socket, timeout_seconds: float) -> bytes:
    deadline = time.monotonic() + timeout_seconds
    received = b""
    while b"\n" not in received:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        connection.settimeout(remaining)
        chunk = connection.recv(MAX_MESSAGE_BYTES + 1 - len(received))
        if not chunk:
            raise ConnectionResetError("the Engine closed the connection without answering")
        received += chunk
        if len(received) > MAX_MESSAGE_BYTES:
            raise ProtocolError(ERROR_MALFORMED_REQUEST, "answer exceeds the size limit")
    return received.split(b"\n", 1)[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ape", description="Aquaponics Engine administrative client.")
    operations = parser.add_mutually_exclusive_group(required=True)
    operations.add_argument("-x", "--stop", action="store_true", help="ask the running Engine to shut down")
    parser.add_argument("--socket", help="administrative IPC endpoint path (default: see documentation)")
    parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, metavar="SECONDS", help="overall timeout (default 5)"
    )
    arguments = parser.parse_args(argv)
    if arguments.timeout <= 0:
        parser.error("--timeout must be positive")

    try:
        endpoint = resolve_endpoint_path(arguments.socket)
    except ValueError as exc:
        print(f"ape: {exc}", file=sys.stderr)
        return EXIT_USAGE

    try:
        response = send_stop_request(endpoint, arguments.timeout)
    except ClientError as exc:
        print(f"ape: {exc}", file=sys.stderr)
        return exc.exit_status

    if not response.ok:
        print(f"ape: the Engine refused the request: {response.error_code}: {response.message}", file=sys.stderr)
        return EXIT_REFUSED

    if response.status == STATUS_SHUTDOWN_ALREADY_INITIATED:
        print("Engine shutdown was already initiated. The Engine process may still be exiting.")
    else:
        print("Engine shutdown initiated. This does not confirm that the Engine process has exited.")
    if response.audit_recorded is False:
        print("ape: WARNING: the Engine could not persist its lifecycle audit record for this request.", file=sys.stderr)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
