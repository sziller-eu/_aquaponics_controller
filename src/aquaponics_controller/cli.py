"""``ape``: the local administrative command-line client.

    ape (-x | --ping | --echo TEXT) [--socket PATH] [--timeout SECONDS]

``-x`` / ``--stop`` asks the already-running Engine, through the local IPC
endpoint, to shut itself down. A successful answer means the Engine has *initiated*
shutdown; it does not mean the Engine process has exited.

``--ping`` and ``--echo TEXT`` are stateless diagnostic queries. The answer is produced
by the running Engine from its own state; this client only sends the request and
shows the answer. Neither query changes anything.

The CLI never starts an Engine and never signals the Engine process. The client never
retries: a repeated request must be an explicit decision. Every wait is bounded by
``--timeout``.

Exit status:
    0  the Engine answered successfully (for -x: confirmed that shutdown was initiated)
    1  the Engine refused the request
    2  usage error
    3  the Engine is not reachable (no endpoint, or nothing listening)
    4  permission denied: by the operating system, or by the Engine (peer_not_authorized)
    5  timed out
    6  the connection failed or the answer was not understood
"""

import argparse
import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .ipc_protocol import (
    ERROR_MALFORMED_REQUEST,
    ERROR_PEER_NOT_AUTHORIZED,
    MAX_MESSAGE_BYTES,
    OPERATION_ECHO,
    OPERATION_PING,
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


def send_request(
    endpoint: Path, operation: str, timeout_seconds: float, arguments: dict | None = None
) -> AdministrativeResponse:
    """Submit one request and return the Engine's response. Raise ClientError on failure.

    The request carries a fresh correlation identifier, and an answer that does not carry the same
    identifier is rejected. ``timeout_seconds`` bounds the whole exchange, so the call cannot hang.
    """
    correlation_id = uuid.uuid4().hex
    request = encode_request(operation, datetime.now(timezone.utc), correlation_id=correlation_id, arguments=arguments)
    if len(request) > MAX_MESSAGE_BYTES:  # transport frame limit only; what the text means is the Engine's business
        raise ClientError(EXIT_USAGE, f"the request would exceed the {MAX_MESSAGE_BYTES}-byte protocol limit")
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(timeout_seconds)
            connection.connect(str(endpoint))
            connection.sendall(request)
            response = decode_response(_read_response_line(connection, timeout_seconds))
        _check_correlation(response, correlation_id)
        return response
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


def _check_correlation(response: AdministrativeResponse, sent_id: str) -> None:
    """An answer must belong to our request. A refusal sent before the Engine could read the id may lack it."""
    if response.correlation_id == sent_id or (response.correlation_id is None and not response.ok):
        return
    raise ProtocolError(ERROR_MALFORMED_REQUEST, "the answer does not carry this request's correlation identifier")


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
    operations.add_argument("--ping", action="store_true", help="ask the running Engine to report that it is alive")
    operations.add_argument(
        "--echo", metavar="TEXT", help="ask the running Engine to return TEXT (at most 256 characters)"
    )
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

    if arguments.ping:
        operation, operation_arguments = OPERATION_PING, None
    elif arguments.echo is not None:
        operation, operation_arguments = OPERATION_ECHO, {"text": arguments.echo}
    else:
        operation, operation_arguments = OPERATION_STOP, None

    try:
        response = send_request(endpoint, operation, arguments.timeout, operation_arguments)
    except ClientError as exc:
        print(f"ape: {exc}", file=sys.stderr)
        return exc.exit_status

    if not response.ok:
        print(f"ape: the Engine refused the request: {response.error_code}: {response.message}", file=sys.stderr)
        return EXIT_PERMISSION_DENIED if response.error_code == ERROR_PEER_NOT_AUTHORIZED else EXIT_REFUSED

    if operation == OPERATION_PING:
        return _report_ping(response)
    if operation == OPERATION_ECHO:
        return _report_echo(response)
    return _report_stop(response)


def _report_stop(response: AdministrativeResponse) -> int:
    if response.status == STATUS_SHUTDOWN_ALREADY_INITIATED:
        print("Engine shutdown was already initiated. The Engine process may still be exiting.")
    else:
        print("Engine shutdown initiated. This does not confirm that the Engine process has exited.")
    if response.audit_recorded is False:
        print("ape: WARNING: the Engine could not persist its lifecycle audit record for this request.", file=sys.stderr)
    return EXIT_OK


def _report_ping(response: AdministrativeResponse) -> int:
    result = response.result
    if (
        not isinstance(result, dict)
        or result.get("reply") != "pong"
        or not isinstance(result.get("execution_id"), str)
        or not isinstance(result.get("engine_pid"), int)
        or not isinstance(result.get("cycles_completed"), int)
        or not isinstance(result.get("engine_time_utc"), str)
    ):
        print("ape: unintelligible answer from the Engine: not a valid ping result", file=sys.stderr)
        return EXIT_COMMUNICATION_FAILURE
    print(f"pong: the Engine is reachable (execution {result['execution_id']}, pid {result['engine_pid']})")
    print(f"operating cycles completed: {result['cycles_completed']}")
    print(f"Engine time: {result['engine_time_utc']}")
    return EXIT_OK


def _report_echo(response: AdministrativeResponse) -> int:
    result = response.result
    if not isinstance(result, dict) or not isinstance(result.get("text"), str):
        print("ape: unintelligible answer from the Engine: not a valid echo result", file=sys.stderr)
        return EXIT_COMMUNICATION_FAILURE
    _print_text(result["text"])
    return EXIT_OK


def _print_text(text: str) -> None:
    """Print without ever failing on characters the terminal's encoding cannot show."""
    encoding = sys.stdout.encoding or "utf-8"
    sys.stdout.write(text.encode(encoding, "backslashreplace").decode(encoding) + "\n")


if __name__ == "__main__":
    sys.exit(main())
