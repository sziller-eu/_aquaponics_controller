"""Wire protocol of the local administrative IPC interface (initial version).

One request and one response per connection, each a single UTF-8 JSON object
terminated by a newline and bounded in size.

Request:   {"v": 1, "op": "<operation>", "submitted_at": "<timezone-aware ISO 8601 time>",
            "id": "<optional correlation identifier>", "args": {<optional argument object>}}
Response:  {"v": 1, "ok": true,  "status": "shutdown_initiated"}                    (lifecycle control)
           {"v": 1, "ok": true,  "result": {...}, "id": "<the request's id>"}       (query)
           {"v": 1, "ok": false, "error": "<error code>", "message": "<text>"}

The optional fields are additive: protocol version 1 is unchanged, and a request
without them is still valid. A correlation identifier lets a client match an answer
to its request and lets diagnostic traces name a request without quoting its
content. It is NOT a durable command identifier and NOT an idempotency key.

This module contains only encoding, decoding and shared limits. It performs no
I/O, so the Engine and the ``ape`` client can both use it without depending on
each other. It does not define what an operation means or which arguments it
accepts: that is the Engine's decision. See "Request Classes" and
"Lifecycle-Control Requests" in the architecture documentation.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROTOCOL_VERSION = 1

# Upper bounds for one request / response line, newline included.
MAX_MESSAGE_BYTES = 4096

# Operations. ping and echo are stateless diagnostic queries; stop is a lifecycle-control request.
OPERATION_STOP = "stop"
OPERATION_PING = "ping"
OPERATION_ECHO = "echo"

# Limits on what a client may send, so every request and answer stays small and bounded.
MAX_CORRELATION_ID_LENGTH = 64
MAX_ECHO_TEXT_LENGTH = 256  # characters (Unicode code points), not bytes

# Response error codes.
ERROR_MALFORMED_REQUEST = "malformed_request"
ERROR_UNSUPPORTED_PROTOCOL_VERSION = "unsupported_protocol_version"
ERROR_REQUEST_TOO_LARGE = "request_too_large"
ERROR_REQUEST_TIMEOUT = "request_timeout"
ERROR_UNSUPPORTED_OPERATION = "unsupported_operation"
ERROR_PEER_NOT_AUTHORIZED = "peer_not_authorized"
ERROR_INVALID_CORRELATION_ID = "invalid_correlation_id"
ERROR_INVALID_ARGUMENTS = "invalid_arguments"

# Response statuses.
STATUS_SHUTDOWN_INITIATED = "shutdown_initiated"
STATUS_SHUTDOWN_ALREADY_INITIATED = "shutdown_already_initiated"

ENDPOINT_ENVIRONMENT_VARIABLE = "AQUAPONICS_ENGINE_SOCKET"

_MAX_OPERATION_LENGTH = 64


class ProtocolError(Exception):
    """A request or response that does not conform to the protocol.

    ``correlation_id`` is set when the request's identifier was valid before the
    problem was found, so the refusal can still be matched to its request.
    """

    def __init__(self, error_code: str, message: str, correlation_id: str | None = None):
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.correlation_id = correlation_id


@dataclass(frozen=True)
class PeerIdentity:
    """Linux credentials of the connecting process, as reported by the kernel."""

    pid: int
    uid: int
    gid: int


@dataclass(frozen=True)
class AdministrativeRequest:
    operation: str
    # Time reported by the client. Informational only: the Engine's own receipt time is authoritative.
    client_submitted_at: datetime | None
    # Optional client-chosen label for matching an answer to its request. Not a durable identifier.
    correlation_id: str | None = None
    # Optional arguments. Only the shape (a JSON object) is checked here; the Engine checks the content.
    arguments: dict | None = None


@dataclass(frozen=True)
class AdministrativeResponse:
    ok: bool
    status: str | None = None
    error_code: str | None = None
    message: str | None = None
    # Whether the Engine could persist its lifecycle audit record for this request (None: not applicable).
    audit_recorded: bool | None = None
    # The request's correlation identifier, returned unchanged where the request carried a valid one.
    correlation_id: str | None = None
    # Query answer: a small JSON object produced by the Engine (None for lifecycle control and errors).
    result: dict | None = None


def resolve_endpoint_path(explicit: str | None) -> Path:
    """Choose the IPC endpoint path: explicit argument, environment variable, then XDG runtime directory."""
    if explicit:
        return Path(explicit)
    from_environment = os.environ.get(ENDPOINT_ENVIRONMENT_VARIABLE)
    if from_environment:
        return Path(from_environment)
    runtime_directory = os.environ.get("XDG_RUNTIME_DIR")
    if runtime_directory:
        return Path(runtime_directory) / "aquaponics" / "engine.sock"
    raise ValueError(
        "no IPC endpoint configured: use --socket, set "
        f"{ENDPOINT_ENVIRONMENT_VARIABLE}, or provide XDG_RUNTIME_DIR"
    )


def encode_request(
    operation: str,
    submitted_at: datetime,
    *,
    correlation_id: str | None = None,
    arguments: dict | None = None,
) -> bytes:
    payload: dict = {"v": PROTOCOL_VERSION, "op": operation, "submitted_at": submitted_at.isoformat()}
    if correlation_id is not None:
        payload["id"] = correlation_id
    if arguments is not None:
        payload["args"] = arguments
    return _to_line(payload)


def decode_request(line: bytes) -> AdministrativeRequest:
    """Parse and validate one request line. Raise ProtocolError if it is not acceptable."""
    payload = _load_object(line, "request")
    if payload.get("v") != PROTOCOL_VERSION:
        raise ProtocolError(
            ERROR_UNSUPPORTED_PROTOCOL_VERSION,
            f"unsupported protocol version (supported: {PROTOCOL_VERSION})",
        )
    # The identifier is checked first so that every later refusal can still carry it.
    correlation_id = _decode_correlation_id(payload)
    try:
        operation = payload.get("op")
        if not isinstance(operation, str) or not operation or len(operation) > _MAX_OPERATION_LENGTH:
            raise ProtocolError(ERROR_MALFORMED_REQUEST, "field 'op' must be a short non-empty string")
        arguments = _decode_arguments(payload)
        submitted_at = _decode_submitted_at(payload)
    except ProtocolError as exc:
        exc.correlation_id = correlation_id
        raise
    return AdministrativeRequest(
        operation=operation,
        client_submitted_at=submitted_at,
        correlation_id=correlation_id,
        arguments=arguments,
    )


def encode_response(response: AdministrativeResponse) -> bytes:
    payload: dict = {"v": PROTOCOL_VERSION, "ok": response.ok}
    if response.status is not None:
        payload["status"] = response.status
    if response.error_code is not None:
        payload["error"] = response.error_code
    if response.message is not None:
        payload["message"] = response.message
    if response.audit_recorded is not None:
        payload["audit_recorded"] = response.audit_recorded
    if response.correlation_id is not None:
        payload["id"] = response.correlation_id
    if response.result is not None:
        payload["result"] = response.result
    return _to_line(payload)


def decode_response(line: bytes) -> AdministrativeResponse:
    payload = _load_object(line, "response")
    if payload.get("v") != PROTOCOL_VERSION or not isinstance(payload.get("ok"), bool):
        raise ProtocolError(ERROR_MALFORMED_REQUEST, "response is not a valid protocol response")
    return AdministrativeResponse(
        ok=payload["ok"],
        status=payload.get("status"),
        error_code=payload.get("error"),
        message=payload.get("message"),
        audit_recorded=payload.get("audit_recorded"),
        correlation_id=payload.get("id"),
        result=payload.get("result"),
    )


def _to_line(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"


def _load_object(line: bytes, what: str) -> dict:
    try:
        payload = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:  # RecursionError: absurdly deep nesting
        raise ProtocolError(ERROR_MALFORMED_REQUEST, f"{what} is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ProtocolError(ERROR_MALFORMED_REQUEST, f"{what} must be a JSON object")
    return payload


def _decode_correlation_id(payload: dict) -> str | None:
    raw = payload.get("id")
    if raw is None:
        return None
    if not isinstance(raw, str) or not 1 <= len(raw) <= MAX_CORRELATION_ID_LENGTH:
        raise ProtocolError(
            ERROR_INVALID_CORRELATION_ID,
            f"field 'id' must be a string of 1 to {MAX_CORRELATION_ID_LENGTH} characters",
        )
    return raw


def _decode_arguments(payload: dict) -> dict | None:
    raw = payload.get("args")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ProtocolError(ERROR_INVALID_ARGUMENTS, "field 'args' must be a JSON object")
    return raw


def _decode_submitted_at(payload: dict) -> datetime | None:
    raw = payload.get("submitted_at")
    if raw is None:
        return None
    try:
        moment = datetime.fromisoformat(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(ERROR_MALFORMED_REQUEST, "field 'submitted_at' is not an ISO 8601 time") from exc
    if moment.tzinfo is None:
        raise ProtocolError(ERROR_MALFORMED_REQUEST, "field 'submitted_at' must include a UTC offset")
    return moment
