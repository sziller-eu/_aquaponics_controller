"""Wire protocol of the local administrative IPC interface (initial version).

One request and one response per connection, each a single UTF-8 JSON object
terminated by a newline and bounded in size.

Request:   {"v": 1, "op": "stop", "submitted_at": "<timezone-aware ISO 8601 time>"}
Response:  {"v": 1, "ok": true,  "status": "shutdown_initiated"}
           {"v": 1, "ok": false, "error": "<error code>", "message": "<text>"}

This module contains only encoding, decoding and shared limits. It performs no
I/O, so the Engine and the ``ape`` client can both use it without depending on
each other. It does not define Engine semantics; see "Lifecycle-Control Requests"
in the architecture documentation.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROTOCOL_VERSION = 1

# Upper bounds for one request / response line, newline included.
MAX_MESSAGE_BYTES = 4096

OPERATION_STOP = "stop"

# Response error codes.
ERROR_MALFORMED_REQUEST = "malformed_request"
ERROR_UNSUPPORTED_PROTOCOL_VERSION = "unsupported_protocol_version"
ERROR_REQUEST_TOO_LARGE = "request_too_large"
ERROR_REQUEST_TIMEOUT = "request_timeout"
ERROR_UNSUPPORTED_OPERATION = "unsupported_operation"
ERROR_PEER_NOT_AUTHORIZED = "peer_not_authorized"

# Response statuses.
STATUS_SHUTDOWN_INITIATED = "shutdown_initiated"
STATUS_SHUTDOWN_ALREADY_INITIATED = "shutdown_already_initiated"

ENDPOINT_ENVIRONMENT_VARIABLE = "AQUAPONICS_ENGINE_SOCKET"

_MAX_OPERATION_LENGTH = 64


class ProtocolError(Exception):
    """A request or response that does not conform to the protocol."""

    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = error_code
        self.message = message


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


@dataclass(frozen=True)
class AdministrativeResponse:
    ok: bool
    status: str | None = None
    error_code: str | None = None
    message: str | None = None
    # Whether the Engine could persist its lifecycle audit record for this request (None: not applicable).
    audit_recorded: bool | None = None


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


def encode_request(operation: str, submitted_at: datetime) -> bytes:
    payload = {"v": PROTOCOL_VERSION, "op": operation, "submitted_at": submitted_at.isoformat()}
    return _to_line(payload)


def decode_request(line: bytes) -> AdministrativeRequest:
    """Parse and validate one request line. Raise ProtocolError if it is not acceptable."""
    payload = _load_object(line, "request")
    if payload.get("v") != PROTOCOL_VERSION:
        raise ProtocolError(
            ERROR_UNSUPPORTED_PROTOCOL_VERSION,
            f"unsupported protocol version (supported: {PROTOCOL_VERSION})",
        )
    operation = payload.get("op")
    if not isinstance(operation, str) or not operation or len(operation) > _MAX_OPERATION_LENGTH:
        raise ProtocolError(ERROR_MALFORMED_REQUEST, "field 'op' must be a short non-empty string")
    return AdministrativeRequest(operation=operation, client_submitted_at=_decode_submitted_at(payload))


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
    )


def _to_line(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"


def _load_object(line: bytes, what: str) -> dict:
    try:
        payload = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError(ERROR_MALFORMED_REQUEST, f"{what} is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ProtocolError(ERROR_MALFORMED_REQUEST, f"{what} must be a JSON object")
    return payload


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
