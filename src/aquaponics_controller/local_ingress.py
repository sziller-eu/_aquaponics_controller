"""Engine-side local administrative IPC ingress: a Unix domain socket.

The Engine services this interface; it never drives the Engine. Every call to
``service_pending_requests`` is bounded in the number of connections, in the time
spent per connection, and in total time, so a stalled or hostile local client
cannot hold the Engine for an unbounded time. ``readiness_source`` lets the Engine
learn, while it idles between operating cycles, that a client is waiting; the
Engine then calls ``service_pending_requests`` itself. The ingress starts no thread
and runs nothing of its own.

Access control is Linux filesystem permissions: the endpoint's directory and the
socket are owner-only (0700 / 0600) by default. The peer credentials reported by
the kernel are passed on to the Engine for validation and audit. They supplement
the file permissions; they do not replace them.

Single instance: an exclusive ``flock`` on ``<endpoint>.lock`` is held for the
life of the Engine. The kernel releases it if the process dies, so a crash never
leaves the endpoint wedged; a socket file left behind by a crash is stale and is
replaced at the next start. The lock file itself is intentionally never deleted.
"""

import errno
import fcntl
import logging
import os
import socket
import stat
import struct
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .clock import SystemClock
from .ipc_protocol import (
    ERROR_MALFORMED_REQUEST,
    ERROR_REQUEST_TIMEOUT,
    ERROR_REQUEST_TOO_LARGE,
    MAX_MESSAGE_BYTES,
    AdministrativeRequest,
    AdministrativeResponse,
    PeerIdentity,
    ProtocolError,
    decode_request,
    encode_response,
)

logger = logging.getLogger(__name__)

RequestHandler = Callable[[AdministrativeRequest, PeerIdentity | None], AdministrativeResponse]


def _describe_id(correlation_id: str | None) -> str:
    """A correlation identifier is client-supplied text: quote it (repr) so it cannot forge log lines."""
    return repr(correlation_id) if correlation_id is not None else "-"


class IngressUnavailableError(Exception):
    """The endpoint could not be established."""


class EndpointInUseError(IngressUnavailableError):
    """Another Engine already owns this endpoint."""


@dataclass(frozen=True)
class IngressLimits:
    max_connections_per_cycle: int = 8
    read_deadline_seconds: float = 0.2  # per connection, until a complete request line has arrived
    write_deadline_seconds: float = 0.2
    cycle_budget_seconds: float = 0.5  # total time one call may spend servicing connections
    listen_backlog: int = 8


class LocalAdministrativeIngress:
    def __init__(self, endpoint: Path, limits: IngressLimits | None = None, clock=None):
        self._endpoint = endpoint
        self._limits = limits if limits is not None else IngressLimits()
        self._clock = clock if clock is not None else SystemClock()
        self._lock_descriptor: int | None = None
        self._listener: socket.socket | None = None
        self._bound_inode: int | None = None

    @property
    def endpoint(self) -> Path:
        return self._endpoint

    @property
    def readiness_source(self) -> socket.socket | None:
        """The listening socket, readable whenever a client is waiting to be served. None unless open.

        For use with a selector only: the caller must not accept, read from or close it.
        """
        return self._listener

    # ------------------------------------------------------------------ lifecycle

    def open(self) -> None:
        """Claim the endpoint and start listening. Raise EndpointInUseError if an Engine owns it."""
        try:
            self._prepare_directory()
            self._acquire_instance_lock()
            self._remove_stale_endpoint()
            self._bind_and_listen()
        except IngressUnavailableError:  # includes EndpointInUseError
            self._release_after_failed_open()
            raise
        except OSError as exc:
            self._release_after_failed_open()
            raise IngressUnavailableError(f"cannot establish IPC endpoint {self._endpoint}: {exc}") from exc

    def _release_after_failed_open(self) -> None:
        try:
            self.close()
        except OSError as exc:
            logger.warning("releasing a partly established IPC endpoint failed: %s", exc)

    def close(self) -> None:
        """Release what this instance acquired. Safe to call more than once and when never opened.

        Everything is attempted; the first failure is raised afterwards so the caller knows
        that cleanup was incomplete.
        """
        first_error: OSError | None = None
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError as exc:
                first_error = exc
            self._listener = None
        if self._bound_inode is not None:
            try:
                if self._endpoint.lstat().st_ino == self._bound_inode:
                    self._endpoint.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                first_error = first_error or exc
            self._bound_inode = None
        if self._lock_descriptor is not None:
            try:
                os.close(self._lock_descriptor)  # releases the flock
            except OSError as exc:
                first_error = first_error or exc
            self._lock_descriptor = None
        if first_error is not None:
            raise first_error

    # ------------------------------------------------------------------ servicing

    def service_pending_requests(self, handler: RequestHandler, time_limit_seconds: float | None = None) -> int:
        """Handle the connections already waiting, within the configured bounds. Return how many.

        ``time_limit_seconds`` can only tighten the configured time budget. The Engine uses it
        while idling, so that servicing never pushes the next operating cycle further back
        than one connection's own (bounded) read and write deadlines. At least one waiting
        connection is handled whenever the limit is positive.
        """
        if self._listener is None:
            return 0
        budget = self._limits.cycle_budget_seconds
        if time_limit_seconds is not None:
            budget = min(budget, time_limit_seconds)
        started = self._clock.monotonic()
        handled = 0
        while handled < self._limits.max_connections_per_cycle:
            if self._clock.monotonic() - started >= budget:
                logger.warning("ingress time budget used up; remaining connections wait for the next cycle")
                break
            try:
                connection, _ = self._listener.accept()
            except BlockingIOError:
                break
            except OSError as exc:
                logger.warning("accepting an IPC connection failed: %s", exc)
                break
            handled += 1
            with connection:
                self._serve_connection(connection, handler)
        return handled

    def _serve_connection(self, connection: socket.socket, handler: RequestHandler) -> None:
        peer = self._read_peer_identity(connection)
        try:
            line = self._read_request_line(connection)
            request = decode_request(line)
        except ProtocolError as exc:
            logger.warning(
                "rejected IPC request %s from %s: %s", _describe_id(exc.correlation_id), peer, exc.message
            )
            self._send(
                connection,
                AdministrativeResponse(
                    False, error_code=exc.error_code, message=exc.message, correlation_id=exc.correlation_id
                ),
            )
            return
        except ConnectionError as exc:
            logger.warning("IPC client %s disconnected before completing its request: %s", peer, exc)
            return
        # Diagnostic trace: receipt, (Engine processing is traced by the handler), answer. Only the
        # operation name and the correlation identifier are named, never the request's arguments.
        logger.info("IPC request received: op=%r id=%s peer=%s", request.operation, _describe_id(request.correlation_id), peer)
        response = handler(request, peer)
        delivered = self._send(connection, response)
        logger.info(
            "IPC request answered: id=%s ok=%s %s",
            _describe_id(request.correlation_id),
            response.ok,
            "delivered" if delivered else "NOT delivered",
        )

    def _read_request_line(self, connection: socket.socket) -> bytes:
        deadline = self._clock.monotonic() + self._limits.read_deadline_seconds
        received = b""
        while b"\n" not in received:
            remaining = deadline - self._clock.monotonic()
            if remaining <= 0:
                raise ProtocolError(ERROR_REQUEST_TIMEOUT, "request not completed in time")
            connection.settimeout(remaining)
            try:
                chunk = connection.recv(MAX_MESSAGE_BYTES + 1 - len(received))
            except TimeoutError:
                raise ProtocolError(ERROR_REQUEST_TIMEOUT, "request not completed in time") from None
            if not chunk:
                if received:
                    raise ProtocolError(ERROR_MALFORMED_REQUEST, "request ended without a newline")
                raise ConnectionResetError("client closed the connection without sending a request")
            received += chunk
            if len(received) > MAX_MESSAGE_BYTES:
                raise ProtocolError(ERROR_REQUEST_TOO_LARGE, f"request exceeds {MAX_MESSAGE_BYTES} bytes")
        return received.split(b"\n", 1)[0]

    def _send(self, connection: socket.socket, response: AdministrativeResponse) -> bool:
        """Send one response within the write deadline. Return whether it was delivered."""
        try:
            connection.settimeout(self._limits.write_deadline_seconds)
            connection.sendall(encode_response(response))
        except OSError as exc:
            logger.warning("could not deliver IPC response: %s", exc)
            return False
        return True

    @staticmethod
    def _read_peer_identity(connection: socket.socket) -> PeerIdentity | None:
        try:
            raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
            pid, uid, gid = struct.unpack("3i", raw)
        except (OSError, AttributeError, struct.error):
            return None
        return PeerIdentity(pid=pid, uid=uid, gid=gid)

    # ------------------------------------------------------------------ endpoint setup

    def _prepare_directory(self) -> None:
        directory = self._endpoint.parent
        if not directory.exists():
            directory.mkdir(mode=0o700, parents=True)
            directory.chmod(0o700)  # independent of the process umask
        elif directory.stat().st_uid != os.geteuid():
            raise IngressUnavailableError(f"IPC directory {directory} is not owned by the Engine's user")

    def _acquire_instance_lock(self) -> None:
        lock_path = self._endpoint.with_name(self._endpoint.name + ".lock")
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise EndpointInUseError(f"another Engine is already using {self._endpoint}") from exc
            raise
        self._lock_descriptor = descriptor

    def _remove_stale_endpoint(self) -> None:
        # Holding the lock proves no live Engine owns the endpoint, so a socket left here is stale.
        try:
            mode = self._endpoint.lstat().st_mode
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(mode):
            raise IngressUnavailableError(f"{self._endpoint} exists and is not a socket; refusing to remove it")
        logger.warning("removing stale IPC endpoint left by an earlier execution: %s", self._endpoint)
        self._endpoint.unlink()

    def _bind_and_listen(self) -> None:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        previous_umask = os.umask(0o177)  # the socket is created 0600, with no window of wider access
        try:
            listener.bind(str(self._endpoint))
        except OSError:
            listener.close()
            raise
        finally:
            os.umask(previous_umask)
        self._listener = listener
        self._bound_inode = self._endpoint.lstat().st_ino
        listener.listen(self._limits.listen_backlog)
        listener.setblocking(False)
