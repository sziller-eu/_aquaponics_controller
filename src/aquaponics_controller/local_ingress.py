"""Engine-side local administrative IPC ingress: a Unix domain socket.

The Engine services this interface; it never drives the Engine. Every call to
``service_pending_requests`` is bounded in the number of connections, in the time
spent per connection, and in total time, so a stalled or hostile local client
cannot hold the Engine for an unbounded time. ``readiness_source`` lets the Engine
learn, while it idles between operating cycles, that a client is waiting; the
Engine then calls ``service_pending_requests`` itself. The ingress starts no thread
and runs nothing of its own.

Access control is Linux filesystem permissions: the endpoint's directory and the
socket are owner-only (0700 / 0600). The peer credentials reported by
the kernel are passed on to the Engine for validation and audit. They supplement
the file permissions; they do not replace them. The endpoint's directory is
validated before use (see ``_prepare_directory``): it must be a real directory, not
a symbolic link, owned by the Engine's user and writable by nobody else.

Failure classification (see "Lifecycle and IPC Failure Handling" in the operations documentation):
only the recoverable errors in ``RECOVERABLE_CONNECTION_ERRNOS``, raised by an
operation on one accepted client connection, are contained: that connection is
closed and the Engine carries on. Any other exception propagates and becomes an
Engine failure; when the request handler raised it, the client is first sent a
fixed ``internal_error`` answer (best effort). The same principle applies to the
listening socket: only the ``accept()`` errors in ``CONTAINED_ACCEPT_ERRNOS`` are
contained (Issue #9, D14); any other one means the listener itself is broken, and it
becomes an Engine failure, so that the service manager restarts the Engine instead
of the Engine carrying on, heartbeat included, without administrative access.

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
from .diagnostic_logging import RepeatedWarningReporter
from .ipc_protocol import (
    ERROR_INTERNAL,
    ERROR_MALFORMED_REQUEST,
    ERROR_REQUEST_TIMEOUT,
    ERROR_REQUEST_TOO_LARGE,
    INTERNAL_ERROR_MESSAGE,
    MAX_MESSAGE_BYTES,
    AdministrativeRequest,
    AdministrativeResponse,
    PeerIdentity,
    ProtocolError,
    decode_request,
    encode_response,
)

logger = logging.getLogger(__name__)

# accept() failures that can repeat every few hundred milliseconds for as long as the condition
# lasts (descriptor or memory exhaustion). Each is its own suppression category.
REPEATING_ACCEPT_FAILURES = {
    errno.EMFILE: "EMFILE",
    errno.ENFILE: "ENFILE",
    errno.ENOBUFS: "ENOBUFS",
    errno.ENOMEM: "ENOMEM",
}

# The ONLY accept() errors contained (Issue #9, D14, approved classification): transient resource
# exhaustion (the categories above) and a client that went away while queued (ECONNABORTED). The
# servicing call ends there and the Engine carries on. An empty queue (EAGAIN / EWOULDBLOCK, raised
# as BlockingIOError) is not an error and simply ends the call. Every other accept() error (EBADF,
# EINVAL, ENOTSOCK, EFAULT, ...) means the listener is broken: it propagates as an Engine failure.
CONTAINED_ACCEPT_ERRNOS = frozenset(REPEATING_ACCEPT_FAILURES) | {errno.ECONNABORTED}

# The ONLY errors contained per connection (Issue #8, approved classification), and only when raised by an
# operation on the accepted client connection itself (recv, sendall, settimeout, getsockopt, close):
# - the peer went away: ECONNRESET, EPIPE, ECONNABORTED, ENOTCONN;
# - a transient kernel resource shortage on that socket: ENOBUFS, ENOMEM, EAGAIN / EWOULDBLOCK.
# The connection's own read or write deadline expiring (TimeoutError) is the third recoverable case.
# Every other OSError (EBADF, EINVAL, ENOTSOCK, EFAULT, ...) points to a defect and is NOT contained.
RECOVERABLE_CONNECTION_ERRNOS = frozenset(
    {
        errno.ECONNRESET,
        errno.EPIPE,
        errno.ECONNABORTED,
        errno.ENOTCONN,
        errno.ENOBUFS,
        errno.ENOMEM,
        errno.EAGAIN,
        errno.EWOULDBLOCK,
    }
)


def is_recoverable_connection_error(exc: OSError) -> bool:
    """Whether an error from an operation on one client connection may be contained to that connection."""
    return isinstance(exc, TimeoutError) or exc.errno in RECOVERABLE_CONNECTION_ERRNOS


class _ClientClosedConnection(Exception):
    """The client closed its connection before sending any request (not an error of the Engine)."""


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
        # Set by withdraw_endpoint(): from then on the only servicing left is the final pass of a shutdown.
        self._withdrawn = False
        # Opt-in repeat suppression for the two known repetitive, low-information warnings only.
        # Protocol errors, refusals and disconnects of individual clients are never suppressed here.
        self._accept_failure_reports = {
            code: RepeatedWarningReporter(
                logger, f"ipc-accept-failed:{name}", "accepting an IPC connection failed", clock=self._clock
            )
            for code, name in REPEATING_ACCEPT_FAILURES.items()
        }
        self._budget_exhausted_report = RepeatedWarningReporter(
            logger,
            "ingress-budget-exhausted",
            "ingress time budget used up; remaining connections wait for the next cycle",
            clock=self._clock,
        )

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
        for report in (*self._accept_failure_reports.values(), self._budget_exhausted_report):
            report.flush()  # nothing suppressed goes unmentioned
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

    def withdraw_endpoint(self) -> None:
        """Remove the endpoint path so that no new client can connect; keep the listener and the lock.

        Used at the start of a controlled shutdown: clients already queued on the listener can still
        be served once (see the Engine's final servicing pass); a client that tries to connect from
        now on finds no endpoint. Only the socket this instance bound is removed. If removing it
        fails, the error is raised and ``close()`` tries again (and reports it as a cleanup failure).
        """
        self._withdrawn = True  # even if removing the path fails: the Engine is shutting down either way
        if self._bound_inode is None:
            return
        try:
            if self._endpoint.lstat().st_ino == self._bound_inode:
                self._endpoint.unlink()
        except FileNotFoundError:
            pass
        self._bound_inode = None  # nothing left for close() to remove

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
                self._report_budget_exhausted(handled, budget)
                break
            try:
                connection, _ = self._listener.accept()
            except BlockingIOError:
                break  # nobody (else) is waiting
            except OSError as exc:
                if exc.errno not in CONTAINED_ACCEPT_ERRNOS:
                    raise  # the listener is broken: an Engine failure (fail-stop), not a warning
                self._report_accept_failure(exc)
                break
            handled += 1
            try:
                self._serve_connection(connection, handler)
            except BaseException:
                # An Engine failure (or interruption) is on its way out: closing must not replace it.
                try:
                    connection.close()
                except OSError:
                    pass
                raise
            self._close_connection(connection)
        return handled

    def _report_budget_exhausted(self, handled: int, budget: float) -> None:
        if self._withdrawn:
            # The final servicing pass of a shutdown: there is no next cycle for whoever is still queued.
            # It happens at most once per execution, so it is not subject to repeat suppression.
            logger.warning(
                "shutdown servicing budget used up (%d connections handled within %.3f s); "
                "remaining queued connections may not receive an answer",
                handled,
                budget,
            )
        else:
            self._budget_exhausted_report.report(f"{handled} connections handled within {budget:.3f} s")

    def _report_accept_failure(self, exc: OSError) -> None:
        report = self._accept_failure_reports.get(exc.errno)
        if report is None:
            logger.warning("accepting an IPC connection failed: %s", exc)  # contained, but not a repeating category
        else:
            report.report(f"[Errno {exc.errno}] {exc.strerror}")

    def _serve_connection(self, connection: socket.socket, handler: RequestHandler) -> None:
        """Read, decode, handle and answer one request.

        Contained here (the connection ends, the Engine carries on): protocol errors (answered with
        their refusal code), a client that closed without sending anything, and the recoverable
        connection errors (``is_recoverable_connection_error``). Everything else propagates.
        """
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
        except _ClientClosedConnection:
            logger.warning("IPC client %s disconnected before completing its request: connection closed", peer)
            return
        except OSError as exc:
            if not is_recoverable_connection_error(exc):
                raise
            if isinstance(exc, ConnectionError):
                logger.warning("IPC client %s disconnected before completing its request: %s", peer, exc)
            else:
                logger.warning("IPC connection from %s failed while reading its request: %s", peer, exc)
            return
        # Diagnostic trace: receipt, (Engine processing is traced by the handler), answer. Only the
        # operation name and the correlation identifier are named, never the request's arguments.
        logger.info("IPC request received: op=%r id=%s peer=%s", request.operation, _describe_id(request.correlation_id), peer)
        try:
            response = handler(request, peer)
        except Exception:
            # Not contained: the Engine takes its fail-stop path. The client is told first, best effort.
            self._notify_internal_error(connection, request.correlation_id)
            raise
        delivered = self._send(connection, response)
        logger.info(
            "IPC request answered: id=%s ok=%s %s",
            _describe_id(request.correlation_id),
            response.ok,
            "delivered" if delivered else "NOT delivered",
        )

    def _notify_internal_error(self, connection: socket.socket, correlation_id: str | None) -> None:
        """Send the fixed internal_error answer: one attempt, within the write deadline, no exception details.

        Called while the handler's exception is being handled. Whatever happens here must not replace
        that exception, so a failure to send is reported and otherwise ignored (this is the only place
        where errors are not classified: it is a courtesy to the client on the way to fail-stop).
        """
        try:
            delivered = self._send(
                connection,
                AdministrativeResponse(
                    False, error_code=ERROR_INTERNAL, message=INTERNAL_ERROR_MESSAGE, correlation_id=correlation_id
                ),
            )
            logger.warning(
                "IPC request %s failed inside the Engine: internal_error answer %s",
                _describe_id(correlation_id),
                "delivered" if delivered else "NOT delivered",
            )
        except Exception as exc:  # noqa: BLE001 - must never replace the handler's exception
            logger.warning("internal_error answer could not be sent (%s)", type(exc).__name__)

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
                raise _ClientClosedConnection()
            received += chunk
            if len(received) > MAX_MESSAGE_BYTES:
                raise ProtocolError(ERROR_REQUEST_TOO_LARGE, f"request exceeds {MAX_MESSAGE_BYTES} bytes")
        return received.split(b"\n", 1)[0]

    def _send(self, connection: socket.socket, response: AdministrativeResponse) -> bool:
        """Send one response within the write deadline. Return whether it was delivered.

        A recoverable connection error means "not delivered"; any other error propagates.
        """
        try:
            connection.settimeout(self._limits.write_deadline_seconds)
            connection.sendall(encode_response(response))
        except OSError as exc:
            if not is_recoverable_connection_error(exc):
                raise
            logger.warning("could not deliver IPC response: %s", exc)
            return False
        return True

    @staticmethod
    def _close_connection(connection: socket.socket) -> None:
        try:
            connection.close()
        except OSError as exc:
            if not is_recoverable_connection_error(exc):
                raise
            logger.warning("closing an IPC connection failed: %s", exc)

    @staticmethod
    def _read_peer_identity(connection: socket.socket) -> PeerIdentity | None:
        """The peer's credentials, or None if they cannot be read (the request is then refused as unauthorized)."""
        try:
            raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
            pid, uid, gid = struct.unpack("3i", raw)
        except OSError as exc:
            if not is_recoverable_connection_error(exc):
                raise
            return None
        except (AttributeError, struct.error):  # no SO_PEERCRED on this platform, or an unexpected size
            return None
        return PeerIdentity(pid=pid, uid=uid, gid=gid)

    # ------------------------------------------------------------------ endpoint setup

    def _prepare_directory(self) -> None:
        """Create the endpoint's directory (0700) if it is missing, otherwise validate it.

        Only this one directory is examined or changed, never its parents. It is opened without
        following a symbolic link, and every check (and the one permitted correction) is made on
        that open descriptor, so the directory checked is the directory changed:

        - a symbolic link, or not a directory: refused;
        - not owned by the Engine's user: refused;
        - writable by group or others: refused (its content may already have been tampered with);
        - only readable or searchable by group or others: restricted to 0700, with a warning.

        A refusal raises IngressUnavailableError: the Engine does not start (there is no fallback
        endpoint, which clients could not find).
        """
        directory = self._endpoint.parent
        created = False
        try:
            descriptor = self._open_directory(directory)
        except FileNotFoundError:
            try:
                directory.mkdir(mode=0o700, parents=True)  # parents keep the default mode; only the leaf is ours
                created = True
            except FileExistsError:
                pass  # created meanwhile: validated below like any existing directory
            descriptor = self._open_directory(directory)
        try:
            status = os.fstat(descriptor)
            if not stat.S_ISDIR(status.st_mode):
                raise IngressUnavailableError(f"IPC directory {directory} is not a directory")
            if status.st_uid != os.geteuid():
                raise IngressUnavailableError(f"IPC directory {directory} is not owned by the Engine's user")
            mode = stat.S_IMODE(status.st_mode)
            if created:
                if mode != 0o700:
                    os.fchmod(descriptor, 0o700)  # independent of the process umask
            elif mode & 0o022:
                raise IngressUnavailableError(
                    f"IPC directory {directory} is writable by group or others (mode {mode:04o}); refusing to use it"
                )
            elif mode & 0o077:
                os.fchmod(descriptor, 0o700)
                logger.warning("IPC directory %s had permissions %04o; restricted to 0700", directory, mode)
        finally:
            os.close(descriptor)

    @staticmethod
    def _open_directory(directory: Path) -> int:
        try:
            return os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError as exc:
            # With O_DIRECTORY | O_NOFOLLOW, Linux reports a symbolic link as ENOTDIR (or ELOOP).
            if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                if directory.is_symlink():
                    raise IngressUnavailableError(
                        f"IPC directory {directory} is a symbolic link, which is not followed; refusing to use it"
                    ) from None
                raise IngressUnavailableError(f"IPC directory {directory} is not a directory") from None
            raise

    def _acquire_instance_lock(self) -> None:
        lock_path = self._endpoint.with_name(self._endpoint.name + ".lock")
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise IngressUnavailableError(f"IPC lock file {lock_path} is a symbolic link; refusing to use it") from None
            raise
        try:
            status = os.fstat(descriptor)
            if not stat.S_ISREG(status.st_mode):
                raise IngressUnavailableError(f"IPC lock file {lock_path} is not a regular file; refusing to use it")
            if status.st_uid != os.geteuid():
                raise IngressUnavailableError(f"IPC lock file {lock_path} is not owned by the Engine's user")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise EndpointInUseError(f"another Engine is already using {self._endpoint}") from exc
            raise
        except BaseException:
            os.close(descriptor)
            raise
        self._lock_descriptor = descriptor

    def _remove_stale_endpoint(self) -> None:
        # Holding the lock proves no live Engine owns the endpoint, so a socket left here is stale.
        try:
            status = self._endpoint.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(status.st_mode):
            raise IngressUnavailableError(f"{self._endpoint} exists and is not a socket; refusing to remove it")
        if status.st_uid != os.geteuid():
            raise IngressUnavailableError(
                f"{self._endpoint} is a socket owned by another user; refusing to remove it"
            )
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
