"""Notifications from the Engine to the systemd service manager (the ``sd_notify`` protocol).

The Engine runs as a ``Type=notify`` service (see "Engine Service (Initial Implementation)"
in the operations documentation). It tells systemd three separate things:

- ``READY=1``    once, at the end of startup: the Engine is running and its IPC endpoint listens;
- ``WATCHDOG=1`` the heartbeat: the Engine's single control loop recently completed an operating cycle;
- ``STOPPING=1`` once, at the start of shutdown, whatever the reason.

Without ``NOTIFY_SOCKET`` (the Engine started from a terminal or a test), every method
is a no-op and the Engine behaves exactly as it does without systemd.

Activation contract (Issue #9, D15):

- notifications are active only if ``NOTIFY_SOCKET`` is set;
- the watchdog is active only if, in addition, ``WATCHDOG_USEC`` is set and ``WATCHDOG_PID``
  is either unset or this process's ID. ``WATCHDOG_PID`` affects ONLY the watchdog: it never
  disables READY or STOPPING.

Transport: one non-blocking Unix datagram socket, opened at startup and closed at
cleanup. Each message is a single non-blocking send, never retried, so a notification
can never block the Engine. This module starts no thread and runs no timer: it acts
only when the Engine calls it, from the Engine's main flow.

WHAT THE HEARTBEAT MEANS: the Engine completed a full operating cycle recently. It does
NOT show that the administrative IPC is reachable, that any request was served, or that
the physical installation is safe.
"""

import logging
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass

from .clock import SystemClock
from .cycle_phases import PHASE_TIME_LIMITS_SECONDS
from .diagnostic_logging import RepeatedWarningReporter

logger = logging.getLogger(__name__)

NOTIFY_SOCKET_VARIABLE = "NOTIFY_SOCKET"
WATCHDOG_USEC_VARIABLE = "WATCHDOG_USEC"
WATCHDOG_PID_VARIABLE = "WATCHDOG_PID"

# A heartbeat is sent at most once per this fraction of the actual watchdog timeout.
WATCHDOG_HEARTBEAT_FRACTION = 4

# The longest an operating cycle may take according to its phase budgets (1.5 s). Used for the
# interval rule below: one cycle interval plus one worst-case cycle must fit in half the timeout.
WORST_CASE_CYCLE_SECONDS = sum(PHASE_TIME_LIMITS_SECONDS.values())

MESSAGE_READY = b"READY=1"
MESSAGE_WATCHDOG = b"WATCHDOG=1"
MESSAGE_STOPPING = b"STOPPING=1"


@dataclass(frozen=True)
class ServiceManagerSettings:
    """What the service manager asked for, read once from the environment at startup."""

    notify_socket: str | None = None  # None: not started by a notify-aware service manager
    watchdog_seconds: float | None = None  # None: the watchdog is not active for this process

    @classmethod
    def from_environment(cls, environment: Mapping[str, str], pid: int) -> "ServiceManagerSettings":
        """Apply the activation contract. Raise ValueError if a variable is set but malformed."""
        notify_socket = environment.get(NOTIFY_SOCKET_VARIABLE) or None
        if notify_socket is None:
            return cls()  # standalone: nothing is sent, so the watchdog variables do not matter
        if not (notify_socket.startswith("/") or notify_socket.startswith("@")) or len(notify_socket) < 2:
            raise ValueError(f"{NOTIFY_SOCKET_VARIABLE} must be an absolute path or an abstract socket name ('@...')")

        watchdog_usec = environment.get(WATCHDOG_USEC_VARIABLE) or None
        if watchdog_usec is None:
            return cls(notify_socket=notify_socket)
        watchdog_microseconds = _positive_integer(WATCHDOG_USEC_VARIABLE, watchdog_usec)

        watchdog_pid = environment.get(WATCHDOG_PID_VARIABLE) or None
        if watchdog_pid is not None and _positive_integer(WATCHDOG_PID_VARIABLE, watchdog_pid) != pid:
            # The watchdog was meant for another process (for example a wrapper). READY and STOPPING still apply.
            return cls(notify_socket=notify_socket)
        return cls(notify_socket=notify_socket, watchdog_seconds=watchdog_microseconds / 1_000_000)


def _positive_integer(name: str, text: str) -> int:
    try:
        value = int(text, 10)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, not {text!r}") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer, not {text!r}")
    return value


def check_cycle_interval_fits_watchdog(cycle_interval_seconds: float, watchdog_seconds: float | None) -> None:
    """Refuse a cycle interval the watchdog cannot tolerate. Raise ValueError (a configuration error).

    Rule: cycle interval + worst-case cycle (1.5 s) <= half the watchdog timeout. With WatchdogSec=10s
    the cycle interval may be at most 3.5 s. Heartbeats are rate-limited to a quarter of the timeout,
    so the longest possible gap between two heartbeats stays below three quarters of it.
    """
    if watchdog_seconds is None:
        return
    needed = cycle_interval_seconds + WORST_CASE_CYCLE_SECONDS
    if needed > watchdog_seconds / 2:
        raise ValueError(
            f"cycle interval {cycle_interval_seconds:g} s is too long for the {watchdog_seconds:g} s service "
            f"watchdog: interval + {WORST_CASE_CYCLE_SECONDS:g} s worst-case cycle ({needed:g} s) must be at "
            f"most half the watchdog timeout ({watchdog_seconds / 2:g} s)"
        )


class ServiceNotifier:
    """Sends READY, WATCHDOG and STOPPING to systemd, or nothing when not started by it.

    Called only by the Engine, from its main flow. Failure behaviour:

    - ``open`` / ``notify_ready``: raise OSError; the Engine treats it as a startup failure (fail-stop).
      Without READY, systemd would end the start attempt at TimeoutStartSec anyway.
    - ``notify_watchdog``: a failed send is a rate-limited WARNING and nothing else. If heartbeats
      really stop, systemd's watchdog acts; that is its purpose.
    - ``notify_stopping``: a failed send is a WARNING; the shutdown continues.
    """

    def __init__(self, settings: ServiceManagerSettings | None = None, *, clock=None):
        self._settings = settings if settings is not None else ServiceManagerSettings()
        self._clock = clock if clock is not None else SystemClock()
        self._socket: socket.socket | None = None
        self._ready_sent = False
        self._stopping_sent = False
        self._last_heartbeat: float | None = None
        self._heartbeat_failures = RepeatedWarningReporter(
            logger,
            "service-notify-failed",
            "sending the watchdog heartbeat to systemd failed",
            clock=self._clock,
        )

    @property
    def active(self) -> bool:
        return self._settings.notify_socket is not None

    @property
    def watchdog_seconds(self) -> float | None:
        return self._settings.watchdog_seconds

    @property
    def heartbeat_interval_seconds(self) -> float | None:
        """The shortest time between two heartbeats (None: the watchdog is not active)."""
        if self._settings.watchdog_seconds is None:
            return None
        return self._settings.watchdog_seconds / WATCHDOG_HEARTBEAT_FRACTION

    # ------------------------------------------------------------------ lifecycle

    def open(self) -> None:
        """Create the notification socket. Raise OSError if that is impossible."""
        if not self.active or self._socket is not None:
            return
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC | socket.SOCK_NONBLOCK)
        if self._settings.watchdog_seconds is None:
            logger.info("running under systemd: readiness is reported; the service watchdog is not active")
        else:
            logger.info(
                "running under systemd: readiness is reported; service watchdog %.3f s, "
                "heartbeat after completed operating cycles, at most every %.3f s",
                self._settings.watchdog_seconds,
                self.heartbeat_interval_seconds,
            )

    def close(self) -> None:
        """Release the socket. Safe to call more than once and when never opened."""
        self._heartbeat_failures.flush()  # nothing suppressed goes unmentioned
        if self._socket is not None:
            descriptor, self._socket = self._socket, None
            descriptor.close()

    # ------------------------------------------------------------------ the three notifications

    def notify_ready(self) -> None:
        """READY=1, once. Raise OSError if it cannot be sent (the Engine then fails its startup)."""
        if not self.active or self._ready_sent:
            return
        self._send(MESSAGE_READY)
        self._ready_sent = True
        logger.info("readiness reported to systemd")

    def notify_watchdog(self) -> None:
        """WATCHDOG=1 after a completed operating cycle, at most once per quarter of the timeout.

        Never before READY and never after STOPPING. A failed send is reported (rate-limited) and
        otherwise ignored.
        """
        if self._settings.watchdog_seconds is None or not self._ready_sent or self._stopping_sent:
            return
        now = self._clock.monotonic()
        if self._last_heartbeat is not None and now - self._last_heartbeat < self.heartbeat_interval_seconds:
            return
        try:
            self._send(MESSAGE_WATCHDOG)
        except OSError as exc:
            self._heartbeat_failures.report(f"[Errno {exc.errno}] {exc.strerror}")
            return
        self._last_heartbeat = now

    def notify_stopping(self) -> None:
        """STOPPING=1, once, at the start of shutdown. A failed send is logged; the shutdown continues."""
        if not self.active or self._stopping_sent:
            return
        self._stopping_sent = True  # also ends the heartbeats, even if this send fails
        try:
            self._send(MESSAGE_STOPPING)
        except OSError as exc:
            logger.warning("reporting the shutdown to systemd failed: %s", exc)

    # ------------------------------------------------------------------ transport

    def _send(self, message: bytes) -> None:
        if self._socket is None:
            raise OSError("the systemd notification socket is not open")
        self._socket.sendto(message, _socket_address(self._settings.notify_socket))


def _socket_address(name: str) -> str | bytes:
    """``@name`` is an abstract socket (a leading NUL byte on Linux); anything else is a filesystem path."""
    if name.startswith("@"):
        return b"\0" + name[1:].encode()
    return name
