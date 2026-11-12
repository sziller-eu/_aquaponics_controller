"""Diagnostic logging for the Engine process: configuration, correlation, bounds, failure isolation.

Diagnostic logs describe what the software did, for a human investigating it. They are
NOT authoritative: they are not the lifecycle audit history, not the durable command
journal and not state history, and nothing reads them to decide what to do.
See "Logging" in the architecture documentation and "Diagnostic Logging (Initial
Implementation)" in the Development and Operations documentation.

One process-wide configuration, made once by the composition root (``engine_main``):

- every record goes to standard error; under systemd that is journald, whose own
  configuration bounds what is kept (the Engine does not);
- optionally also to a size-rotated file the Engine bounds itself (``--log-file``).

Every record carries the Engine execution identifier (the same value as in the lifecycle
audit history) and the operating-cycle number, or ``-`` outside a cycle:

    2026-11-12T10:00:49.917Z INFO engine exec=6f1c...e2 cycle=42 message

Failure isolation: a handler that cannot write never raises into the caller. Its first
failure is reported once, as one line written straight to file descriptor 2 (not
through ``logging``, so it cannot recurse), naming only the handler, the exception type
and the errno. Later failures are counted.

KNOWN LIMITATION: logging is synchronous. A write can block (a stalled reader of
standard error, slow storage), and while it blocks the Engine thread does nothing else.
The time spent logging is therefore not strictly bounded. This must be resolved before
physical equipment control is enabled.

Signal handlers must never log: logging takes locks that are not safe to re-enter from
a signal handler.
"""

import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path

from .clock import SystemClock

logger = logging.getLogger(__name__)

PACKAGE_PREFIX = "aquaponics_controller."

# Optional log file: at most 1 current + 4 rotated files of 5 MiB (25 MiB). Code constants by decision (Issue #7).
LOG_FILE_MAX_BYTES = 5 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 4

# Opt-in repeat suppression (see RepeatedWarningReporter).
SUPPRESSION_WINDOW_SECONDS = 300.0
SUPPRESSION_LOGGED_PER_WINDOW = 3
SUMMARY_MAX_CHARACTERS = 200

STDERR_HANDLER_NAME = "stderr"
FILE_HANDLER_NAME = "log-file"

LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(subsystem)s exec=%(execution_id)s cycle=%(cycle)s %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


# ---------------------------------------------------------------------- correlation context


class DiagnosticContext:
    """The correlation values stamped on every record. One per process; the Engine is single-threaded.

    ``execution_id`` is set once by ``configure_logging``. ``cycle`` is set by the Engine at the start
    of an operating cycle and cleared after the cycle completes and when shutdown begins. It is
    deliberately NOT cleared when a cycle raises, so the failure is logged with its cycle number.
    """

    def __init__(self) -> None:
        self.execution_id: str | None = None
        self.cycle: int | None = None

    def reset(self) -> None:
        self.execution_id = None
        self.cycle = None


log_context = DiagnosticContext()


class DiagnosticContextFilter(logging.Filter):
    """Stamps execution identifier, cycle and subsystem on a record. Never fails, never drops a record.

    Filters run outside the protection of ``Handler.emit``, so an exception here would reach the
    code that logged. This one only sets attributes; the formatter has defaults if it ever could not.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.execution_id = log_context.execution_id or "-"
            record.cycle = "-" if log_context.cycle is None else str(log_context.cycle)
            name = record.name
            record.subsystem = name[len(PACKAGE_PREFIX) :] if name.startswith(PACKAGE_PREFIX) else name
        except Exception:  # noqa: BLE001 - a filter must never interfere with the code that logs
            pass
        return True


def build_formatter() -> logging.Formatter:
    """UTC ISO 8601 timestamps with milliseconds. Missing context fields render as ``-``."""
    formatter = logging.Formatter(
        LOG_FORMAT, DATE_FORMAT, defaults={"execution_id": "-", "cycle": "-", "subsystem": "-"}
    )
    formatter.converter = time.gmtime
    return formatter


# ---------------------------------------------------------------------- failure-tolerant handlers


class FailureTolerantHandlerMixin:
    """Replaces ``logging``'s default error handling, which prints a traceback plus the record's
    message and arguments to standard error on every failure.

    Here: nothing is raised, nothing is logged (so there is no recursion), the first failure is
    reported once on file descriptor 2 with only the handler name, exception type and errno, and
    every failure is counted. Each new record is attempted again, so logging recovers when it can.
    """

    failure_count = 0

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802 - logging's API name
        try:
            self.failure_count += 1
            if self.failure_count > 1:
                return
            exc_type, exc, _ = sys.exc_info()
            type_name = exc_type.__name__ if exc_type is not None else "unknown error"
            code = getattr(exc, "errno", None)
            del exc  # do not keep the exception (and its frames) alive
            line = (
                f"aquaponics-engine: diagnostic log handler {self.get_name() or type(self).__name__} failed "
                f"({type_name}, errno {code if code is not None else '-'}); further failures are counted\n"
            )
            os.write(2, line.encode("ascii", "backslashreplace"))
        except Exception:  # noqa: BLE001 - reporting a logging failure must never fail the caller
            pass


class FailureTolerantStreamHandler(FailureTolerantHandlerMixin, logging.StreamHandler):
    pass


class BoundedRotatingFileHandler(FailureTolerantHandlerMixin, logging.handlers.RotatingFileHandler):
    """A size-rotated log file whose total size the Engine bounds.

    Differences from the standard handler:
    - sizes are counted in encoded bytes, not characters;
    - only a single record larger than the limit can make a file exceed it (written to an empty file);
    - if rotation fails, the record is dropped (reported by ``handleError``) rather than appended
      beyond the limit;
    - the file is created with mode 0600.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(
            path,
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
            errors="backslashreplace",
        )

    def _open(self):
        descriptor = os.open(self.baseFilename, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_CLOEXEC, 0o600)
        return open(descriptor, "a", encoding=self.encoding, errors=self.errors)

    def shouldRollover(self, record: logging.LogRecord) -> bool:  # noqa: N802 - logging's API name
        if self.stream is None:
            self.stream = self._open()
        size = self.stream.seek(0, os.SEEK_END)
        encoded_size = len((self.format(record) + self.terminator).encode(self.encoding, self.errors))
        return size > 0 and size + encoded_size > self.maxBytes

    def emit(self, record: logging.LogRecord) -> None:
        # The standard emit, written out so that a failed rotation drops the record instead of
        # appending it to a file that is already full.
        try:
            if self.shouldRollover(record):
                self.doRollover()
            logging.FileHandler.emit(self, record)
        except Exception:  # noqa: BLE001
            self.handleError(record)


def open_log_file(path: Path) -> BoundedRotatingFileHandler:
    """Create the directory (0700) if needed and open the log file (0600). Raise OSError if impossible."""
    directory = path.parent
    if not directory.exists():
        directory.mkdir(mode=0o700, parents=True)
        directory.chmod(0o700)  # independent of the process umask
    return BoundedRotatingFileHandler(path)


# ---------------------------------------------------------------------- configuration

_configured_handlers: list[FailureTolerantHandlerMixin] = []


def configure_logging(level_name: str, log_file: str | Path | None = None, execution_id: str | None = None) -> None:
    """Configure process-wide diagnostic logging. Called once, by the composition root.

    ``execution_id`` must be the same value the lifecycle audit and the Engine receive.
    A log file that cannot be opened is reported and logging continues on standard error only:
    a problem with diagnostic logging must not prevent the Engine from starting.
    """
    log_context.execution_id = execution_id
    formatter = build_formatter()
    context_filter = DiagnosticContextFilter()

    stderr_handler = FailureTolerantStreamHandler(sys.stderr)
    stderr_handler.set_name(STDERR_HANDLER_NAME)
    handlers: list[logging.Handler] = [stderr_handler]

    file_problem: str | None = None
    if log_file:
        try:
            file_handler = open_log_file(Path(log_file))
        except OSError as exc:
            file_problem = f"{type(exc).__name__}: {exc.strerror or exc}"
        else:
            file_handler.set_name(FILE_HANDLER_NAME)
            handlers.append(file_handler)

    for handler in handlers:
        handler.setFormatter(formatter)
        handler.addFilter(context_filter)
    logging.basicConfig(level=level_name.upper(), handlers=handlers, force=True)
    _configured_handlers[:] = handlers

    if file_problem is not None:
        logger.warning("diagnostic log file %s cannot be used (%s); logging to standard error only", log_file, file_problem)


def log_handler_failure_counts() -> None:
    """Report, through the remaining handlers, how many records each configured handler failed to write."""
    for handler in list(_configured_handlers):
        if handler.failure_count:
            logger.warning(
                "%d diagnostic log records could not be written to the %s log handler",
                handler.failure_count,
                handler.get_name(),
            )


# ---------------------------------------------------------------------- opt-in repeat suppression


def sanitize_summary(value: object) -> str:
    """A bounded, printable rendering of a detail: control characters escaped, at most SUMMARY_MAX_CHARACTERS.

    Anything that is not already a string is reduced to its type name, so no object is rendered
    (or kept alive) by accident.
    """
    text = value if isinstance(value, str) else type(value).__name__
    text = text[: SUMMARY_MAX_CHARACTERS + 1]
    escaped = "".join(c if c.isprintable() else c.encode("unicode_escape").decode("ascii") for c in text)
    if len(escaped) > SUMMARY_MAX_CHARACTERS:
        escaped = escaped[: SUMMARY_MAX_CHARACTERS - 3] + "..."
    return escaped


class RepeatedWarningReporter:
    """Opt-in suppression for ONE predefined, repetitive, low-information warning category.

    Only call sites that create a reporter are affected; nothing is suppressed globally.
    One reporter = one category: meaningfully different failures get different reporters
    (or are logged without one). Never use a reporter for ERROR or CRITICAL records, lifecycle
    transitions, Engine failures, security-relevant events, audit failures or cleanup failures.

    Within a fixed window (300 s from the first logged occurrence) the first 3 occurrences are
    logged; later ones are only counted. The next occurrence after the window has ended first
    logs one summary of what was suppressed, then starts a new window. ``flush()`` (at shutdown)
    logs a pending summary.

    Memory is fixed: a few counters and one sanitized detail string of at most 200 characters.
    No exception objects or message arguments are kept.
    """

    def __init__(
        self,
        logger_: logging.Logger,
        category: str,
        message: str,
        *,
        level: int = logging.WARNING,
        clock=None,
        window_seconds: float = SUPPRESSION_WINDOW_SECONDS,
        logged_per_window: int = SUPPRESSION_LOGGED_PER_WINDOW,
    ) -> None:
        if level > logging.WARNING:
            raise ValueError("only WARNING or lower can be suppressed; ERROR and CRITICAL are always logged")
        if not category or not category.isprintable() or len(category) > 64:
            raise ValueError("a suppression category is a short printable identifier")
        if window_seconds <= 0 or logged_per_window < 1:
            raise ValueError("the suppression window and the logged count must be positive")
        self._logger = logger_
        self.category = category
        self._message = message
        self._level = level
        self._clock = clock if clock is not None else SystemClock()
        self._window_seconds = window_seconds
        self._logged_per_window = logged_per_window

        self._window_started: float | None = None
        self._logged_in_window = 0
        self._suppressed_in_window = 0
        self._last_suppressed_detail: str | None = None
        self.total_suppressed = 0

    def report(self, detail: str) -> None:
        """Report one occurrence. ``detail`` is a short description (for example an errno text); it is sanitized."""
        detail = sanitize_summary(detail)
        now = self._clock.monotonic()
        if self._window_started is None or now - self._window_started >= self._window_seconds:
            self._log_summary(now)
            self._window_started = now
            self._logged_in_window = 0
        if self._logged_in_window < self._logged_per_window:
            self._logged_in_window += 1
            if self._logged_in_window == self._logged_per_window:
                self._logger.log(
                    self._level,
                    "%s: %s (category %s: further repeats are counted for up to %.0f s)",
                    self._message,
                    detail,
                    self.category,
                    self._window_seconds,
                )
            else:
                self._logger.log(self._level, "%s: %s", self._message, detail)
            return
        self._suppressed_in_window += 1
        self.total_suppressed += 1
        self._last_suppressed_detail = detail

    def flush(self) -> None:
        """Log the summary of anything suppressed in the current window, and end the window."""
        self._log_summary(self._clock.monotonic())
        self._window_started = None
        self._logged_in_window = 0

    def _log_summary(self, now: float) -> None:
        if not self._suppressed_in_window:
            return
        assert self._window_started is not None
        self._logger.log(
            self._level,
            "%s: %d similar warnings suppressed in %.0f s (category %s; last: %s)",
            self._message,
            self._suppressed_in_window,
            min(now - self._window_started, self._window_seconds),  # all suppressed ones fell inside the window
            self.category,
            self._last_suppressed_detail,
        )
        self._suppressed_in_window = 0
        self._last_suppressed_detail = None
