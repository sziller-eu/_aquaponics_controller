"""Persistent, append-only lifecycle audit history of Engine executions.

The history is a JSON Lines file: one JSON object per line, appended and never
rewritten. It is historical evidence. It is NOT a diagnostic log, NOT the
operational command journal, and nothing in this package reads it to decide what
to do. A STOP record in this file is never a pending command and is never replayed.

Durability: every record is written with a single ``write`` to a file opened in
append mode and is followed by ``fsync``. When the file is first created its
directory is also ``fsync``-ed. Limits of these guarantees are documented in
"Lifecycle Audit Persistence" (Development and Operations documentation):
``fsync`` can block on a failing disk, and storage hardware may acknowledge data
it has not yet made durable.
"""

import enum
import json
import logging
import os
from pathlib import Path

from .clock import SystemClock, format_utc

logger = logging.getLogger(__name__)

AUDIT_FILE_ENVIRONMENT_VARIABLE = "AQUAPONICS_LIFECYCLE_AUDIT_FILE"
RECORD_FORMAT_VERSION = 1


class LifecycleEvent(enum.StrEnum):
    ENGINE_STARTING = "ENGINE_STARTING"
    ENGINE_RUNNING = "ENGINE_RUNNING"
    STOP_REQUEST_RECEIVED = "STOP_REQUEST_RECEIVED"
    SHUTDOWN_INITIATED = "SHUTDOWN_INITIATED"
    SHUTDOWN_COMPLETED = "SHUTDOWN_COMPLETED"
    ENGINE_FAILURE = "ENGINE_FAILURE"


class AuditUnavailableError(Exception):
    """The audit file cannot be opened for appending."""


def resolve_audit_path(explicit: str | None) -> Path:
    """Choose the audit file: explicit argument, environment variable, then the user's XDG state directory."""
    if explicit:
        return Path(explicit)
    from_environment = os.environ.get(AUDIT_FILE_ENVIRONMENT_VARIABLE)
    if from_environment:
        return Path(from_environment)
    state_home = os.environ.get("XDG_STATE_HOME")
    base = Path(state_home) if state_home else Path.home() / ".local" / "state"
    return base / "aquaponics" / "lifecycle_audit.jsonl"


class LifecycleAudit:
    """Appends lifecycle records for one Engine execution."""

    def __init__(self, path: Path, execution_id: str, clock=None):
        self._path = path
        self._execution_id = execution_id
        self._clock = clock if clock is not None else SystemClock()
        self._descriptor: int | None = None
        self._sequence = 0
        self._failure_reported = False

    @property
    def path(self) -> Path:
        return self._path

    def open(self) -> None:
        """Open the file for appending, creating it (owner-only) and its directory if needed."""
        try:
            directory_created = not self._path.parent.exists()
            self._path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            file_created = not self._path.exists()
            self._descriptor = os.open(
                self._path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_CLOEXEC, 0o600
            )
            if file_created or directory_created:
                self._fsync_directory(self._path.parent)
        except OSError as exc:
            raise AuditUnavailableError(f"cannot open lifecycle audit file {self._path}: {exc}") from exc

    def record(
        self,
        event: LifecycleEvent,
        *,
        reason: str | None = None,
        requester: dict | None = None,
        context: dict | None = None,
    ) -> bool:
        """Append one record and make it durable. Return False if it could not be persisted.

        A failure is reported through logging and never raised: an audit problem must not
        prevent a necessary shutdown or cleanup.
        """
        self._sequence += 1
        record = {
            "format": RECORD_FORMAT_VERSION,
            "timestamp_utc": format_utc(self._clock.utc_now()),
            "execution_id": self._execution_id,
            "sequence": self._sequence,
            "event": event.value,
            "reason": reason,
            "requester": requester,
            "context": context or {},
        }
        try:
            if self._descriptor is None:
                raise OSError("lifecycle audit file is not open")
            line = json.dumps(record, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
            self._write_fully(line)
            os.fsync(self._descriptor)
        except (OSError, ValueError) as exc:
            self._report_failure(event, exc)
            return False
        return True

    def close(self) -> None:
        if self._descriptor is None:
            return
        try:
            os.close(self._descriptor)
        except OSError as exc:
            logger.error("closing lifecycle audit file %s failed: %s", self._path, exc)
        self._descriptor = None

    def _write_fully(self, data: bytes) -> None:
        assert self._descriptor is not None
        view = memoryview(data)
        while view:
            written = os.write(self._descriptor, view)
            view = view[written:]

    def _report_failure(self, event: LifecycleEvent, exc: Exception) -> None:
        # The first failure is an ERROR; repeats during one shutdown would only repeat the same news.
        level = logging.WARNING if self._failure_reported else logging.ERROR
        self._failure_reported = True
        logger.log(
            level,
            "LIFECYCLE AUDIT RECORD NOT PERSISTED (event %s, file %s): %s",
            event.value,
            self._path,
            exc,
        )

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def read_lifecycle_records(path: Path) -> list[dict]:
    """Read the history for inspection. Lines that are not complete JSON objects are skipped.

    A partly written final line can remain after an abrupt termination. This function is
    for humans and tests; the Engine never uses it to decide what to execute.
    """
    records: list[dict] = []
    with open(path, "rb") as audit_file:
        for number, line in enumerate(audit_file, start=1):
            try:
                record = json.loads(line)
            except ValueError:
                logger.warning("skipping unreadable line %d of %s", number, path)
                continue
            if isinstance(record, dict):
                records.append(record)
    return records
