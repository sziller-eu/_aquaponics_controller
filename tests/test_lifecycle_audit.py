import os
import stat
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aquaponics_controller import lifecycle_audit
from aquaponics_controller.lifecycle_audit import (
    AuditUnavailableError,
    LifecycleAudit,
    LifecycleEvent,
    read_lifecycle_records,
)


class FixedClock:
    def utc_now(self):
        return datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)


def test_record_contains_the_required_fields(short_directory):
    audit = LifecycleAudit(short_directory / "a" / "audit.jsonl", "exec-1", FixedClock())
    audit.open()

    persisted = audit.record(
        LifecycleEvent.STOP_REQUEST_RECEIVED,
        reason=None,
        requester={"pid": 7, "uid": 1000, "gid": 1000},
        context={"client_reported_submission_time_utc": "2026-11-11T11:59:59.000000Z"},
    )
    audit.close()

    assert persisted is True
    (record,) = read_lifecycle_records(short_directory / "a" / "audit.jsonl")
    assert record["timestamp_utc"] == "2026-11-11T12:00:00.000000Z"
    assert record["execution_id"] == "exec-1"
    assert record["event"] == "STOP_REQUEST_RECEIVED"
    assert record["requester"] == {"pid": 7, "uid": 1000, "gid": 1000}
    assert record["context"]["client_reported_submission_time_utc"] == "2026-11-11T11:59:59.000000Z"
    assert record["sequence"] == 1


def test_history_is_appended_across_executions_and_stays_readable(short_directory):
    path = short_directory / "audit.jsonl"
    for execution_id in ("first", "second"):
        audit = LifecycleAudit(path, execution_id)
        audit.open()
        audit.record(LifecycleEvent.ENGINE_STARTING)
        audit.record(LifecycleEvent.SHUTDOWN_COMPLETED)
        audit.close()

    records = read_lifecycle_records(path)

    assert [(r["execution_id"], r["event"]) for r in records] == [
        ("first", "ENGINE_STARTING"),
        ("first", "SHUTDOWN_COMPLETED"),
        ("second", "ENGINE_STARTING"),
        ("second", "SHUTDOWN_COMPLETED"),
    ]


def test_file_and_directory_are_owner_only(short_directory):
    previous_umask = os.umask(0)
    try:
        audit = LifecycleAudit(short_directory / "state" / "audit.jsonl", "exec")
        audit.open()
    finally:
        os.umask(previous_umask)
    audit.close()

    assert stat.S_IMODE((short_directory / "state").stat().st_mode) == 0o700
    assert stat.S_IMODE((short_directory / "state" / "audit.jsonl").stat().st_mode) == 0o600


def test_every_record_is_flushed_to_storage(short_directory, monkeypatch):
    synced = []
    real_fsync = os.fsync
    monkeypatch.setattr(lifecycle_audit.os, "fsync", lambda descriptor: (synced.append(descriptor), real_fsync(descriptor)))
    audit = LifecycleAudit(short_directory / "audit.jsonl", "exec")
    audit.open()
    synced.clear()  # ignore the directory sync done when the file is created

    audit.record(LifecycleEvent.ENGINE_STARTING)
    audit.record(LifecycleEvent.ENGINE_RUNNING)
    audit.close()

    assert len(synced) == 2


def test_failure_to_persist_is_reported_not_raised(caplog):
    audit = LifecycleAudit(Path("/dev/full"), "exec")  # every write fails with ENOSPC
    audit.open()

    with caplog.at_level("ERROR"):
        persisted = audit.record(LifecycleEvent.SHUTDOWN_COMPLETED)
    audit.close()

    assert persisted is False
    assert "NOT PERSISTED" in caplog.text


def test_recording_before_open_is_reported_not_raised(short_directory, caplog):
    audit = LifecycleAudit(short_directory / "audit.jsonl", "exec")

    with caplog.at_level("ERROR"):
        assert audit.record(LifecycleEvent.ENGINE_STARTING) is False

    assert "NOT PERSISTED" in caplog.text


def test_unopenable_file_raises_audit_unavailable(short_directory):
    with pytest.raises(AuditUnavailableError):
        LifecycleAudit(short_directory, "exec").open()  # a directory cannot be appended to


def test_reader_skips_a_partly_written_final_line(short_directory):
    path = short_directory / "audit.jsonl"
    audit = LifecycleAudit(path, "exec")
    audit.open()
    audit.record(LifecycleEvent.ENGINE_STARTING)
    audit.close()
    with open(path, "ab") as damaged:
        damaged.write(b'{"format":1,"event":"ENGINE_RUN')

    records = read_lifecycle_records(path)

    assert [r["event"] for r in records] == ["ENGINE_STARTING"]


def test_default_path_resolution(monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", "/state")
    monkeypatch.delenv(lifecycle_audit.AUDIT_FILE_ENVIRONMENT_VARIABLE, raising=False)
    assert str(lifecycle_audit.resolve_audit_path(None)) == "/state/aquaponics/lifecycle_audit.jsonl"

    monkeypatch.setenv(lifecycle_audit.AUDIT_FILE_ENVIRONMENT_VARIABLE, "/from/env.jsonl")
    assert str(lifecycle_audit.resolve_audit_path(None)) == "/from/env.jsonl"
    assert str(lifecycle_audit.resolve_audit_path("/explicit.jsonl")) == "/explicit.jsonl"
