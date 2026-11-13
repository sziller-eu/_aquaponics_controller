"""Diagnostic logging (Issue #7): fields, correlation, opt-in suppression, failure isolation, storage bounds.

Most tests work on log records in one process, with a manual clock where time matters. The
end-to-end checks start the Engine as a separate process and read what it wrote.
"""

import calendar
import errno
import gc
import logging
import os
import signal
import socket
import stat
import threading
import weakref

import pytest

from aquaponics_controller import diagnostic_logging
from aquaponics_controller import ipc_protocol as protocol
from aquaponics_controller.cycle_phases import PHASE_TIME_LIMITS_SECONDS, CyclePhase
from aquaponics_controller.diagnostic_logging import (
    BoundedRotatingFileHandler,
    DiagnosticContextFilter,
    FailureTolerantStreamHandler,
    RepeatedWarningReporter,
    build_formatter,
    configure_logging,
    log_context,
    log_handler_failure_counts,
    sanitize_summary,
)
from aquaponics_controller.engine import ExitStatus
from aquaponics_controller.lifecycle_audit import read_lifecycle_records
from aquaponics_controller.local_ingress import LocalAdministrativeIngress

from helpers import EngineProcess, run_ape
from test_cycle_phases import ManualClock, build, raise_in_phase, stop_after_cycle
from test_engine_lifecycle import ReadyIngress, ScriptedIngress, send

OWN_PEER = protocol.PeerIdentity(pid=4242, uid=os.geteuid(), gid=os.getegid())
STRANGER = protocol.PeerIdentity(pid=4343, uid=os.geteuid() + 1000, gid=os.getegid())
PING = protocol.AdministrativeRequest("ping", None, correlation_id="ping-1")
SECRET = "do-not-log-this-7f3a"


class ListHandler(logging.Handler):
    """Collects records, stamped with the diagnostic context at the moment they are logged."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []
        self.addFilter(DiagnosticContextFilter())

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def captured():
    handler = ListHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    return handler.records


@pytest.fixture
def audit_path(short_directory):
    return short_directory / "audit" / "lifecycle.jsonl"


def make_record(message="hello", *args, name="aquaponics_controller.engine", level=logging.INFO):
    return logging.LogRecord(name, level, __file__, 1, message, args, None)


def messages(records, level=None):
    return [r.getMessage() for r in records if level is None or r.levelno == level]


# ---------------------------------------------------------------- format and fields


def test_a_record_is_one_line_with_utc_milliseconds_level_subsystem_execution_and_cycle():
    record = make_record("Engine starting")
    record.created = calendar.timegm((2026, 11, 12, 10, 0, 49)) + 0.917
    record.msecs = 917.0
    log_context.execution_id = "6f1c" * 8
    log_context.cycle = 42

    DiagnosticContextFilter().filter(record)

    assert build_formatter().format(record) == (
        f"2026-11-12T10:00:49.917Z INFO engine exec={'6f1c' * 8} cycle=42 Engine starting"
    )


def test_outside_a_cycle_and_before_an_execution_the_fields_read_dash():
    record = make_record(name="other.library")
    DiagnosticContextFilter().filter(record)
    line = build_formatter().format(record)
    assert " INFO other.library exec=- cycle=- hello" in line


def test_a_record_that_never_passed_the_filter_still_formats():
    assert " INFO - exec=- cycle=- hello" in build_formatter().format(make_record())


def test_the_context_filter_never_raises_and_never_drops_a_record():
    class Unprintable:
        def __str__(self):
            raise RuntimeError("cannot render")

    log_context.cycle = Unprintable()
    assert DiagnosticContextFilter().filter(make_record()) is True


# ---------------------------------------------------------------- correlation inside the Engine


def test_records_inside_a_cycle_carry_its_number_and_records_outside_carry_dash(audit_path, captured):
    ingress = ScriptedIngress({2: send(PING, OWN_PEER)})
    engine = build(audit_path, ingress, at=stop_after_cycle(3))

    assert engine.run() is ExitStatus.SUCCESS

    by_message = {r.getMessage().split(":")[0]: r.cycle for r in captured}
    assert by_message["IPC request processed by the Engine"] == "2"
    assert [r.cycle for r in captured if r.getMessage().startswith("operating cycle")] == ["1", "2", "3"]
    assert by_message["Engine starting (execution phases, pid %d)" % os.getpid()] == "-"
    assert by_message["shutdown initiated"] == "-"
    assert by_message["Engine shutdown completed (exit status 0)"] == "-"
    assert log_context.cycle is None


def test_requests_answered_while_idling_carry_no_cycle(audit_path, captured):
    def on_service(ingress, handler):
        ingress.responses.append(handler(PING, OWN_PEER))
        return 1

    ingress = ReadyIngress(on_service=on_service)
    ingress.make_ready()
    engine = build(audit_path, ingress, interval=0.3)
    timer = threading.Timer(0.7, engine.request_stop_for_signal, [signal.SIGTERM])
    timer.start()
    try:
        engine.run()
    finally:
        timer.cancel()

    idle_answers = [r for r in captured if r.getMessage().startswith("IPC request processed")]
    assert len(idle_answers) > 3
    assert {r.cycle for r in idle_answers} == {"-"}


@pytest.mark.parametrize("failing_phase", list(CyclePhase), ids=str)
def test_a_failure_inside_a_cycle_is_logged_with_that_cycle_like_the_audit_record(audit_path, captured, failing_phase):
    engine = build(audit_path, ScriptedIngress(), at={(2, failing_phase): raise_in_phase})

    assert engine.run() is ExitStatus.FAILURE

    failure_log = next(r for r in captured if r.levelno == logging.ERROR and r.exc_info)
    audit_failure = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "ENGINE_FAILURE")
    assert failure_log.cycle == "2" == str(audit_failure["context"]["cycle_number"])
    assert f"cycle phase {failing_phase} of cycle 2" in failure_log.getMessage()
    assert "injected phase failure" in str(failure_log.exc_info[1])  # the original exception is kept
    after_failure = captured[captured.index(failure_log) + 1 :]
    assert after_failure and {r.cycle for r in after_failure} == {"-"}  # shutdown is not part of the cycle


def test_idle_cycles_produce_no_info_records_and_one_debug_record_each(audit_path, captured):
    def run_for(cycles):
        captured.clear()
        build(audit_path, ScriptedIngress(), at=stop_after_cycle(cycles)).run()
        in_cycles = [r for r in captured if r.cycle != "-"]
        assert [r.levelno for r in in_cycles] == [logging.DEBUG] * cycles  # "operating cycle N completed"
        return len([r for r in captured if r.levelno >= logging.INFO])

    assert run_for(3) == run_for(30)  # INFO output does not grow with the number of idle cycles


def test_a_signal_handler_logs_nothing(audit_path, captured):
    engine = build(audit_path, ScriptedIngress())

    engine.request_stop_for_signal(signal.SIGTERM)

    assert captured == []


# ---------------------------------------------------------------- opt-in repeat suppression


def reporter(clock, category="ipc-accept-failed:EMFILE", **options):
    return RepeatedWarningReporter(
        logging.getLogger("aquaponics_controller.test"), category, "accepting failed", clock=clock, **options
    )


def test_the_first_three_occurrences_are_logged_and_later_ones_counted_then_summarized(captured):
    clock = ManualClock()
    report = reporter(clock)

    for number in range(10):
        report.report(f"occurrence {number}")
        clock.now += 1.0

    logged = messages(captured)
    assert logged[:2] == ["accepting failed: occurrence 0", "accepting failed: occurrence 1"]
    assert logged[2] == (
        "accepting failed: occurrence 2 "
        "(category ipc-accept-failed:EMFILE: further repeats are counted for up to 300 s)"
    )
    assert len(logged) == 3
    assert report.total_suppressed == 7

    clock.now += 300.0  # the window has ended; the next occurrence first reports what was suppressed
    report.report("occurrence 10")

    assert messages(captured)[3:] == [
        "accepting failed: 7 similar warnings suppressed in 300 s (category ipc-accept-failed:EMFILE; last: occurrence 9)",
        "accepting failed: occurrence 10",
    ]


def test_flush_reports_what_was_suppressed_and_nothing_otherwise(captured):
    clock = ManualClock()
    report = reporter(clock)
    report.flush()
    assert captured == []

    for _ in range(5):
        report.report("again")
    clock.now += 12.0
    report.flush()

    assert messages(captured)[-1] == (
        "accepting failed: 2 similar warnings suppressed in 12 s (category ipc-accept-failed:EMFILE; last: again)"
    )
    report.flush()
    assert len(captured) == 4  # nothing left to report


@pytest.mark.parametrize("level", [logging.ERROR, logging.CRITICAL])
def test_errors_and_critical_records_can_never_be_suppressed(level):
    with pytest.raises(ValueError):
        reporter(ManualClock(), level=level)


@pytest.mark.parametrize("category", ["", "x" * 65, "line\nbreak"])
def test_a_category_must_be_a_short_printable_identifier(category):
    with pytest.raises(ValueError):
        reporter(ManualClock(), category=category)


def test_the_reporter_keeps_only_a_bounded_sanitized_summary_and_no_objects():
    clock = ManualClock()
    report = reporter(clock)
    for _ in range(3):
        report.report("logged")

    report.report("A" * 10_000 + "\n\x1b[31m")
    assert len(report._last_suppressed_detail) == diagnostic_logging.SUMMARY_MAX_CHARACTERS
    assert report._last_suppressed_detail.isprintable()

    class InjectedError(OSError):  # a subclass, because built-in exceptions cannot be weakly referenced
        pass

    error = InjectedError(errno.EMFILE, "Too many open files")
    error_reference = weakref.ref(error)
    report.report(error)  # not a string: only its type name is used
    del error
    gc.collect()
    assert error_reference() is None  # the exception was not kept alive
    assert report._last_suppressed_detail == "InjectedError"

    kept = vars(report).values()
    assert not [value for value in kept if isinstance(value, (BaseException, tuple, list, dict))]


def test_sanitizing_escapes_control_characters_and_bounds_the_length():
    assert sanitize_summary("a\nb\tc\x00") == "a\\nb\\tc\\x00"
    assert len(sanitize_summary("é" * 1000)) == diagnostic_logging.SUMMARY_MAX_CHARACTERS
    assert sanitize_summary(12345) == "int"


def test_different_categories_are_counted_and_summarized_separately(captured):
    clock = ManualClock()
    first, second = reporter(clock, "ipc-accept-failed:EMFILE"), reporter(clock, "ipc-accept-failed:ENFILE")

    for _ in range(5):
        first.report("process limit")
        second.report("system limit")
    first.flush()
    second.flush()

    summaries = [m for m in messages(captured) if "suppressed" in m]
    assert summaries == [
        "accepting failed: 2 similar warnings suppressed in 0 s (category ipc-accept-failed:EMFILE; last: process limit)",
        "accepting failed: 2 similar warnings suppressed in 0 s (category ipc-accept-failed:ENFILE; last: system limit)",
    ]


def test_ingress_accept_failures_use_one_category_per_errno_and_unlisted_ones_are_never_suppressed(
    short_directory, captured
):
    ingress = LocalAdministrativeIngress(short_directory / "engine.sock", clock=ManualClock())

    for _ in range(10):
        ingress._report_accept_failure(OSError(errno.EMFILE, "Too many open files"))
        ingress._report_accept_failure(OSError(errno.ENFILE, "Too many open files in system"))
        ingress._report_accept_failure(OSError(errno.ECONNABORTED, "Software caused connection abort"))
    ingress.close()

    logged = messages(captured, logging.WARNING)
    assert len([m for m in logged if "[Errno 24]" in m and "suppressed" not in m]) == 3
    assert len([m for m in logged if "[Errno 23]" in m and "suppressed" not in m]) == 3
    assert len([m for m in logged if "Software caused connection abort" in m]) == 10  # every one of them
    assert [m for m in logged if "suppressed" in m] == [
        "accepting an IPC connection failed: 7 similar warnings suppressed in 0 s "
        "(category ipc-accept-failed:EMFILE; last: [Errno 24] Too many open files)",
        "accepting an IPC connection failed: 7 similar warnings suppressed in 0 s "
        "(category ipc-accept-failed:ENFILE; last: [Errno 23] Too many open files in system)",
    ]


def test_security_relevant_refusals_are_never_suppressed(audit_path, captured):
    engine = build(audit_path, ScriptedIngress())
    unsupported = protocol.AdministrativeRequest("format_the_disk", None)

    for _ in range(100):
        engine._handle_administrative_request(PING, STRANGER)
        engine._handle_administrative_request(unsupported, OWN_PEER)

    warnings = messages(captured, logging.WARNING)
    assert len([m for m in warnings if "is not authorized" in m]) == 100
    assert len([m for m in warnings if "unsupported operation" in m]) == 100


def test_a_persistently_slow_phase_is_reported_three_times_then_summarized_at_shutdown(audit_path, captured):
    clock = ManualClock()
    limit = PHASE_TIME_LIMITS_SECONDS[CyclePhase.RECONCILE_STATE]

    def slow(engine):
        clock.now += limit + 0.5

    engine = build(audit_path, ScriptedIngress(), clock=clock, at={(n, CyclePhase.RECONCILE_STATE): slow for n in range(1, 11)})
    engine.startup()
    for _ in range(10):
        engine.run_operating_cycle()
    engine.request_stop_for_signal(signal.SIGTERM)
    engine._shutdown()

    overruns = [m for m in messages(captured, logging.WARNING) if "RECONCILE_STATE" in m]
    assert len(overruns) == 4
    assert "(cycle 1)" in overruns[0] and "(cycle 3)" in overruns[2]
    assert overruns[3].startswith(
        "operating-cycle phase RECONCILE_STATE exceeded its time limit: 7 similar warnings suppressed"
    )
    assert "category phase-overrun:RECONCILE_STATE; last: 0.600 s elapsed, limit 0.100 s (cycle 10)" in overruns[3]


# ---------------------------------------------------------------- handler failure isolation


class BrokenStream:
    def __init__(self, error):
        self.error = error

    def write(self, text):
        raise self.error

    def flush(self):
        pass


def broken_handler(error=None, name="broken"):
    handler = FailureTolerantStreamHandler(BrokenStream(error or OSError(errno.ENOSPC, "No space left on device")))
    handler.set_name(name)
    handler.setFormatter(build_formatter())
    return handler


def test_a_failing_handler_reports_once_without_record_content_and_never_raises(capfd):
    handler = broken_handler()

    for _ in range(5):
        handler.handle(make_record("echo text is %s", SECRET))

    assert handler.failure_count == 5
    error_output = capfd.readouterr().err
    assert error_output == (
        "aquaponics-engine: diagnostic log handler broken failed (OSError, errno 28); further failures are counted\n"
    )
    assert SECRET not in error_output and "Traceback" not in error_output


def test_a_formatting_error_is_reported_without_the_arguments(capfd):
    handler = FailureTolerantStreamHandler(BrokenStream(OSError()))  # never reached: formatting fails first
    handler.set_name("stderr")
    handler.handle(make_record("%d", SECRET))

    error_output = capfd.readouterr().err
    assert "diagnostic log handler stderr failed (TypeError, errno -)" in error_output
    assert SECRET not in error_output


def test_reporting_a_failure_cannot_fail_even_when_file_descriptor_2_is_unusable(monkeypatch):
    def refuse(descriptor, data):
        raise OSError(errno.EBADF, "Bad file descriptor")

    monkeypatch.setattr(diagnostic_logging.os, "write", refuse)
    handler = broken_handler()

    handler.handle(make_record())  # no exception, no recursion

    assert handler.failure_count == 1


def test_the_engine_runs_and_stops_normally_while_a_handler_fails_and_reports_the_count(audit_path, captured, capfd):
    broken = broken_handler()
    broken.addFilter(DiagnosticContextFilter())
    logging.getLogger().addHandler(broken)
    diagnostic_logging._configured_handlers[:] = [broken]
    engine = build(audit_path, ScriptedIngress({2: send(PING, OWN_PEER)}), at=stop_after_cycle(5))

    assert engine.run() is ExitStatus.SUCCESS

    assert engine.cycles_completed == 5
    assert broken.failure_count > 0
    count_reports = [m for m in messages(captured, logging.WARNING) if "could not be written" in m]
    assert len(count_reports) == 1 and "to the broken log handler" in count_reports[0]
    assert capfd.readouterr().err.count("diagnostic log handler broken failed") == 1


def test_no_failure_count_is_reported_when_nothing_failed(captured):
    healthy = FailureTolerantStreamHandler(open(os.devnull, "w"))
    diagnostic_logging._configured_handlers[:] = [healthy]
    log_handler_failure_counts()
    healthy.close()
    assert captured == []


# ---------------------------------------------------------------- the optional log file and its bounds


def test_the_approved_file_bounds():
    assert diagnostic_logging.LOG_FILE_MAX_BYTES == 5 * 1024 * 1024
    assert diagnostic_logging.LOG_FILE_BACKUP_COUNT == 4


@pytest.fixture
def small_files(monkeypatch):
    monkeypatch.setattr(diagnostic_logging, "LOG_FILE_MAX_BYTES", 2000)
    monkeypatch.setattr(diagnostic_logging, "LOG_FILE_BACKUP_COUNT", 2)


def file_handler(path):
    handler = BoundedRotatingFileHandler(path)
    handler.setFormatter(logging.Formatter("%(message)s"))
    return handler


def log_files(directory):
    return sorted(p for p in directory.iterdir() if p.name.startswith("engine.log"))


@pytest.mark.parametrize("text", ["plain ascii line", "multibyte é ü 水"])
def test_the_log_file_and_its_backups_never_exceed_the_bound(short_directory, small_files, text):
    handler = file_handler(short_directory / "engine.log")
    for number in range(500):
        handler.handle(make_record("%04d %s", number, text * 3))
    handler.close()

    files = log_files(short_directory)
    assert len(files) == 3  # current + 2 backups
    assert all(p.stat().st_size <= 2000 for p in files)  # counted in bytes, not characters
    assert (short_directory / "engine.log").read_text(encoding="utf-8").splitlines()[-1].startswith("0499 ")


def test_only_a_single_oversized_record_can_exceed_the_file_limit(short_directory, small_files):
    handler = file_handler(short_directory / "engine.log")
    handler.handle(make_record("x" * 5000))
    handler.handle(make_record("small"))
    handler.close()

    sizes = sorted(p.stat().st_size for p in log_files(short_directory))
    assert sizes == [len("small\n"), 5001]


def test_a_failed_rotation_drops_the_record_instead_of_growing_the_file(short_directory, small_files, capfd):
    handler = file_handler(short_directory / "engine.log")
    for number in range(15):
        handler.handle(make_record("%04d %s", number, "y" * 120))

    def refuse(source, destination):
        raise OSError(errno.EROFS, "Read-only file system")

    handler.rotate = refuse
    for number in range(100):
        handler.handle(make_record("%04d %s", number, "z" * 120))
    handler.close()

    assert (short_directory / "engine.log").stat().st_size <= 2000
    assert handler.failure_count > 0
    assert capfd.readouterr().err.count("diagnostic log handler") == 1


def test_the_log_file_is_owner_only(short_directory):
    path = short_directory / "logs" / "engine.log"
    handler = diagnostic_logging.open_log_file(path)
    handler.close()

    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_configured_logging_writes_formatted_records_to_the_file_with_the_execution_id(short_directory):
    path = short_directory / "logs" / "engine.log"
    configure_logging("INFO", path, "e" * 32)

    logging.getLogger("aquaponics_controller.engine").info("hello file")
    for handler in logging.getLogger().handlers:
        handler.flush()

    line = path.read_text().splitlines()[-1]
    assert line.endswith(f" INFO engine exec={'e' * 32} cycle=- hello file")


def test_an_unusable_log_file_is_reported_and_logging_continues_on_stderr(short_directory, capfd):
    not_a_directory = short_directory / "plain-file"
    not_a_directory.write_text("")

    configure_logging("INFO", not_a_directory / "engine.log", "f" * 32)
    logging.getLogger("aquaponics_controller.engine").info("still logging")

    error_output = capfd.readouterr().err
    assert "diagnostic log file" in error_output and "logging to standard error only" in error_output
    assert "still logging" in error_output
    assert [h.get_name() for h in logging.getLogger().handlers] == ["stderr"]


# ---------------------------------------------------------------- unsafe log file destinations


def assert_stderr_only_with_warning(capfd, reason):
    logging.getLogger("aquaponics_controller.engine").info("still logging")
    error_output = capfd.readouterr().err
    assert "diagnostic log file" in error_output and "logging to standard error only" in error_output
    assert reason in error_output
    assert "still logging" in error_output
    assert [h.get_name() for h in logging.getLogger().handlers] == ["stderr"]


def test_an_existing_permissive_log_file_is_restricted_to_0600_with_a_warning_and_kept(short_directory, capfd):
    path = short_directory / "engine.log"
    path.write_text("earlier line\n")
    path.chmod(0o644)

    configure_logging("INFO", path, "a" * 32)
    logging.getLogger("aquaponics_controller.engine").info("appended")
    for handler in logging.getLogger().handlers:
        handler.flush()

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    lines = path.read_text().splitlines()
    assert lines[0] == "earlier line" and lines[-1].endswith(" appended")  # appended, not replaced
    assert "had permissions 0644; restricted to 0600" in capfd.readouterr().err
    assert [h.get_name() for h in logging.getLogger().handlers] == ["stderr", "log-file"]


def test_a_permissive_log_file_that_cannot_be_restricted_is_not_used(short_directory, capfd, monkeypatch):
    path = short_directory / "engine.log"
    path.write_text("earlier line\n")
    path.chmod(0o644)

    def refuse(descriptor, mode):
        raise PermissionError(errno.EPERM, "Operation not permitted")  # for example: owned by another user

    monkeypatch.setattr(diagnostic_logging.os, "fchmod", refuse)
    configure_logging("INFO", path, "a" * 32)

    assert_stderr_only_with_warning(capfd, "cannot be restricted to 0600")
    assert stat.S_IMODE(path.stat().st_mode) == 0o644 and path.read_text() == "earlier line\n"


def test_a_log_file_path_that_is_a_symbolic_link_is_not_followed(short_directory, capfd):
    target = short_directory / "someone-elses-file"
    target.write_text("do not touch\n")
    target.chmod(0o644)
    link = short_directory / "engine.log"
    link.symlink_to(target)

    configure_logging("INFO", link, "a" * 32)

    assert_stderr_only_with_warning(capfd, "symbolic link")
    assert target.read_text() == "do not touch\n" and stat.S_IMODE(target.stat().st_mode) == 0o644
    assert link.is_symlink()


def test_a_symbolic_link_planted_during_rotation_is_not_followed(short_directory, small_files, capfd):
    target = short_directory / "someone-elses-file"
    target.write_text("do not touch\n")
    path = short_directory / "engine.log"
    handler = file_handler(path)

    def rotate_then_plant_a_link(source, destination):
        os.rename(source, destination)
        path.symlink_to(target)

    handler.rotate = rotate_then_plant_a_link
    for number in range(40):
        handler.handle(make_record("%04d %s", number, "y" * 120))
    handler.close()

    assert target.read_text() == "do not touch\n"
    assert handler.failure_count > 0  # records are dropped and counted, nothing raised
    assert capfd.readouterr().err.count("diagnostic log handler") == 1


@pytest.mark.parametrize("mode", [0o600, 0o644])
def test_a_hard_linked_log_file_is_refused_and_neither_name_changes(short_directory, capfd, mode):
    other_name = short_directory / "someone-elses-file"
    other_name.write_text("do not touch\n")
    other_name.chmod(mode)  # 0644: refused before its permissions would have been restricted
    path = short_directory / "engine.log"
    os.link(other_name, path)

    configure_logging("INFO", path, "a" * 32)

    assert_stderr_only_with_warning(capfd, "hard links")
    for name in (path, other_name):
        assert name.read_text() == "do not touch\n"
        assert stat.S_IMODE(name.stat().st_mode) == mode
    assert path.stat().st_nlink == 2 and os.path.samefile(path, other_name)


def test_a_hard_link_planted_during_rotation_is_refused(short_directory, small_files, capfd):
    other_name = short_directory / "someone-elses-file"
    other_name.write_text("do not touch\n")
    path = short_directory / "engine.log"
    handler = file_handler(path)

    def rotate_then_plant_a_hard_link(source, destination):
        os.rename(source, destination)
        os.link(other_name, path)

    handler.rotate = rotate_then_plant_a_hard_link
    for number in range(40):
        handler.handle(make_record("%04d %s", number, "y" * 120))
    handler.close()

    assert other_name.read_text() == "do not touch\n" and path.read_text() == "do not touch\n"
    assert handler.failure_count > 0
    assert capfd.readouterr().err.count("diagnostic log handler") == 1


def test_a_named_pipe_without_a_reader_is_refused_without_blocking(short_directory, capfd):
    pipe = short_directory / "engine.log"
    os.mkfifo(pipe, 0o600)

    worker = threading.Thread(target=configure_logging, args=("INFO", pipe, "a" * 32), daemon=True)
    worker.start()
    worker.join(timeout=5)

    assert not worker.is_alive(), "opening the log file blocked"
    assert_stderr_only_with_warning(capfd, "not a regular file")


def test_a_named_pipe_with_a_reader_is_refused_and_receives_nothing(short_directory, capfd):
    pipe = short_directory / "engine.log"
    os.mkfifo(pipe, 0o600)
    reader = os.open(pipe, os.O_RDONLY | os.O_NONBLOCK)
    try:
        configure_logging("INFO", pipe, "a" * 32)
        assert_stderr_only_with_warning(capfd, "not a regular file")
        assert os.read(reader, 100) == b""  # nothing was written; the refused writer is already closed
    finally:
        os.close(reader)


def test_a_device_file_is_refused(capfd):
    configure_logging("INFO", "/dev/null", "a" * 32)
    assert_stderr_only_with_warning(capfd, "not a regular file")


def test_a_directory_as_the_log_file_is_refused(short_directory, capfd):
    configure_logging("INFO", short_directory, "a" * 32)
    assert_stderr_only_with_warning(capfd, "logging to standard error only")


def test_an_existing_directory_keeps_its_permissions(short_directory):
    directory = short_directory / "shared"
    directory.mkdir()
    directory.chmod(0o755)
    neighbour = directory / "unrelated.txt"
    neighbour.write_text("unrelated\n")
    parent_mode = stat.S_IMODE(short_directory.stat().st_mode)

    handler = diagnostic_logging.open_log_file(directory / "engine.log")
    handler.close()

    assert stat.S_IMODE(directory.stat().st_mode) == 0o755
    assert stat.S_IMODE(short_directory.stat().st_mode) == parent_mode
    assert neighbour.read_text() == "unrelated\n"
    assert stat.S_IMODE((directory / "engine.log").stat().st_mode) == 0o600


# ---------------------------------------------------------------- end to end, as separate processes


def diagnostic_lines(text):
    return [line for line in text.splitlines() if " exec=" in line and line[:4].isdigit()]


def test_every_record_of_an_execution_carries_the_audit_execution_id_and_requests_can_be_traced(short_directory):
    log_file = short_directory / "logs" / "engine.log"
    engine = EngineProcess(short_directory, cycle_interval=0.2, extra_args=("--log-file", str(log_file)))
    try:
        engine.wait_until_running()
        assert run_ape("--echo", SECRET, "--socket", str(engine.endpoint)).returncode == 0
        assert run_ape("-x", "--socket", str(engine.endpoint)).returncode == 0
        assert engine.wait() == 0
        execution_id = engine.execution_id()

        for text in (engine.log_text(), log_file.read_text()):
            lines = diagnostic_lines(text)
            assert lines and all(f" exec={execution_id} " in line for line in lines)
            assert SECRET not in text  # the echo text is never logged

            received = next(line for line in lines if "IPC request received: op='echo'" in line)
            correlation = received.split(" id=")[1].split(" ")[0]
            assert any(f"IPC request processed by the Engine: op='echo' id={correlation}" in line for line in lines)
            assert any(f"IPC request answered: id={correlation} ok=True" in line for line in lines)
    finally:
        engine.close()


def test_an_engine_whose_log_file_cannot_be_opened_still_starts_and_stops(short_directory):
    blocker = short_directory / "plain-file"
    blocker.write_text("")
    engine = EngineProcess(short_directory, cycle_interval=0.2, extra_args=("--log-file", str(blocker / "x.log")))
    try:
        engine.wait_until_running()
        assert run_ape("-x", "--socket", str(engine.endpoint)).returncode == 0
        assert engine.wait() == 0
        assert "logging to standard error only" in engine.log_text()
    finally:
        engine.close()


@pytest.mark.parametrize("destination", ["named-pipe", "symbolic-link"])
def test_an_engine_given_an_unsafe_log_file_starts_without_it_and_stops(short_directory, destination):
    path = short_directory / "diagnostic.log"  # not engine.log: that is where EngineProcess captures standard error
    target = short_directory / "target"
    if destination == "named-pipe":
        os.mkfifo(path, 0o600)
    else:
        target.write_text("do not touch\n")
        path.symlink_to(target)
    engine = EngineProcess(short_directory, cycle_interval=0.2, extra_args=("--log-file", str(path)))
    try:
        engine.wait_until_running()
        assert run_ape("-x", "--socket", str(engine.endpoint)).returncode == 0
        assert engine.wait() == 0
        assert "logging to standard error only" in engine.log_text()
        if destination == "symbolic-link":
            assert target.read_text() == "do not touch\n"
    finally:
        engine.close()


def test_a_client_disconnect_storm_does_not_hide_individual_client_events(short_directory):
    engine = EngineProcess(short_directory, cycle_interval=0.2)
    try:
        engine.wait_until_running()
        for _ in range(5):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.connect(str(engine.endpoint))
        assert run_ape("-x", "--socket", str(engine.endpoint)).returncode == 0
        assert engine.wait() == 0
        assert engine.log_text().count("disconnected before completing its request") == 5
    finally:
        engine.close()
