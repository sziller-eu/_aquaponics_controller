"""End-to-end tests with the Engine and the ``ape`` client as separate operating-system processes.

They need no privileges, no hardware and no network. Every wait has a timeout and
every Engine process is killed at teardown, so a defect cannot hang the suite.

Not covered here (needs a second Linux account, see the Development and Operations
documentation): a real permission denial for a different user.
"""

import os
import signal
import socket
import stat
import subprocess
import sys
import textwrap
import time

import pytest

from aquaponics_controller import cli

from helpers import (
    EngineProcess,
    child_environment,
    events_of,
    parse_response,
    raw_request,
    read_audit,
    run_ape,
)


@pytest.fixture
def engines(short_directory):
    started = []

    def start(**options):
        options.setdefault("name", f"engine{len(started)}")
        engine = EngineProcess(short_directory, **options)
        started.append(engine)
        return engine

    yield start
    for engine in started:
        engine.close()


@pytest.fixture
def running_engine(engines):
    engine = engines(cycle_interval=0.1)
    engine.wait_until_running()
    return engine


# ---------------------------------------------------------------- operator STOP


def test_ape_stop_from_a_separate_process_shuts_the_engine_down_cleanly(running_engine):
    result = run_ape("-x", "--socket", str(running_engine.endpoint))

    assert result.returncode == cli.EXIT_OK
    assert "initiated" in result.stdout
    assert "does not confirm" in result.stdout  # initiation is not confirmation of exit
    assert running_engine.wait() == 0
    assert not running_engine.endpoint.exists()
    records = read_audit(running_engine.audit_file)
    assert events_of(records) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]
    received = next(r for r in records if r["event"] == "STOP_REQUEST_RECEIVED")
    assert received["requester"]["uid"] == os.geteuid()
    assert received["context"]["client_reported_submission_time_utc"] <= received["context"]["engine_receipt_time_utc"]
    log = running_engine.log_text()
    assert "operator-requested STOP" in log and "shutdown completed" in log


def test_endpoint_and_audit_file_permissions_are_restrictive(running_engine):
    assert stat.S_IMODE(running_engine.endpoint.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(running_engine.endpoint.stat().st_mode) == 0o600
    assert stat.S_IMODE(running_engine.audit_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(running_engine.audit_file.parent.stat().st_mode) == 0o700


def test_the_history_survives_a_restart_and_the_old_stop_is_not_replayed(engines, running_engine):
    assert run_ape("-x", "--socket", str(running_engine.endpoint)).returncode == 0
    assert running_engine.wait() == 0
    first_id = running_engine.execution_id()

    restarted = engines(cycle_interval=0.1, name="restarted")
    restarted.wait_until_running()
    time.sleep(0.6)  # many cycles: a replayed STOP would already have ended it

    assert restarted.process.poll() is None
    records = read_audit(restarted.audit_file)
    assert events_of(records, first_id)[-1] == "SHUTDOWN_COMPLETED"  # the earlier history is still readable
    second_id = restarted.execution_id()
    assert second_id != first_id
    assert events_of(records, second_id) == ["ENGINE_STARTING", "ENGINE_RUNNING"]

    assert run_ape("-x", "--socket", str(restarted.endpoint)).returncode == 0
    assert restarted.wait() == 0


# ---------------------------------------------------------------- signals


@pytest.mark.parametrize("signal_number", [signal.SIGINT, signal.SIGTERM])
def test_signals_shut_the_engine_down_promptly_even_during_a_long_idle_wait(engines, signal_number):
    engine = engines(cycle_interval=30)  # the idle wait is 30 s long, but interruptible
    engine.wait_until_running()

    started = time.monotonic()
    engine.send_signal(signal_number)

    assert engine.wait(timeout=5) == 0
    assert time.monotonic() - started < 3
    assert not engine.endpoint.exists()
    initiated = next(r for r in read_audit(engine.audit_file) if r["event"] == "SHUTDOWN_INITIATED")
    assert initiated["reason"] == "SIGNAL"
    assert initiated["context"]["signal"] == signal_number.name
    assert "external interruption" in engine.log_text()


def test_unexpected_exception_gives_nonzero_status_traceback_and_cleanup(short_directory):
    script = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from aquaponics_controller.engine import Engine, EngineConfig
        from aquaponics_controller.engine_main import configure_logging, run_with_signal_handling
        from aquaponics_controller.lifecycle_audit import LifecycleAudit
        from aquaponics_controller.local_ingress import LocalAdministrativeIngress

        class FailingEngine(Engine):
            def run_operating_cycle(self):
                raise RuntimeError("injected failure")

        directory = Path(sys.argv[1])
        configure_logging("INFO")
        engine = FailingEngine(
            execution_id="failing",
            config=EngineConfig(cycle_interval_seconds=0.05),
            ingress=LocalAdministrativeIngress(directory / "ipc" / "engine.sock"),
            audit=LifecycleAudit(directory / "audit.jsonl", "failing"),
        )
        sys.exit(int(run_with_signal_handling(engine)))
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script, str(short_directory)],
        env=child_environment(),
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert result.returncode == 1
    assert "Traceback" in result.stderr and "injected failure" in result.stderr  # not silently suppressed
    assert not (short_directory / "ipc" / "engine.sock").exists()  # cleanup was attempted and worked
    assert events_of(read_audit(short_directory / "audit.jsonl"))[-3:] == [
        "ENGINE_FAILURE",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]


# ---------------------------------------------------------------- startup conflicts and abrupt termination


def test_a_second_engine_on_the_same_endpoint_is_rejected_and_the_first_keeps_running(engines, running_engine):
    second = engines(cycle_interval=0.1, name="second")

    assert second.wait() == 3
    assert "already using" in second.log_text()
    assert running_engine.process.poll() is None
    assert running_engine.endpoint.exists()
    assert run_ape("-x", "--socket", str(running_engine.endpoint)).returncode == 0  # still the live owner
    assert running_engine.wait() == 0


def test_after_sigkill_the_stale_endpoint_is_replaced_and_no_final_event_is_claimed(engines, running_engine):
    running_engine.send_signal(signal.SIGKILL)
    running_engine.wait()
    assert running_engine.endpoint.exists()  # left behind: no cleanup is possible after SIGKILL
    killed_id = running_engine.execution_id()
    assert events_of(read_audit(running_engine.audit_file), killed_id) == ["ENGINE_STARTING", "ENGINE_RUNNING"]

    successor = engines(cycle_interval=0.1, name="successor")
    successor.wait_until_running()

    assert run_ape("-x", "--socket", str(successor.endpoint)).returncode == 0
    assert successor.wait() == 0


def test_idle_engine_uses_little_cpu(engines):
    engine = engines(cycle_interval=1.0)
    engine.wait_until_running()
    before = engine.cpu_seconds()

    time.sleep(3)

    assert engine.cpu_seconds() - before < 0.1


# ---------------------------------------------------------------- malformed requests against a live Engine


@pytest.mark.parametrize("payload", [b"garbage\n", b"\x00\xff\xfe\n", b'{"v": 1}\n', b"x" * 10000, b""])
def test_malformed_requests_do_not_crash_the_engine(running_engine, payload):
    answer = raw_request(running_engine.endpoint, payload)

    if answer:
        assert parse_response(answer)["ok"] is False
    time.sleep(0.3)
    assert running_engine.process.poll() is None
    assert run_ape("-x", "--socket", str(running_engine.endpoint)).returncode == 0  # still serving
    assert running_engine.wait() == 0


def test_an_unsupported_operation_is_refused_and_the_engine_runs_on(running_engine):
    answer = parse_response(raw_request(running_engine.endpoint, b'{"v": 1, "op": "reboot"}\n'))

    assert answer["ok"] is False and answer["error"] == "unsupported_operation"
    assert running_engine.process.poll() is None


# ---------------------------------------------------------------- the client: failures are bounded and reported


def test_ape_reports_a_missing_engine_quickly(short_directory):
    started = time.monotonic()

    result = run_ape("-x", "--socket", str(short_directory / "nothing" / "engine.sock"))

    assert result.returncode == cli.EXIT_UNREACHABLE
    assert "no Engine is listening" in result.stderr
    assert time.monotonic() - started < 5


def test_ape_reports_a_stale_endpoint_as_unreachable(short_directory):
    stale = short_directory / "stale.sock"
    leftover = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    leftover.bind(str(stale))
    leftover.close()

    assert run_ape("-x", "--socket", str(stale)).returncode == cli.EXIT_UNREACHABLE


def test_ape_times_out_against_an_engine_that_never_answers(short_directory):
    silent = short_directory / "silent.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(silent))
    server.listen(1)  # accepts connections in the kernel, never answers
    try:
        started = time.monotonic()
        result = run_ape("-x", "--socket", str(silent), "--timeout", "0.5")
    finally:
        server.close()

    assert result.returncode == cli.EXIT_TIMEOUT
    assert time.monotonic() - started < 5


def test_ape_reports_an_unintelligible_answer(short_directory):
    path = short_directory / "odd.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    server.listen(1)
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "aquaponics_controller.cli", "-x", "--socket", str(path), "--timeout", "3"],
            env=child_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        connection, _ = server.accept()
        connection.recv(4096)
        connection.sendall(b"this is not the protocol\n")
        connection.close()
        _, stderr = process.communicate(timeout=10)
    finally:
        server.close()

    assert process.returncode == cli.EXIT_COMMUNICATION_FAILURE
    assert "unintelligible" in stderr


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permission checks")
def test_ape_reports_permission_denied_when_the_endpoint_is_not_accessible(running_engine):
    running_engine.endpoint.chmod(0o000)  # as if the socket belonged to another user's restricted group
    try:
        result = run_ape("-x", "--socket", str(running_engine.endpoint))
    finally:
        running_engine.endpoint.chmod(0o600)

    assert result.returncode == cli.EXIT_PERMISSION_DENIED
    assert "permission denied" in result.stderr
    assert running_engine.process.poll() is None  # the denied request had no effect


def test_ape_requires_an_operation(short_directory):
    result = run_ape("--socket", str(short_directory / "x.sock"))

    assert result.returncode == cli.EXIT_USAGE
