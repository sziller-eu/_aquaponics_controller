"""Issue #9: notifications to the systemd service manager (READY, WATCHDOG, STOPPING).

A fake service manager (a Unix datagram socket the test owns) receives what the Engine
sends, so the contract is checked without systemd: activation from the environment,
ordering and uniqueness, the heartbeat rate, the heartbeat only after completed
operating cycles, the readiness failure path, the watchdog interval rule, and that a
notification never blocks or raises where it must not. Nothing here touches hardware,
the network or a real service manager.
"""

import errno
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

import pytest

from aquaponics_controller import ipc_protocol as protocol
from aquaponics_controller.engine import Engine, EngineConfig, ExitStatus, LifecycleState
from aquaponics_controller.lifecycle_audit import LifecycleAudit, read_lifecycle_records
from aquaponics_controller.service_notify import (
    WORST_CASE_CYCLE_SECONDS,
    ServiceManagerSettings,
    ServiceNotifier,
    check_cycle_interval_fits_watchdog,
)

from helpers import PROCESS_TIMEOUT_SECONDS, EngineProcess, child_environment, events_of, run_ape
from test_cycle_phases import ManualClock
from test_engine_lifecycle import (  # noqa: F401 - audit_path is a fixture
    OWN_PEER,
    STOP,
    ReadyIngress,
    ScriptedIngress,
    audit_path,
    raise_error,
    send,
    start_watchdog,
)

PING = protocol.AdministrativeRequest("ping", None)


class FakeServiceManager:
    """The receiving end of NOTIFY_SOCKET: collects every datagram sent to it."""

    def __init__(self, address):
        self.address = address  # str path, or "@name" for an abstract socket
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.socket.bind(b"\0" + address[1:].encode() if address.startswith("@") else address)
        self.socket.setblocking(False)
        self.received: list[bytes] = []

    def collect(self) -> list[bytes]:
        while True:
            try:
                self.received.append(self.socket.recv(4096))
            except BlockingIOError:
                return list(self.received)

    def close(self):
        self.socket.close()


@pytest.fixture
def manager(short_directory):
    fake = FakeServiceManager(str(short_directory / "notify.sock"))
    yield fake
    fake.close()


def notifier_for(manager, *, watchdog_seconds=None, clock=None):
    return ServiceNotifier(ServiceManagerSettings(manager.address, watchdog_seconds), clock=clock)


def build_engine(audit_path, ingress, notifier, *, interval=0.01):
    return Engine(
        execution_id="notify-test",
        config=EngineConfig(cycle_interval_seconds=interval),
        ingress=ingress,
        audit=LifecycleAudit(audit_path, "notify-test"),
        service_notifier=notifier,
    )


# ================================================================ activation from the environment


def test_without_notify_socket_nothing_is_active_whatever_the_watchdog_variables_say():
    settings = ServiceManagerSettings.from_environment({"WATCHDOG_USEC": "10000000", "WATCHDOG_PID": "1"}, pid=1)

    assert settings == ServiceManagerSettings()
    assert not ServiceNotifier(settings).active


def test_notify_socket_alone_activates_readiness_but_not_the_watchdog():
    settings = ServiceManagerSettings.from_environment({"NOTIFY_SOCKET": "/run/systemd/notify"}, pid=7)

    assert settings == ServiceManagerSettings("/run/systemd/notify", None)


@pytest.mark.parametrize("pid_variable", [{}, {"WATCHDOG_PID": "7"}])
def test_the_watchdog_is_active_with_its_timeout_when_meant_for_this_process(pid_variable):
    environment = {"NOTIFY_SOCKET": "@/org/test/notify", "WATCHDOG_USEC": "10000000", **pid_variable}

    settings = ServiceManagerSettings.from_environment(environment, pid=7)

    assert settings == ServiceManagerSettings("@/org/test/notify", 10.0)


def test_a_watchdog_meant_for_another_process_disables_only_the_watchdog():
    environment = {"NOTIFY_SOCKET": "/run/systemd/notify", "WATCHDOG_USEC": "10000000", "WATCHDOG_PID": "8"}

    settings = ServiceManagerSettings.from_environment(environment, pid=7)

    assert settings == ServiceManagerSettings("/run/systemd/notify", None)  # READY and STOPPING still apply
    assert ServiceNotifier(settings).active


@pytest.mark.parametrize(
    "environment",
    [
        {"NOTIFY_SOCKET": "relative/notify"},
        {"NOTIFY_SOCKET": "@"},
        {"NOTIFY_SOCKET": "/run/notify", "WATCHDOG_USEC": "ten seconds"},
        {"NOTIFY_SOCKET": "/run/notify", "WATCHDOG_USEC": "0"},
        {"NOTIFY_SOCKET": "/run/notify", "WATCHDOG_USEC": "-5"},
        {"NOTIFY_SOCKET": "/run/notify", "WATCHDOG_USEC": "10000000", "WATCHDOG_PID": "self"},
    ],
)
def test_a_malformed_variable_is_a_configuration_error(environment):
    with pytest.raises(ValueError):
        ServiceManagerSettings.from_environment(environment, pid=7)


# ================================================================ the watchdog interval rule


def test_the_worst_case_cycle_is_the_sum_of_the_phase_budgets():
    assert WORST_CASE_CYCLE_SECONDS == pytest.approx(1.5)


@pytest.mark.parametrize("interval", [0.01, 1.0, 3.5])
def test_a_cycle_interval_within_the_rule_is_accepted(interval):
    check_cycle_interval_fits_watchdog(interval, 10.0)


@pytest.mark.parametrize("interval, watchdog", [(3.6, 10.0), (1.0, 4.9), (60.0, 100.0)])
def test_a_cycle_interval_beyond_half_the_watchdog_is_refused(interval, watchdog):
    with pytest.raises(ValueError, match="too long"):
        check_cycle_interval_fits_watchdog(interval, watchdog)


def test_without_a_watchdog_any_valid_interval_is_accepted():
    check_cycle_interval_fits_watchdog(60.0, None)


# ================================================================ the notifier on its own


def test_standalone_every_notification_is_a_no_op_and_no_socket_is_created():
    notifier = ServiceNotifier()

    notifier.open()
    notifier.notify_ready()
    notifier.notify_watchdog()
    notifier.notify_stopping()
    notifier.close()

    assert notifier._socket is None


def test_ready_and_stopping_are_sent_once_each(manager):
    notifier = notifier_for(manager)
    notifier.open()

    notifier.notify_ready()
    notifier.notify_ready()
    notifier.notify_stopping()
    notifier.notify_stopping()
    notifier.close()

    assert manager.collect() == [b"READY=1", b"STOPPING=1"]


def test_an_abstract_notify_socket_is_supported():
    fake = FakeServiceManager(f"@aquaponics-test-{uuid.uuid4().hex}")
    try:
        notifier = notifier_for(fake)
        notifier.open()
        notifier.notify_ready()
        notifier.close()
        assert fake.collect() == [b"READY=1"]
    finally:
        fake.close()


def test_heartbeats_are_sent_only_between_ready_and_stopping(manager):
    clock = ManualClock()
    notifier = notifier_for(manager, watchdog_seconds=10.0, clock=clock)
    notifier.open()

    notifier.notify_watchdog()  # before READY: nothing
    notifier.notify_ready()
    notifier.notify_watchdog()
    clock.now += 5
    notifier.notify_stopping()
    notifier.notify_watchdog()  # after STOPPING: nothing
    notifier.close()

    assert manager.collect() == [b"READY=1", b"WATCHDOG=1", b"STOPPING=1"]


def test_heartbeats_are_rate_limited_to_a_quarter_of_the_actual_watchdog_timeout(manager):
    clock = ManualClock()
    notifier = notifier_for(manager, watchdog_seconds=8.0, clock=clock)  # quarter: 2 s
    notifier.open()
    notifier.notify_ready()

    sent_at = []
    for _ in range(21):  # one completed cycle every 0.5 s, for 10 s
        before = len(manager.collect())
        notifier.notify_watchdog()
        if len(manager.collect()) > before:
            sent_at.append(clock.monotonic())
        clock.now += 0.5
    notifier.close()

    assert notifier.heartbeat_interval_seconds == 2.0
    assert len(sent_at) == 6
    assert all(later - earlier >= 2.0 for earlier, later in zip(sent_at, sent_at[1:]))


def test_without_an_active_watchdog_no_heartbeat_is_ever_sent(manager):
    notifier = notifier_for(manager, watchdog_seconds=None)
    notifier.open()
    notifier.notify_ready()
    for _ in range(5):
        notifier.notify_watchdog()
    notifier.close()

    assert manager.collect() == [b"READY=1"]


def test_ready_raises_when_the_service_manager_cannot_be_reached(short_directory):
    notifier = ServiceNotifier(ServiceManagerSettings(str(short_directory / "missing.sock"), 10.0))
    notifier.open()

    with pytest.raises(OSError):
        notifier.notify_ready()
    notifier.close()


def test_failed_heartbeats_and_a_failed_stopping_never_raise_and_are_reported(manager, caplog):
    clock = ManualClock()
    notifier = notifier_for(manager, watchdog_seconds=1.0, clock=clock)
    notifier.open()
    notifier.notify_ready()
    manager.close()  # the service manager has gone away
    os.unlink(manager.address)

    with caplog.at_level(logging.WARNING):
        for _ in range(10):
            clock.now += 1
            notifier.notify_watchdog()
        notifier.notify_stopping()
        notifier.close()

    heartbeat_warnings = [r.getMessage() for r in caplog.records if "watchdog heartbeat" in r.getMessage()]
    assert len([m for m in heartbeat_warnings if "suppressed" not in m]) == 3  # rate-limited category
    assert any("7 similar warnings suppressed" in m and "service-notify-failed" in m for m in heartbeat_warnings)
    assert any("reporting the shutdown to systemd failed" in r.getMessage() for r in caplog.records)


def test_a_full_service_manager_queue_never_blocks_a_notification(manager):
    notifier = notifier_for(manager, watchdog_seconds=0.004)
    notifier.open()
    notifier.notify_ready()
    filler = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    filler.setblocking(False)
    try:
        while True:
            filler.sendto(b"x" * 1024, manager.address)
    except BlockingIOError:
        pass  # the receiving queue is full
    finally:
        filler.close()

    started = time.monotonic()
    for _ in range(20):
        time.sleep(0.002)
        notifier.notify_watchdog()  # EAGAIN: reported, never raised, never waited for
    notifier.notify_stopping()
    notifier.close()

    assert time.monotonic() - started < 1.0


def test_the_notification_socket_is_non_blocking_and_closed_on_exec(manager):
    notifier = notifier_for(manager)
    notifier.open()
    try:
        assert notifier._socket.getblocking() is False
        assert notifier._socket.get_inheritable() is False
    finally:
        notifier.close()
    assert notifier._socket is None


# ================================================================ the Engine's use of the notifier


def test_the_engine_sends_ready_then_one_heartbeat_per_completed_cycle_then_stopping(audit_path, manager):
    engine = build_engine(audit_path, ScriptedIngress({4: send(STOP, OWN_PEER)}), notifier_for(manager, watchdog_seconds=1e-6))

    assert engine.run() is ExitStatus.SUCCESS

    assert engine.cycles_completed == 4
    assert manager.collect() == [b"READY=1"] + [b"WATCHDOG=1"] * 4 + [b"STOPPING=1"]


class RecordingNotifier(ServiceNotifier):
    """Records when the Engine calls each notification, and what the Engine's state was at that moment."""

    def __init__(self, engine_ref, audit_path, *, ready_error=None):
        super().__init__()
        self.engine_ref = engine_ref
        self.audit_path = audit_path
        self.ready_error = ready_error
        self.calls = []

    def _snapshot(self, name):
        engine = self.engine_ref[0]
        self.calls.append(
            {
                "call": name,
                "state": engine.state,
                "cycles_completed": engine.cycles_completed,
                "endpoint_open": engine._ingress.opened > engine._ingress.closed,
                "audit": events_of(read_lifecycle_records(self.audit_path)) if self.audit_path.exists() else [],
            }
        )

    def open(self):
        self._snapshot("open")

    def notify_ready(self):
        self._snapshot("ready")
        if self.ready_error:
            raise self.ready_error

    def notify_watchdog(self):
        self._snapshot("watchdog")

    def notify_stopping(self):
        self._snapshot("stopping")

    def close(self):
        self._snapshot("close")


def build_recorded(audit_path, ingress, *, interval=0.01, **options):
    engine_ref = []
    notifier = RecordingNotifier(engine_ref, audit_path, **options)
    engine = build_engine(audit_path, ingress, notifier, interval=interval)
    engine_ref.append(engine)
    return engine, notifier


def test_ready_comes_after_engine_running_is_recorded_and_the_endpoint_listens(audit_path):
    ingress = ScriptedIngress({1: send(STOP, OWN_PEER)})
    engine, notifier = build_recorded(audit_path, ingress)

    engine.run()

    ready = next(c for c in notifier.calls if c["call"] == "ready")
    assert ready["audit"] == ["ENGINE_STARTING", "ENGINE_RUNNING"]
    assert ready["state"] is LifecycleState.STARTING  # RUNNING only once READY has been sent
    assert ready["endpoint_open"]
    assert [c["call"] for c in notifier.calls] == ["open", "ready", "watchdog", "stopping", "close"]


def test_a_heartbeat_follows_every_completed_cycle_and_only_that(audit_path):
    # Requests are also answered while idling (ReadyIngress); that servicing must not send a heartbeat.
    def answer_one(ingress, handler):
        ingress.consume_readiness()
        handler(STOP if len(ingress.idle_calls()) >= 5 else PING, OWN_PEER)
        return 1

    ingress = ReadyIngress(on_service=answer_one)
    engine, notifier = build_recorded(audit_path, ingress, interval=0.2)

    def poke():
        for _ in range(5):
            time.sleep(0.05)
            ingress.make_ready()

    poker = threading.Thread(target=poke)
    start_watchdog(engine)
    poker.start()
    assert engine.run() is ExitStatus.SUCCESS
    poker.join()

    heartbeats = [c for c in notifier.calls if c["call"] == "watchdog"]
    assert len(ingress.idle_calls()) >= 1  # idle-time servicing did happen
    assert [c["cycles_completed"] for c in heartbeats] == list(range(1, engine.cycles_completed + 1))
    assert all(c["state"] is LifecycleState.RUNNING for c in heartbeats)


def test_no_heartbeat_follows_a_failed_cycle(audit_path, manager):
    ingress = ScriptedIngress({3: raise_error(RuntimeError("defect in cycle 3"))})
    engine = build_engine(audit_path, ingress, notifier_for(manager, watchdog_seconds=1e-6))

    assert engine.run() is ExitStatus.FAILURE

    assert engine.cycles_completed == 2
    assert manager.collect() == [b"READY=1", b"WATCHDOG=1", b"WATCHDOG=1", b"STOPPING=1"]


def test_no_heartbeat_during_the_final_servicing_pass_of_a_shutdown(audit_path):
    served_in_final_pass = []
    ingress = ScriptedIngress(
        {2: send(STOP, OWN_PEER)},
        final_pass=lambda handler, responses: served_in_final_pass.append(handler(STOP, OWN_PEER)),
    )
    engine, notifier = build_recorded(audit_path, ingress)

    assert engine.run() is ExitStatus.SUCCESS

    assert served_in_final_pass and served_in_final_pass[0].status == "shutdown_already_initiated"
    calls = [c["call"] for c in notifier.calls]
    assert calls == ["open", "ready", "watchdog", "watchdog", "stopping", "close"]


def test_stopping_is_sent_once_for_every_shutdown_reason(audit_path, manager):
    cases = [
        ScriptedIngress({1: send(STOP, OWN_PEER)}),
        ScriptedIngress({1: raise_error(RuntimeError("defect"))}),
    ]
    for ingress in cases:
        fake = FakeServiceManager(f"@aquaponics-test-{uuid.uuid4().hex}")
        try:
            engine = build_engine(audit_path, ingress, notifier_for(fake))
            engine.run()
            assert fake.collect().count(b"STOPPING=1") == 1
        finally:
            fake.close()

    # A handled signal before the first cycle completes: READY, then STOPPING, and no heartbeat.
    engine = build_engine(audit_path, ScriptedIngress(), notifier_for(manager, watchdog_seconds=1e-6))
    engine.request_stop_for_signal(signal.SIGTERM)
    assert engine.run() is ExitStatus.SUCCESS
    assert engine.cycles_completed == 0
    assert manager.collect() == [b"READY=1", b"STOPPING=1"]


def test_a_failed_readiness_notification_is_a_startup_failure_and_cleanup_runs_once(audit_path, caplog):
    ingress = ScriptedIngress()
    engine, notifier = build_recorded(audit_path, ingress, ready_error=ConnectionRefusedError(errno.ECONNREFUSED, "refused"))

    with caplog.at_level(logging.ERROR):
        status = engine.run()

    assert status is ExitStatus.FAILURE
    assert engine.cycles_completed == 0  # never entered continuous operation
    assert ingress.closed == 1
    records = read_lifecycle_records(audit_path)
    assert events_of(records) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "ENGINE_FAILURE",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]
    failure = next(r for r in records if r["event"] == "ENGINE_FAILURE")
    assert failure["reason"] == "ConnectionRefusedError"
    assert failure["context"]["phase"] == "startup"
    assert [c["call"] for c in notifier.calls] == ["open", "ready", "stopping", "close"]
    assert len([r for r in caplog.records if "Engine failure during startup" in r.getMessage()]) == 1


def test_a_failed_readiness_with_the_real_notifier_ends_with_exit_status_1(audit_path, short_directory):
    unreachable = ServiceNotifier(ServiceManagerSettings(str(short_directory / "missing.sock"), 10.0))
    engine = build_engine(audit_path, ScriptedIngress(), unreachable)

    assert engine.run() is ExitStatus.FAILURE
    failure = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "ENGINE_FAILURE")
    assert failure["context"]["phase"] == "startup"


# ================================================================ the Engine process (entry point)


def run_engine_main(short_directory, environment_update, *arguments, timeout=PROCESS_TIMEOUT_SECONDS):
    environment = child_environment()
    environment.update(environment_update)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "aquaponics_controller.engine_main",
            "--socket",
            str(short_directory / "ipc" / "engine.sock"),
            "--audit-file",
            str(short_directory / "audit" / "lifecycle.jsonl"),
            *arguments,
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_a_cycle_interval_too_long_for_the_watchdog_is_a_configuration_error_exit_2(short_directory, manager):
    result = run_engine_main(
        short_directory,
        {"NOTIFY_SOCKET": manager.address, "WATCHDOG_USEC": "10000000"},
        "--cycle-interval",
        "3.6",
    )

    assert result.returncode == 2
    assert "too long for the 10 s service watchdog" in result.stderr
    assert not (short_directory / "audit" / "lifecycle.jsonl").exists()  # the Engine was never started
    assert manager.collect() == []


def test_a_malformed_notify_socket_is_a_configuration_error_exit_2(short_directory):
    result = run_engine_main(short_directory, {"NOTIFY_SOCKET": "not-absolute"}, "--cycle-interval", "1.0")

    assert result.returncode == 2
    assert "NOTIFY_SOCKET" in result.stderr


def NotifiedEngineProcess(directory, variables, **options):  # noqa: N802 - reads like the class it builds
    """An Engine process started with the given service-manager variables."""
    return EngineProcess(directory, environment=variables, **options)


def wait_for_message(manager, message, timeout=PROCESS_TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if message in manager.collect():
            return
        time.sleep(0.02)
    raise AssertionError(f"{message!r} not received; got {manager.collect()!r}")


def test_the_engine_process_reports_ready_heartbeats_and_stopping_to_the_service_manager(short_directory, manager):
    engine = NotifiedEngineProcess(
        short_directory, {"NOTIFY_SOCKET": manager.address, "WATCHDOG_USEC": "4000000"}, cycle_interval=0.05
    )
    try:
        wait_for_message(manager, b"READY=1")
        time.sleep(2.3)
        assert run_ape("--socket", str(engine.endpoint), "-x").returncode == 0
        assert engine.wait() == 0
    finally:
        engine.close()

    messages = manager.collect()
    assert messages[0] == b"READY=1" and messages[-1] == b"STOPPING=1"
    assert messages.count(b"READY=1") == 1 and messages.count(b"STOPPING=1") == 1
    # About 45 completed cycles, but at most one heartbeat per second (a quarter of the 4 s watchdog).
    assert 2 <= messages.count(b"WATCHDOG=1") <= 4
    assert set(messages[1:-1]) == {b"WATCHDOG=1"}


def test_a_watchdog_meant_for_another_process_still_lets_the_engine_report_ready(short_directory, manager):
    variables = {"NOTIFY_SOCKET": manager.address, "WATCHDOG_USEC": "4000000", "WATCHDOG_PID": "1"}
    engine = NotifiedEngineProcess(short_directory, variables, cycle_interval=0.05)
    try:
        wait_for_message(manager, b"READY=1")
        time.sleep(0.4)
        engine.send_signal(signal.SIGTERM)
        assert engine.wait() == 0
    finally:
        engine.close()

    assert manager.collect() == [b"READY=1", b"STOPPING=1"]


def test_an_unreachable_service_manager_makes_the_engine_process_exit_1_after_cleanup(short_directory):
    engine = NotifiedEngineProcess(short_directory, {"NOTIFY_SOCKET": str(short_directory / "nobody-listens.sock")})
    try:
        assert engine.wait() == 1
    finally:
        engine.close()

    records = read_lifecycle_records(engine.audit_file)
    failure = next(r for r in records if r["event"] == "ENGINE_FAILURE")
    assert failure["context"]["phase"] == "startup"
    assert events_of(records)[-1] == "SHUTDOWN_COMPLETED"
    assert not engine.endpoint.exists()  # the endpoint was cleaned up
