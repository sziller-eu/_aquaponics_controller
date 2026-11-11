"""Engine lifecycle behaviour, in one process, with a scripted stand-in for the IPC ingress.

The stand-in lets a test decide what happens in which cycle (a STOP request, a
failure) without sockets or timing. Cycle intervals are tiny so the real, bounded
idle wait is used but tests stay fast.
"""

import os
import signal
import threading
import time
from datetime import datetime, timezone

import pytest

from aquaponics_controller import ipc_protocol as protocol
from aquaponics_controller.engine import Engine, EngineConfig, ExitStatus, LifecycleState
from aquaponics_controller.lifecycle_audit import LifecycleAudit, read_lifecycle_records
from aquaponics_controller.local_ingress import EndpointInUseError

from helpers import events_of

OWN_PEER = protocol.PeerIdentity(pid=4242, uid=os.geteuid(), gid=os.getegid())
STOP = protocol.AdministrativeRequest("stop", datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc))


class ScriptedIngress:
    """Stands in for LocalAdministrativeIngress: acts on chosen cycles."""

    endpoint = "scripted-endpoint"

    def __init__(self, script=None, *, open_error=None, close_error=None):
        self.script = script or {}
        self.open_error = open_error
        self.close_error = close_error
        self.opened = 0
        self.closed = 0
        self.cycle = 0
        self.responses = []

    def open(self):
        self.opened += 1
        if self.open_error:
            raise self.open_error

    def close(self):
        self.closed += 1
        if self.close_error:
            raise self.close_error

    def service_pending_requests(self, handler):
        self.cycle += 1
        action = self.script.get(self.cycle)
        if action:
            action(handler, self.responses)
        return 0


def send(request, peer):
    def action(handler, responses):
        responses.append(handler(request, peer))

    return action


def raise_error(error):
    def action(handler, responses):
        raise error

    return action


def run_actions(*actions):
    def action(handler, responses):
        for step in actions:
            step(handler, responses)

    return action


@pytest.fixture
def audit_path(short_directory):
    return short_directory / "audit" / "lifecycle.jsonl"


def build(audit_path, ingress, *, interval=0.01, execution_id="exec-1", **config):
    audit = LifecycleAudit(audit_path, execution_id)
    return Engine(
        execution_id=execution_id,
        config=EngineConfig(cycle_interval_seconds=interval, **config),
        ingress=ingress,
        audit=audit,
    )


# ---------------------------------------------------------------- normal operation


def test_startup_happens_once_cycles_repeat_and_stop_ends_with_success(audit_path):
    ingress = ScriptedIngress({4: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress)

    status = engine.run()

    assert status is ExitStatus.SUCCESS
    assert engine.startup_count == 1
    assert ingress.opened == 1
    assert engine.cycles_completed == 4  # several cycles ran before the stop took effect
    assert engine.state is LifecycleState.STOPPED
    assert events_of(read_lifecycle_records(audit_path)) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]


def test_an_engine_cannot_be_started_twice(audit_path):
    engine = build(audit_path, ScriptedIngress({1: send(STOP, OWN_PEER)}))
    engine.run()

    with pytest.raises(RuntimeError):
        engine.startup()


def test_one_operating_cycle_runs_per_invocation(audit_path):
    ingress = ScriptedIngress()
    engine = build(audit_path, ingress)

    engine.run_operating_cycle()
    engine.run_operating_cycle()

    assert (engine.cycles_completed, ingress.cycle) == (2, 2)


def test_stop_request_is_recorded_with_requester_and_both_times(audit_path):
    engine = build(audit_path, ScriptedIngress({2: send(STOP, OWN_PEER)}))

    engine.run()

    records = read_lifecycle_records(audit_path)
    received = next(r for r in records if r["event"] == "STOP_REQUEST_RECEIVED")
    assert received["requester"] == {"pid": 4242, "uid": os.geteuid(), "gid": os.getegid()}
    assert received["context"]["client_reported_submission_time_utc"] == "2026-11-11T12:00:00.000000Z"
    assert received["context"]["engine_receipt_time_utc"].endswith("Z")  # the Engine's own UTC receipt time
    assert received["timestamp_utc"].endswith("Z")
    initiated = next(r for r in records if r["event"] == "SHUTDOWN_INITIATED")
    assert initiated["reason"] == "OPERATOR_STOP"
    assert initiated["requester"]["uid"] == os.geteuid()
    assert {r["execution_id"] for r in records} == {"exec-1"}


def test_repeated_stop_requests_are_both_recorded_and_shutdown_happens_once(audit_path):
    ingress = ScriptedIngress({2: run_actions(send(STOP, OWN_PEER), send(STOP, OWN_PEER))})
    engine = build(audit_path, ingress)

    engine.run()

    assert [r.status for r in ingress.responses] == [
        protocol.STATUS_SHUTDOWN_INITIATED,
        protocol.STATUS_SHUTDOWN_ALREADY_INITIATED,
    ]
    events = events_of(read_lifecycle_records(audit_path))
    assert events.count("STOP_REQUEST_RECEIVED") == 2
    assert events.count("SHUTDOWN_INITIATED") == 1
    assert ingress.closed == 1


def test_cleanup_is_attempted_once_even_if_a_signal_follows_a_stop(audit_path):
    ingress = ScriptedIngress({2: run_actions(send(STOP, OWN_PEER), lambda h, r: engine.request_stop_for_signal(signal.SIGTERM))})
    engine = build(audit_path, ingress)

    status = engine.run()

    assert status is ExitStatus.SUCCESS
    assert ingress.closed == 1
    initiated = [r for r in read_lifecycle_records(audit_path) if r["event"] == "SHUTDOWN_INITIATED"]
    assert [r["reason"] for r in initiated] == ["OPERATOR_STOP"]  # the first cause wins


def test_stop_ack_says_initiated_and_reports_whether_the_audit_was_persisted(audit_path):
    ingress = ScriptedIngress({1: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress)

    engine.run()

    (response,) = ingress.responses
    assert (response.ok, response.status, response.audit_recorded) == (True, "shutdown_initiated", True)


# ---------------------------------------------------------------- requests that must be refused


@pytest.mark.parametrize("peer", [None, protocol.PeerIdentity(pid=1, uid=os.geteuid() + 1, gid=0)])
def test_unauthorized_or_unidentified_peers_cannot_stop_the_engine(audit_path, peer):
    ingress = ScriptedIngress({1: send(STOP, peer), 3: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress, administrative_uids=frozenset({os.geteuid()}))

    engine.run()

    refusal = ingress.responses[0]
    assert (refusal.ok, refusal.error_code) == (False, protocol.ERROR_PEER_NOT_AUTHORIZED)
    assert engine.cycles_completed == 3  # kept running until the authorized request
    assert events_of(read_lifecycle_records(audit_path)).count("STOP_REQUEST_RECEIVED") == 1


def test_unsupported_operations_are_refused_without_stopping_the_engine(audit_path):
    unsupported = protocol.AdministrativeRequest("format_the_disk", None)
    ingress = ScriptedIngress({1: send(unsupported, OWN_PEER), 3: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress)

    engine.run()

    assert ingress.responses[0].error_code == protocol.ERROR_UNSUPPORTED_OPERATION
    assert engine.cycles_completed == 3


# ---------------------------------------------------------------- idle behaviour


def test_idle_engine_is_paced_and_uses_almost_no_cpu(audit_path):
    ingress = ScriptedIngress({20: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress, interval=0.05)

    wall_start, cpu_start = time.monotonic(), time.process_time()
    engine.run()
    wall, cpu = time.monotonic() - wall_start, time.process_time() - cpu_start

    assert wall >= 0.8  # 20 cycles at 50 ms: the loop waits, it does not spin
    assert cpu < 0.3


def test_idle_wait_is_interruptible_by_a_signal_request(audit_path):
    engine = build(audit_path, ScriptedIngress(), interval=30)
    timer = threading.Timer(0.3, engine.request_stop_for_signal, [signal.SIGTERM])
    timer.start()

    started = time.monotonic()
    try:
        status = engine.run()
    finally:
        timer.cancel()

    assert status is ExitStatus.SUCCESS
    assert time.monotonic() - started < 3  # not 30 s


# ---------------------------------------------------------------- signals


@pytest.mark.parametrize("signal_number", [signal.SIGINT, signal.SIGTERM])
def test_signals_cause_a_controlled_shutdown_recorded_with_their_origin(audit_path, signal_number):
    engine = build(audit_path, ScriptedIngress({2: lambda h, r: engine.request_stop_for_signal(signal_number)}))

    status = engine.run()

    assert status is ExitStatus.SUCCESS
    records = read_lifecycle_records(audit_path)
    assert "STOP_REQUEST_RECEIVED" not in events_of(records)  # a signal is not an IPC request
    initiated = next(r for r in records if r["event"] == "SHUTDOWN_INITIATED")
    assert initiated["reason"] == "SIGNAL"
    assert initiated["context"]["signal"] == signal_number.name
    assert initiated["context"]["operator_authorization"] == "not_established"


def test_a_stop_flag_set_before_the_loop_starts_gives_a_clean_immediate_shutdown(audit_path):
    ingress = ScriptedIngress()
    engine = build(audit_path, ingress)
    engine.request_stop_for_signal(signal.SIGTERM)

    assert engine.run() is ExitStatus.SUCCESS
    assert engine.cycles_completed == 0
    assert ingress.closed == 1


# ---------------------------------------------------------------- failures


def test_unexpected_exception_is_logged_cleaned_up_and_gives_failure_status(audit_path, caplog):
    ingress = ScriptedIngress({2: raise_error(RuntimeError("injected failure"))})
    engine = build(audit_path, ingress)

    with caplog.at_level("ERROR"):
        status = engine.run()

    assert status is ExitStatus.FAILURE
    assert ingress.closed == 1
    failure_logs = [r for r in caplog.records if r.exc_info]
    assert failure_logs and "injected failure" in str(failure_logs[0].exc_info[1])  # traceback kept
    records = read_lifecycle_records(audit_path)
    assert events_of(records)[-3:] == ["ENGINE_FAILURE", "SHUTDOWN_INITIATED", "SHUTDOWN_COMPLETED"]
    failure = next(r for r in records if r["event"] == "ENGINE_FAILURE")
    assert failure["reason"] == "RuntimeError"
    assert failure["context"]["phase"] == "operation"


def test_a_failure_after_a_stop_request_is_still_a_failure(audit_path):
    ingress = ScriptedIngress({2: run_actions(send(STOP, OWN_PEER), raise_error(RuntimeError("late failure")))})
    engine = build(audit_path, ingress)

    assert engine.run() is ExitStatus.FAILURE


def test_failed_cleanup_after_a_stop_is_not_a_successful_exit(audit_path):
    ingress = ScriptedIngress({1: send(STOP, OWN_PEER)}, close_error=OSError("cannot remove endpoint"))
    engine = build(audit_path, ingress)

    status = engine.run()

    assert status is ExitStatus.CLEANUP_FAILED
    completed = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "SHUTDOWN_COMPLETED")
    assert completed["context"]["cleanup_succeeded"] is False


def test_an_endpoint_owned_by_another_engine_stops_startup_with_its_own_status(audit_path):
    ingress = ScriptedIngress(open_error=EndpointInUseError("in use"))
    engine = build(audit_path, ingress)

    status = engine.run()

    assert status is ExitStatus.ALREADY_RUNNING
    assert engine.cycles_completed == 0
    records = read_lifecycle_records(audit_path)
    assert next(r for r in records if r["event"] == "ENGINE_FAILURE")["context"]["phase"] == "startup"
    assert "ENGINE_RUNNING" not in events_of(records)


def test_an_unavailable_audit_file_stops_startup_before_anything_is_opened(short_directory, caplog):
    ingress = ScriptedIngress()
    engine = Engine(
        execution_id="exec",
        config=EngineConfig(cycle_interval_seconds=0.01),
        ingress=ingress,
        audit=LifecycleAudit(short_directory, "exec"),  # a directory: cannot be appended to
    )

    with caplog.at_level("ERROR"):
        status = engine.run()

    assert status is ExitStatus.FAILURE
    assert ingress.opened == 0
    assert "cannot open lifecycle audit file" in caplog.text


def test_audit_persistence_failure_is_reported_but_does_not_prevent_shutdown(caplog):
    from pathlib import Path

    ingress = ScriptedIngress({2: send(STOP, OWN_PEER)})
    engine = Engine(
        execution_id="exec",
        config=EngineConfig(cycle_interval_seconds=0.01),
        ingress=ingress,
        audit=LifecycleAudit(Path("/dev/full"), "exec"),  # every write fails
    )

    with caplog.at_level("ERROR"):
        status = engine.run()

    assert status is ExitStatus.SUCCESS
    assert ingress.closed == 1
    assert ingress.responses[0].audit_recorded is False
    assert "NOT PERSISTED" in caplog.text


# ---------------------------------------------------------------- history is not a command source


def test_earlier_stop_records_are_never_replayed_after_a_restart(audit_path):
    first = build(audit_path, ScriptedIngress({1: send(STOP, OWN_PEER)}), execution_id="first")
    first.run()
    previous_events = events_of(read_lifecycle_records(audit_path))
    assert "STOP_REQUEST_RECEIVED" in previous_events

    ingress = ScriptedIngress({5: send(STOP, OWN_PEER)})
    second = build(audit_path, ingress, execution_id="second")
    second.run()

    assert second.cycles_completed == 5  # it ran on until the new request; the old record did nothing
    records = read_lifecycle_records(audit_path)
    assert events_of(records, "first") == previous_events  # history is intact and readable
    assert events_of(records, "second").count("STOP_REQUEST_RECEIVED") == 1


def test_configuration_rejects_unusable_cycle_intervals():
    for interval in (0, -1, 0.001, 3600):
        with pytest.raises(ValueError):
            EngineConfig(cycle_interval_seconds=interval)
