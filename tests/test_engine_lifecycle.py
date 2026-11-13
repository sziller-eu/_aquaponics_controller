"""Engine lifecycle behaviour, in one process, with a scripted stand-in for the IPC ingress.

The stand-in lets a test decide what happens in which cycle (a STOP request, a
failure) without sockets or timing. Cycle intervals are tiny so the real, bounded
idle wait is used but tests stay fast.
"""

import os
import signal
import socket
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
    readiness_source = None  # no socket: the Engine falls back to a plain interruptible timed wait

    def __init__(self, script=None, *, open_error=None, close_error=None, final_pass=None, withdraw_error=None):
        self.script = script or {}
        self.open_error = open_error
        self.close_error = close_error
        self.final_pass = final_pass  # what the final servicing pass during shutdown does (an action, or None)
        self.withdraw_error = withdraw_error
        self.opened = 0
        self.closed = 0
        self.withdrawn = 0
        self.cycle = 0
        self.responses = []
        self.final_pass_limits = []  # the time limit of every call made after the endpoint was withdrawn

    def open(self):
        self.opened += 1
        if self.open_error:
            raise self.open_error

    def close(self):
        self.closed += 1
        if self.close_error:
            raise self.close_error

    def withdraw_endpoint(self):
        self.withdrawn += 1
        if self.withdraw_error:
            raise self.withdraw_error

    def service_pending_requests(self, handler, time_limit_seconds=None):
        if self.withdrawn:
            return self._final_pass(handler, time_limit_seconds)
        self.cycle += 1
        action = self.script.get(self.cycle)
        if action:
            action(handler, self.responses)
        return 0

    def _final_pass(self, handler, time_limit_seconds):
        self.final_pass_limits.append(time_limit_seconds)
        if self.final_pass:
            self.final_pass(handler, self.responses)
        return 0


class ReadyIngress(ScriptedIngress):
    """A stand-in that has a real readiness source, so the Engine's idle wait can be driven by it.

    ``service_pending_requests`` records every call with its time limit (None: the call made inside an
    operating cycle; a number: the call made while idling). ``on_service`` decides what one call does and
    returns how many connections it handled.
    """

    def __init__(self, script=None, *, on_service=None, **options):
        super().__init__(script, **options)
        self.signal_sender, self.readiness_source = socket.socketpair()
        self.readiness_source.setblocking(False)
        self.on_service = on_service
        self.calls = []  # (monotonic time, time limit)

    def make_ready(self):
        self.signal_sender.send(b"x")

    def consume_readiness(self):
        try:
            self.readiness_source.recv(100)
        except BlockingIOError:
            pass

    def service_pending_requests(self, handler, time_limit_seconds=None):
        if self.withdrawn:
            return self._final_pass(handler, time_limit_seconds)
        self.calls.append((time.monotonic(), time_limit_seconds))
        if time_limit_seconds is None:
            return super().service_pending_requests(handler)  # in-cycle call: the usual scripted behaviour
        return self.on_service(self, handler) if self.on_service else 0

    def close(self):
        super().close()
        self.signal_sender.close()
        self.readiness_source.close()

    def idle_calls(self):
        return [call for call in self.calls if call[1] is not None]


def start_watchdog(engine, seconds=4.0):
    """Stop an Engine that a defect would otherwise leave running, so a failing test fails instead of hanging."""
    timer = threading.Timer(seconds, engine.request_stop_for_signal, [signal.SIGTERM])
    timer.daemon = True
    timer.start()
    return timer


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


# ---------------------------------------------------------------- request classes and queries (Issue #5)

PING = protocol.AdministrativeRequest("ping", None, correlation_id="ping-1")


def echo(text, **fields):
    return protocol.AdministrativeRequest("echo", None, correlation_id="echo-1", arguments={"text": text}, **fields)


def ask(audit_path, request, peer=OWN_PEER, **config):
    """Run an Engine for two cycles and return the response to the request made in the second one."""
    ingress = ScriptedIngress({2: send(request, peer), 3: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress, **config)
    engine.run()
    return ingress.responses[0], engine


def test_ping_is_answered_by_the_engine_from_its_own_state(audit_path):
    response, engine = ask(audit_path, PING)

    assert (response.ok, response.correlation_id) == (True, "ping-1")
    assert response.result == {
        "reply": "pong",
        "execution_id": "exec-1",
        "engine_pid": os.getpid(),
        "cycles_completed": 1,  # the request arrived during cycle 2: one cycle had been completed
        "engine_time_utc": response.result["engine_time_utc"],
    }
    assert response.result["engine_time_utc"].endswith("Z")
    assert engine.cycles_completed == 3


def test_ping_reports_cycles_completed_as_they_advance(audit_path):
    ingress = ScriptedIngress({2: send(PING, OWN_PEER), 5: send(PING, OWN_PEER), 6: send(STOP, OWN_PEER)})

    build(audit_path, ingress).run()

    first, second, _stop = ingress.responses
    assert second.result["cycles_completed"] - first.result["cycles_completed"] == 3


@pytest.mark.parametrize("text", ["", "hello", 'quotes " and \\ backslash', "line\nbreak\ttab", "héllo wörld", "日本語", "\U0001f600‍\U0001f525"])
def test_echo_returns_the_text_unchanged(audit_path, text):
    response, _ = ask(audit_path, echo(text))

    assert (response.ok, response.result, response.correlation_id) == (True, {"text": text}, "echo-1")


@pytest.mark.parametrize("text", ["x" * 256, "é" * 256, "\U0001f600" * 256])
def test_echo_accepts_exactly_the_maximum_length_counted_in_characters(audit_path, text):
    response, _ = ask(audit_path, echo(text))

    assert response.ok and response.result == {"text": text}


@pytest.mark.parametrize("text", ["x" * 257, "é" * 257, "\U0001f600" * 257])
def test_echo_refuses_longer_text_and_never_repeats_it(audit_path, text):
    response, engine = ask(audit_path, echo(text))

    assert (response.ok, response.error_code) == (False, protocol.ERROR_INVALID_ARGUMENTS)
    assert response.result is None and text not in response.message
    assert engine.cycles_completed == 3  # the Engine ran on


@pytest.mark.parametrize(
    "arguments",
    [None, {}, {"text": 5}, {"text": None}, {"text": ["a"]}, {"text": {"a": 1}}, {"text": True}, {"txt": "x"}, {"text": "x", "extra": 1}],
    ids=["missing", "empty", "int", "null", "list", "object", "bool", "wrong-name", "extra-argument"],
)
def test_echo_refuses_missing_or_malformed_arguments(audit_path, arguments):
    request = protocol.AdministrativeRequest("echo", None, correlation_id="e", arguments=arguments)

    response, engine = ask(audit_path, request)

    assert (response.ok, response.error_code, response.correlation_id) == (False, protocol.ERROR_INVALID_ARGUMENTS, "e")
    assert engine.cycles_completed == 3


def test_echo_refuses_text_that_cannot_be_encoded_as_unicode(audit_path):
    response, _ = ask(audit_path, echo("broken \ud800 surrogate"))

    assert (response.ok, response.error_code) == (False, protocol.ERROR_INVALID_ARGUMENTS)


@pytest.mark.parametrize("operation", ["ping", "stop"])
def test_operations_that_take_no_arguments_refuse_them(audit_path, operation):
    request = protocol.AdministrativeRequest(operation, None, arguments={"surprise": 1})
    ingress = ScriptedIngress({1: send(request, OWN_PEER), 3: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress)

    engine.run()

    assert ingress.responses[0].error_code == protocol.ERROR_INVALID_ARGUMENTS
    assert engine.cycles_completed == 3  # a refused stop did not stop the Engine


@pytest.mark.parametrize("request_", [PING, echo("hello")], ids=["ping", "echo"])
@pytest.mark.parametrize("peer", [None, protocol.PeerIdentity(pid=1, uid=os.geteuid() + 1, gid=0)], ids=["unidentified", "other-user"])
def test_queries_need_the_same_authorization_as_everything_else(audit_path, request_, peer):
    response, _ = ask(audit_path, request_, peer, administrative_uids=frozenset({os.geteuid()}))

    assert (response.ok, response.error_code) == (False, protocol.ERROR_PEER_NOT_AUTHORIZED)
    assert response.result is None  # nothing about the Engine is revealed to an unauthorized peer
    assert response.correlation_id == request_.correlation_id


def test_unsupported_operations_still_get_a_correlated_refusal(audit_path):
    request = protocol.AdministrativeRequest("feed_the_fish", None, correlation_id="f-1")

    response, _ = ask(audit_path, request)

    assert (response.error_code, response.correlation_id) == (protocol.ERROR_UNSUPPORTED_OPERATION, "f-1")


def test_the_stop_acknowledgement_carries_the_correlation_id_and_keeps_its_meaning(audit_path):
    stop = protocol.AdministrativeRequest("stop", None, correlation_id="s-1")
    ingress = ScriptedIngress({1: send(stop, OWN_PEER)})

    build(audit_path, ingress).run()

    (response,) = ingress.responses
    assert (response.ok, response.status, response.correlation_id) == (True, "shutdown_initiated", "s-1")
    assert response.audit_recorded is True


def test_queries_are_neither_audited_nor_persisted(audit_path):
    ingress = ScriptedIngress({2: run_actions(send(PING, OWN_PEER), send(echo("secret-text"), OWN_PEER)), 3: send(STOP, OWN_PEER)})

    build(audit_path, ingress).run()

    assert events_of(read_lifecycle_records(audit_path)) == [  # exactly the lifecycle events: nothing for the queries
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]
    assert "secret-text" not in audit_path.read_text()
    assert [path.name for path in audit_path.parent.iterdir()] == [audit_path.name]


def test_the_supported_operations_and_their_request_classes_are_exactly_these(audit_path):
    from aquaponics_controller.engine import RequestClass

    engine = build(audit_path, ScriptedIngress())

    classes = {name: operation.request_class for name, operation in engine._supported_operations.items()}

    assert classes == {
        "ping": RequestClass.QUERY,
        "echo": RequestClass.QUERY,
        "stop": RequestClass.LIFECYCLE_CONTROL,
    }  # no operational command exists, and none can appear without a deliberate change here


def test_the_engine_writes_a_trace_of_the_processing_but_not_of_the_text(audit_path, caplog):
    ingress = ScriptedIngress({1: send(echo("do-not-log-me"), OWN_PEER), 2: send(STOP, OWN_PEER)})

    with caplog.at_level("INFO"):
        build(audit_path, ingress).run()

    assert "IPC request processed by the Engine: op='echo' id='echo-1' outcome=ok" in caplog.text
    assert "do-not-log-me" not in caplog.text


def test_no_hardware_library_is_involved():
    import sys

    import aquaponics_controller.cli  # noqa: F401
    import aquaponics_controller.engine  # noqa: F401

    assert not {"RPi", "RPi.GPIO", "gpiozero", "smbus", "smbus2", "spidev", "pigpio"} & set(sys.modules)


# ---------------------------------------------------------------- responsive idle waiting (Issue #5)


def test_a_request_is_answered_during_the_idle_wait_without_running_another_cycle(audit_path):
    def on_service(ingress, handler):
        ingress.consume_readiness()
        ingress.responses.append(handler(PING, OWN_PEER))
        ingress.responses.append(handler(STOP, OWN_PEER))
        return 2

    ingress = ReadyIngress(on_service=on_service)
    engine = build(audit_path, ingress, interval=30)  # a 30-second interval must not mean a 30-second delay
    timer = threading.Timer(0.2, ingress.make_ready)
    timer.start()
    watchdog = start_watchdog(engine)

    started = time.monotonic()
    try:
        status = engine.run()
    finally:
        timer.cancel()
        watchdog.cancel()

    assert time.monotonic() - started < 3  # answered and stopped within moments, not after the interval
    assert status is ExitStatus.SUCCESS
    assert engine.cycles_completed == 1  # the request did not start a cycle
    ping_response = ingress.responses[0]
    assert ping_response.result["reply"] == "pong" and ping_response.result["cycles_completed"] == 1
    assert [limit is None for _, limit in ingress.calls] == [True, False]  # one in-cycle call, one idle call


def test_idle_servicing_is_limited_to_the_time_left_before_the_next_cycle(audit_path):
    def on_service(ingress, handler):
        ingress.consume_readiness()
        ingress.responses.append(handler(STOP, OWN_PEER))
        return 1

    ingress = ReadyIngress(on_service=on_service)
    engine = build(audit_path, ingress, interval=5)
    threading.Timer(0.2, ingress.make_ready).start()
    watchdog = start_watchdog(engine)

    engine.run()
    watchdog.cancel()

    (_, limit), = ingress.idle_calls()
    assert 0 < limit <= 5 - 0.15  # what remained of the interval when the request arrived


def test_readiness_without_progress_cannot_make_the_engine_spin(audit_path):
    ingress = ReadyIngress()  # always "ready", yet servicing never handles anything (as when accept() fails)
    ingress.make_ready()
    engine = build(audit_path, ingress, interval=30)
    timer = threading.Timer(1.2, engine.request_stop_for_signal, [signal.SIGTERM])
    timer.start()

    cpu_start = time.process_time()
    try:
        status = engine.run()
    finally:
        timer.cancel()
    cpu = time.process_time() - cpu_start

    assert status is ExitStatus.SUCCESS
    assert len(ingress.idle_calls()) <= 8  # a few back-off retries, not hundreds of thousands
    assert cpu < 0.3


def test_the_idle_loop_keeps_its_deadline_under_a_burst_of_requests(audit_path):
    starts = []

    class RecordingEngine(Engine):
        def run_operating_cycle(self):
            starts.append(time.monotonic())
            super().run_operating_cycle()
            if len(starts) == 8:
                self.request_stop_for_signal(signal.SIGTERM)

    def on_service(ingress, handler):
        time.sleep(0.01)  # every call handles one request: the burst never ends
        return 1

    ingress = ReadyIngress(on_service=on_service)
    ingress.make_ready()  # stays ready for the whole run
    engine = RecordingEngine(
        execution_id="burst",
        config=EngineConfig(cycle_interval_seconds=0.1),
        ingress=ingress,
        audit=LifecycleAudit(audit_path, "burst"),
    )

    engine.run()

    for index, start in enumerate(starts):
        assert abs((start - starts[0]) - index * 0.1) < 0.08  # a burst does not stretch or shorten the cadence
    assert ingress.idle_calls()  # the burst really was serviced while idling...
    assert all(0 < limit <= 0.1 for _, limit in ingress.idle_calls())  # ...and never allowed to run past the deadline


def test_requests_are_never_run_as_cycles(audit_path):
    def on_service(ingress, handler):
        ingress.responses.append(handler(PING, OWN_PEER))
        return 1

    ingress = ReadyIngress(on_service=on_service)
    ingress.make_ready()
    engine = build(audit_path, ingress, interval=0.3)
    timer = threading.Timer(1.0, engine.request_stop_for_signal, [signal.SIGTERM])
    timer.start()

    started = time.monotonic()
    try:
        engine.run()
    finally:
        timer.cancel()
    elapsed = time.monotonic() - started

    assert len(ingress.responses) > 5  # many requests were answered...
    assert engine.cycles_completed <= elapsed / 0.3 + 1.5  # ...and the cycles still followed the timer alone


def test_a_signal_arriving_just_before_the_engine_starts_waiting_is_not_lost(audit_path):
    engine = build(audit_path, ScriptedIngress(), interval=30)
    original_wait = engine._idle_wait.wait

    def signal_then_wait(timeout, **options):
        engine.request_stop_for_signal(signal.SIGTERM)  # after the stop flag was last checked, before select
        return original_wait(timeout, **options)

    engine._idle_wait.wait = signal_then_wait

    started = time.monotonic()
    status = engine.run()

    assert status is ExitStatus.SUCCESS
    assert time.monotonic() - started < 2


@pytest.mark.parametrize("ingress_class", [ScriptedIngress, ReadyIngress], ids=["timed-wait", "readiness-wait"])
def test_stop_signals_arriving_at_any_moment_end_the_wait_promptly(audit_path, ingress_class):
    for delay in (0, 0.001, 0.003, 0.007, 0.012, 0.02, 0.035, 0.05):
        engine = build(audit_path, ingress_class(), interval=30, execution_id=f"race-{delay}")
        timer = threading.Timer(delay, engine.request_stop_for_signal, [signal.SIGINT])
        timer.start()
        started = time.monotonic()
        try:
            assert engine.run() is ExitStatus.SUCCESS
        finally:
            timer.cancel()
        assert time.monotonic() - started < 2, f"wake-up lost with delay {delay}"


def test_an_engine_without_a_readiness_source_waits_on_the_timer_and_the_stop_flag_only(audit_path):
    ingress = ScriptedIngress({6: send(STOP, OWN_PEER)})

    engine = build(audit_path, ingress, interval=0.02)
    engine.run()

    assert (engine.cycles_completed, ingress.cycle) == (6, 6)  # one servicing call per cycle, as before


def test_a_failure_while_servicing_during_the_idle_wait_takes_the_failure_path(audit_path):
    def on_service(ingress, handler):
        raise RuntimeError("injected idle-servicing failure")

    ingress = ReadyIngress(on_service=on_service)
    engine = build(audit_path, ingress, interval=30)
    threading.Timer(0.2, ingress.make_ready).start()
    watchdog = start_watchdog(engine)

    status = engine.run()
    watchdog.cancel()

    assert status is ExitStatus.FAILURE
    assert ingress.closed == 1
    records = read_lifecycle_records(audit_path)
    assert events_of(records)[-3:] == ["ENGINE_FAILURE", "SHUTDOWN_INITIATED", "SHUTDOWN_COMPLETED"]
    assert next(r for r in records if r["event"] == "ENGINE_FAILURE")["context"]["phase"] == "operation"


def test_the_wake_up_resources_are_released_after_every_kind_of_shutdown(audit_path):
    def descriptors():
        return len(os.listdir("/proc/self/fd"))

    before = descriptors()
    for index in range(10):
        ingress = ScriptedIngress({1: send(STOP, OWN_PEER)} if index % 2 else {1: raise_error(RuntimeError("x"))})
        build(audit_path, ingress, execution_id=f"leak-{index}").run()
    stopped_by_signal = build(audit_path, ScriptedIngress(), execution_id="leak-signal")
    stopped_by_signal.request_stop_for_signal(signal.SIGTERM)
    stopped_by_signal.run()

    assert descriptors() == before


def test_a_failed_startup_leaves_no_wake_up_resources(audit_path):
    before = len(os.listdir("/proc/self/fd"))

    build(audit_path, ScriptedIngress(open_error=EndpointInUseError("in use"))).run()

    assert len(os.listdir("/proc/self/fd")) == before


def test_a_failure_to_release_the_wake_up_channel_is_reported_and_does_not_skip_the_endpoint(audit_path):
    ingress = ScriptedIngress({1: send(STOP, OWN_PEER)})
    engine = build(audit_path, ingress)
    real_close = engine._idle_wait.close

    def failing_close():
        real_close()
        raise OSError("cannot close")

    engine._idle_wait.close = failing_close

    assert engine.run() is ExitStatus.CLEANUP_FAILED
    assert ingress.closed == 1


def test_the_endpoint_cleanup_failure_does_not_skip_the_wake_up_channel(audit_path):
    before = len(os.listdir("/proc/self/fd"))
    ingress = ScriptedIngress({1: send(STOP, OWN_PEER)}, close_error=OSError("cannot remove endpoint"))

    assert build(audit_path, ingress).run() is ExitStatus.CLEANUP_FAILED
    assert len(os.listdir("/proc/self/fd")) == before
