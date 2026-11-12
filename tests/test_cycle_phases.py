"""The phases of one operating cycle (Issue #6): order, counting, failures, time limits, STOP.

Tests run in one process with the scripted stand-ins for the IPC ingress. Durations are
simulated with an injected clock wherever the test is about time, so no test depends on
how fast the machine is.
"""

import os
import signal
import threading
import time
from datetime import datetime

import pytest

from aquaponics_controller import cli
from aquaponics_controller import ipc_protocol as protocol
from aquaponics_controller.cycle_phases import PHASE_TIME_LIMITS_SECONDS, CyclePhase
from aquaponics_controller.engine import Engine, EngineConfig, ExitStatus
from aquaponics_controller.lifecycle_audit import LifecycleAudit, read_lifecycle_records
from aquaponics_controller.local_ingress import IngressLimits

from helpers import events_of
from test_engine_lifecycle import STOP, ReadyIngress, ScriptedIngress, send, start_watchdog

OWN_PEER = protocol.PeerIdentity(pid=4242, uid=os.geteuid(), gid=os.getegid())
PING = protocol.AdministrativeRequest("ping", None)

APPROVED_ORDER = [
    "ACQUIRE_OBSERVATIONS",
    "RECONCILE_STATE",
    "INGEST_EXTERNAL_INTENTS",
    "EVALUATE_SCHEDULED_WORK",
    "EVALUATE_CONTROL_POLICIES",
    "EXECUTE_READY_COMMANDS",
    "FINALIZE_CYCLE",
]

PHASE_METHODS = {
    CyclePhase.ACQUIRE_OBSERVATIONS: "_acquire_observations",
    CyclePhase.RECONCILE_STATE: "_reconcile_state",
    CyclePhase.INGEST_EXTERNAL_INTENTS: "_ingest_external_intents",
    CyclePhase.EVALUATE_SCHEDULED_WORK: "_evaluate_scheduled_work",
    CyclePhase.EVALUATE_CONTROL_POLICIES: "_evaluate_control_policies",
    CyclePhase.EXECUTE_READY_COMMANDS: "_execute_ready_commands",
    CyclePhase.FINALIZE_CYCLE: "_finalize_cycle",
}
PLACEHOLDERS = [phase for phase in CyclePhase if phase is not CyclePhase.INGEST_EXTERNAL_INTENTS]


class ManualClock:
    """Monotonic time that moves only when a test moves it. UTC time is real (it only labels records)."""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def utc_now(self):
        return datetime.now().astimezone()


class OffsetClock:
    """Real monotonic time plus a jump a test can add, to simulate a long phase without sleeping."""

    def __init__(self):
        self.offset = 0.0

    def monotonic(self):
        return time.monotonic() + self.offset

    def utc_now(self):
        return datetime.now().astimezone()


class PhaseRecordingEngine(Engine):
    """Records every phase it runs, and can run a test action at a chosen phase of a chosen cycle."""

    def __init__(self, *args, at=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.phases_run = []
        self.at = at or {}  # (cycle number, phase) -> callable(engine)

    def _run_phase(self, phase, perform_phase):
        self.phases_run.append(phase)
        action = self.at.get((self.cycles_completed + 1, phase))
        if action is None:
            return super()._run_phase(phase, perform_phase)

        def perform_with_action():
            action(self)
            perform_phase()

        return super()._run_phase(phase, perform_with_action)


@pytest.fixture
def audit_path(short_directory):
    return short_directory / "audit" / "lifecycle.jsonl"


def build(audit_path, ingress, *, interval=0.01, clock=None, at=None):
    return PhaseRecordingEngine(
        execution_id="phases",
        config=EngineConfig(cycle_interval_seconds=interval),
        ingress=ingress,
        audit=LifecycleAudit(audit_path, "phases"),
        clock=clock,
        at=at,
    )


def stop_after_cycle(cycle):
    """An action for ``at``: request a stop (as SIGTERM) when the given cycle reaches FINALIZE_CYCLE."""
    return {(cycle, CyclePhase.FINALIZE_CYCLE): lambda engine: engine.request_stop_for_signal(signal.SIGTERM)}


# ---------------------------------------------------------------- order and counting


def test_the_phases_are_exactly_the_approved_ones_in_the_approved_order():
    assert [str(phase) for phase in CyclePhase] == APPROVED_ORDER


def test_every_cycle_runs_each_phase_once_in_order(audit_path):
    engine = build(audit_path, ScriptedIngress())

    for _ in range(3):
        engine.run_operating_cycle()

    assert engine.phases_run == list(CyclePhase) * 3
    assert engine.cycles_completed == 3


def test_a_running_engine_passes_every_cycle_through_all_phases(audit_path):
    engine = build(audit_path, ScriptedIngress(), at=stop_after_cycle(4))

    assert engine.run() is ExitStatus.SUCCESS

    assert engine.phases_run == list(CyclePhase) * 4
    assert engine.cycles_completed == 4


def test_a_cycle_is_counted_only_after_its_last_phase(audit_path):
    seen = {}
    record = lambda phase: lambda engine: seen.__setitem__(phase, engine.cycles_completed)  # noqa: E731
    engine = build(audit_path, ScriptedIngress(), at={(1, phase): record(phase) for phase in CyclePhase})

    engine.run_operating_cycle()

    assert seen == {phase: 0 for phase in CyclePhase}  # still uncounted while FINALIZE_CYCLE runs
    assert engine.cycles_completed == 1


# ---------------------------------------------------------------- IPC servicing and phases


class PhaseObservingIngress(ScriptedIngress):
    """Notes which phase the Engine was in whenever it services requests."""

    def __init__(self):
        super().__init__()
        self.engine = None
        self.phases_seen = []

    def service_pending_requests(self, handler, time_limit_seconds=None):
        self.phases_seen.append(self.engine._current_phase)
        return 0


def test_requests_inside_a_cycle_are_serviced_only_in_the_ingest_phase(audit_path):
    ingress = PhaseObservingIngress()
    engine = build(audit_path, ingress)
    ingress.engine = engine

    for _ in range(3):
        engine.run_operating_cycle()

    assert ingress.phases_seen == [CyclePhase.INGEST_EXTERNAL_INTENTS] * 3


def test_requests_serviced_while_idling_run_no_phase(audit_path):
    phases_during_idle_servicing = []

    def on_service(ingress, handler):
        phases_during_idle_servicing.append(engine._current_phase)
        ingress.responses.append(handler(PING, OWN_PEER))
        return 1

    ingress = ReadyIngress(on_service=on_service)
    ingress.make_ready()  # stays ready: requests are serviced all through every idle wait
    engine = build(audit_path, ingress, interval=0.3)
    timer = threading.Timer(1.0, engine.request_stop_for_signal, [signal.SIGTERM])
    timer.start()
    try:
        engine.run()
    finally:
        timer.cancel()

    assert len(phases_during_idle_servicing) > 5  # many requests were answered while idling...
    assert set(phases_during_idle_servicing) == {None}  # ...outside every phase...
    assert engine.phases_run == list(CyclePhase) * engine.cycles_completed  # ...and ran no extra phase


# ---------------------------------------------------------------- failures


def raise_in_phase(engine):
    raise RuntimeError("injected phase failure")


@pytest.mark.parametrize("failing_phase", list(CyclePhase), ids=str)
def test_an_exception_in_any_phase_ends_the_cycle_and_takes_the_failure_path(audit_path, failing_phase):
    ingress = ScriptedIngress()
    engine = build(audit_path, ingress, at={(2, failing_phase): raise_in_phase})

    status = engine.run()

    assert status is ExitStatus.FAILURE
    phases = list(CyclePhase)
    assert engine.phases_run == phases + phases[: phases.index(failing_phase) + 1]  # later phases skipped
    assert engine.cycles_completed == 1  # the failed cycle is not counted
    assert ingress.closed == 1  # cleanup, once
    records = read_lifecycle_records(audit_path)
    assert events_of(records)[-3:] == ["ENGINE_FAILURE", "SHUTDOWN_INITIATED", "SHUTDOWN_COMPLETED"]
    failure = next(r for r in records if r["event"] == "ENGINE_FAILURE")
    assert failure["reason"] == "RuntimeError"  # the original exception type, not a wrapper
    assert failure["context"]["phase"] == "operation"
    assert failure["context"]["cycle_phase"] == str(failing_phase)
    assert failure["context"]["cycle_number"] == 2


def test_the_failure_log_names_the_phase_and_keeps_the_traceback(audit_path, caplog):
    engine = build(audit_path, ScriptedIngress(), at={(1, CyclePhase.RECONCILE_STATE): raise_in_phase})

    with caplog.at_level("ERROR"):
        engine.run()

    failure = next(r for r in caplog.records if r.exc_info)
    assert "RECONCILE_STATE" in failure.getMessage() and "cycle 1" in failure.getMessage()
    assert "injected phase failure" in str(failure.exc_info[1])


def test_a_failure_outside_any_phase_names_no_phase(audit_path):
    def on_service(ingress, handler):
        raise RuntimeError("injected idle-servicing failure")

    ingress = ReadyIngress(on_service=on_service)
    engine = build(audit_path, ingress, interval=30)
    threading.Timer(0.2, ingress.make_ready).start()
    watchdog = start_watchdog(engine)

    assert engine.run() is ExitStatus.FAILURE
    watchdog.cancel()

    context = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "ENGINE_FAILURE")["context"]
    assert context["phase"] == "operation"
    assert "cycle_phase" not in context and "cycle_number" not in context


def test_a_startup_failure_names_no_phase(short_directory):
    ingress = ScriptedIngress(open_error=OSError("cannot open endpoint"))
    audit_path = short_directory / "audit" / "lifecycle.jsonl"
    engine = build(audit_path, ingress)

    assert engine.run() is ExitStatus.FAILURE

    assert engine.phases_run == []
    context = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "ENGINE_FAILURE")["context"]
    assert context["phase"] == "startup" and "cycle_phase" not in context


# ---------------------------------------------------------------- time limits


def test_every_phase_has_a_positive_time_limit():
    assert set(PHASE_TIME_LIMITS_SECONDS) == set(CyclePhase)
    assert all(limit > 0 for limit in PHASE_TIME_LIMITS_SECONDS.values())


def test_the_approved_initial_time_limits():
    assert PHASE_TIME_LIMITS_SECONDS[CyclePhase.INGEST_EXTERNAL_INTENTS] == 0.9
    assert all(PHASE_TIME_LIMITS_SECONDS[phase] == 0.1 for phase in PLACEHOLDERS)


def test_a_whole_cycle_fits_well_within_the_cli_timeout():
    # A request arriving while a cycle runs waits for the end of that cycle.
    assert sum(PHASE_TIME_LIMITS_SECONDS.values()) < cli.DEFAULT_TIMEOUT_SECONDS


def test_the_ingress_bounds_fit_the_ingest_phase_limit():
    # The ingress starts no new connection after its admission budget; the last one it started
    # can still take its read and write deadlines.
    limits = IngressLimits()
    worst_case = limits.cycle_budget_seconds + limits.read_deadline_seconds + limits.write_deadline_seconds
    assert worst_case <= PHASE_TIME_LIMITS_SECONDS[CyclePhase.INGEST_EXTERNAL_INTENTS]


def take(seconds, clock):
    def action(engine):
        clock.now += seconds

    return action


@pytest.mark.parametrize("slow_phase", list(CyclePhase), ids=str)
def test_a_phase_over_its_limit_is_reported_and_the_cycle_continues(audit_path, slow_phase, caplog):
    clock = ManualClock()
    limit = PHASE_TIME_LIMITS_SECONDS[slow_phase]
    engine = build(audit_path, ScriptedIngress(), clock=clock, at={(1, slow_phase): take(limit + 0.25, clock)})

    with caplog.at_level("WARNING"):
        engine.run_operating_cycle()

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert str(slow_phase) in message
    assert f"{limit + 0.25:.3f} s elapsed" in message and f"limit {limit:.3f} s" in message
    assert engine.phases_run == list(CyclePhase)  # the remaining phases still ran
    assert engine.cycles_completed == 1
    assert not audit_path.exists()  # an overrun is a diagnostic, not a lifecycle event


@pytest.mark.parametrize("phase", list(CyclePhase), ids=str)
def test_a_phase_within_its_limit_is_not_reported(audit_path, phase, caplog):
    clock = ManualClock()
    engine = build(audit_path, ScriptedIngress(), clock=clock, at={(1, phase): take(PHASE_TIME_LIMITS_SECONDS[phase] * 0.9, clock)})

    with caplog.at_level("WARNING"):
        engine.run_operating_cycle()

    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_an_overrun_does_not_stop_a_running_engine(audit_path):
    clock = OffsetClock()

    def slow(engine):
        clock.offset += 0.5

    engine = build(audit_path, ScriptedIngress(), clock=clock, at={(1, CyclePhase.RECONCILE_STATE): slow, **stop_after_cycle(3)})

    assert engine.run() is ExitStatus.SUCCESS
    assert engine.cycles_completed == 3


def test_a_long_cycle_is_not_followed_by_a_burst_of_catch_up_cycles(audit_path):
    interval = 0.1
    clock = OffsetClock()
    starts = []

    def note_start(engine):
        starts.append(clock.monotonic())

    def long_cycle(engine):
        clock.offset += 5 * interval  # this cycle "took" five intervals

    at = {(cycle, CyclePhase.ACQUIRE_OBSERVATIONS): note_start for cycle in range(1, 6)}
    at[(1, CyclePhase.EVALUATE_CONTROL_POLICIES)] = long_cycle
    at.update(stop_after_cycle(5))
    engine = build(audit_path, ScriptedIngress(), interval=interval, clock=clock, at=at)

    engine.run()

    assert starts[1] - starts[0] == pytest.approx(5 * interval, abs=0.05)  # overdue: starts at once
    for earlier, later in zip(starts[1:], starts[2:]):
        assert later - earlier == pytest.approx(interval, abs=0.05)  # then the normal pace: no catch-up


# ---------------------------------------------------------------- STOP and signals


def test_a_stop_received_in_the_ingest_phase_lets_the_cycle_finish_and_starts_no_other(audit_path):
    engine = build(audit_path, ScriptedIngress({1: send(STOP, OWN_PEER)}))

    assert engine.run() is ExitStatus.SUCCESS

    assert engine.phases_run == list(CyclePhase)  # every phase of cycle 1, and nothing after it
    assert engine.cycles_completed == 1
    assert events_of(read_lifecycle_records(audit_path)) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]


@pytest.mark.parametrize("phase", list(CyclePhase), ids=str)
def test_a_signal_during_any_phase_lets_the_cycle_finish_and_starts_no_other(audit_path, phase):
    engine = build(audit_path, ScriptedIngress(), at={(2, phase): lambda e: e.request_stop_for_signal(signal.SIGTERM)})

    assert engine.run() is ExitStatus.SUCCESS

    assert engine.phases_run == list(CyclePhase) * 2
    assert engine.cycles_completed == 2
    shutdown = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "SHUTDOWN_INITIATED")
    assert shutdown["reason"] == "SIGNAL"


# ---------------------------------------------------------------- placeholders


@pytest.mark.parametrize("phase", PLACEHOLDERS, ids=str)
def test_placeholders_do_nothing(phase):
    method = getattr(Engine, PHASE_METHODS[phase])
    assert "NOT IMPLEMENTED" in method.__doc__
    # The body is the docstring alone: no attribute, global or function is even referenced.
    assert method.__code__.co_names == ()


def test_cycles_leave_no_trace_on_disk(audit_path, short_directory):
    engine = build(audit_path, ScriptedIngress())
    engine.startup()
    files_before = sorted(path for path in short_directory.rglob("*"))
    audit_before = audit_path.read_bytes()

    for _ in range(5):
        engine.run_operating_cycle()

    assert sorted(path for path in short_directory.rglob("*")) == files_before
    assert audit_path.read_bytes() == audit_before
    engine.request_stop_for_signal(signal.SIGTERM)
    engine._shutdown()


def test_the_phase_module_involves_no_hardware_library():
    import sys

    import aquaponics_controller.cycle_phases  # noqa: F401

    assert not {"RPi", "RPi.GPIO", "gpiozero", "smbus", "smbus2", "spidev", "pigpio"} & set(sys.modules)
