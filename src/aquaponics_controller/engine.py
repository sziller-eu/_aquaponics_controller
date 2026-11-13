"""The Aquaponics Engine: lifecycle coordination (development foundation).

IMPORTANT: this is a development foundation, not a production controller.
It has NO hardware, GPIO or relay control, NO sensors, NO scheduling, NO durable
operational command journal, NO operational state database, NO state
reconciliation, NO REST API and NO process supervision. An operating cycle passes
through seven named phases (see ``cycle_phases``); the only one that does real work
is servicing pending local administrative requests, the others are placeholders.
Requests are also answered while the Engine idles between cycles, which never
starts an additional cycle and runs no phase.

Responsibilities are kept apart:

- ``Engine.run``                  lifecycle coordinator: startup once, repeat cycles, shut down once.
- ``Engine.run_operating_cycle``  exactly one cycle per call: the phases, in order.
- ``Engine._run_phase``           runs one phase: records which phase is running, measures its duration.
- ``Engine._wait_for_next_cycle`` idle until the next cycle is due, answering requests meanwhile.
- ``Engine._handle_administrative_request``  the one entry point for every request: authorize, dispatch.
- ``Engine._shutdown``            final bounded servicing of queued requests (controlled shutdown only), cleanup, attempted once.

Execution is sequential: no threads, no multiprocessing, no asynchronous framework.
Signal handlers (installed by the entry point) only set a stop flag and wake the
idle wait; everything else happens here, in the normal flow.
"""

import enum
import logging
import os
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace

from .clock import SystemClock, format_utc
from .cycle_phases import PHASE_TIME_LIMITS_SECONDS, CyclePhase
from .diagnostic_logging import RepeatedWarningReporter, log_context, log_handler_failure_counts
from .idle_wait import IdleWait
from .ipc_protocol import (
    ERROR_INVALID_ARGUMENTS,
    ERROR_PEER_NOT_AUTHORIZED,
    ERROR_UNSUPPORTED_OPERATION,
    MAX_ECHO_TEXT_LENGTH,
    OPERATION_ECHO,
    OPERATION_PING,
    OPERATION_STOP,
    STATUS_SHUTDOWN_ALREADY_INITIATED,
    STATUS_SHUTDOWN_INITIATED,
    AdministrativeRequest,
    AdministrativeResponse,
    PeerIdentity,
)
from .lifecycle_audit import LifecycleAudit, LifecycleEvent
from .local_ingress import EndpointInUseError, LocalAdministrativeIngress

logger = logging.getLogger(__name__)


# After readiness produced no progress (for example accept() keeps failing because the process is out of
# file descriptors) the listening socket is ignored for this long. Without it a socket that stays readable
# would make the idle wait spin at full CPU. Requests are delayed by at most this much in that fault case.
INGRESS_NO_PROGRESS_BACKOFF_SECONDS = 0.25

# Controlled shutdown: after the endpoint is withdrawn, clients already queued are served once, for at most
# this long (plus one connection's read and write deadlines), within the usual ingress limits. Shutdown is
# never extended to drain the backlog: whoever is still queued afterwards gets no answer.
SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS = 0.25


class RequestClass(enum.Enum):
    """What kind of request this is (see "Request Classes" in the architecture documentation).

    Operational commands, which change physical or logical operating state and need the durable
    command journal, are deliberately absent: none is implemented yet, and none may be added here
    without that journal.
    """

    QUERY = "query"  # read-only and stateless: no journal, nothing persisted, nothing replayed
    LIFECYCLE_CONTROL = "lifecycle-control"  # changes the Engine process's own lifecycle (STOP)


@dataclass(frozen=True)
class SupportedOperation:
    request_class: RequestClass
    process: Callable[[AdministrativeRequest, PeerIdentity], AdministrativeResponse]


class LifecycleState(enum.Enum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class ShutdownReason(enum.StrEnum):
    OPERATOR_STOP = "OPERATOR_STOP"  # lifecycle-control request received through the administrative IPC
    SIGNAL = "SIGNAL"  # SIGINT / SIGTERM: not an IPC request, and not proof of operator authorization
    FAILURE = "FAILURE"  # unexpected failure


class ExitStatus(enum.IntEnum):
    SUCCESS = 0  # controlled shutdown completed and cleanup succeeded
    FAILURE = 1  # unexpected failure
    # 2 is left to command-line usage errors (argparse convention)
    ALREADY_RUNNING = 3  # another Engine owns the configured endpoint
    CLEANUP_FAILED = 4  # shutdown was requested and carried out, but cleanup reported an error


@dataclass(frozen=True)
class EngineConfig:
    cycle_interval_seconds: float = 1.0
    # Linux users allowed to use the administrative interface, in addition to filesystem
    # permissions. Default: the Engine's own user and root.
    administrative_uids: frozenset[int] = field(default_factory=lambda: frozenset({os.geteuid(), 0}))

    def __post_init__(self) -> None:
        if not 0.01 <= self.cycle_interval_seconds <= 60:
            raise ValueError("cycle interval must be between 0.01 and 60 seconds")


class Engine:
    def __init__(
        self,
        *,
        execution_id: str,
        config: EngineConfig,
        ingress: LocalAdministrativeIngress,
        audit: LifecycleAudit,
        clock=None,
    ):
        self._execution_id = execution_id
        self._config = config
        self._ingress = ingress
        self._audit = audit
        self._clock = clock if clock is not None else SystemClock()

        self._state = LifecycleState.CREATED
        self._stop_requested = threading.Event()  # the stop flag only; no threads are created
        self._idle_wait = IdleWait()
        self._supported_operations = {
            OPERATION_PING: SupportedOperation(RequestClass.QUERY, self._answer_ping_query),
            OPERATION_ECHO: SupportedOperation(RequestClass.QUERY, self._answer_echo_query),
            OPERATION_STOP: SupportedOperation(RequestClass.LIFECYCLE_CONTROL, self._handle_stop_request),
        }
        self._stop_reason: ShutdownReason | None = None
        self._stop_context: dict = {}
        self._failure: Exception | None = None
        self._shutdown_attempted = False
        self._exit_status: ExitStatus | None = None
        self._startup_count = 0
        self._cycles_completed = 0
        self._current_phase: CyclePhase | None = None  # set while a phase runs; left set if it raises
        self._current_cycle_number: int | None = None  # the cycle in progress; left set if it raises
        # Opt-in repeat suppression: one predefined category per phase (a persistently slow phase
        # would otherwise warn every cycle). Nothing else in the Engine is suppressed.
        self._phase_overrun_reports = {
            phase: RepeatedWarningReporter(
                logger,
                f"phase-overrun:{phase}",
                f"operating-cycle phase {phase} exceeded its time limit",
                clock=self._clock,
            )
            for phase in CyclePhase
        }

    # ------------------------------------------------------------------ observation

    @property
    def state(self) -> LifecycleState:
        return self._state

    @property
    def startup_count(self) -> int:
        return self._startup_count

    @property
    def cycles_completed(self) -> int:
        return self._cycles_completed

    # ------------------------------------------------------------------ lifecycle coordinator

    def run(self) -> ExitStatus:
        """Run one complete Engine execution and return the process exit status."""
        try:
            self.startup()
            self._run_until_stop_requested()
        except KeyboardInterrupt:
            # Only reachable if no SIGINT handler was installed; treated like SIGINT.
            self.request_stop_for_signal(signal.SIGINT)
        except Exception as exc:
            self._record_failure(exc)
        return self._shutdown()

    def startup(self) -> None:
        """One-time startup. Establishes the audit history and the administrative endpoint."""
        if self._state is not LifecycleState.CREATED:
            raise RuntimeError("an Engine can be started only once")
        self._state = LifecycleState.STARTING
        self._startup_count += 1

        self._audit.open()
        self._audit.record(
            LifecycleEvent.ENGINE_STARTING,
            context={
                "engine_pid": os.getpid(),
                "endpoint": str(self._ingress.endpoint),
                "audit_file": str(self._audit.path),
                "cycle_interval_seconds": self._config.cycle_interval_seconds,
            },
        )
        logger.info("Engine starting (execution %s, pid %d)", self._execution_id, os.getpid())
        logger.info(
            "Development foundation only: no hardware control, no persistence of operating state, "
            "no state reconciliation is performed."
        )

        self._ingress.open()
        self._idle_wait.open(self._ingress.readiness_source)

        self._state = LifecycleState.RUNNING
        self._audit.record(LifecycleEvent.ENGINE_RUNNING)
        logger.info(
            "Engine entering continuous operation (cycle interval %.3f s, endpoint %s)",
            self._config.cycle_interval_seconds,
            self._ingress.endpoint,
        )

    def _run_until_stop_requested(self) -> None:
        while not self._stop_requested.is_set():
            cycle_started = self._clock.monotonic()
            self.run_operating_cycle()
            self._wait_for_next_cycle(cycle_started)

    def run_operating_cycle(self) -> None:
        """Execute exactly one operating cycle: every phase once, in this order.

        Only INGEST_EXTERNAL_INTENTS does real work so far; the other phases are placeholders that
        do nothing. The cycle performs no physical control.

        An exception from a phase ends the cycle there: the remaining phases do not run, the cycle
        is not counted, and the exception reaches ``run``, which takes the failure path. A stop
        request does not interrupt a cycle; it is checked between cycles (every phase is bounded).

        The cycle number is fixed once, here. It labels every diagnostic record of the cycle and,
        if the cycle fails, the failure record; that is why it is not cleared when a phase raises.
        """
        self._current_cycle_number = self._cycles_completed + 1
        log_context.cycle = self._current_cycle_number
        self._run_phase(CyclePhase.ACQUIRE_OBSERVATIONS, self._acquire_observations)
        self._run_phase(CyclePhase.RECONCILE_STATE, self._reconcile_state)
        self._run_phase(CyclePhase.INGEST_EXTERNAL_INTENTS, self._ingest_external_intents)
        self._run_phase(CyclePhase.EVALUATE_SCHEDULED_WORK, self._evaluate_scheduled_work)
        self._run_phase(CyclePhase.EVALUATE_CONTROL_POLICIES, self._evaluate_control_policies)
        self._run_phase(CyclePhase.EXECUTE_READY_COMMANDS, self._execute_ready_commands)
        self._run_phase(CyclePhase.FINALIZE_CYCLE, self._finalize_cycle)
        self._cycles_completed += 1
        logger.debug("operating cycle %d completed", self._cycles_completed)
        self._current_cycle_number = None
        log_context.cycle = None

    def _run_phase(self, phase: CyclePhase, perform_phase: Callable[[], None]) -> None:
        """Run one phase, and warn if it took longer than its time limit.

        The limit is a cooperative budget: a running phase cannot be interrupted, so an overrun is
        detected afterwards and reported, and the Engine carries on. A phase that stays slow would
        warn every cycle, so overruns are reported through a repeat-suppressing reporter (one
        category per phase). If the phase raises, the current phase stays recorded, so the failure
        record can name it.
        """
        self._current_phase = phase
        started = self._clock.monotonic()
        perform_phase()
        elapsed = self._clock.monotonic() - started
        self._current_phase = None
        time_limit = PHASE_TIME_LIMITS_SECONDS[phase]
        if elapsed > time_limit:
            self._phase_overrun_reports[phase].report(
                f"{elapsed:.3f} s elapsed, limit {time_limit:.3f} s (cycle {self._current_cycle_number})"
            )

    # ------------------------------------------------------------------ operating-cycle phases

    def _acquire_observations(self) -> None:
        """NOT IMPLEMENTED: placeholder. Reads no sensor and records nothing."""

    def _reconcile_state(self) -> None:
        """NOT IMPLEMENTED: placeholder. Keeps no system state and persists nothing."""

    def _ingest_external_intents(self) -> None:
        """Service the local administrative requests that are waiting, within the ingress bounds.

        The ingress stops starting new connections after its own admission budget, which together
        with one connection's read and write deadlines fits this phase's time limit.
        """
        self._ingress.service_pending_requests(self._handle_administrative_request)

    def _evaluate_scheduled_work(self) -> None:
        """NOT IMPLEMENTED: placeholder. There is no scheduler; no command is created."""

    def _evaluate_control_policies(self) -> None:
        """NOT IMPLEMENTED: placeholder. No control policy exists; nothing is decided."""

    def _execute_ready_commands(self) -> None:
        """NOT IMPLEMENTED: placeholder. Executes nothing and touches no hardware.

        When implemented, this phase may execute only commands from the durable command journal,
        through the unified command path (see "Unified Command Execution Architecture").
        """

    def _finalize_cycle(self) -> None:
        """NOT IMPLEMENTED: placeholder. Persists no state and refreshes no health heartbeat."""

    def _wait_for_next_cycle(self, cycle_started: float) -> None:
        """Idle until the next cycle is due, answering client requests that arrive meanwhile.

        The deadline is fixed once, from the start of the cycle that just ran, and never moves:
        a request is answered sooner, it does not make a cycle start sooner or later (beyond the
        bounded servicing time below). Servicing here is the same bounded, Engine-owned path as
        inside a cycle; it runs no operating cycle. The wait ends early on a stop request.
        """
        deadline = cycle_started + self._config.cycle_interval_seconds
        ignore_ingress_until = 0.0
        while not self._stop_requested.is_set():
            now = self._clock.monotonic()
            if now >= deadline:
                return
            backing_off = now < ignore_ingress_until
            wake_at = min(deadline, ignore_ingress_until) if backing_off else deadline
            if not self._idle_wait.wait(wake_at - now, watch_ingress=not backing_off):
                continue  # timed out or woken: re-check the stop flag and the deadline
            if self._stop_requested.is_set():
                return
            time_left = deadline - self._clock.monotonic()
            if time_left <= 0:
                return  # the cycle is due; whatever is waiting is serviced inside it
            handled = self._ingress.service_pending_requests(self._handle_administrative_request, time_left)
            if handled == 0:
                logger.debug("IPC socket reported ready but no request was serviced; backing off")
                ignore_ingress_until = self._clock.monotonic() + INGRESS_NO_PROGRESS_BACKOFF_SECONDS

    # ------------------------------------------------------------------ stop requests

    def request_stop_for_signal(self, signal_number: int) -> None:
        """Called from a signal handler: record the cause and set the stop flag. Nothing else."""
        self._request_stop(
            ShutdownReason.SIGNAL,
            {
                "signal": signal.Signals(signal_number).name,
                "signal_number": int(signal_number),
                # A handled signal does not show that an operator authorized it.
                "operator_authorization": "not_established",
            },
        )

    def _request_stop(self, reason: ShutdownReason, context: dict) -> bool:
        """Set the stop flag and wake the idle wait. The first reason wins. Return True if this call initiated the stop."""
        if self._stop_requested.is_set():
            return False
        self._stop_reason = reason
        self._stop_context = context
        self._stop_requested.set()  # the flag first: it is the truth; the wake-up only shortens a wait
        self._idle_wait.wake()
        return True

    def _handle_administrative_request(
        self, request: AdministrativeRequest, peer: PeerIdentity | None
    ) -> AdministrativeResponse:
        """The Engine's one entry point for requests received through the administrative IPC.

        Every request is authorized, dispatched to a supported operation and answered, in that
        order. The answer carries the request's correlation identifier. Nothing a client sends
        can make this method raise or stop the Engine except an authorized STOP.
        """
        response = self._authorize_and_dispatch(request, peer)
        logger.info(
            "IPC request processed by the Engine: op=%r id=%r outcome=%s",
            request.operation,
            request.correlation_id,
            "ok" if response.ok else response.error_code,
        )
        return replace(response, correlation_id=request.correlation_id)

    def _authorize_and_dispatch(
        self, request: AdministrativeRequest, peer: PeerIdentity | None
    ) -> AdministrativeResponse:
        if peer is None or peer.uid not in self._config.administrative_uids:
            logger.warning("administrative request refused: peer %s is not authorized", peer)
            return AdministrativeResponse(
                False,
                error_code=ERROR_PEER_NOT_AUTHORIZED,
                message="the connecting Linux user is not authorized for administration",
            )
        operation = self._supported_operations.get(request.operation)
        if operation is None:
            logger.warning("administrative request refused: unsupported operation %r", request.operation)
            return AdministrativeResponse(
                False,
                error_code=ERROR_UNSUPPORTED_OPERATION,
                message=f"unsupported operation {request.operation!r}",
            )
        return operation.process(request, peer)

    # ------------------------------------------------------------------ queries (read-only, stateless)

    def _answer_ping_query(self, request: AdministrativeRequest, peer: PeerIdentity) -> AdministrativeResponse:
        """PING: report that the Engine is reachable, from the Engine's own state. Changes nothing."""
        if request.arguments:
            return self._refuse_arguments("ping takes no arguments")
        return AdministrativeResponse(
            True,
            result={
                "reply": "pong",
                "execution_id": self._execution_id,
                "engine_pid": os.getpid(),
                "cycles_completed": self._cycles_completed,  # completed cycles: one in progress is not counted
                "engine_time_utc": format_utc(self._clock.utc_now()),
            },
        )

    def _answer_echo_query(self, request: AdministrativeRequest, peer: PeerIdentity) -> AdministrativeResponse:
        """ECHO: return the validated text unchanged. Changes nothing, and the text is never logged."""
        arguments = request.arguments
        if not isinstance(arguments, dict) or set(arguments) != {"text"}:
            return self._refuse_arguments("echo requires exactly one argument: 'text'")
        text = arguments["text"]
        if not isinstance(text, str):
            return self._refuse_arguments("argument 'text' must be a string")
        if len(text) > MAX_ECHO_TEXT_LENGTH:
            return self._refuse_arguments(f"argument 'text' must have at most {MAX_ECHO_TEXT_LENGTH} characters")
        try:
            text.encode("utf-8")
        except UnicodeEncodeError:
            return self._refuse_arguments("argument 'text' is not valid Unicode text (unpaired surrogate)")
        return AdministrativeResponse(True, result={"text": text})

    @staticmethod
    def _refuse_arguments(message: str) -> AdministrativeResponse:
        logger.warning("administrative request refused: invalid arguments: %s", message)
        return AdministrativeResponse(False, error_code=ERROR_INVALID_ARGUMENTS, message=message)

    # ------------------------------------------------------------------ lifecycle control

    def _handle_stop_request(self, request: AdministrativeRequest, peer: PeerIdentity) -> AdministrativeResponse:
        """STOP is a lifecycle-control request: audited as evidence, acted on once, never journaled or replayed."""
        if request.arguments:
            return self._refuse_arguments("stop takes no arguments")
        requester = {"pid": peer.pid, "uid": peer.uid, "gid": peer.gid}
        already_initiated = self._stop_requested.is_set()
        client_time = request.client_submitted_at
        audit_recorded = self._audit.record(
            LifecycleEvent.STOP_REQUEST_RECEIVED,
            requester=requester,
            context={
                "engine_receipt_time_utc": format_utc(self._clock.utc_now()),
                "client_reported_submission_time_utc": format_utc(client_time) if client_time else None,
                "shutdown_already_initiated": already_initiated,
            },
        )
        logger.info("operator STOP request received (uid %d, pid %d)", peer.uid, peer.pid)
        if already_initiated:
            return AdministrativeResponse(True, status=STATUS_SHUTDOWN_ALREADY_INITIATED, audit_recorded=audit_recorded)
        self._request_stop(ShutdownReason.OPERATOR_STOP, {"requester": requester})
        return AdministrativeResponse(True, status=STATUS_SHUTDOWN_INITIATED, audit_recorded=audit_recorded)

    # ------------------------------------------------------------------ failure and shutdown

    def _record_failure(self, exc: Exception) -> None:
        phase = {LifecycleState.STARTING: "startup", LifecycleState.STOPPING: "shutdown"}.get(self._state, "operation")
        context = {"phase": phase, "message": str(exc)}
        if self._current_phase is not None:
            # The failure happened inside an operating-cycle phase: name the phase and the cycle. The
            # cycle number is the one the cycle's diagnostic records carry (still set: see run_operating_cycle).
            context["cycle_phase"] = str(self._current_phase)
            context["cycle_number"] = self._current_cycle_number
        self._failure = exc
        # A failure outranks any stop already requested: the outcome must not look like a clean stop.
        self._stop_reason = ShutdownReason.FAILURE
        self._stop_context = {}
        self._stop_requested.set()
        where = phase
        if "cycle_phase" in context:
            where += f", cycle phase {context['cycle_phase']} of cycle {context['cycle_number']}"
        logger.error("Engine failure during %s: %s", where, exc, exc_info=exc)
        self._audit.record(LifecycleEvent.ENGINE_FAILURE, reason=type(exc).__name__, context=context)

    def _shutdown(self) -> ExitStatus:
        """Controlled shutdown and cleanup, attempted once. Return the process exit status."""
        if self._shutdown_attempted:
            assert self._exit_status is not None
            return self._exit_status
        self._shutdown_attempted = True
        self._state = LifecycleState.STOPPING
        log_context.cycle = None  # shutdown is not part of any cycle (a failed cycle's records are already written)

        reason = self._stop_reason or ShutdownReason.FAILURE
        context = dict(self._stop_context)
        requester = context.pop("requester", None)
        self._log_shutdown_cause(reason, context)
        self._audit.record(LifecycleEvent.SHUTDOWN_INITIATED, reason=reason.value, requester=requester, context=context)

        if reason is not ShutdownReason.FAILURE:
            self._serve_queued_requests_once()
        for report in self._phase_overrun_reports.values():
            report.flush()  # nothing suppressed goes unmentioned
        cleanup_succeeded = self._clean_up()
        # A failure during the final servicing pass makes the outcome a failure (it outranks the stop).
        self._exit_status = self._decide_exit_status(self._stop_reason or reason, cleanup_succeeded)
        self._state = LifecycleState.STOPPED

        self._audit.record(
            LifecycleEvent.SHUTDOWN_COMPLETED,
            reason=reason.value,
            context={
                "cleanup_succeeded": cleanup_succeeded,
                "exit_status": int(self._exit_status),
                "cycles_completed": self._cycles_completed,
            },
        )
        self._audit.close()
        log_handler_failure_counts()
        logger.info("Engine shutdown completed (exit status %d)", int(self._exit_status))
        return self._exit_status

    def _serve_queued_requests_once(self) -> None:
        """Controlled shutdown: withdraw the endpoint, then serve the clients already queued, once and bounded.

        New clients can no longer connect (they find no endpoint). Those already queued are served
        through the normal path, with its limits and authorization, for at most
        SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS: a STOP is answered "already initiated" (and audited),
        a query is answered. Nothing is promised beyond that; clients still queued afterwards get no
        answer when the listener closes. Skipped after an Engine failure.

        An unexpected exception here is an Engine failure like any other (fail-stop): recorded as
        ENGINE_FAILURE with phase "shutdown", and the shutdown then continues with cleanup.
        """
        try:
            try:
                self._ingress.withdraw_endpoint()
            except OSError as exc:  # not hidden: cleanup tries again and reports a failure as a cleanup failure
                logger.warning("withdrawing the IPC endpoint failed (%s); it is removed again during cleanup", exc)
            handled = self._ingress.service_pending_requests(
                self._handle_administrative_request, SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS
            )
        except Exception as exc:  # the fail-stop path, not containment: recorded as ENGINE_FAILURE, exit status 1
            self._record_failure(exc)
            return
        if handled:
            logger.info("shutdown: %d queued IPC connection(s) served before the endpoint closes", handled)

    @staticmethod
    def _log_shutdown_cause(reason: ShutdownReason, context: dict) -> None:
        if reason is ShutdownReason.OPERATOR_STOP:
            logger.info("shutdown initiated: operator-requested STOP")
        elif reason is ShutdownReason.SIGNAL:
            logger.info(
                "shutdown initiated: external interruption by %s (operator authorization not established)",
                context.get("signal"),
            )
        else:
            logger.error("shutdown initiated: after an Engine failure")

    def _clean_up(self) -> bool:
        """Release the resources this Engine acquired. Best effort; failures are logged, never hidden."""
        succeeded = True
        for description, release in (
            ("administrative IPC endpoint", self._ingress.close),
            ("idle-wait channel", self._idle_wait.close),
        ):
            try:
                release()
            except Exception:
                logger.exception("cleanup of the %s failed", description)
                succeeded = False
        return succeeded

    def _decide_exit_status(self, reason: ShutdownReason, cleanup_succeeded: bool) -> ExitStatus:
        if reason is ShutdownReason.FAILURE:
            if isinstance(self._failure, EndpointInUseError):
                return ExitStatus.ALREADY_RUNNING
            return ExitStatus.FAILURE
        if not cleanup_succeeded:
            return ExitStatus.CLEANUP_FAILED
        return ExitStatus.SUCCESS
