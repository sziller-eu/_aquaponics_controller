"""Engine lifecycle management and autonomous control loop.

This module provides the central AquaponicsEngine class, coordinating startup,
continuous operating cycles, external intent processing, prompt idle waiting,
and controlled cleanup.
"""

from __future__ import annotations

from enum import Enum
import logging
from pathlib import Path
import select
import threading
import time
from typing import Final

from aquaponics_controller.engine.ingress import EngineIngress
from aquaponics_controller.ipc import (
    DEFAULT_CYCLE_INTERVAL_SECONDS,
    resolve_socket_path,
)

logger = logging.getLogger("aquaponics.engine")


class LifecycleState(str, Enum):
    """Explicit lifecycle states of the Aquaponics Engine."""

    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


# Maximum time slice in seconds for idle waiting before re-checking stop conditions
MAX_WAIT_SLICE_SECONDS: Final[float] = 0.05


class AquaponicsEngine:
    """Authoritative autonomous Aquaponics controller.

    Separates lifecycle phases:
    1. startup: runs once to initialize subsystems and open ingress.
    2. run_loop: coordinates cycles and prompt bounded idle waiting.
    3. run_cycle: executes a single control cycle following the Zoom Principle.
    4. request_stop: registers administrative or signal shutdown requests.
    5. cleanup: performs idempotent best-effort teardown and logs completion.
    """

    def __init__(
        self,
        socket_path: Path | str | None = None,
        cycle_interval_seconds: float = DEFAULT_CYCLE_INTERVAL_SECONDS,
        ingress: EngineIngress | None = None,
    ) -> None:
        """Initialize the Engine with required configuration and dependencies.

        Args:
            socket_path: Optional explicit Unix domain socket path.
            cycle_interval_seconds: Desired interval in seconds between cycle invocations.
            ingress: Optional custom EngineIngress instance for dependency injection.
        """
        self._socket_path = resolve_socket_path(socket_path)
        self._cycle_interval_seconds = max(0.001, cycle_interval_seconds)

        if ingress is not None:
            self._ingress = ingress
        else:
            self._ingress = EngineIngress(socket_path=self._socket_path)

        self._state: LifecycleState = LifecycleState.CREATED
        self._startup_executed: bool = False
        self._cleanup_executed: bool = False

        self._stop_requested: bool = False
        self._stop_reason: str | None = None
        self._is_operator_stop: bool = False

        self._cycle_count: int = 0
        self._wake_event: threading.Event = threading.Event()

    @property
    def state(self) -> LifecycleState:
        """Return the current engine lifecycle state."""
        return self._state

    @property
    def cycle_count(self) -> int:
        """Return the number of completed operating cycles."""
        return self._cycle_count

    @property
    def stop_requested(self) -> bool:
        """Return whether a shutdown has been requested."""
        return self._stop_requested

    @property
    def is_operator_stop(self) -> bool:
        """Return whether the stop request originated from an operator."""
        return self._is_operator_stop

    @property
    def ingress(self) -> EngineIngress:
        """Return the active ingress instance."""
        return self._ingress

    def startup(self) -> None:
        """Execute one-time engine startup.

        Prepares all subsystems before normal autonomous operation begins.

        Raises:
            RuntimeError: If startup has already executed or if initialization fails.
        """
        if self._startup_executed:
            raise RuntimeError("Engine startup has already been executed for this instance")

        self._state = LifecycleState.STARTING
        logger.info("Engine startup initiated")

        self._load_configuration()
        self._initialize_persistence()
        self._initialize_hardware()
        self._initialize_ingress()
        self._reconcile_startup_state()

        self._startup_executed = True
        self._state = LifecycleState.RUNNING
        logger.info("Engine startup completed, entering normal autonomous operation")

    def run_loop(self) -> None:
        """Coordinate continuous operating cycles until shutdown is requested.

        Ensures external requests are processed without busy-spinning or starving
        autonomous cycles.
        """
        logger.info("Continuous autonomous operation started")

        while self._state == LifecycleState.RUNNING and not self._stop_requested:
            self.run_cycle()

            if self._stop_requested:
                self._state = LifecycleState.STOPPING
                break

            self._wait_for_next_cycle(self._cycle_interval_seconds)

            if self._stop_requested:
                self._state = LifecycleState.STOPPING
                break

    def run_cycle(self) -> None:
        """Execute a single operating cycle.

        High-level sequence representing the autonomous control process:
        - acquire observations (sensors)
        - reconcile system state
        - ingest external intents (CLI / API ingress)
        - materialize scheduled commands
        - evaluate control policies
        - execute ready commands
        - persist cycle state
        - update health heartbeat
        """
        self._acquire_observations()
        self._reconcile_system_state()
        self._ingest_external_intents()
        self._materialize_due_scheduled_commands()
        self._evaluate_control_policies()
        self._execute_ready_commands()
        self._persist_cycle_state()
        self._update_health_heartbeat()

        self._cycle_count += 1
        logger.debug("Operating cycle #%d completed", self._cycle_count)

    def request_stop(self, reason: str, is_operator: bool = False) -> None:
        """Request orderly termination of the Engine.

        Args:
            reason: Human-readable explanation for the stop request.
            is_operator: True if requested by an administrative operator (e.g. ape CLI).
        """
        self._stop_requested = True
        self._stop_reason = reason
        self._is_operator_stop = is_operator
        self._wake_event.set()

        if is_operator:
            logger.info("Operator-requested shutdown: %s", reason)
        else:
            logger.info("External interruption or shutdown requested: %s", reason)

    def _wait_for_next_cycle(self, duration_seconds: float) -> None:
        """Perform prompt bounded idle waiting.

        Wakes up immediately if an incoming IPC connection arrives, if request_stop()
        is invoked, or when duration_seconds elapses. Avoids busy-spinning.

        Args:
            duration_seconds: Maximum seconds to wait before running the next cycle.
        """
        if duration_seconds <= 0 or self._stop_requested:
            return

        deadline = time.monotonic() + duration_seconds
        server_sock = self._ingress.server_socket

        while not self._stop_requested:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            slice_timeout = min(remaining, MAX_WAIT_SLICE_SECONDS)

            # Check if server socket has incoming connections waiting
            if server_sock is not None:
                try:
                    readable, _, _ = select.select([server_sock], [], [], slice_timeout)
                    if readable:
                        # An incoming connection arrived! Wake up promptly to process it
                        break
                except (OSError, ValueError):
                    self._wake_event.wait(timeout=slice_timeout)
            else:
                self._wake_event.wait(timeout=slice_timeout)

        self._wake_event.clear()

    def cleanup(self) -> None:
        """Perform one-time controlled teardown and resource release.

        Best-effort cleanup that guarantees execution even if previous operations
        encountered exceptions.
        """
        if self._cleanup_executed:
            return

        self._cleanup_executed = True
        logger.info("Engine shutdown initiated")

        # 1. Close ingress listener and cleanup socket files
        try:
            self._ingress.close()
        except Exception as exc:
            logger.error("Error closing ingress during cleanup: %s", exc, exc_info=True)

        # 2. Release hardware handles (placeholder for issue #6+)
        self._release_hardware()

        # 3. Flush persistence (placeholder for issue #5+)
        self._flush_persistence()

        if self._state != LifecycleState.FAILED:
            self._state = LifecycleState.STOPPED

        logger.info("Shutdown complete")

    def run(self) -> int:
        """Run the full engine lifecycle from startup to shutdown.

        Returns:
            0 on successful or operator-requested termination; nonzero on unexpected failure.
        """
        try:
            self.startup()
            self.run_loop()
            return 0
        except Exception as exc:
            self._state = LifecycleState.FAILED
            logger.error("Unexpected engine failure: %s", exc, exc_info=True)
            return 1
        finally:
            self.cleanup()

    # -------------------------------------------------------------------------
    # Internal Lifecycle Steps (Zoom Principle)
    # -------------------------------------------------------------------------

    def _load_configuration(self) -> None:
        """Load and validate engine configuration."""
        logger.debug("Configuration loaded")

    def _initialize_persistence(self) -> None:
        """Initialize database connections (placeholder for issue #5)."""
        logger.debug("Persistence initialized (provisional in-memory)")

    def _initialize_hardware(self) -> None:
        """Initialize hardware adapters (placeholder for physical control issues)."""
        logger.debug("Hardware abstraction initialized (no physical peripherals)")

    def _initialize_ingress(self) -> None:
        """Open and bind the IPC ingress listener."""
        self._ingress.start()

    def _reconcile_startup_state(self) -> None:
        """Reconcile persisted state with hardware state on startup."""
        logger.debug("Startup state reconciled")

    def _acquire_observations(self) -> None:
        """Acquire latest observations from sensor adapters."""
        # Intentionally does no physical control in Issue #4

    def _reconcile_system_state(self) -> None:
        """Reconcile system state based on recent observations."""
        # Intentionally does no physical control in Issue #4

    def _ingest_external_intents(self) -> None:
        """Service pending external requests via ingress without blocking indefinitely."""
        self._ingress.process_pending_intents(engine=self)

    def _materialize_due_scheduled_commands(self) -> None:
        """Check schedules and materialize due commands into the command pipeline."""
        # Scheduling framework is out of scope for Issue #4

    def _evaluate_control_policies(self) -> None:
        """Evaluate autonomous control policies against current system state."""
        # Physical control policies are out of scope for Issue #4

    def _execute_ready_commands(self) -> None:
        """Execute ready commands from the command journal."""
        # Command executor is out of scope for Issue #4

    def _persist_cycle_state(self) -> None:
        """Persist required cycle state to durable storage."""
        # Database persistence is out of scope for Issue #4

    def _update_health_heartbeat(self) -> None:
        """Update runtime health status and heartbeat."""
        # Health reporting is out of scope for Issue #4

    def _release_hardware(self) -> None:
        """Safely release physical hardware handles."""
        logger.debug("Hardware handles released")

    def _flush_persistence(self) -> None:
        """Flush unwritten state to persistent storage."""
        logger.debug("Persistence flushed")

