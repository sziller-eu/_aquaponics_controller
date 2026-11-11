"""Automated tests for Aquaponics Engine lifecycle management.

Tests cover:
- startup called once (and only once)
- continuous cycles repeating without busy-spinning
- stop request ending loop and running cleanup once
- exit code 0 on operator stop
- signal handling (SIGINT/SIGTERM) triggering graceful shutdown
- unexpected exception handling: logged failure, cleanup attempt, nonzero exit
- prevention of duplicate Engine instances on the same socket
"""

from __future__ import annotations

import logging
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from aquaponics_controller.engine.ingress import EngineIngress
from aquaponics_controller.engine.lifecycle import AquaponicsEngine, LifecycleState
from aquaponics_controller.ipc import COMMAND_STOP, STATUS_ACCEPTED


class TestEngineLifecycle(unittest.TestCase):
    """Test suite for AquaponicsEngine lifecycle responsibilities."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.socket_path = Path(self.temp_dir.name) / "test_engine.sock"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_startup_runs_exactly_once(self) -> None:
        """Startup should succeed once and transition to RUNNING; second call raises RuntimeError."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.01,
        )
        self.assertEqual(engine.state, LifecycleState.CREATED)

        engine.startup()
        self.assertEqual(engine.state, LifecycleState.RUNNING)
        self.assertTrue(engine.ingress.socket_path.exists())

        # Second startup call must be rejected
        with self.assertRaises(RuntimeError) as ctx:
            engine.startup()
        self.assertIn("already been executed", str(ctx.exception))

        engine.cleanup()
        self.assertEqual(engine.state, LifecycleState.STOPPED)

    def test_cycle_runs_repeatedly(self) -> None:
        """The run loop should execute multiple cycles consecutively."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.005,
        )
        engine.startup()

        # Run loop in background thread
        thread = threading.Thread(target=engine.run_loop, daemon=True)
        thread.start()

        # Allow several cycles to execute
        time.sleep(0.05)
        self.assertGreaterEqual(engine.cycle_count, 3)

        engine.request_stop("Testing multiple cycles", is_operator=True)
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())

        engine.cleanup()
        self.assertEqual(engine.state, LifecycleState.STOPPED)

    def test_stop_request_ends_loop_and_runs_cleanup_once(self) -> None:
        """An operator stop request terminates the loop and runs cleanup exactly once."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.005,
        )

        cleanup_mock = MagicMock(wraps=engine.cleanup)
        engine.cleanup = cleanup_mock  # type: ignore[method-assign]

        def stop_after_delay() -> None:
            time.sleep(0.02)
            engine.request_stop("Operator stop request", is_operator=True)

        stopper_thread = threading.Thread(target=stop_after_delay, daemon=True)
        stopper_thread.start()

        exit_code = engine.run()
        stopper_thread.join(timeout=1.0)

        self.assertEqual(exit_code, 0)
        self.assertTrue(engine.stop_requested)
        self.assertTrue(engine.is_operator_stop)
        self.assertEqual(engine.state, LifecycleState.STOPPED)
        # Cleanup called exactly once
        self.assertEqual(cleanup_mock.call_count, 1)

    def test_exit_code_zero_on_operator_stop(self) -> None:
        """Operator-requested shutdown is not an error and must exit with code 0."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.005,
        )

        def trigger_operator_stop() -> None:
            time.sleep(0.02)
            engine.request_stop("Admin CLI stop", is_operator=True)

        stopper = threading.Thread(target=trigger_operator_stop, daemon=True)
        stopper.start()

        exit_code = engine.run()
        stopper.join(timeout=1.0)
        self.assertEqual(exit_code, 0)

    def test_sigint_sigterm_triggers_graceful_shutdown(self) -> None:
        """Simulated signal interruption should trigger graceful stop and exit code 0."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.005,
        )

        def simulate_signal() -> None:
            time.sleep(0.02)
            engine.request_stop("Received signal SIGTERM", is_operator=False)

        signaler = threading.Thread(target=simulate_signal, daemon=True)
        signaler.start()

        exit_code = engine.run()
        signaler.join(timeout=1.0)

        self.assertEqual(exit_code, 0)
        self.assertTrue(engine.stop_requested)
        self.assertFalse(engine.is_operator_stop)
        self.assertEqual(engine.state, LifecycleState.STOPPED)

    def test_unexpected_exception_logged_cleanup_attempted_nonzero_exit(self) -> None:
        """Unexpected crash inside run_cycle must be logged, attempt cleanup, and return 1."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.005,
        )

        cleanup_called = False
        original_cleanup = engine.cleanup

        def wrapped_cleanup() -> None:
            nonlocal cleanup_called
            cleanup_called = True
            original_cleanup()

        engine.cleanup = wrapped_cleanup  # type: ignore[method-assign]

        # Patch internal policy method to raise unexpected exception
        def crash_in_cycle() -> None:
            raise RuntimeError("Fatal simulated database error")

        engine._evaluate_control_policies = crash_in_cycle  # type: ignore[method-assign]

        with self.assertLogs("aquaponics.engine", level="ERROR") as log_capture:
            exit_code = engine.run()

        self.assertEqual(exit_code, 1)
        self.assertEqual(engine.state, LifecycleState.FAILED)
        self.assertTrue(cleanup_called)
        self.assertTrue(any("Fatal simulated database error" in msg for msg in log_capture.output))

    def test_refuses_to_start_second_engine_on_active_socket(self) -> None:
        """Engine ingress must refuse to bind if another active Engine is already listening."""
        engine1 = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.01,
        )
        engine1.startup()

        # Attempt to start second engine on the exact same socket
        engine2 = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.01,
        )

        with self.assertRaises(RuntimeError) as ctx:
            engine2.startup()

        self.assertIn("already active and listening", str(ctx.exception))

        engine1.cleanup()


if __name__ == "__main__":
    unittest.main()

