"""Automated tests for the ape administrative CLI tool.

Tests cover:
- Successful stop request to a running Engine (exit code 0)
- Failure when Engine is not running / socket does not exist (exit code 1)
- Permission denied handling when accessing socket (exit code 2)
- Socket communication timeout handling (exit code 3)
- CLI invocation without -x flag displaying usage error
"""

from __future__ import annotations

import io
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from aquaponics_controller.cli.main import main as run_cli
from aquaponics_controller.engine.lifecycle import AquaponicsEngine, LifecycleState


class TestApeCli(unittest.TestCase):
    """Test suite for the ape administrative CLI."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.socket_path = Path(self.temp_dir.name) / "test_cli.sock"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_cli_success_path_stops_running_engine(self) -> None:
        """ape -x should communicate with running Engine, accept stop, and exit 0."""
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            cycle_interval_seconds=0.01,
        )

        engine_thread = threading.Thread(target=engine.run, daemon=True)
        engine_thread.start()

        # Wait briefly for Engine startup to complete
        for _ in range(50):
            if engine.state == LifecycleState.RUNNING and self.socket_path.exists():
                break
            time.sleep(0.01)

        self.assertEqual(engine.state, LifecycleState.RUNNING)

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with patch("sys.stdout", stdout_capture), patch("sys.stderr", stderr_capture):
            exit_code = run_cli(["-x", "--socket-path", str(self.socket_path)])

        engine_thread.join(timeout=2.0)

        self.assertEqual(exit_code, 0)
        self.assertIn("stop request accepted", stdout_capture.getvalue())
        self.assertEqual(engine.state, LifecycleState.STOPPED)
        self.assertTrue(engine.is_operator_stop)

    def test_cli_engine_not_running_socket_missing(self) -> None:
        """ape -x on nonexistent socket should fail with code 1 and helpful error."""
        nonexistent = Path(self.temp_dir.name) / "nonexistent.sock"

        stderr_capture = io.StringIO()
        with patch("sys.stderr", stderr_capture):
            exit_code = run_cli(["-x", "--socket-path", str(nonexistent)])

        self.assertEqual(exit_code, 1)
        self.assertIn("Engine is not running", stderr_capture.getvalue())

    def test_cli_permission_denied_handled_with_exit_code_2(self) -> None:
        """ape -x encountering PermissionError must output clear error and exit 2."""
        # Create a dummy socket file path
        dummy_file = self.socket_path
        dummy_file.write_text("12345")

        stderr_capture = io.StringIO()
        with patch("socket.socket.connect", side_effect=PermissionError("Access denied")):
            with patch("sys.stderr", stderr_capture):
                exit_code = run_cli(["-x", "--socket-path", str(dummy_file)])

        self.assertEqual(exit_code, 2)
        self.assertIn("Permission denied", stderr_capture.getvalue())

    def test_cli_communication_timeout_handled_with_exit_code_3(self) -> None:
        """ape -x encountering a hung connection must time out cleanly with exit code 3."""
        # Spin up a server socket that accepts but never sends data
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        self.socket_path.write_text(str(port), encoding="utf-8")

        def accept_and_hang() -> None:
            try:
                conn, _ = server.accept()
                time.sleep(0.5)
                conn.close()
            except OSError:
                pass

        hang_thread = threading.Thread(target=accept_and_hang, daemon=True)
        hang_thread.start()

        stderr_capture = io.StringIO()
        try:
            with patch("sys.stderr", stderr_capture):
                exit_code = run_cli(
                    ["-x", "--socket-path", str(self.socket_path), "--timeout", "0.1"]
                )

            self.assertEqual(exit_code, 3)
            self.assertIn("timed out", stderr_capture.getvalue().lower())
        finally:
            server.close()
            hang_thread.join(timeout=1.0)

    def test_cli_missing_action_flag_prints_help_and_exits(self) -> None:
        """Invoking ape without -x/--stop prints help and exits with error code."""
        stderr_capture = io.StringIO()
        with patch("sys.stderr", stderr_capture):
            exit_code = run_cli([])

        self.assertEqual(exit_code, 4)
        self.assertIn("usage", stderr_capture.getvalue().lower())


if __name__ == "__main__":
    unittest.main()
