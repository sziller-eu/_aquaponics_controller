"""Shared test helpers. Nothing here touches real hardware or the network.

Unix socket paths are limited to about 107 bytes, so tests use a short directory
under /tmp instead of pytest's (often long) ``tmp_path``.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import aquaponics_controller

SOURCE_DIRECTORY = str(Path(aquaponics_controller.__file__).resolve().parent.parent)

PROCESS_TIMEOUT_SECONDS = 15


def child_environment() -> dict:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = SOURCE_DIRECTORY
    # Never let a developer's configuration redirect a test to real endpoints or files.
    for name in ("AQUAPONICS_ENGINE_SOCKET", "AQUAPONICS_LIFECYCLE_AUDIT_FILE"):
        environment.pop(name, None)
    return environment


def read_audit(path: Path) -> list[dict]:
    from aquaponics_controller.lifecycle_audit import read_lifecycle_records

    return read_lifecycle_records(path)


def events_of(records: list[dict], execution_id: str | None = None) -> list[str]:
    return [r["event"] for r in records if execution_id is None or r["execution_id"] == execution_id]


class EngineProcess:
    """An Engine running as a separate process, with its diagnostic log captured in a file."""

    def __init__(self, directory: Path, *, cycle_interval: float = 0.1, name: str = "engine", extra_args=()):
        self.endpoint = directory / "ipc" / "engine.sock"
        self.audit_file = directory / "audit" / "lifecycle.jsonl"
        self.log_file = directory / f"{name}.log"
        self._log = open(self.log_file, "wb")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "aquaponics_controller.engine_main",
                "--socket",
                str(self.endpoint),
                "--audit-file",
                str(self.audit_file),
                "--cycle-interval",
                str(cycle_interval),
                *extra_args,
            ],
            env=child_environment(),
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

    def wait_until_running(self, timeout: float = PROCESS_TIMEOUT_SECONDS) -> None:
        """Wait until this process has recorded ENGINE_RUNNING (the endpoint is then listening)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"Engine exited early with {self.process.returncode}:\n{self.log_text()}")
            if self.audit_file.exists():
                records = read_audit(self.audit_file)
                mine = {
                    r["execution_id"]
                    for r in records
                    if r["event"] == "ENGINE_STARTING" and r["context"].get("engine_pid") == self.process.pid
                }
                if any(r["event"] == "ENGINE_RUNNING" and r["execution_id"] in mine for r in records):
                    return
            time.sleep(0.02)
        raise AssertionError(f"Engine did not start running:\n{self.log_text()}")

    def _execution_ids(self) -> set[str]:
        return {
            r["execution_id"]
            for r in read_audit(self.audit_file)
            if r["event"] == "ENGINE_STARTING" and r["context"].get("engine_pid") == self.process.pid
        }

    def execution_id(self) -> str:
        (execution_id,) = self._execution_ids()
        return execution_id

    def send_signal(self, signal_number: int) -> None:
        self.process.send_signal(signal_number)

    def wait(self, timeout: float = PROCESS_TIMEOUT_SECONDS) -> int:
        return self.process.wait(timeout=timeout)

    def log_text(self) -> str:
        self._log.flush()
        return self.log_file.read_text(errors="replace")

    def cpu_seconds(self) -> float:
        with open(f"/proc/{self.process.pid}/stat") as stat_file:
            fields = stat_file.read().rsplit(")", 1)[1].split()
        ticks = int(fields[11]) + int(fields[12])  # utime + stime
        return ticks / os.sysconf("SC_CLK_TCK")

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL)
            self.process.wait(timeout=5)
        self._log.close()


def run_ape(*arguments: str, timeout: float = PROCESS_TIMEOUT_SECONDS) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "aquaponics_controller.cli", *arguments],
        env=child_environment(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def raw_request(endpoint: Path, payload: bytes, *, timeout: float = 2.0) -> bytes:
    """Send raw bytes to an endpoint and return whatever the Engine answers (empty if nothing)."""
    import socket

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(str(endpoint))
        connection.sendall(payload)
        try:
            return connection.recv(4096)
        except TimeoutError:
            return b""


def valid_stop_request() -> bytes:
    from datetime import datetime, timezone

    from aquaponics_controller.ipc_protocol import encode_request

    return encode_request("stop", datetime.now(timezone.utc))


def parse_response(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))
