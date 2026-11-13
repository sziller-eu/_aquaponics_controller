"""Issue #9 (D11, D16): the Engine under real systemd supervision, as transient USER units.

OPT-IN. Runs only with AQUAPONICS_SYSTEMD_TESTS=1 and a running systemd user manager;
skipped otherwise. Each test starts its own transient unit with ``systemd-run --user``,
named ``aquaponics-test-<random>``, built from the reviewed production unit
(deploy/systemd/aquaponics-engine.service): every behaviour directive is kept, only the
system-only directives (identity and sandboxing, listed in SYSTEM_ONLY_DIRECTIVES) are
dropped, and ExecStart runs this checkout's Engine with the production arguments.
Teardown stops and resets the unit and removes its own runtime and state directories.
Nothing is installed or enabled, no unit file is written, and no system service is touched.

WHAT THESE TESTS DO NOT VALIDATE: the production service identity (the ``aquaponics``
user), system-level sandboxing (including PrivateDevices=yes), start at boot, and
Raspberry Pi behaviour. Those are covered by the manual system-level demonstration in
"Engine Service (Initial Implementation)" (operations documentation).

Slow by nature: restarts wait RestartSec (5 s) and watchdog tests wait WatchdogSec (10 s).
"""

import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from aquaponics_controller import cli
from aquaponics_controller.ipc_protocol import OPERATION_PING

from helpers import SOURCE_DIRECTORY, events_of, read_audit, run_ape
from unit_file import PRODUCTION_EXECUTABLE, exec_start_arguments, read_unit


def _user_manager_available() -> bool:
    if shutil.which("systemd-run") is None or shutil.which("systemctl") is None:
        return False
    state = subprocess.run(
        ["systemctl", "--user", "is-system-running"], capture_output=True, text=True, timeout=10
    ).stdout.strip()
    return state in ("running", "degraded")


pytestmark = [
    pytest.mark.skipif(os.environ.get("AQUAPONICS_SYSTEMD_TESTS") != "1", reason="opt-in: set AQUAPONICS_SYSTEMD_TESTS=1"),
    pytest.mark.skipif(
        os.environ.get("AQUAPONICS_SYSTEMD_TESTS") == "1" and not _user_manager_available(),
        reason="no running systemd user manager",
    ),
]

# Dropped from the production unit for a user-level test: they need a system manager or root.
SYSTEM_ONLY_DIRECTIVES = frozenset(
    {
        "User",
        "Group",
        "NoNewPrivileges",
        "CapabilityBoundingSet",
        "AmbientCapabilities",
        "ProtectSystem",
        "ProtectHome",
        "PrivateTmp",
        "PrivateDevices",
        "ProtectKernelTunables",
        "ProtectKernelModules",
        "ProtectKernelLogs",
        "ProtectControlGroups",
        "ProtectClock",
        "ProtectHostname",
        "RestrictAddressFamilies",
        "RestrictNamespaces",
        "RestrictRealtime",
        "RestrictSUIDSGID",
        "LockPersonality",
        "SystemCallArchitectures",
        "SystemCallFilter",
        "SystemCallErrorNumber",
        "IPAddressDeny",
    }
)
# Taken from the production unit unchanged, apart from the per-test directory names.
KEPT_DIRECTIVES = frozenset(
    {
        "Type",
        "NotifyAccess",
        "UMask",
        "RuntimeDirectory",
        "RuntimeDirectoryMode",
        "StateDirectory",
        "StateDirectoryMode",
        "Environment",
        "Restart",
        "RestartSec",
        "RestartPreventExitStatus",
        "RestartForceExitStatus",
        "TimeoutStartSec",
        "TimeoutStopSec",
        "WatchdogSec",
        "SyslogIdentifier",
    }
)

RESTART_SECONDS = 5
WATCHDOG_SECONDS = 10
WAIT_SECONDS = 40

# Test scripts: the real Engine entry point with one fault injected into a phase.
FAILING_ENGINE = """
import sys
from aquaponics_controller import engine, engine_main
def injected_failure(self):
    raise RuntimeError("injected failure for the restart test")
engine.Engine._acquire_observations = injected_failure
sys.exit(engine_main.main())
"""
HANGING_ENGINE = """
import sys, time
from aquaponics_controller import engine, engine_main
def simulated_hang_in_finalize_cycle(self):
    if self.cycles_completed >= 3:
        while True:
            time.sleep(1)
engine.Engine._finalize_cycle = simulated_hang_in_finalize_cycle
sys.exit(engine_main.main())
"""


def _systemctl(*arguments, check=False) -> subprocess.CompletedProcess:
    return subprocess.run(["systemctl", "--user", *arguments], capture_output=True, text=True, timeout=60, check=check)


class TransientEngineUnit:
    """One transient user unit running the Engine with the production unit's behaviour directives."""

    def __init__(self, *, script: str | None = None, cycle_interval: str | None = None, socket: str | None = None):
        self.name = f"aquaponics-test-{uuid.uuid4().hex[:12]}"
        self.unit = f"{self.name}.service"
        self.runtime_directory = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")) / self.name
        self.endpoint = self.runtime_directory / "engine.sock"
        self.state_directory: Path | None = None
        self.command = self._command(script, cycle_interval, socket)
        self.start_result: subprocess.CompletedProcess | None = None

    # ------------------------------------------------------------------ building the unit

    def _command(self, script, cycle_interval, socket) -> list[str]:
        service = dict(read_unit()["Service"])
        arguments = exec_start_arguments(service)
        assert arguments[0] == PRODUCTION_EXECUTABLE
        engine = [sys.executable, "-c", script] if script else [sys.executable, "-m", "aquaponics_controller.engine_main"]
        options = arguments[1:]
        if cycle_interval is not None:
            options[options.index("--cycle-interval") + 1] = cycle_interval
        if socket is not None:
            options[options.index("--socket") + 1] = socket
        return engine + options

    def properties(self) -> list[str]:
        sections = read_unit()
        unit_values = dict(sections["Unit"])
        properties = [
            f"--description={unit_values['Description']} [user-level test]",
            f"--property=StartLimitIntervalSec={unit_values['StartLimitIntervalSec']}",
            f"--property=StartLimitBurst={unit_values['StartLimitBurst']}",
        ]
        for key, value in sections["Service"]:
            if key in SYSTEM_ONLY_DIRECTIVES or key == "ExecStart":
                continue
            assert key in KEPT_DIRECTIVES, f"directive {key} is neither kept nor dropped: classify it"
            if key in ("RuntimeDirectory", "StateDirectory"):
                value = self.name
            properties.append(f"--property={key}={value}")
        properties.append(f"--property=Environment=PYTHONPATH={SOURCE_DIRECTORY}")
        return properties

    # ------------------------------------------------------------------ operating it

    def start(self) -> subprocess.CompletedProcess:
        """systemd-run returns once the start job finished: for Type=notify, after READY (or a failure)."""
        self.start_result = subprocess.run(
            ["systemd-run", "--user", f"--unit={self.name}", *self.properties(), "--", *self.command],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self._remember_state_directory()
        return self.start_result

    def _remember_state_directory(self) -> None:
        pid = self.show("MainPID")["MainPID"]
        if pid and pid != "0":
            try:
                environment = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
            except OSError:
                return
            for entry in environment:
                if entry.startswith(b"STATE_DIRECTORY="):
                    self.state_directory = Path(entry.split(b"=", 1)[1].decode())

    @property
    def audit_file(self) -> Path:
        assert self.state_directory is not None, "the unit never ran long enough to report its state directory"
        return self.state_directory / "lifecycle_audit.jsonl"

    def show(self, *names: str) -> dict[str, str]:
        output = _systemctl("show", self.unit, "--property=" + ",".join(names)).stdout
        return dict(line.split("=", 1) for line in output.splitlines() if "=" in line)

    def wait_for(self, condition, description: str, timeout: float = WAIT_SECONDS) -> dict[str, str]:
        deadline = time.monotonic() + timeout
        names = ("ActiveState", "SubState", "Result", "NRestarts", "MainPID", "ExecMainStatus", "ExecMainCode")
        while True:
            properties = self.show(*names)
            if condition(properties):
                return properties
            if time.monotonic() > deadline:
                raise AssertionError(f"{self.unit}: timed out waiting for {description}; last: {properties}\n{self.journal()}")
            time.sleep(0.2)

    def kill(self, signal_name: str) -> None:
        _systemctl("kill", f"--signal={signal_name}", self.unit, check=True)

    def ping(self) -> dict:
        response = cli.send_request(self.endpoint, OPERATION_PING, 5.0)
        assert response.ok, response
        return response.result

    def journal(self) -> str:
        return subprocess.run(
            ["journalctl", "--user", f"--unit={self.unit}", "--output=cat", "--no-pager"],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout

    def journal_at_priority(self, priority: str) -> str:
        return subprocess.run(
            ["journalctl", "--user", f"--unit={self.unit}", f"--priority={priority}", "--output=cat", "--no-pager"],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout

    def wait_for_journal(self, text: str, timeout: float = 15.0, priority: str | None = None) -> str:
        deadline = time.monotonic() + timeout
        while True:
            journal = self.journal_at_priority(priority) if priority else self.journal()
            if text in journal:
                return journal
            if time.monotonic() > deadline:
                raise AssertionError(f"{text!r} not found in the journal of {self.unit}:\n{journal}")
            time.sleep(0.3)

    def remove(self) -> None:
        """Stop the unit (a pending restart included), forget it, and remove only its own directories."""
        _systemctl("kill", "--signal=SIGKILL", self.unit)  # fast even if the Engine is hung or frozen
        _systemctl("stop", self.unit)
        _systemctl("reset-failed", self.unit)
        candidates = {self.state_directory} if self.state_directory else set()
        for base in (
            os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config",
            os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state",
        ):
            candidates.add(Path(base) / self.name)
        for directory in candidates:
            if directory.name.startswith("aquaponics-test-") and directory.is_dir():
                shutil.rmtree(directory)


@pytest.fixture
def units():
    created: list[TransientEngineUnit] = []

    def make(**options) -> TransientEngineUnit:
        unit = TransientEngineUnit(**options)
        created.append(unit)
        return unit

    yield make
    for unit in created:
        unit.remove()
    leftover = _systemctl("list-units", "--all", "--no-legend", *[u.unit for u in created]).stdout
    assert all(u.unit not in leftover for u in created), leftover


def started(unit: TransientEngineUnit) -> TransientEngineUnit:
    result = unit.start()
    assert result.returncode == 0, f"{result.stderr}\n{unit.journal()}"
    assert unit.show("ActiveState")["ActiveState"] == "active"  # READY was received
    return unit


def restarted_once(unit: TransientEngineUnit) -> dict[str, str]:
    return unit.wait_for(
        lambda p: p["NRestarts"] == "1" and p["ActiveState"] == "active" and p["SubState"] == "running",
        "one restart, then running again",
    )


# ================================================================ the generated unit


def test_every_production_directive_is_either_kept_or_explicitly_dropped():
    keys = {key for key, _ in read_unit()["Service"]} - {"ExecStart"}

    assert keys == KEPT_DIRECTIVES | SYSTEM_ONLY_DIRECTIVES
    assert not KEPT_DIRECTIVES & SYSTEM_ONLY_DIRECTIVES


# ================================================================ deliberate stops are never undone


def test_ready_ping_and_an_operator_stop_that_is_not_restarted(units):
    unit = started(units())
    reply = unit.ping()
    assert reply["reply"] == "pong"

    assert run_ape("--socket", str(unit.endpoint), "-x").returncode == 0

    final = unit.wait_for(lambda p: p["ActiveState"] in ("inactive", "failed"), "the Engine to stop")
    assert (final["ActiveState"], final["Result"], final["ExecMainStatus"]) == ("inactive", "success", "0")
    time.sleep(RESTART_SECONDS + 1)
    assert unit.show("NRestarts", "ActiveState") == {"NRestarts": "0", "ActiveState": "inactive"}
    assert events_of(read_audit(unit.audit_file))[-1] == "SHUTDOWN_COMPLETED"


def test_systemctl_stop_is_graceful_within_the_stop_timeout_and_not_restarted(units):
    unit = started(units())
    execution_id = unit.ping()["execution_id"]

    stopping = time.monotonic()
    assert _systemctl("stop", unit.unit).returncode == 0
    assert time.monotonic() - stopping < WATCHDOG_SECONDS  # well within TimeoutStopSec (10 s)

    final = unit.show("ActiveState", "Result", "ExecMainStatus", "NRestarts")
    assert final == {"ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0", "NRestarts": "0"}
    records = read_audit(unit.audit_file)
    assert events_of(records, execution_id)[-2:] == ["SHUTDOWN_INITIATED", "SHUTDOWN_COMPLETED"]
    initiated = next(r for r in records if r["event"] == "SHUTDOWN_INITIATED")
    assert initiated["reason"] == "SIGNAL" and initiated["context"]["signal"] == "SIGTERM"
    # The journal names the execution and the cause of its termination.
    journal = unit.wait_for_journal("Engine shutdown completed (exit status 0)")
    assert f"exec={execution_id}" in journal
    assert "shutdown initiated: external interruption by SIGTERM" in journal


# ================================================================ unexpected terminations are restarted


@pytest.mark.parametrize("signal_name", ["SIGKILL", "SIGHUP"])
def test_a_death_by_signal_is_restarted_with_a_new_execution_on_the_same_endpoint(units, signal_name):
    unit = started(units())
    first = unit.ping()["execution_id"]

    unit.kill(signal_name)
    restarted_once(unit)

    second = unit.ping()["execution_id"]
    assert second != first
    records = read_audit(unit.audit_file)
    assert events_of(records, first) == ["ENGINE_STARTING", "ENGINE_RUNNING"]  # no cleanup ran
    assert events_of(records, second) == ["ENGINE_STARTING", "ENGINE_RUNNING"]


def test_an_engine_failure_is_restarted_until_the_start_limit_stops_the_loop(units):
    unit = units(script=FAILING_ENGINE)
    unit.start()  # may already have failed again by the time systemd-run returns

    # While restarts are pending the unit is "activating (auto-restart)"; it is "failed" only once the
    # start limit refuses the next start. (systemd 249 then keeps Result=exit-code, the last failure.)
    final = unit.wait_for(lambda p: p["ActiveState"] == "failed", "the start limit (5 starts within 300 s)", timeout=60)
    assert final["ExecMainStatus"] == "1"
    journal = unit.wait_for_journal("Start request repeated too quickly")
    assert journal.count("Engine starting (execution") == 5, journal  # StartLimitBurst=5, then no further start
    errors = unit.wait_for_journal("Engine failure during operation", priority="err")
    assert "injected failure for the restart test" in errors
    time.sleep(RESTART_SECONDS + 1)
    assert unit.show("ActiveState")["ActiveState"] == "failed"  # stays failed until reset-failed


# ================================================================ exits that must not be restarted


def test_a_configuration_error_exits_2_and_is_not_restarted(units):
    unit = units(cycle_interval="3.6")  # too long for WatchdogSec=10s

    assert unit.start().returncode != 0

    final = unit.wait_for(lambda p: p["ActiveState"] == "failed", "the unit to fail")
    assert (final["ExecMainStatus"], final["Result"]) == ("2", "exit-code")
    time.sleep(RESTART_SECONDS + 1)
    assert unit.show("NRestarts", "ActiveState") == {"NRestarts": "0", "ActiveState": "failed"}
    unit.wait_for_journal("too long for the 10 s service watchdog", priority="err")


def test_a_second_engine_on_the_same_endpoint_exits_3_and_is_not_restarted(units):
    owner = started(units())
    intruder = units(socket=str(owner.endpoint))

    assert intruder.start().returncode != 0

    final = intruder.wait_for(lambda p: p["ActiveState"] == "failed", "the second unit to fail")
    assert final["ExecMainStatus"] == "3"
    time.sleep(RESTART_SECONDS + 1)
    assert intruder.show("NRestarts")["NRestarts"] == "0"
    assert owner.ping()["reply"] == "pong"  # the first Engine is unaffected


# ================================================================ the watchdog


def test_a_frozen_engine_is_killed_by_the_watchdog_and_restarted(units):
    unit = started(units())
    first = unit.ping()["execution_id"]

    unit.kill("SIGSTOP")
    restarted_once(unit)

    assert unit.ping()["execution_id"] != first
    unit.wait_for_journal("Watchdog timeout")


def test_a_hung_engine_is_killed_by_the_watchdog_with_its_stack_in_the_journal_and_restarted(units):
    unit = started(units(script=HANGING_ENGINE))

    restarted_once(unit)

    journal = unit.wait_for_journal("simulated_hang_in_finalize_cycle")
    assert "Watchdog timeout" in journal
    assert "Fatal Python error: Aborted" in journal
