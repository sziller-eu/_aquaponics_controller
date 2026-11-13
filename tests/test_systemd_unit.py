"""Issue #9: the reviewed systemd unit and service identity, checked statically.

Every contract directive of deploy/systemd/aquaponics-engine.service is asserted here,
so a change to the supervision, restart or security contract cannot slip in unnoticed:
a new or removed directive fails ``test_the_unit_contains_exactly_the_reviewed_directives``.
Where the local machine has ``systemd-analyze``, the unit is also verified by systemd
itself (this is how compatibility with the oldest supported systemd, 249, is checked).

The restart and signal outcome contract links the unit to the Engine: exit statuses the
unit must not restart, and the Engine's own signal handling. Nothing here installs,
enables or starts anything.
"""

import shutil
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from aquaponics_controller.engine import ExitStatus
from aquaponics_controller.engine_main import USAGE_ERROR
from aquaponics_controller.service_notify import check_cycle_interval_fits_watchdog

from helpers import EngineProcess, events_of, read_audit
from unit_file import (
    ENGINE_UNIT_PATH,
    PRODUCTION_EXECUTABLE,
    SYSUSERS_PATH,
    exec_start_arguments,
    read_unit,
    single_values,
)

UNIT_SECTION = {
    "Description": "Aquaponics Engine (autonomous controller, development foundation)",
    "StartLimitIntervalSec": "300",
    "StartLimitBurst": "5",
}

SUPERVISION = {
    "Type": "notify",
    "NotifyAccess": "main",
    "Restart": "on-failure",
    "RestartSec": "5s",
    "RestartPreventExitStatus": "2 3 4",
    "RestartForceExitStatus": "SIGHUP",
    "TimeoutStartSec": "30s",
    "TimeoutStopSec": "10s",
    "WatchdogSec": "10s",
}

IDENTITY_AND_FILES = {
    "User": "aquaponics",
    "Group": "aquaponics",
    "UMask": "0077",
    "RuntimeDirectory": "aquaponics",
    "RuntimeDirectoryMode": "0700",
    "StateDirectory": "aquaponics",
    "StateDirectoryMode": "0700",
    "Environment": "PYTHONFAULTHANDLER=1 PYTHONDONTWRITEBYTECODE=1",
    "SyslogIdentifier": "aquaponics-engine",
}

SANDBOX = {
    "NoNewPrivileges": "yes",
    "CapabilityBoundingSet": "",
    "AmbientCapabilities": "",
    "ProtectSystem": "strict",
    "ProtectHome": "yes",
    "PrivateTmp": "yes",
    "PrivateDevices": "yes",  # no device access in Milestone 1; GPIO / I2C need a separate security review
    "ProtectKernelTunables": "yes",
    "ProtectKernelModules": "yes",
    "ProtectKernelLogs": "yes",
    "ProtectControlGroups": "yes",
    "ProtectClock": "yes",
    "ProtectHostname": "yes",
    "RestrictAddressFamilies": "AF_UNIX",
    "RestrictNamespaces": "yes",
    "RestrictRealtime": "yes",
    "RestrictSUIDSGID": "yes",
    "LockPersonality": "yes",
    "SystemCallArchitectures": "native",
    "SystemCallFilter": "@system-service",
    "SystemCallErrorNumber": "EPERM",
    "IPAddressDeny": "any",
}

INSTALL_SECTION = {"WantedBy": "multi-user.target"}


@pytest.fixture(scope="module")
def unit():
    sections = read_unit()
    return {name: single_values(entries) for name, entries in sections.items()}


# ---------------------------------------------------------------- the reviewed directives, exactly


def test_the_unit_contains_exactly_the_reviewed_directives(unit):
    assert set(unit) == {"Unit", "Service", "Install"}
    assert unit["Unit"] == UNIT_SECTION
    service = dict(unit["Service"])
    service.pop("ExecStart")  # checked separately below
    assert service == {**SUPERVISION, **IDENTITY_AND_FILES, **SANDBOX}
    assert unit["Install"] == INSTALL_SECTION


def test_device_access_is_disabled(unit):
    assert unit["Service"]["PrivateDevices"] == "yes"
    assert "DeviceAllow" not in unit["Service"] and "SupplementaryGroups" not in unit["Service"]


def test_the_service_has_no_network_access_and_no_network_dependency(unit):
    assert unit["Service"]["RestrictAddressFamilies"] == "AF_UNIX"
    assert unit["Service"]["IPAddressDeny"] == "any"
    for key, value in unit["Unit"].items():
        assert "network" not in value.lower(), f"{key}={value}"
    assert not {"After", "Wants", "Requires", "BindsTo"} & set(unit["Unit"])


def test_the_service_runs_as_its_own_unprivileged_user(unit):
    assert unit["Service"]["User"] not in ("", "root", "0")
    assert unit["Service"]["CapabilityBoundingSet"] == "" and unit["Service"]["AmbientCapabilities"] == ""
    assert "DynamicUser" not in unit["Service"]  # the audit needs a stable owner


def test_stopping_is_sigterm_only(unit):
    for directive in ("ExecStop", "ExecStopPost", "ExecReload", "KillSignal", "KillMode", "FinalKillSignal"):
        assert directive not in unit["Service"]  # systemd's default: SIGTERM, then SIGKILL after TimeoutStopSec


def test_the_engine_is_started_with_explicit_service_paths_and_logs_to_journald(unit):
    arguments = exec_start_arguments(unit["Service"])

    assert arguments[0] == PRODUCTION_EXECUTABLE
    options = dict(zip(arguments[1::2], arguments[2::2]))
    assert options == {
        "--socket": "${RUNTIME_DIRECTORY}/engine.sock",
        "--audit-file": "${STATE_DIRECTORY}/lifecycle_audit.jsonl",
        "--cycle-interval": "1.0",
    }
    assert "--log-file" not in arguments  # journald keeps the diagnostic log


def test_the_configured_cycle_interval_fits_the_configured_watchdog(unit):
    options = dict(zip(exec_start_arguments(unit["Service"])[1::2], exec_start_arguments(unit["Service"])[2::2]))
    watchdog_seconds = float(unit["Service"]["WatchdogSec"].removesuffix("s"))

    check_cycle_interval_fits_watchdog(float(options["--cycle-interval"]), watchdog_seconds)  # does not raise


def test_the_runtime_and_state_directories_are_owner_only(unit):
    assert unit["Service"]["RuntimeDirectoryMode"] == "0700"
    assert unit["Service"]["StateDirectoryMode"] == "0700"
    assert unit["Service"]["UMask"] == "0077"


def test_the_service_user_is_declared_for_systemd_sysusers():
    lines = [line for line in SYSUSERS_PATH.read_text().splitlines() if line.strip() and not line.startswith("#")]

    assert lines == ['u aquaponics - "Aquaponics Engine" - -']


# ---------------------------------------------------------------- restart and signal outcome contract


def test_exactly_the_deliberate_non_restartable_exit_statuses_are_excluded_from_restart(unit):
    excluded = {int(code) for code in unit["Service"]["RestartPreventExitStatus"].split()}

    assert excluded == {USAGE_ERROR, ExitStatus.ALREADY_RUNNING, ExitStatus.CLEANUP_FAILED}
    assert ExitStatus.FAILURE not in excluded  # an Engine failure is restarted
    assert ExitStatus.SUCCESS == 0  # a deliberate stop: Restart=on-failure never restarts exit status 0


def test_the_engine_does_not_handle_sighup_so_systemd_sees_a_death_by_signal(short_directory):
    engine = EngineProcess(short_directory, cycle_interval=0.1)
    try:
        engine.wait_until_running()
        execution_id = engine.execution_id()
        engine.send_signal(signal.SIGHUP)
        assert engine.wait() == -signal.SIGHUP  # restarted by RestartForceExitStatus=SIGHUP
    finally:
        engine.close()

    # No cleanup ran: the execution's audit ends without shutdown records.
    assert events_of(read_audit(engine.audit_file), execution_id) == ["ENGINE_STARTING", "ENGINE_RUNNING"]


def test_a_handled_sigterm_is_a_deliberate_stop_with_exit_status_0(short_directory):
    engine = EngineProcess(short_directory, cycle_interval=0.1)
    try:
        engine.wait_until_running()
        engine.send_signal(signal.SIGTERM)
        assert engine.wait() == 0  # Restart=on-failure does not restart it
    finally:
        engine.close()
    assert events_of(read_audit(engine.audit_file))[-2:] == ["SHUTDOWN_INITIATED", "SHUTDOWN_COMPLETED"]


# ---------------------------------------------------------------- systemd's own verification


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="systemd-analyze is not available")
def test_systemd_verifies_the_unit_without_any_complaint(short_directory):
    # Only ExecStart is pointed at this test environment (verify checks that the executable exists).
    installed = Path(sys.executable).with_name("aquaponics-engine")
    executable = installed if installed.exists() else Path(sys.executable)
    copy = short_directory / ENGINE_UNIT_PATH.name
    copy.write_text(ENGINE_UNIT_PATH.read_text().replace(PRODUCTION_EXECUTABLE, str(executable)))

    result = subprocess.run(
        ["systemd-analyze", "verify", "--man=no", str(copy)], capture_output=True, text=True, timeout=60
    )

    # systemd-analyze also reports unrelated units installed on the machine; only this unit matters.
    complaints = [line for line in (result.stdout + result.stderr).splitlines() if ENGINE_UNIT_PATH.name in line]
    assert complaints == []


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="systemd-analyze is not available")
def test_the_verification_would_catch_an_unknown_directive(short_directory):
    # Shows that the check above is effective: a misspelt directive is reported against this unit.
    installed = Path(sys.executable).with_name("aquaponics-engine")
    executable = installed if installed.exists() else Path(sys.executable)
    copy = short_directory / ENGINE_UNIT_PATH.name
    text = ENGINE_UNIT_PATH.read_text().replace(PRODUCTION_EXECUTABLE, str(executable))
    copy.write_text(text.replace("PrivateDevices=yes", "PrivateDevice=yes"))

    result = subprocess.run(
        ["systemd-analyze", "verify", "--man=no", str(copy)], capture_output=True, text=True, timeout=60
    )

    assert any("PrivateDevice" in line and ENGINE_UNIT_PATH.name in line for line in result.stderr.splitlines())
