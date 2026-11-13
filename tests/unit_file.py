"""Reading the reviewed systemd unit (deploy/systemd/aquaponics-engine.service) in tests.

A deliberately small reader for the subset of the unit-file syntax the reviewed unit
uses: ``[Section]`` headers, ``Key=Value`` lines, ``#`` / ``;`` comments and lines
continued with a trailing backslash. Repeated keys are kept in order.
"""

import shlex
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parent.parent
ENGINE_UNIT_PATH = REPOSITORY / "deploy" / "systemd" / "aquaponics-engine.service"
SYSUSERS_PATH = REPOSITORY / "deploy" / "sysusers.d" / "aquaponics.conf"

PRODUCTION_EXECUTABLE = "/opt/aquaponics/venv/bin/aquaponics-engine"


def read_unit(path: Path = ENGINE_UNIT_PATH) -> dict[str, list[tuple[str, str]]]:
    """Return {section: [(key, value), ...]} in file order."""
    sections: dict[str, list[tuple[str, str]]] = {}
    current: list[tuple[str, str]] | None = None
    pending = ""
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not pending and (not line or line.startswith(("#", ";"))):
            continue
        if line.endswith("\\"):
            pending += line[:-1].strip() + " "
            continue
        line, pending = (pending + line).strip(), ""
        if line.startswith("[") and line.endswith("]"):
            current = sections.setdefault(line[1:-1], [])
            continue
        if current is None or "=" not in line:
            raise ValueError(f"unexpected line in {path.name}: {raw_line!r}")
        key, value = line.split("=", 1)
        current.append((key.strip(), value.strip()))
    if pending:
        raise ValueError(f"{path.name} ends inside a continued line")
    return sections


def single_values(section: list[tuple[str, str]]) -> dict[str, str]:
    """{key: value} for a section; a key that appears twice is an error (the reviewed unit has none)."""
    values: dict[str, str] = {}
    for key, value in section:
        if key in values:
            raise ValueError(f"directive {key} appears more than once")
        values[key] = value
    return values


def exec_start_arguments(service: dict[str, str]) -> list[str]:
    return shlex.split(service["ExecStart"])
