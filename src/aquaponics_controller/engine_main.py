"""Entry point and composition root of the Engine process.

    python -m aquaponics_controller.engine_main [--socket PATH] [--audit-file PATH] [--cycle-interval SECONDS]
    aquaponics-engine ...                         (console script, after ``pip install -e .``)

This module assembles the Engine from explicit parts, configures logging, installs
the SIGINT / SIGTERM handlers and maps the outcome to the process exit status. It
contains no Engine logic. Only the real, non-hardware parts are composed here:
there is no hardware to compose in this version.

Exit status: 0 controlled shutdown; 1 unexpected failure; 2 usage / configuration
error; 3 another Engine already owns the endpoint; 4 shutdown carried out but
cleanup failed.
"""

import argparse
import logging
import signal
import sys
import time
import uuid

from .engine import Engine, EngineConfig, ExitStatus
from .ipc_protocol import resolve_endpoint_path
from .lifecycle_audit import LifecycleAudit, resolve_audit_path
from .local_ingress import LocalAdministrativeIngress

USAGE_ERROR = 2


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    configure_logging(arguments.log_level)

    try:
        endpoint = resolve_endpoint_path(arguments.socket)
        audit_path = resolve_audit_path(arguments.audit_file)
        config = EngineConfig(cycle_interval_seconds=arguments.cycle_interval)
    except ValueError as exc:
        print(f"aquaponics-engine: {exc}", file=sys.stderr)
        return USAGE_ERROR

    execution_id = uuid.uuid4().hex
    engine = Engine(
        execution_id=execution_id,
        config=config,
        ingress=LocalAdministrativeIngress(endpoint),
        audit=LifecycleAudit(audit_path, execution_id),
    )
    return int(run_with_signal_handling(engine))


def run_with_signal_handling(engine: Engine) -> ExitStatus:
    """Install SIGINT / SIGTERM handlers (flag-setting only) and run one Engine execution."""
    signal.signal(signal.SIGINT, lambda signal_number, frame: engine.request_stop_for_signal(signal_number))
    signal.signal(signal.SIGTERM, lambda signal_number, frame: engine.request_stop_for_signal(signal_number))
    return engine.run()


def configure_logging(level_name: str) -> None:
    """Diagnostic logging to standard error with UTC timestamps. Retention and rotation belong to a later issue."""
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s: %(message)s", "%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    logging.basicConfig(level=level_name.upper(), handlers=[handler], force=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aquaponics-engine",
        description="Aquaponics Engine (development foundation: no hardware control).",
    )
    parser.add_argument("--socket", help="administrative IPC endpoint path (default: see documentation)")
    parser.add_argument("--audit-file", help="lifecycle audit file path (default: see documentation)")
    parser.add_argument(
        "--cycle-interval", type=float, default=1.0, metavar="SECONDS", help="operating cycle interval (default 1.0)"
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


if __name__ == "__main__":
    sys.exit(main())
