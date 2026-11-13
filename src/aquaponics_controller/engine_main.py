"""Entry point and composition root of the Engine process.

    python -m aquaponics_controller.engine_main [--socket PATH] [--audit-file PATH] [--cycle-interval SECONDS]
                                                [--log-level LEVEL] [--log-file PATH]
    aquaponics-engine ...                         (console script, after ``pip install -e .``)

This module assembles the Engine from explicit parts, configures diagnostic logging
(see ``diagnostic_logging``) with the same execution identifier the Engine and the
lifecycle audit receive, installs
the SIGINT / SIGTERM handlers and maps the outcome to the process exit status. It
also reads, once, what the systemd service manager asked for (``NOTIFY_SOCKET``,
``WATCHDOG_USEC``, ``WATCHDOG_PID``; see ``service_notify``) and refuses a cycle
interval the service watchdog could not tolerate. It contains no Engine logic.
Only the real, non-hardware parts are composed here: there is no hardware to
compose in this version.

Exit status: 0 controlled shutdown; 1 unexpected failure; 2 usage / configuration
error (including a cycle interval too long for the service watchdog); 3 another
Engine already owns the endpoint; 4 shutdown carried out but cleanup failed.
"""

import argparse
import logging
import os
import signal
import sys
import uuid

from .diagnostic_logging import configure_logging
from .engine import Engine, EngineConfig, ExitStatus
from .ipc_protocol import resolve_endpoint_path
from .lifecycle_audit import LifecycleAudit, resolve_audit_path
from .local_ingress import LocalAdministrativeIngress
from .service_notify import ServiceManagerSettings, ServiceNotifier, check_cycle_interval_fits_watchdog

logger = logging.getLogger(__name__)

USAGE_ERROR = 2


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    # One identifier per execution, generated before anything is logged: diagnostic logs, the
    # lifecycle audit and the Engine all receive this same value.
    execution_id = uuid.uuid4().hex
    configure_logging(arguments.log_level, arguments.log_file, execution_id)

    try:
        endpoint = resolve_endpoint_path(arguments.socket)
        audit_path = resolve_audit_path(arguments.audit_file)
        config = EngineConfig(cycle_interval_seconds=arguments.cycle_interval)
        service_settings = ServiceManagerSettings.from_environment(os.environ, os.getpid())
        check_cycle_interval_fits_watchdog(config.cycle_interval_seconds, service_settings.watchdog_seconds)
    except ValueError as exc:
        # Reported on standard error through logging, so that under journald it carries error priority.
        logger.error("configuration error, the Engine does not start: %s", exc)
        return USAGE_ERROR

    engine = Engine(
        execution_id=execution_id,
        config=config,
        ingress=LocalAdministrativeIngress(endpoint),
        audit=LifecycleAudit(audit_path, execution_id),
        service_notifier=ServiceNotifier(service_settings),
    )
    return int(run_with_signal_handling(engine))


def run_with_signal_handling(engine: Engine) -> ExitStatus:
    """Install SIGINT / SIGTERM handlers (flag-setting only) and run one Engine execution."""
    signal.signal(signal.SIGINT, lambda signal_number, frame: engine.request_stop_for_signal(signal_number))
    signal.signal(signal.SIGTERM, lambda signal_number, frame: engine.request_stop_for_signal(signal_number))
    return engine.run()


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
    parser.add_argument(
        "--log-file",
        metavar="PATH",
        help="also write diagnostic logs to this size-rotated file (at most 25 MiB in total); default: standard error only",
    )
    return parser


if __name__ == "__main__":
    sys.exit(main())
