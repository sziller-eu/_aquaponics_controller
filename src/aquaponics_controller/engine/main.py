"""Executable entry point for the Aquaponics Engine process.

Handles process arguments, logging initialization, signal handling (SIGINT/SIGTERM),
and lifecycle execution.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from typing import Sequence

from aquaponics_controller.engine.lifecycle import AquaponicsEngine
from aquaponics_controller.ipc import DEFAULT_CYCLE_INTERVAL_SECONDS, resolve_socket_path

logger = logging.getLogger("aquaponics.engine")


def configure_logging(verbose: bool = False) -> None:
    """Configure standard Python logging format and level.

    Args:
        verbose: True to enable DEBUG-level logging; False for INFO-level.
    """
    level = logging.DEBUG if verbose else logging.INFO
    format_pattern = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=format_pattern, stream=sys.stdout)


def parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the Engine daemon.

    Args:
        argv: Command-line arguments sequence; defaults to sys.argv[1:].

    Returns:
        Parsed Namespace object.
    """
    parser = argparse.ArgumentParser(
        prog="aquaponics-engine",
        description="Autonomous Aquaponics Engine daemon.",
    )
    parser.add_argument(
        "--socket-path",
        type=str,
        default=None,
        help="Custom filesystem path for the local IPC Unix domain socket.",
    )
    parser.add_argument(
        "--cycle-interval",
        type=float,
        default=DEFAULT_CYCLE_INTERVAL_SECONDS,
        help=f"Cycle interval in seconds (default: {DEFAULT_CYCLE_INTERVAL_SECONDS}s).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    return parser.parse_args(argv)


def register_signal_handlers(engine: AquaponicsEngine) -> None:
    """Register operating-system signal handlers for graceful shutdown.

    Traps SIGINT (Ctrl+C) and SIGTERM (systemd / process manager termination).

    Args:
        engine: The AquaponicsEngine instance to signal upon termination.
    """

    def handle_signal(sig_num: int, frame: object) -> None:
        signame = signal.Signals(sig_num).name if hasattr(signal, "Signals") else str(sig_num)
        logger.info("External interruption received (%s), initiating graceful shutdown", signame)
        engine.request_stop(reason=f"Received signal {signame}", is_operator=False)

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)


def main(argv: Sequence[str] | None = None) -> int:
    """Main execution entry point for the Engine.

    Args:
        argv: Command-line argument list.

    Returns:
        Process exit code: 0 on controlled shutdown; 1 on unexpected failure.
    """
    args = parse_arguments(argv)
    configure_logging(verbose=args.verbose)

    socket_path = resolve_socket_path(args.socket_path)
    logger.info("Starting Aquaponics Engine (socket: %s, cycle: %.2fs)", socket_path, args.cycle_interval)

    engine = AquaponicsEngine(
        socket_path=socket_path,
        cycle_interval_seconds=args.cycle_interval,
    )

    register_signal_handlers(engine)

    exit_code = engine.run()
    logger.info("Aquaponics Engine process terminating with exit code %d", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
