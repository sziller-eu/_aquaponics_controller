# Aquaponics Controller

Open-source, offline-first Aquaponics controller for autonomous monitoring and control of scalable physical aquaponics systems.

## Overview

The Aquaponics Engine is the authoritative software controller for physical aquaponics installations. It operates autonomously in a continuous lifecycle loop, independent of external network connections, cloud services, or peripherals.

Local administrative interactions (such as requesting a graceful stop) are performed via Inter-Process Communication (IPC) using the local administrative CLI tool (`ape`).

## Installation & Setup

Requirements:
- Python `>= 3.11`
- Linux (production target, e.g. Raspberry Pi OS / Ubuntu) or POSIX development workstations

Install in editable mode:
```bash
pip install -e .
```

Alternatively, set `PYTHONPATH`:
```bash
export PYTHONPATH=src
```

## Running the Engine

Start the autonomous Aquaponics Engine daemon:

```bash
# Using installed console script
aquaponics-engine

# Or using Python module execution
python -m aquaponics_controller.engine
```

Options:
- `--socket-path <path>`: Specify custom local IPC Unix domain socket path (default: `/run/aquaponics/engine.sock` in production, or `$XDG_RUNTIME_DIR/aquaponics/engine.sock` / `~/.aquaponics/engine.sock` in development).
- `--cycle-interval <seconds>`: Set operating cycle interval in seconds (default: `1.0`).
- `-v, --verbose`: Enable debug logging.

The Engine runs continuously until an operator issues a controlled stop request or a process termination signal (`SIGINT` / `SIGTERM`) is received.

## Administrative CLI (`ape`)

To request a controlled shutdown of an existing, actively running Engine instance from another terminal:

```bash
# Using installed console script
ape -x

# Or using Python module execution
python -m aquaponics_controller.cli -x
```

Options:
- `-x, --stop`: Request controlled shutdown of the existing running Engine.
- `--socket-path <path>`: Path to the running Engine socket.
- `--timeout <seconds>`: Connection and communication timeout (default: `2.0`s).

### Exit Codes
- `0`: Controlled stop request successfully accepted by the Engine.
- `1`: Engine not running (socket not found or connection refused).
- `2`: Permission denied accessing socket or rejected by peer credential verification (`SO_PEERCRED`).
- `3`: Communication timed out.
- `4`: Invalid usage or unexpected error.

## Running Tests

Execute the automated test suite:

```bash
python -m unittest discover tests
```
