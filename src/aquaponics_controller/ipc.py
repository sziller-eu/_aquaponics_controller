"""Inter-Process Communication (IPC) protocol and transport definitions.

This module provides common IPC constants, message serialization helpers,
and default socket path resolution for local communication between the
Engine and administrative clients (such as the ape CLI).
"""

import json
import os
from pathlib import Path
from typing import Any, Final

# Default timing configurations
DEFAULT_CYCLE_INTERVAL_SECONDS: Final[float] = 1.0
DEFAULT_CLIENT_TIMEOUT_SECONDS: Final[float] = 2.0
DEFAULT_INGRESS_TIMEOUT_SECONDS: Final[float] = 0.05

# Supported administrative command verbs
COMMAND_STOP: Final[str] = "STOP"

# Standard response statuses
STATUS_ACCEPTED: Final[str] = "ACCEPTED"
STATUS_REJECTED: Final[str] = "REJECTED"
STATUS_ERROR: Final[str] = "ERROR"

# Linux filesystem permission modes
# 0o660: Read/write for owner and group, no permissions for world
SOCKET_FILE_MODE: Final[int] = 0o660
# 0o770: Read/write/execute for owner and group, no permissions for world
SOCKET_DIR_MODE: Final[int] = 0o770

# Linux default runtime socket path
DEFAULT_LINUX_PRODUCTION_SOCKET_PATH: Final[str] = "/run/aquaponics/engine.sock"


def resolve_socket_path(explicit_path: str | Path | None = None) -> Path:
    """Resolve the effective Unix domain socket path.

    Resolution order:
    1. Explicitly supplied path (if provided).
    2. Environment variable AQUAPONICS_SOCKET_PATH (if set).
    3. Production standard Linux path /run/aquaponics/engine.sock (if running as root
       or if /run/aquaponics exists and is writable).
    4. User runtime directory via XDG_RUNTIME_DIR/aquaponics/engine.sock.
    5. Fallback user home directory ~/.aquaponics/engine.sock.

    Args:
        explicit_path: Optional explicit filesystem path.

    Returns:
        Resolved Path object for the Unix domain socket.
    """
    if explicit_path is not None:
        return Path(explicit_path).expanduser().resolve()

    env_path = os.environ.get("AQUAPONICS_SOCKET_PATH")
    if env_path:
        return Path(env_path).expanduser().resolve()

    # If running on POSIX and /run/aquaponics is writable or user is root
    prod_path = Path(DEFAULT_LINUX_PRODUCTION_SOCKET_PATH)
    if os.name == "posix":
        if os.geteuid() == 0 or (prod_path.parent.exists() and os.access(prod_path.parent, os.W_OK)):
            return prod_path

    # Fall back to XDG_RUNTIME_DIR if set
    xdg_runtime = os.environ.get("XDG_RUNTIME_DIR")
    if xdg_runtime:
        return (Path(xdg_runtime) / "aquaponics" / "engine.sock").resolve()

    # User home directory fallback
    return (Path.home() / ".aquaponics" / "engine.sock").resolve()


def serialize_message(payload: dict[str, Any]) -> bytes:
    """Serialize a message payload to newline-terminated UTF-8 JSON bytes.

    Args:
        payload: Dictionary representing the message data.

    Returns:
        Newline-terminated bytes suitable for socket transmission.
    """
    json_string = json.dumps(payload, ensure_ascii=False)
    return f"{json_string}\n".encode("utf-8")


def deserialize_message(raw_bytes: bytes) -> dict[str, Any]:
    """Deserialize newline-terminated JSON bytes into a message dictionary.

    Args:
        raw_bytes: Raw bytes received from the socket.

    Returns:
        Deserialized dictionary.

    Raises:
        ValueError: If payload cannot be decoded as valid JSON or is not a dict.
    """
    text = raw_bytes.decode("utf-8").strip()
    if not text:
        raise ValueError("Received empty IPC message payload")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object, got {type(data).__name__}")
    return data

