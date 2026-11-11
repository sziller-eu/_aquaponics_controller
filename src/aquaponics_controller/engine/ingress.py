"""Engine ingress subsystem for local IPC command ingestion.

This module implements the non-blocking ingress listener for the Aquaponics Engine,
enforcing Linux filesystem permissions, peer credential verification (SO_PEERCRED),
bounded processing time, and the provisional command-journal hook.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import select
import socket
import struct
from typing import TYPE_CHECKING, Any, Final

from aquaponics_controller.ipc import (
    COMMAND_STOP,
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_INGRESS_TIMEOUT_SECONDS,
    SOCKET_DIR_MODE,
    SOCKET_FILE_MODE,
    STATUS_ACCEPTED,
    STATUS_ERROR,
    STATUS_REJECTED,
    deserialize_message,
    serialize_message,
)

if TYPE_CHECKING:
    from aquaponics_controller.engine.lifecycle import AquaponicsEngine

logger = logging.getLogger("aquaponics.engine.ingress")

# Buffer size for reading a single IPC request line
MAX_IPC_REQUEST_BYTES: Final[int] = 4096


class EngineIngress:
    """Manages the Engine's local IPC listener and external intent ingestion.

    Responsibilities:
    - Binds and manages the local Unix domain socket with strict permissions (0o660).
    - Prevents starting duplicate instances over an active socket.
    - Inspects peer credentials (SO_PEERCRED on Linux) to prevent unauthorized control.
    - Enforces bounded processing so incoming communication never starves the control loop.
    - Routes accepted intents through the provisional command-journal hook.
    """

    def __init__(
        self,
        socket_path: Path,
        max_intents_per_cycle: int = 10,
        ingress_timeout: float = DEFAULT_INGRESS_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the Engine ingress listener.

        Args:
            socket_path: Filesystem path to the Unix domain socket.
            max_intents_per_cycle: Maximum number of incoming requests to process per cycle.
            ingress_timeout: Timeout in seconds for individual client socket read/write.
        """
        self._socket_path = socket_path
        self._max_intents_per_cycle = max_intents_per_cycle
        self._ingress_timeout = ingress_timeout
        self._server_socket: socket.socket | None = None
        self._is_active: bool = False
        self._is_tcp_fallback: bool = False
        self._tcp_port_file: Path | None = None

        # Provisional in-memory command journal records (until persistence issue #5)
        self._journaled_intents: list[dict[str, Any]] = []

    @property
    def socket_path(self) -> Path:
        """Return the socket path."""
        return self._socket_path

    @property
    def server_socket(self) -> socket.socket | None:
        """Return the underlying server listening socket."""
        return self._server_socket

    @property
    def journaled_intents(self) -> list[dict[str, Any]]:
        """Return a copy of the provisional command journal."""
        return list(self._journaled_intents)

    def start(self) -> None:
        """Bind and activate the IPC listening socket.

        Raises:
            RuntimeError: If another running Engine instance already occupies the socket.
            OSError: If socket creation or permission assignment fails.
        """
        if self._is_active:
            return

        self._ensure_parent_directory()
        self._verify_not_already_running()

        if hasattr(socket, "AF_UNIX"):
            self._start_unix_socket()
        else:
            # Fallback for platforms without AF_UNIX support (e.g. some Windows environments)
            self._start_tcp_fallback_socket()

        self._is_active = True
        logger.info("Engine ingress listening on %s", self._socket_path)

    def _ensure_parent_directory(self) -> None:
        """Create parent directory with restricted directory permissions."""
        parent = self._socket_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(parent, SOCKET_DIR_MODE)
        except OSError as exc:
            logger.debug("Could not chmod socket directory %s: %s", parent, exc)

    def _verify_not_already_running(self) -> None:
        """Check if an active Engine is already listening on the target socket path."""
        if not self._socket_path.exists():
            return

        # Attempt to probe the existing socket to see if a process is alive
        probe_sock: socket.socket | None = None
        try:
            if hasattr(socket, "AF_UNIX"):
                probe_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                probe_sock.settimeout(0.5)
                probe_sock.connect(str(self._socket_path))
            else:
                # Read TCP port from file
                content = self._socket_path.read_text().strip()
                if content.isdigit():
                    probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    probe_sock.settimeout(0.5)
                    probe_sock.connect(("127.0.0.1", int(content)))
                else:
                    self._socket_path.unlink(missing_ok=True)
                    return

            # If connection succeeded, an active Engine is already running!
            raise RuntimeError(
                f"Another Engine instance is already active and listening on {self._socket_path}"
            )
        except (ConnectionRefusedError, FileNotFoundError, OSError):
            # Dead socket file left behind by a previous unclean crash; safe to remove
            logger.debug("Removing stale socket file: %s", self._socket_path)
            self._socket_path.unlink(missing_ok=True)
        finally:
            if probe_sock is not None:
                probe_sock.close()

    def _start_unix_socket(self) -> None:
        """Bind and configure standard Unix domain stream socket."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.bind(str(self._socket_path))

        # Restrict socket file permissions to 0o660 (owner + group only)
        try:
            os.chmod(self._socket_path, SOCKET_FILE_MODE)
        except OSError as exc:
            logger.warning("Failed to set 0o660 permissions on %s: %s", self._socket_path, exc)

        sock.listen(5)
        self._server_socket = sock
        self._is_tcp_fallback = False

    def _start_tcp_fallback_socket(self) -> None:
        """Bind loopback TCP socket for non-POSIX development environments."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.bind(("127.0.0.1", 0))
        sock.listen(5)
        port = sock.getsockname()[1]
        self._socket_path.write_text(str(port), encoding="utf-8")
        self._server_socket = sock
        self._is_tcp_fallback = True
        logger.debug("Using TCP fallback on 127.0.0.1:%d written to %s", port, self._socket_path)

    def verify_peer_authorization(self, client_sock: socket.socket) -> bool:
        """Verify peer credentials using SO_PEERCRED where supported by the OS.

        On Linux, inspects the caller's UID/GID against the running process's UID/GID.
        On platforms where SO_PEERCRED is unavailable, returns True relying on filesystem
        permissions.

        Args:
            client_sock: Connected client socket.

        Returns:
            True if authorized, False otherwise.
        """
        # SO_PEERCRED is Linux-specific (SOL_SOCKET level)
        if hasattr(socket, "SO_PEERCRED") and hasattr(os, "geteuid"):
            try:
                cred = client_sock.getsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_PEERCRED,
                    struct.calcsize("3i"),
                )
                pid, uid, gid = struct.unpack("3i", cred)
                current_uid = os.geteuid()
                current_gid = os.getegid()

                # Authorized if caller has same UID, is root (0), or belongs to primary group
                if uid in (0, current_uid) or gid == current_gid:
                    return True

                logger.warning(
                    "Unauthorized peer credentials rejected: pid=%d, uid=%d, gid=%d (expected uid=%d)",
                    pid,
                    uid,
                    gid,
                    current_uid,
                )
                return False
            except OSError as exc:
                logger.warning("Failed to inspect SO_PEERCRED on client connection: %s", exc)
                return False

        # On non-Linux systems or where SO_PEERCRED is not available
        return True

    def process_pending_intents(self, engine: AquaponicsEngine) -> int:
        """Process incoming external intent requests without blocking indefinitely.

        Processes up to max_intents_per_cycle connections and then yields control back
        to the Engine loop.

        Args:
            engine: Running AquaponicsEngine instance to coordinate lifecycle actions.

        Returns:
            Number of intents processed during this invocation.
        """
        if self._server_socket is None or not self._is_active:
            return 0

        intents_processed = 0

        while intents_processed < self._max_intents_per_cycle:
            # Check if an incoming connection is ready with timeout 0 (non-blocking poll)
            readable, _, _ = select.select([self._server_socket], [], [], 0)
            if not readable:
                break

            try:
                client_sock, client_addr = self._server_socket.accept()
            except (BlockingIOError, InterruptedError):
                break
            except OSError as exc:
                logger.error("Error accepting IPC connection: %s", exc)
                break

            try:
                self._handle_client_connection(client_sock, engine)
                intents_processed += 1
            finally:
                try:
                    client_sock.close()
                except OSError:
                    pass

        return intents_processed

    def _handle_client_connection(
        self,
        client_sock: socket.socket,
        engine: AquaponicsEngine,
    ) -> None:
        """Handle a single client connection with bounded timeout and validation.

        Args:
            client_sock: Connected client socket.
            engine: Running AquaponicsEngine instance.
        """
        client_sock.settimeout(self._ingress_timeout)

        # 1. Authorize peer
        if not self.verify_peer_authorization(client_sock):
            self._send_response(
                client_sock,
                {
                    "status": STATUS_REJECTED,
                    "reason": "Permission denied: unauthorized peer credentials",
                },
            )
            return

        # 2. Read incoming intent payload
        try:
            raw_data = client_sock.recv(MAX_IPC_REQUEST_BYTES)
            if not raw_data:
                return
            payload = deserialize_message(raw_data)
        except (TimeoutError, socket.timeout):
            logger.warning("IPC client timed out during request transmission")
            return
        except ValueError as exc:
            logger.warning("Malformed IPC payload received: %s", exc)
            self._send_response(
                client_sock,
                {"status": STATUS_ERROR, "reason": f"Malformed payload: {exc}"},
            )
            return

        # 3. Route through provisional command journal hook
        self._journal_command_intent(payload)

        # 4. Process command
        command = payload.get("command")
        if command == COMMAND_STOP:
            logger.info("Received administrative STOP request from authorized client")
            self._send_response(
                client_sock,
                {
                    "status": STATUS_ACCEPTED,
                    "command": COMMAND_STOP,
                    "message": "Shutdown request accepted by Engine",
                },
            )
            engine.request_stop(
                reason="Operator requested controlled shutdown via CLI",
                is_operator=True,
            )
        else:
            logger.warning("Rejected unsupported command verb: %s", command)
            self._send_response(
                client_sock,
                {
                    "status": STATUS_REJECTED,
                    "reason": f"Unsupported command: '{command}'",
                },
            )

    def _journal_command_intent(self, intent_payload: dict[str, Any]) -> None:
        """Record accepted state-changing intent into the provisional command journal.

        Note:
            Issue #4 leaves full database persistence out of scope. This method serves
            as the explicit architectural hook required by ap_architecture_and_design.adoc
            so that the command path is preserved. Full durable SQLite persistence will
            be attached here in Issue #5.

        Args:
            intent_payload: The validated intent data structure.
        """
        logger.debug("Journaling incoming intent (provisional hook): %s", intent_payload)
        self._journaled_intents.append(dict(intent_payload))

    def _send_response(self, client_sock: socket.socket, response_data: dict[str, Any]) -> None:
        """Send a serialized response message back to the client.

        Args:
            client_sock: Connected client socket.
            response_data: Dictionary response payload.
        """
        try:
            data = serialize_message(response_data)
            client_sock.sendall(data)
        except (OSError, TimeoutError) as exc:
            logger.warning("Failed to deliver IPC response to client: %s", exc)

    def close(self) -> None:
        """Close the server socket and remove runtime filesystem artifacts."""
        self._is_active = False

        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError as exc:
                logger.debug("Error closing server socket: %s", exc)
            self._server_socket = None

        if self._socket_path.exists():
            try:
                self._socket_path.unlink()
            except OSError as exc:
                logger.debug("Error unlinking socket file %s: %s", self._socket_path, exc)

        logger.info("Engine ingress closed")
