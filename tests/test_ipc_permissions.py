"""Automated tests for IPC permission enforcement and authorization.

Tests cover:
- Socket directory (0o770) and file (0o660) mode enforcement
- SO_PEERCRED credential inspection logic
- Rejection of unauthorized peers without stopping the Engine
- Socket path resolution priority
- IPC serialization and deserialization
"""

from __future__ import annotations

import os
from pathlib import Path
import socket
import struct
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from aquaponics_controller.engine.ingress import EngineIngress
from aquaponics_controller.engine.lifecycle import AquaponicsEngine
from aquaponics_controller.ipc import (
    SOCKET_DIR_MODE,
    SOCKET_FILE_MODE,
    STATUS_ACCEPTED,
    STATUS_REJECTED,
    deserialize_message,
    resolve_socket_path,
    serialize_message,
)


class TestIpcPermissionsAndSecurity(unittest.TestCase):
    """Test suite for IPC transport permissions and access controls."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.socket_path = Path(self.temp_dir.name) / "test_perm.sock"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_permission_constants_comply_with_linux_security_model(self) -> None:
        """Socket file mode must be 0o660 (rw-rw----) and directory 0o770 (rwxrwx---)."""
        self.assertEqual(SOCKET_FILE_MODE, 0o660)
        self.assertEqual(SOCKET_DIR_MODE, 0o770)

    def test_resolve_socket_path_precedence(self) -> None:
        """Explicit path overrides environment variable and defaults."""
        explicit = Path("/tmp/custom.sock")
        self.assertEqual(resolve_socket_path(explicit), explicit.resolve())

        # Test AQUAPONICS_SOCKET_PATH environment variable
        with patch.dict(os.environ, {"AQUAPONICS_SOCKET_PATH": "/tmp/env.sock"}):
            self.assertEqual(resolve_socket_path(), Path("/tmp/env.sock").resolve())

    def test_serialization_and_deserialization_roundtrip(self) -> None:
        """JSON IPC messages serialize to newline-delimited bytes and deserialize cleanly."""
        payload = {"command": "STOP", "origin": "cli", "timestamp": 12345678}
        raw_bytes = serialize_message(payload)
        self.assertTrue(raw_bytes.endswith(b"\n"))

        recovered = deserialize_message(raw_bytes)
        self.assertEqual(recovered, payload)

    def test_deserialization_rejects_empty_or_malformed_payloads(self) -> None:
        """Invalid JSON or empty bytes must raise ValueError."""
        with self.assertRaises(ValueError):
            deserialize_message(b"")

        with self.assertRaises(ValueError):
            deserialize_message(b"   \n")

        with self.assertRaises(ValueError):
            deserialize_message(b"not json\n")

        with self.assertRaises(ValueError):
            deserialize_message(b'["array", "not", "dict"]\n')

    def test_peer_credential_verification_matching_uid_allowed(self) -> None:
        """Peer with matching UID is authorized."""
        ingress = EngineIngress(socket_path=self.socket_path)
        client_mock = MagicMock()

        # Simulate Linux SO_PEERCRED with matching current UID
        current_uid = 1000
        current_gid = 1000
        cred_bytes = struct.pack("3i", 1234, current_uid, current_gid)
        client_mock.getsockopt.return_value = cred_bytes

        with patch.object(socket, "SO_PEERCRED", 17, create=True):
            with patch("os.geteuid", return_value=current_uid, create=True):
                with patch("os.getegid", return_value=current_gid, create=True):
                    is_authorized = ingress.verify_peer_authorization(client_mock)

        self.assertTrue(is_authorized)

    def test_peer_credential_verification_root_uid_allowed(self) -> None:
        """Peer with root UID (0) is authorized."""
        ingress = EngineIngress(socket_path=self.socket_path)
        client_mock = MagicMock()

        cred_bytes = struct.pack("3i", 1, 0, 0)
        client_mock.getsockopt.return_value = cred_bytes

        with patch.object(socket, "SO_PEERCRED", 17, create=True):
            with patch("os.geteuid", return_value=1000, create=True):
                with patch("os.getegid", return_value=1000, create=True):
                    is_authorized = ingress.verify_peer_authorization(client_mock)

        self.assertTrue(is_authorized)

    def test_peer_credential_verification_unauthorized_uid_rejected(self) -> None:
        """Peer with mismatched non-root UID/GID is rejected."""
        ingress = EngineIngress(socket_path=self.socket_path)
        client_mock = MagicMock()

        # Peer UID 9999 when current UID is 1000
        cred_bytes = struct.pack("3i", 4321, 9999, 9999)
        client_mock.getsockopt.return_value = cred_bytes

        with patch.object(socket, "SO_PEERCRED", 17, create=True):
            with patch("os.geteuid", return_value=1000, create=True):
                with patch("os.getegid", return_value=1000, create=True):
                    is_authorized = ingress.verify_peer_authorization(client_mock)

        self.assertFalse(is_authorized)

    def test_unauthorized_peer_connection_does_not_stop_engine(self) -> None:
        """An unauthorized client connection must be rejected and must NOT trigger stop."""
        ingress = EngineIngress(socket_path=self.socket_path)
        engine = AquaponicsEngine(
            socket_path=self.socket_path,
            ingress=ingress,
            cycle_interval_seconds=0.01,
        )

        client_mock = MagicMock()
        # Force unauthorized
        ingress.verify_peer_authorization = MagicMock(return_value=False)  # type: ignore[method-assign]

        ingress._handle_client_connection(client_mock, engine)

        # Engine must NOT have received stop request
        self.assertFalse(engine.stop_requested)

        # Ingress must have sent REJECTED response
        client_mock.sendall.assert_called_once()
        sent_data = client_mock.sendall.call_args[0][0]
        response = deserialize_message(sent_data)
        self.assertEqual(response.get("status"), STATUS_REJECTED)
        self.assertIn("Permission denied", response.get("reason", ""))


if __name__ == "__main__":
    unittest.main()
