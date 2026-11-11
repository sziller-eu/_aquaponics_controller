from datetime import datetime, timezone

import pytest

from aquaponics_controller import ipc_protocol as protocol


def test_request_round_trip_keeps_the_client_reported_time():
    submitted = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)

    request = protocol.decode_request(protocol.encode_request("stop", submitted).rstrip(b"\n"))

    assert request.operation == "stop"
    assert request.client_submitted_at == submitted


@pytest.mark.parametrize(
    "line, expected_code",
    [
        (b"", protocol.ERROR_MALFORMED_REQUEST),
        (b"not json", protocol.ERROR_MALFORMED_REQUEST),
        (b"\xff\xfe", protocol.ERROR_MALFORMED_REQUEST),
        (b"[1, 2]", protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 1}', protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 1, "op": 7}', protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 1, "op": ""}', protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 1, "op": "stop", "submitted_at": "yesterday"}', protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 1, "op": "stop", "submitted_at": "2026-11-11T12:00:00"}', protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 2, "op": "stop"}', protocol.ERROR_UNSUPPORTED_PROTOCOL_VERSION),
        (b'{"op": "stop"}', protocol.ERROR_UNSUPPORTED_PROTOCOL_VERSION),
    ],
)
def test_unacceptable_requests_are_rejected_with_a_code(line, expected_code):
    with pytest.raises(protocol.ProtocolError) as raised:
        protocol.decode_request(line)

    assert raised.value.error_code == expected_code


def test_response_round_trip():
    response = protocol.AdministrativeResponse(True, status=protocol.STATUS_SHUTDOWN_INITIATED, audit_recorded=False)

    assert protocol.decode_response(protocol.encode_response(response).rstrip(b"\n")) == response


def test_unknown_operations_are_decoded_so_the_engine_can_refuse_them():
    request = protocol.decode_request(b'{"v": 1, "op": "reboot_the_world"}')

    assert request.operation == "reboot_the_world"
    assert request.client_submitted_at is None


def test_endpoint_resolution_order(monkeypatch):
    monkeypatch.setenv(protocol.ENDPOINT_ENVIRONMENT_VARIABLE, "/from/env.sock")
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/42")
    assert str(protocol.resolve_endpoint_path("/explicit.sock")) == "/explicit.sock"
    assert str(protocol.resolve_endpoint_path(None)) == "/from/env.sock"

    monkeypatch.delenv(protocol.ENDPOINT_ENVIRONMENT_VARIABLE)
    assert str(protocol.resolve_endpoint_path(None)) == "/run/user/42/aquaponics/engine.sock"

    monkeypatch.delenv("XDG_RUNTIME_DIR")
    with pytest.raises(ValueError):
        protocol.resolve_endpoint_path(None)
