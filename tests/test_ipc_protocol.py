import json
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


# ---------------------------------------------------------------- additive request/response fields (Issue #5)

SUBMITTED = datetime(2026, 11, 12, 6, 0, 0, tzinfo=timezone.utc)


def test_a_request_without_the_optional_fields_is_still_valid_protocol_version_1():
    request = protocol.decode_request(b'{"v": 1, "op": "stop"}')

    assert request.correlation_id is None
    assert request.arguments is None


def test_correlation_identifier_and_arguments_round_trip():
    line = protocol.encode_request("echo", SUBMITTED, correlation_id="req-1", arguments={"text": "héllo \U0001f600"})

    request = protocol.decode_request(line.rstrip(b"\n"))

    assert (request.operation, request.correlation_id) == ("echo", "req-1")
    assert request.arguments == {"text": "héllo \U0001f600"}


def test_the_optional_fields_are_absent_from_the_wire_when_not_given():
    sent = json.loads(protocol.encode_request("ping", SUBMITTED))

    assert set(sent) == {"v", "op", "submitted_at"}


@pytest.mark.parametrize("identifier", [7, True, ["a"], {"a": 1}, "", "x" * 65])
def test_invalid_correlation_identifiers_are_rejected_with_their_own_code(identifier):
    line = json.dumps({"v": 1, "op": "ping", "id": identifier}).encode()

    with pytest.raises(protocol.ProtocolError) as raised:
        protocol.decode_request(line)

    assert raised.value.error_code == protocol.ERROR_INVALID_CORRELATION_ID
    assert raised.value.correlation_id is None  # there is no valid identifier to hand back


def test_a_correlation_identifier_of_the_maximum_length_is_accepted():
    line = json.dumps({"v": 1, "op": "ping", "id": "x" * protocol.MAX_CORRELATION_ID_LENGTH}).encode()

    assert len(protocol.decode_request(line).correlation_id) == 64


@pytest.mark.parametrize("arguments", [[1], "text", 5, True])
def test_arguments_must_be_a_json_object(arguments):
    line = json.dumps({"v": 1, "op": "echo", "id": "abc", "args": arguments}).encode()

    with pytest.raises(protocol.ProtocolError) as raised:
        protocol.decode_request(line)

    assert raised.value.error_code == protocol.ERROR_INVALID_ARGUMENTS
    assert raised.value.correlation_id == "abc"  # the refusal can still be matched to its request


def test_a_refusal_after_a_valid_identifier_carries_it_but_a_version_refusal_does_not():
    with pytest.raises(protocol.ProtocolError) as malformed:
        protocol.decode_request(b'{"v": 1, "op": 7, "id": "abc"}')
    with pytest.raises(protocol.ProtocolError) as wrong_version:
        protocol.decode_request(b'{"v": 2, "op": "ping", "id": "abc"}')

    assert malformed.value.correlation_id == "abc"
    assert wrong_version.value.correlation_id is None


def test_query_response_round_trip_with_identifier_and_result():
    response = protocol.AdministrativeResponse(True, correlation_id="req-1", result={"reply": "pong", "n": 3})

    wire = json.loads(protocol.encode_response(response))

    assert wire == {"v": 1, "ok": True, "id": "req-1", "result": {"reply": "pong", "n": 3}}
    assert protocol.decode_response(protocol.encode_response(response).rstrip(b"\n")) == response


def test_the_largest_valid_echo_request_and_answer_fit_the_message_limit():
    worst_text = "\U0001f600" * protocol.MAX_ECHO_TEXT_LENGTH  # each character is a 12-byte JSON escape pair
    identifier = "i" * protocol.MAX_CORRELATION_ID_LENGTH

    request = protocol.encode_request("echo", SUBMITTED, correlation_id=identifier, arguments={"text": worst_text})
    response = protocol.encode_response(
        protocol.AdministrativeResponse(True, correlation_id=identifier, result={"text": worst_text})
    )

    assert len(request) <= protocol.MAX_MESSAGE_BYTES
    assert len(response) <= protocol.MAX_MESSAGE_BYTES


def test_absurdly_deep_nesting_is_a_protocol_error_not_an_exception_that_could_stop_the_engine():
    with pytest.raises(protocol.ProtocolError):
        protocol.decode_request(b"[" * 3000 + b"]" * 3000)  # CPython versions differ: RecursionError or a plain list
