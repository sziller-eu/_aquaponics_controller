"""The Engine-side IPC endpoint, exercised with real Unix sockets inside one process.

No threads are needed: connecting to a listening socket succeeds before the Engine
accepts, so a test can connect, send, let the ingress service its pending
connections, then read the answer.
"""

import json
import os
import socket
import stat
import time

import pytest

from aquaponics_controller import ipc_protocol as protocol
from aquaponics_controller.local_ingress import (
    EndpointInUseError,
    IngressLimits,
    IngressUnavailableError,
    LocalAdministrativeIngress,
)

from helpers import valid_stop_request


class RecordingHandler:
    def __init__(self):
        self.calls = []

    def __call__(self, request, peer):
        self.calls.append((request, peer))
        return protocol.AdministrativeResponse(True, status=protocol.STATUS_SHUTDOWN_INITIATED)


@pytest.fixture
def endpoint(short_directory):
    return short_directory / "ipc" / "engine.sock"


@pytest.fixture
def ingress(endpoint):
    ingress = LocalAdministrativeIngress(endpoint)
    ingress.open()
    yield ingress
    ingress.close()


def connect(endpoint) -> socket.socket:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(2)
    client.connect(str(endpoint))
    return client


def exchange(ingress, endpoint, payload: bytes, handler=None):
    handler = handler or RecordingHandler()
    with connect(endpoint) as client:
        client.sendall(payload)
        ingress.service_pending_requests(handler)
        return json.loads(client.recv(4096)), handler


def test_valid_request_reaches_the_handler_with_the_peer_credentials(ingress, endpoint):
    answer, handler = exchange(ingress, endpoint, valid_stop_request())

    assert answer["ok"] is True
    (request, peer), = handler.calls
    assert request.operation == "stop"
    assert request.client_submitted_at is not None
    assert (peer.uid, peer.gid, peer.pid) == (os.getuid(), os.getgid(), os.getpid())


@pytest.mark.parametrize(
    "payload, expected_code",
    [
        (b"garbage\n", protocol.ERROR_MALFORMED_REQUEST),
        (b'{"v": 9, "op": "stop"}\n', protocol.ERROR_UNSUPPORTED_PROTOCOL_VERSION),
        (b"x" * 5000, protocol.ERROR_REQUEST_TOO_LARGE),
        (b'{"v": 1, "op": "sto', protocol.ERROR_MALFORMED_REQUEST),  # no newline, then the client stops sending
    ],
)
def test_malformed_requests_get_an_error_and_never_reach_the_handler(ingress, endpoint, payload, expected_code):
    handler = RecordingHandler()
    with connect(endpoint) as client:
        client.sendall(payload)
        client.shutdown(socket.SHUT_WR)
        ingress.service_pending_requests(handler)
        answer = json.loads(client.recv(4096))

    assert answer["ok"] is False
    assert answer["error"] == expected_code
    assert handler.calls == []


def test_a_stalled_client_costs_only_a_bounded_time_and_is_told_so(endpoint):
    ingress = LocalAdministrativeIngress(endpoint, IngressLimits(read_deadline_seconds=0.1))
    ingress.open()
    try:
        with connect(endpoint) as stalled:  # connects, then sends nothing
            started = time.monotonic()
            ingress.service_pending_requests(RecordingHandler())
            elapsed = time.monotonic() - started
            answer = json.loads(stalled.recv(4096))
    finally:
        ingress.close()

    assert elapsed < 1.0
    assert answer["error"] == protocol.ERROR_REQUEST_TIMEOUT


def test_a_client_that_disconnects_at_once_does_not_disturb_the_engine(ingress, endpoint):
    handler = RecordingHandler()
    connect(endpoint).close()

    assert ingress.service_pending_requests(handler) == 1
    assert handler.calls == []

    answer, _ = exchange(ingress, endpoint, valid_stop_request())  # still serving
    assert answer["ok"] is True


def test_connections_per_call_are_bounded_and_the_rest_wait(endpoint):
    ingress = LocalAdministrativeIngress(endpoint, IngressLimits(max_connections_per_cycle=2))
    ingress.open()
    handler = RecordingHandler()
    clients = [connect(endpoint) for _ in range(5)]
    try:
        for client in clients:
            client.sendall(valid_stop_request())

        handled = [ingress.service_pending_requests(handler) for _ in range(3)]
    finally:
        for client in clients:
            client.close()
        ingress.close()

    assert handled == [2, 2, 1]


def test_total_time_per_call_is_bounded_even_with_many_stalled_clients(endpoint):
    limits = IngressLimits(read_deadline_seconds=0.1, cycle_budget_seconds=0.25, max_connections_per_cycle=50, listen_backlog=32)
    ingress = LocalAdministrativeIngress(endpoint, limits)
    ingress.open()
    stalled = [connect(endpoint) for _ in range(20)]
    try:
        started = time.monotonic()
        handled = ingress.service_pending_requests(RecordingHandler())
        elapsed = time.monotonic() - started
    finally:
        for client in stalled:
            client.close()
        ingress.close()

    assert elapsed < 0.6
    assert 0 < handled < 20


def test_directory_and_socket_are_owner_only_whatever_the_umask(endpoint):
    previous_umask = os.umask(0)
    try:
        ingress = LocalAdministrativeIngress(endpoint)
        ingress.open()
    finally:
        os.umask(previous_umask)
    try:
        assert stat.S_IMODE(endpoint.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(endpoint.stat().st_mode) == 0o600
        assert stat.S_ISSOCK(endpoint.stat().st_mode)
    finally:
        ingress.close()


def test_a_second_engine_on_the_same_endpoint_is_refused_and_the_first_is_undisturbed(ingress, endpoint):
    second = LocalAdministrativeIngress(endpoint)

    with pytest.raises(EndpointInUseError):
        second.open()
    second.close()  # what an Engine does in its cleanup after failing to start

    answer, _ = exchange(ingress, endpoint, valid_stop_request())  # the first Engine still owns the endpoint
    assert answer["ok"] is True


def test_a_stale_socket_left_by_a_crash_is_replaced(endpoint):
    endpoint.parent.mkdir(mode=0o700)
    leftover = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    leftover.bind(str(endpoint))
    leftover.close()  # the file stays behind, as after SIGKILL
    assert endpoint.exists()

    ingress = LocalAdministrativeIngress(endpoint)
    ingress.open()
    try:
        answer, _ = exchange(ingress, endpoint, valid_stop_request())
        assert answer["ok"] is True
    finally:
        ingress.close()


def test_something_that_is_not_a_socket_is_never_removed(endpoint):
    endpoint.parent.mkdir(mode=0o700)
    endpoint.write_text("precious")

    with pytest.raises(IngressUnavailableError):
        LocalAdministrativeIngress(endpoint).open()

    assert endpoint.read_text() == "precious"


def test_close_removes_the_endpoint_is_repeatable_and_allows_a_new_start(endpoint):
    ingress = LocalAdministrativeIngress(endpoint)
    ingress.open()

    ingress.close()
    ingress.close()

    assert not endpoint.exists()
    successor = LocalAdministrativeIngress(endpoint)
    successor.open()
    successor.close()


def test_an_overlong_endpoint_path_is_reported_as_unavailable(short_directory):
    too_long = short_directory / ("d" * 120) / "engine.sock"

    with pytest.raises(IngressUnavailableError):
        LocalAdministrativeIngress(too_long).open()


def test_servicing_before_open_does_nothing(endpoint):
    assert LocalAdministrativeIngress(endpoint).service_pending_requests(RecordingHandler()) == 0


# ---------------------------------------------------------------- readiness, time limit, correlation, traces (Issue #5)


def test_the_readiness_source_exists_only_while_the_ingress_is_open(endpoint):
    ingress = LocalAdministrativeIngress(endpoint)
    assert ingress.readiness_source is None

    ingress.open()
    assert ingress.readiness_source is not None
    ingress.close()

    assert ingress.readiness_source is None


def test_the_readiness_source_becomes_readable_when_a_client_waits(ingress, endpoint):
    import select

    source = ingress.readiness_source
    assert select.select([source], [], [], 0)[0] == []  # nobody is waiting

    with connect(endpoint):
        assert select.select([source], [], [], 2)[0] == [source]


def test_a_time_limit_tightens_the_budget_but_still_serves_one_connection(endpoint):
    limits = IngressLimits(read_deadline_seconds=0.1, cycle_budget_seconds=5, max_connections_per_cycle=50, listen_backlog=32)
    ingress = LocalAdministrativeIngress(endpoint, limits)
    ingress.open()
    stalled = [connect(endpoint) for _ in range(10)]
    try:
        started = time.monotonic()
        handled = ingress.service_pending_requests(RecordingHandler(), time_limit_seconds=0.05)
        elapsed = time.monotonic() - started
    finally:
        for client in stalled:
            client.close()
        ingress.close()

    assert handled == 1  # progress is guaranteed; the rest wait for the next call
    assert elapsed < 0.5


def test_a_zero_time_limit_serves_nothing(ingress, endpoint):
    with connect(endpoint):
        assert ingress.service_pending_requests(RecordingHandler(), time_limit_seconds=0) == 0


def test_a_time_limit_cannot_widen_the_configured_budget(endpoint):
    limits = IngressLimits(read_deadline_seconds=0.1, cycle_budget_seconds=0.25, max_connections_per_cycle=50, listen_backlog=32)
    ingress = LocalAdministrativeIngress(endpoint, limits)
    ingress.open()
    stalled = [connect(endpoint) for _ in range(20)]
    try:
        started = time.monotonic()
        handled = ingress.service_pending_requests(RecordingHandler(), time_limit_seconds=60)
        elapsed = time.monotonic() - started
    finally:
        for client in stalled:
            client.close()
        ingress.close()

    assert elapsed < 0.6 and 0 < handled < 20


def test_the_request_reaches_the_handler_with_its_identifier_and_arguments(ingress, endpoint):
    from datetime import datetime, timezone

    payload = protocol.encode_request(
        "echo", datetime.now(timezone.utc), correlation_id="req-7", arguments={"text": "hi"}
    )

    _, handler = exchange(ingress, endpoint, payload)

    (request, _peer), = handler.calls
    assert (request.correlation_id, request.arguments) == ("req-7", {"text": "hi"})


def test_a_refusal_carries_the_identifier_when_it_was_valid(ingress, endpoint):
    answer, handler = exchange(ingress, endpoint, b'{"v": 1, "op": 7, "id": "req-8"}\n')

    assert (answer["ok"], answer["error"], answer["id"]) == (False, protocol.ERROR_MALFORMED_REQUEST, "req-8")
    assert handler.calls == []


def test_an_invalid_identifier_is_refused_without_echoing_it_back(ingress, endpoint):
    answer, handler = exchange(ingress, endpoint, b'{"v": 1, "op": "ping", "id": ' + b'"' + b"x" * 80 + b'"}\n')

    assert answer["error"] == protocol.ERROR_INVALID_CORRELATION_ID
    assert "id" not in answer
    assert handler.calls == []


def test_diagnostic_traces_name_the_request_but_never_its_arguments(ingress, endpoint, caplog):
    from datetime import datetime, timezone

    secret = "do-not-log-this-text"
    payload = protocol.encode_request(
        "echo", datetime.now(timezone.utc), correlation_id="trace-1", arguments={"text": secret}
    )

    with caplog.at_level("INFO"):
        exchange(ingress, endpoint, payload)

    assert "IPC request received: op='echo' id='trace-1'" in caplog.text
    assert "IPC request answered: id='trace-1' ok=True delivered" in caplog.text
    assert secret not in caplog.text


def test_a_hostile_identifier_cannot_forge_log_lines(ingress, endpoint, caplog):
    forged = "x\nINFO fake: shutdown completed"
    payload = json.dumps({"v": 1, "op": "ping", "id": forged}).encode() + b"\n"

    with caplog.at_level("INFO"):
        exchange(ingress, endpoint, payload)

    assert all(not line.startswith("INFO fake") for line in caplog.text.splitlines())
    assert "\\n" in caplog.text  # quoted, not interpreted


# ---------------------------------------------------------------- IPC directory validation (Issue #8)


def mode_of(path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def assert_refused(endpoint, reason):
    with pytest.raises(IngressUnavailableError, match=reason):
        LocalAdministrativeIngress(endpoint).open()


def test_a_missing_directory_is_created_owner_only_and_its_parent_keeps_its_mode(short_directory):
    parent_mode = mode_of(short_directory)
    endpoint = short_directory / "ipc" / "engine.sock"

    ingress = LocalAdministrativeIngress(endpoint)
    ingress.open()
    ingress.close()

    assert mode_of(endpoint.parent) == 0o700
    assert mode_of(short_directory) == parent_mode


def test_a_directory_that_is_a_symbolic_link_is_refused_and_its_target_left_alone(short_directory):
    target = short_directory / "elsewhere"
    target.mkdir(mode=0o700)
    (short_directory / "ipc").symlink_to(target)

    assert_refused(short_directory / "ipc" / "engine.sock", "symbolic link")

    assert list(target.iterdir()) == [] and mode_of(target) == 0o700


def test_a_dangling_symbolic_link_is_refused_and_nothing_is_created_through_it(short_directory):
    target = short_directory / "does-not-exist"
    (short_directory / "ipc").symlink_to(target)

    assert_refused(short_directory / "ipc" / "engine.sock", "symbolic link")

    assert not target.exists()


def test_a_path_that_is_not_a_directory_is_refused(short_directory):
    (short_directory / "ipc").write_text("a file")

    assert_refused(short_directory / "ipc" / "engine.sock", "not a directory")

    assert (short_directory / "ipc").read_text() == "a file"


def test_a_directory_owned_by_another_user_is_refused(short_directory, monkeypatch):
    directory = short_directory / "ipc"
    directory.mkdir(mode=0o700)
    real_uid = os.geteuid()
    monkeypatch.setattr(os, "geteuid", lambda: real_uid + 1)  # as if the directory belonged to someone else

    assert_refused(directory / "engine.sock", "not owned")

    assert list(directory.iterdir()) == []


@pytest.mark.parametrize("unsafe_mode", [0o770, 0o730, 0o707, 0o777, 0o1777])
def test_a_directory_writable_by_others_is_refused_unchanged_and_nothing_is_created_in_it(short_directory, unsafe_mode):
    directory = short_directory / "ipc"
    directory.mkdir()
    directory.chmod(unsafe_mode)

    assert_refused(directory / "engine.sock", "writable by group or others")

    assert stat.S_IMODE(directory.stat().st_mode) == unsafe_mode
    assert list(directory.iterdir()) == []  # no lock file, no socket: refused before anything is created


@pytest.mark.parametrize("readable_mode", [0o750, 0o755, 0o711, 0o705])
def test_a_directory_only_readable_by_others_is_restricted_with_a_warning(short_directory, caplog, readable_mode):
    directory = short_directory / "ipc"
    directory.mkdir()
    directory.chmod(readable_mode)
    parent_mode = mode_of(short_directory)

    ingress = LocalAdministrativeIngress(directory / "engine.sock")
    with caplog.at_level("WARNING"):
        ingress.open()
    ingress.close()

    assert mode_of(directory) == 0o700
    assert f"had permissions {readable_mode:04o}; restricted to 0700" in caplog.text
    assert mode_of(short_directory) == parent_mode  # never a parent directory


def test_a_lock_file_that_is_a_symbolic_link_is_refused_and_its_target_left_alone(endpoint, short_directory):
    endpoint.parent.mkdir(mode=0o700)
    target = short_directory / "someone-elses-file"
    target.write_text("do not touch\n")
    endpoint.with_name(endpoint.name + ".lock").symlink_to(target)

    assert_refused(endpoint, "symbolic link")

    assert target.read_text() == "do not touch\n"
    assert not endpoint.exists()


def test_a_lock_file_that_is_a_named_pipe_is_refused_without_blocking(endpoint):
    endpoint.parent.mkdir(mode=0o700)
    os.mkfifo(endpoint.with_name(endpoint.name + ".lock"), 0o600)

    started = time.monotonic()
    assert_refused(endpoint, "not a regular file")

    assert time.monotonic() - started < 1.0
    assert not endpoint.exists()


def test_a_stale_socket_owned_by_another_user_is_not_removed(endpoint, monkeypatch):
    endpoint.parent.mkdir(mode=0o700)
    left_behind = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    left_behind.bind(str(endpoint))
    left_behind.close()
    real_lstat = type(endpoint).lstat

    def lstat_as_if_foreign(path):
        status = real_lstat(path)
        if path == endpoint:
            fields = list(status[:10])
            fields[4] = status.st_uid + 1  # st_uid
            return os.stat_result(fields)
        return status

    monkeypatch.setattr(type(endpoint), "lstat", lstat_as_if_foreign)

    assert_refused(endpoint, "owned by another user")

    monkeypatch.undo()
    assert stat.S_ISSOCK(endpoint.lstat().st_mode)  # still there
