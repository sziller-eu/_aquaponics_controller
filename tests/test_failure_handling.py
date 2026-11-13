"""Issue #8: lifecycle and IPC failure handling.

- the client: connection-only retry while the Engine's connection queue is full, one overall
  deadline, never a resent request, and distinct outcomes for "unavailable" and "busy";
- the ingress: only the approved recoverable connection errors are contained per connection,
  anything else propagates; a failing request handler first sends a fixed internal_error answer;
- the Engine: the bounded final servicing pass of a controlled shutdown, and failures during it;
- recovery from descriptor exhaustion, and repeated STOP and signals end to end.

Fake connections and fake servers are used where a real kernel condition cannot be produced on
demand. Nothing here touches hardware or the network.
"""

import errno
import json
import logging
import os
import signal
import socket
import stat
import struct
import threading
import time
from datetime import datetime, timezone

import pytest

from aquaponics_controller import cli
from aquaponics_controller import ipc_protocol as protocol
from aquaponics_controller.engine import SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS, Engine, EngineConfig, ExitStatus
from aquaponics_controller.lifecycle_audit import LifecycleAudit, read_lifecycle_records
from aquaponics_controller.local_ingress import (
    RECOVERABLE_CONNECTION_ERRNOS,
    LocalAdministrativeIngress,
    is_recoverable_connection_error,
)

from helpers import SHUTDOWN_TOLERANCE_SECONDS, EngineProcess, events_of, read_audit, run_ape
from test_engine_lifecycle import (  # noqa: F401 - audit_path is a fixture
    OWN_PEER,
    STOP,
    ScriptedIngress,
    audit_path,
    build,
    raise_error,
    send,
    start_watchdog,
)

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def request_bytes(operation="ping", identifier="req-7") -> bytes:
    return protocol.encode_request(operation, NOW, correlation_id=identifier)


def answer_of(raw: bytes) -> dict:
    return json.loads(raw.split(b"\n", 1)[0])


# ================================================================ the client (ape)


@pytest.fixture
def test_listener(short_directory):
    """A listening socket nobody serves unless the test does: backlog 0 (the kernel then queues one)."""
    path = short_directory / "test.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    server.listen(0)
    server.setblocking(False)
    yield server, path
    server.close()


def fill_connection_queue(path) -> list[socket.socket]:
    """Connect until the listener's queue is full; return the connections that got in."""
    fillers = []
    while len(fillers) < 64:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.setblocking(False)
        try:
            client.connect(str(path))
        except BlockingIOError:
            client.close()
            return fillers
        fillers.append(client)
    raise AssertionError("the connection queue never filled")


def accept_all_waiting(server) -> list[socket.socket]:
    accepted = []
    server.setblocking(False)
    while True:
        try:
            accepted.append(server.accept()[0])
        except BlockingIOError:
            return accepted


def read_line(connection, timeout=3.0) -> bytes:
    connection.settimeout(timeout)
    received = b""
    while b"\n" not in received:
        chunk = connection.recv(4096)
        if not chunk:
            break
        received += chunk
    return received


def test_a_full_connection_queue_is_retried_until_the_timeout_then_reported_as_busy(test_listener):
    server, path = test_listener
    fillers = fill_connection_queue(path)

    started = time.monotonic()
    with pytest.raises(cli.ClientError) as caught:
        cli.send_request(path, protocol.OPERATION_PING, 0.5)
    elapsed = time.monotonic() - started

    assert caught.value.exit_status == cli.EXIT_TIMEOUT
    assert "busy" in str(caught.value) and "connection queue" in str(caught.value)
    assert 0.45 <= elapsed < 2.0  # it waited for the whole timeout: neither at once nor longer
    assert len(accept_all_waiting(server)) == len(fillers)  # the client never got in, so it sent nothing
    for filler in fillers:
        filler.close()


def test_ape_exits_5_when_the_engine_stays_busy(test_listener, capsys):
    _, path = test_listener
    fillers = fill_connection_queue(path)

    status = cli.main(["--ping", "--socket", str(path), "--timeout", "0.3"])

    assert status == cli.EXIT_TIMEOUT == 5
    assert "the Engine is busy" in capsys.readouterr().err
    for filler in fillers:
        filler.close()


def test_when_the_queue_clears_the_request_is_sent_exactly_once_and_answered(test_listener):
    server, path = test_listener
    fillers = fill_connection_queue(path)
    requests = []

    def engine_that_catches_up():
        time.sleep(0.2)
        server.settimeout(3)
        for _ in fillers:  # first-in, first-out: the client's connection can only come after these
            server.accept()[0].close()
        connection, _ = server.accept()
        with connection:
            line = read_line(connection)
            requests.append(line)
            identifier = json.loads(line)["id"]
            connection.sendall(
                protocol.encode_response(
                    protocol.AdministrativeResponse(True, result={"reply": "pong"}, correlation_id=identifier)
                )
            )

    worker = threading.Thread(target=engine_that_catches_up)
    worker.start()
    response = cli.send_request(path, protocol.OPERATION_PING, 5.0)
    worker.join(5)

    assert response.ok and response.result == {"reply": "pong"}
    assert len(requests) == 1
    assert accept_all_waiting(server) == []  # no second connection, no resent request
    for filler in fillers:
        filler.close()


def test_a_request_without_an_answer_is_never_resent(test_listener):
    server, path = test_listener
    requests = []

    def engine_that_reads_and_hangs_up():
        server.settimeout(3)
        connection, _ = server.accept()
        with connection:
            requests.append(read_line(connection))

    worker = threading.Thread(target=engine_that_reads_and_hangs_up)
    worker.start()
    with pytest.raises(cli.ClientError) as caught:
        cli.send_request(path, protocol.OPERATION_STOP, 3.0)
    worker.join(5)

    assert caught.value.exit_status == cli.EXIT_COMMUNICATION_FAILURE
    assert "not acknowledged" in str(caught.value)
    assert len(requests) == 1
    assert accept_all_waiting(server) == []


def test_one_overall_deadline_covers_connecting_sending_and_waiting(test_listener):
    _, path = test_listener  # connecting succeeds (queued), but nobody ever answers

    started = time.monotonic()
    with pytest.raises(cli.ClientError) as caught:
        cli.send_request(path, protocol.OPERATION_PING, 0.4)

    assert caught.value.exit_status == cli.EXIT_TIMEOUT
    assert time.monotonic() - started < 1.0


def test_an_absent_engine_is_reported_at_once_without_retrying(short_directory):
    missing = short_directory / "nothing-here.sock"
    stale = short_directory / "stale.sock"
    left_behind = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    left_behind.bind(str(stale))
    left_behind.close()  # the file stays: a stale endpoint, nothing listening

    for path in (missing, stale):
        started = time.monotonic()
        with pytest.raises(cli.ClientError) as caught:
            cli.send_request(path, protocol.OPERATION_PING, 5.0)
        assert caught.value.exit_status == cli.EXIT_UNREACHABLE
        assert time.monotonic() - started < 0.5  # not retried until the timeout


@pytest.mark.skipif(os.geteuid() == 0, reason="root is not denied by file permissions")
def test_a_permission_problem_is_reported_at_once(test_listener):
    _, path = test_listener
    path.chmod(0o000)

    started = time.monotonic()
    with pytest.raises(cli.ClientError) as caught:
        cli.send_request(path, protocol.OPERATION_PING, 5.0)

    assert caught.value.exit_status == cli.EXIT_PERMISSION_DENIED
    assert time.monotonic() - started < 0.5


def test_an_internal_error_answer_gives_exit_6_and_says_the_engine_is_stopping(test_listener, capsys):
    server, path = test_listener

    def engine_that_fails_internally():
        server.settimeout(3)
        connection, _ = server.accept()
        with connection:
            identifier = json.loads(read_line(connection))["id"]
            connection.sendall(
                protocol.encode_response(
                    protocol.AdministrativeResponse(
                        False,
                        error_code=protocol.ERROR_INTERNAL,
                        message=protocol.INTERNAL_ERROR_MESSAGE,
                        correlation_id=identifier,
                    )
                )
            )

    worker = threading.Thread(target=engine_that_fails_internally)
    worker.start()
    status = cli.main(["--ping", "--socket", str(path), "--timeout", "3"])
    worker.join(5)

    assert status == cli.EXIT_COMMUNICATION_FAILURE == 6
    assert "internal error and is stopping" in capsys.readouterr().err


# ================================================================ the ingress: failure classification


def test_exactly_the_approved_connection_errors_are_recoverable():
    assert RECOVERABLE_CONNECTION_ERRNOS == {
        errno.ECONNRESET,
        errno.EPIPE,
        errno.ECONNABORTED,
        errno.ENOTCONN,
        errno.ENOBUFS,
        errno.ENOMEM,
        errno.EAGAIN,
        errno.EWOULDBLOCK,
    }
    assert is_recoverable_connection_error(TimeoutError("timed out"))
    for code in (errno.EBADF, errno.EINVAL, errno.ENOTSOCK, errno.EFAULT, errno.EIO):
        assert not is_recoverable_connection_error(OSError(code, os.strerror(code)))


class FakeConnection:
    """An accepted connection whose operations can be made to fail with a chosen error."""

    def __init__(self, request=b"", *, recv_error=None, send_error=None, peer_error=None, close_error=None):
        self.incoming = request
        self.close_error = close_error
        self.recv_error = recv_error
        self.send_error = send_error
        self.peer_error = peer_error
        self.sent = b""
        self.closed = False

    def getsockopt(self, *arguments):
        if self.peer_error:
            raise self.peer_error
        return struct.pack("3i", os.getpid(), os.geteuid(), os.getegid())

    def settimeout(self, seconds):
        pass

    def recv(self, size):
        if self.recv_error:
            raise self.recv_error
        chunk, self.incoming = self.incoming[:size], self.incoming[size:]
        return chunk

    def sendall(self, data):
        if self.send_error:
            raise self.send_error
        self.sent += data

    def close(self):
        self.closed = True
        if self.close_error:
            raise self.close_error


class ListenerWithFakes:
    """Wraps the real listening socket: hands out the given fake connections first."""

    def __init__(self, real, fakes):
        self.real = real
        self.fakes = list(fakes)

    def accept(self):
        if self.fakes:
            return self.fakes.pop(0), None
        return self.real.accept()

    def fileno(self):
        return self.real.fileno()

    def close(self):
        self.real.close()


class RecordingHandler:
    def __init__(self):
        self.calls = []

    def __call__(self, request, peer):
        self.calls.append((request, peer))
        return protocol.AdministrativeResponse(True, result={"reply": "pong"}, correlation_id=request.correlation_id)


@pytest.fixture
def ingress(short_directory):
    ingress = LocalAdministrativeIngress(short_directory / "ipc" / "engine.sock")
    ingress.open()
    yield ingress
    ingress.close()


def hand_out(ingress, *fakes):
    ingress._listener = ListenerWithFakes(ingress._listener, fakes)


def ask_for_real(ingress, handler) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(str(ingress.endpoint))
        client.sendall(request_bytes(identifier="afterwards"))
        ingress.service_pending_requests(handler)
        return answer_of(client.recv(4096))


RECOVERABLE_ERRORS = [
    ConnectionResetError(errno.ECONNRESET, "Connection reset by peer"),
    BrokenPipeError(errno.EPIPE, "Broken pipe"),
    ConnectionAbortedError(errno.ECONNABORTED, "Software caused connection abort"),
    OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
    OSError(errno.ENOBUFS, "No buffer space available"),
    OSError(errno.ENOMEM, "Cannot allocate memory"),
    BlockingIOError(errno.EAGAIN, "Resource temporarily unavailable"),
]


@pytest.mark.parametrize("error", RECOVERABLE_ERRORS, ids=lambda e: errno.errorcode[e.errno])
@pytest.mark.parametrize("where", ["recv", "sendall"])
def test_a_recoverable_connection_error_ends_only_that_connection(ingress, caplog, where, error):
    if where == "recv":
        fake = FakeConnection(recv_error=error)
    else:
        fake = FakeConnection(request_bytes(), send_error=error)
    hand_out(ingress, fake)
    handler = RecordingHandler()

    with caplog.at_level(logging.WARNING):
        assert ingress.service_pending_requests(handler) == 1  # nothing raised

    assert fake.closed
    assert any(record.levelno == logging.WARNING for record in caplog.records)
    assert ask_for_real(ingress, handler)["ok"] is True  # the next client is served as usual


def test_a_timeout_while_answering_is_recoverable_too(ingress):
    fake = FakeConnection(request_bytes(), send_error=TimeoutError("timed out"))
    hand_out(ingress, fake)

    assert ingress.service_pending_requests(RecordingHandler()) == 1
    assert fake.closed


@pytest.mark.parametrize("code", [errno.EBADF, errno.EINVAL])
@pytest.mark.parametrize("where", ["recv", "sendall", "getsockopt"])
def test_an_unclassified_connection_error_is_not_contained(ingress, where, code):
    error = OSError(code, os.strerror(code))
    fake = {
        "recv": lambda: FakeConnection(recv_error=error),
        "sendall": lambda: FakeConnection(request_bytes(), send_error=error),
        "getsockopt": lambda: FakeConnection(request_bytes(), peer_error=error),
    }[where]()
    hand_out(ingress, fake)

    with pytest.raises(OSError) as caught:
        ingress.service_pending_requests(RecordingHandler())

    assert caught.value is error  # propagated unchanged: it becomes an Engine failure
    assert fake.closed


def test_unreadable_peer_credentials_after_a_disconnect_mean_an_unidentified_peer(ingress):
    fake = FakeConnection(request_bytes(), peer_error=OSError(errno.ENOTCONN, "not connected"))
    hand_out(ingress, fake)
    handler = RecordingHandler()

    ingress.service_pending_requests(handler)

    assert handler.calls[0][1] is None  # the Engine refuses an unidentified peer


def test_a_failing_handler_sends_a_fixed_internal_error_and_its_exception_propagates(ingress):
    failure = RuntimeError("secret internal detail")

    def failing_handler(request, peer):
        raise failure

    fake = FakeConnection(request_bytes(identifier="req-7"))
    hand_out(ingress, fake)

    with pytest.raises(RuntimeError) as caught:
        ingress.service_pending_requests(failing_handler)

    assert caught.value is failure
    answer = answer_of(fake.sent)
    assert answer["ok"] is False
    assert answer["error"] == protocol.ERROR_INTERNAL
    assert answer["message"] == protocol.INTERNAL_ERROR_MESSAGE
    assert answer["id"] == "req-7"
    assert b"secret" not in fake.sent and b"RuntimeError" not in fake.sent  # no exception details
    assert len(fake.sent) < 300  # one small, fixed answer


@pytest.mark.parametrize(
    "send_error",
    [BrokenPipeError(errno.EPIPE, "Broken pipe"), OSError(errno.EBADF, "Bad file descriptor")],
    ids=["client-gone", "unclassified-send-error"],
)
def test_the_handler_failure_stays_the_one_reported_even_if_the_notice_cannot_be_sent(ingress, send_error):
    failure = RuntimeError("the real problem")

    def failing_handler(request, peer):
        raise failure

    hand_out(ingress, FakeConnection(request_bytes(), send_error=send_error))

    with pytest.raises(RuntimeError) as caught:
        ingress.service_pending_requests(failing_handler)

    assert caught.value is failure


def test_closing_a_connection_follows_the_same_classification(ingress):
    hand_out(ingress, FakeConnection(request_bytes(), close_error=ConnectionResetError(errno.ECONNRESET, "reset")))
    assert ingress.service_pending_requests(RecordingHandler()) == 1  # recoverable: contained

    defect = OSError(errno.EBADF, "Bad file descriptor")
    hand_out(ingress, FakeConnection(request_bytes(), close_error=defect))
    with pytest.raises(OSError) as caught:
        ingress.service_pending_requests(RecordingHandler())
    assert caught.value is defect  # unclassified: propagates


def test_a_close_error_never_replaces_an_engine_failure_on_its_way_out(ingress):
    failure = RuntimeError("the real problem")

    def failing_handler(request, peer):
        raise failure

    hand_out(ingress, FakeConnection(request_bytes(), close_error=OSError(errno.EBADF, "Bad file descriptor")))

    with pytest.raises(RuntimeError) as caught:
        ingress.service_pending_requests(failing_handler)

    assert caught.value is failure


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, SystemExit])
def test_interruptions_are_never_turned_into_an_answer(ingress, interruption):
    def interrupted_handler(request, peer):
        raise interruption()

    fake = FakeConnection(request_bytes())
    hand_out(ingress, fake)

    with pytest.raises(interruption):
        ingress.service_pending_requests(interrupted_handler)

    assert fake.sent == b""


# ================================================================ the ingress: withdrawing the endpoint


def test_after_withdrawal_new_clients_find_no_engine_but_queued_ones_are_still_served(ingress):
    queued = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    queued.settimeout(2)
    queued.connect(str(ingress.endpoint))
    queued.sendall(request_bytes(identifier="queued"))

    ingress.withdraw_endpoint()

    assert not ingress.endpoint.exists()
    with pytest.raises(cli.ClientError) as caught:
        cli.send_request(ingress.endpoint, protocol.OPERATION_PING, 2.0)
    assert caught.value.exit_status == cli.EXIT_UNREACHABLE
    assert ingress.service_pending_requests(RecordingHandler()) == 1
    assert answer_of(queued.recv(4096))["id"] == "queued"
    queued.close()


@pytest.mark.parametrize("shutting_down", [False, True], ids=["operation", "final-shutdown-pass"])
def test_a_used_up_servicing_budget_is_reported_for_what_it_means(ingress, caplog, shutting_down):
    stalled = queue_clients(ingress.endpoint, [b""] * 3)  # each costs its 0.2 s read deadline
    if shutting_down:
        ingress.withdraw_endpoint()

    with caplog.at_level(logging.WARNING):
        handled = ingress.service_pending_requests(RecordingHandler(), SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS)

    assert handled == 2  # the limits are the same either way: budget used up after two stalled clients
    budget_messages = [r.getMessage() for r in caplog.records if "budget used up" in r.getMessage()]
    assert len(budget_messages) == 1
    if shutting_down:
        assert budget_messages[0].startswith("shutdown servicing budget used up (2 connections handled within 0.250 s)")
        assert "remaining queued connections may not receive an answer" in budget_messages[0]
        assert "next cycle" not in budget_messages[0]
    else:
        assert "remaining connections wait for the next cycle" in budget_messages[0]
    for client in stalled:
        client.close()


def test_withdrawal_never_removes_a_socket_this_ingress_did_not_bind(ingress):
    ingress.endpoint.unlink()
    impostor = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    impostor.bind(str(ingress.endpoint))
    try:
        ingress.withdraw_endpoint()
        ingress.withdraw_endpoint()  # repeatable
        assert ingress.endpoint.exists()
    finally:
        impostor.close()
        ingress.endpoint.unlink()


# ================================================================ the Engine: the final servicing pass


def test_a_controlled_shutdown_withdraws_the_endpoint_then_serves_the_queue_once_within_its_limit(audit_path):
    ingress = ScriptedIngress({2: send(STOP, OWN_PEER)}, final_pass=send(STOP, OWN_PEER))
    engine = build(audit_path, ingress)

    assert engine.run() is ExitStatus.SUCCESS

    assert ingress.withdrawn == 1
    assert ingress.final_pass_limits == [SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS]
    first, second = ingress.responses
    assert first.status == protocol.STATUS_SHUTDOWN_INITIATED
    assert second.status == protocol.STATUS_SHUTDOWN_ALREADY_INITIATED  # acknowledged, not acted on again
    records = read_lifecycle_records(audit_path)
    assert events_of(records) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "STOP_REQUEST_RECEIVED",  # the STOP served during shutdown: audited, no second transition
        "SHUTDOWN_COMPLETED",
    ]
    assert records[4]["context"]["shutdown_already_initiated"] is True


def test_a_signal_shutdown_serves_the_queue_too(audit_path):
    ingress = ScriptedIngress(final_pass=send(STOP, OWN_PEER))
    engine = build(audit_path, ingress)
    engine.request_stop_for_signal(signal.SIGTERM)

    assert engine.run() is ExitStatus.SUCCESS
    assert ingress.final_pass_limits == [SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS]
    assert ingress.responses[0].status == protocol.STATUS_SHUTDOWN_ALREADY_INITIATED


def test_after_an_engine_failure_there_is_no_final_servicing_pass(audit_path):
    ingress = ScriptedIngress({2: raise_error(RuntimeError("injected"))}, final_pass=send(STOP, OWN_PEER))
    engine = build(audit_path, ingress)

    assert engine.run() is ExitStatus.FAILURE
    assert ingress.withdrawn == 0 and ingress.final_pass_limits == []
    assert ingress.closed == 1


def test_a_failure_during_the_final_pass_is_an_engine_failure_and_cleanup_still_runs_once(audit_path):
    ingress = ScriptedIngress({2: send(STOP, OWN_PEER)}, final_pass=raise_error(RuntimeError("injected")))
    engine = build(audit_path, ingress)

    assert engine.run() is ExitStatus.FAILURE

    assert ingress.closed == 1
    records = read_lifecycle_records(audit_path)
    assert events_of(records) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "ENGINE_FAILURE",
        "SHUTDOWN_COMPLETED",
    ]
    failure, completed = records[4], records[5]
    assert failure["reason"] == "RuntimeError" and failure["context"]["phase"] == "shutdown"
    assert completed["context"]["exit_status"] == int(ExitStatus.FAILURE) == 1
    assert completed["reason"] == "OPERATOR_STOP"  # what started the shutdown


def test_a_failure_to_withdraw_the_endpoint_is_reported_and_the_shutdown_goes_on(audit_path, caplog):
    ingress = ScriptedIngress({2: send(STOP, OWN_PEER)}, withdraw_error=PermissionError(errno.EACCES, "denied"))
    engine = build(audit_path, ingress)

    with caplog.at_level(logging.WARNING):
        assert engine.run() is ExitStatus.SUCCESS

    assert "withdrawing the IPC endpoint failed" in caplog.text
    assert ingress.final_pass_limits == [SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS]
    assert ingress.closed == 1


def test_an_unexpected_error_while_withdrawing_the_endpoint_is_an_engine_failure_and_cleanup_still_runs(audit_path):
    ingress = ScriptedIngress({2: send(STOP, OWN_PEER)}, withdraw_error=RuntimeError("defect"))
    engine = build(audit_path, ingress)

    assert engine.run() is ExitStatus.FAILURE

    assert ingress.closed == 1
    assert ingress.final_pass_limits == []  # the pass does not run after the failure
    failure = next(r for r in read_lifecycle_records(audit_path) if r["event"] == "ENGINE_FAILURE")
    assert failure["context"]["phase"] == "shutdown"


class QueueingIngress(LocalAdministrativeIngress):
    """A real ingress that, once open, lets the test act (queue clients) before the Engine serves anyone."""

    def __init__(self, endpoint, on_open):
        super().__init__(endpoint)
        self.on_open = on_open

    def open(self):
        super().open()
        self.on_open(self)


def real_engine(short_directory, on_open, *, engine_class=Engine, **config):
    ingress = QueueingIngress(short_directory / "ipc" / "engine.sock", on_open)
    engine = engine_class(
        execution_id="queue-test",
        config=EngineConfig(cycle_interval_seconds=config.pop("interval", 0.05), **config),
        ingress=ingress,
        audit=LifecycleAudit(short_directory / "audit.jsonl", "queue-test"),
    )
    return engine, ingress


def queue_clients(endpoint, payloads) -> list[socket.socket]:
    clients = []
    for payload in payloads:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(2)
        client.connect(str(endpoint))
        if payload:
            client.sendall(payload)
        clients.append(client)
    return clients


def outcome(client) -> dict | str:
    try:
        raw = client.recv(4096)
    except ConnectionResetError:
        return "no answer"
    finally:
        client.close()
    return answer_of(raw) if raw else "no answer"


def test_queued_clients_beyond_the_limit_get_no_answer_and_the_shutdown_is_not_extended(short_directory):
    clients = []

    def queue_then_stop(ingress):
        payloads = [request_bytes("stop", "stop-0")] + [request_bytes("ping", f"ping-{n}") for n in range(1, 9)]
        clients.extend(queue_clients(ingress.endpoint, payloads))  # 9: the backlog of 8 admits 9
        engine.request_stop_for_signal(signal.SIGTERM)

    engine, ingress = real_engine(short_directory, queue_then_stop)
    started = time.monotonic()
    assert engine.run() is ExitStatus.SUCCESS
    assert time.monotonic() - started < SHUTDOWN_TOLERANCE_SECONDS

    outcomes = [outcome(client) for client in clients]
    assert outcomes[0]["status"] == protocol.STATUS_SHUTDOWN_ALREADY_INITIATED
    assert all(answer["ok"] for answer in outcomes[1:8])  # 8 per call: the usual limit
    assert outcomes[8] == "no answer"  # beyond the limit: not answered, not waited for
    assert not ingress.endpoint.exists()
    records = read_lifecycle_records(short_directory / "audit.jsonl")
    assert events_of(records) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "SHUTDOWN_INITIATED",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_COMPLETED",
    ]


def test_stalled_clients_at_shutdown_cost_only_the_final_pass_limit(short_directory):
    clients = []

    def queue_stalled_then_stop(ingress):
        clients.extend(queue_clients(ingress.endpoint, [b""] * 6))  # connected, never send anything
        engine.request_stop_for_signal(signal.SIGTERM)

    engine, _ = real_engine(short_directory, queue_stalled_then_stop)
    started = time.monotonic()
    assert engine.run() is ExitStatus.SUCCESS
    elapsed = time.monotonic() - started

    # The pass stops admitting connections after 0.25 s; one admitted connection can take its 0.2 s
    # read deadline beyond that. Six stalled clients would otherwise cost 1.2 s.
    assert elapsed < SHUTDOWN_SERVICING_TIME_LIMIT_SECONDS + 0.4 + 0.3
    for client in clients:
        client.close()


def test_the_final_pass_keeps_the_authorization_rules(short_directory):
    clients = []

    def queue_then_stop(ingress):
        clients.extend(queue_clients(ingress.endpoint, [request_bytes("ping", "unauthorized")]))
        engine.request_stop_for_signal(signal.SIGTERM)

    engine, _ = real_engine(short_directory, queue_then_stop, administrative_uids=frozenset({999_999}))
    assert engine.run() is ExitStatus.SUCCESS

    answer = outcome(clients[0])
    assert answer["ok"] is False and answer["error"] == protocol.ERROR_PEER_NOT_AUTHORIZED


class EngineWithAFaultyPing(Engine):
    """An Engine whose ping handling has a defect: the request handler raises."""

    def _answer_ping_query(self, request, peer):
        raise RuntimeError("defect inside the Engine: secret detail")


def test_a_request_handler_failure_answers_internal_error_then_the_engine_fails_stop(short_directory, caplog):
    clients = []

    def queue_a_ping(ingress):
        clients.extend(queue_clients(ingress.endpoint, [request_bytes("ping", "will-fail")]))

    engine, ingress = real_engine(short_directory, queue_a_ping, engine_class=EngineWithAFaultyPing)
    start_watchdog(engine, seconds=8.0)
    with caplog.at_level(logging.INFO):
        status = engine.run()

    assert status is ExitStatus.FAILURE and int(status) == 1
    raw = clients[0].recv(4096)
    clients[0].close()
    answer = answer_of(raw)
    assert answer["ok"] is False and answer["error"] == protocol.ERROR_INTERNAL and answer["id"] == "will-fail"
    assert answer["message"] == protocol.INTERNAL_ERROR_MESSAGE
    assert b"secret" not in raw and b"RuntimeError" not in raw  # no exception details for the client

    records = read_lifecycle_records(short_directory / "audit.jsonl")
    assert events_of(records) == [  # the existing failure sequence, unchanged; no final pass after a failure
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "ENGINE_FAILURE",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]
    failure = records[2]
    assert failure["reason"] == "RuntimeError"
    assert failure["context"]["message"] == "defect inside the Engine: secret detail"  # the original exception
    assert failure["context"]["phase"] == "operation"
    failure_logs = [r for r in caplog.records if r.levelno == logging.ERROR and r.exc_info]
    assert len(failure_logs) == 1 and failure_logs[0].exc_info[1].args == ("defect inside the Engine: secret detail",)
    assert len([r for r in caplog.records if "Engine shutdown completed" in r.getMessage()]) == 1  # cleanup once
    assert not ingress.endpoint.exists()


# ================================================================ descriptor exhaustion and recovery


class ExhaustedListener:
    """Wraps the real listening socket: accept() fails with EMFILE until a given time, then works again."""

    def __init__(self, real, until):
        self.real = real
        self.until = until
        self.failed_attempts = 0

    def accept(self):
        if time.monotonic() < self.until:
            self.failed_attempts += 1
            raise OSError(errno.EMFILE, "Too many open files")
        return self.real.accept()

    def fileno(self):
        return self.real.fileno()

    def close(self):
        self.real.close()


def test_the_engine_recovers_from_descriptor_exhaustion_without_spinning(short_directory, caplog):
    exhaustion_seconds = 0.8
    listeners = []

    outcomes = {}

    def client(endpoint):
        sent = time.monotonic()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(5)
            connection.connect(str(endpoint))  # queued while the Engine cannot accept
            connection.sendall(request_bytes())
            outcomes["answer"] = answer_of(connection.recv(4096))
        outcomes["waited"] = time.monotonic() - sent
        outcomes["stop"] = cli.send_request(endpoint, protocol.OPERATION_STOP, 5.0)

    worker = threading.Thread(target=client, args=(short_directory / "ipc" / "engine.sock",))

    def exhaust(ingress):
        listeners.append(ExhaustedListener(ingress._listener, time.monotonic() + exhaustion_seconds))
        ingress._listener = listeners[0]
        worker.start()  # the endpoint exists from here on

    engine, _ = real_engine(short_directory, exhaust, interval=1.0)
    start_watchdog(engine, seconds=8.0)
    with caplog.at_level(logging.WARNING):
        status = engine.run()
    worker.join(5)

    assert status is ExitStatus.SUCCESS
    assert outcomes["answer"]["ok"] is True  # served after recovery
    assert outcomes["waited"] < exhaustion_seconds + 1.5
    assert outcomes["stop"].ok
    # Without the idle backoff the readable socket would be retried thousands of times; with it,
    # about one attempt per 0.25 s while idle plus the one in each (1 s) cycle.
    assert listeners[0].failed_attempts <= 8
    logged = [r for r in caplog.records if "accepting an IPC connection failed" in r.getMessage()]
    assert len([r for r in logged if "suppressed" not in r.getMessage()]) <= 3  # rate-limited by category


# ================================================================ end to end: repeated STOP and signals


def test_repeated_stop_requests_have_defined_outcomes_end_to_end(short_directory):
    engine = EngineProcess(short_directory, cycle_interval=0.2)
    try:
        engine.wait_until_running()
        first = run_ape("-x", "--socket", str(engine.endpoint))
        second = run_ape("-x", "--socket", str(engine.endpoint))
        assert engine.wait() == 0
        after_exit = run_ape("-x", "--socket", str(engine.endpoint))

        assert first.returncode == 0 and "shutdown initiated" in first.stdout
        # The second STOP is either answered "already initiated" (acknowledged, audited) or finds the
        # endpoint already withdrawn (exit 3): never a second shutdown.
        assert (second.returncode, "already initiated" in second.stdout) in {(0, True), (3, False)}
        assert after_exit.returncode == cli.EXIT_UNREACHABLE
        events = events_of(read_audit(engine.audit_file), engine.execution_id())
        assert events.count("SHUTDOWN_INITIATED") == 1 and events.count("SHUTDOWN_COMPLETED") == 1
        assert events.count("STOP_REQUEST_RECEIVED") == (2 if second.returncode == 0 else 1)
    finally:
        engine.close()


@pytest.mark.parametrize("signal_number", [signal.SIGINT, signal.SIGTERM], ids=["SIGINT", "SIGTERM"])
def test_a_signal_with_clients_queued_and_stalled_ends_the_engine_once_and_promptly(short_directory, signal_number):
    engine = EngineProcess(short_directory, cycle_interval=0.2)
    stalled = []
    try:
        engine.wait_until_running()
        stalled = queue_clients(engine.endpoint, [b""] * 4)
        started = time.monotonic()
        engine.send_signal(signal_number)

        assert engine.wait(timeout=SHUTDOWN_TOLERANCE_SECONDS + 5) == 0
        assert time.monotonic() - started < SHUTDOWN_TOLERANCE_SECONDS
        events = events_of(read_audit(engine.audit_file), engine.execution_id())
        assert events.count("SHUTDOWN_INITIATED") == 1 and events.count("SHUTDOWN_COMPLETED") == 1
        assert not engine.endpoint.exists()
    finally:
        for client in stalled:
            client.close()
        engine.close()


def test_an_unsafe_ipc_directory_stops_startup_cleanly_and_is_left_untouched(short_directory):
    shared = short_directory / "shared"
    shared.mkdir()
    shared.chmod(0o770)  # writable by the group
    engine = EngineProcess(short_directory, extra_args=("--socket", str(shared / "engine.sock")))
    try:
        assert engine.wait() == ExitStatus.FAILURE == 1

        assert list(shared.iterdir()) == []  # no lock file and no socket were created
        assert stat.S_IMODE(shared.stat().st_mode) == 0o770
        records = read_audit(engine.audit_file)
        failure = next(r for r in records if r["event"] == "ENGINE_FAILURE")
        assert failure["reason"] == "IngressUnavailableError" and failure["context"]["phase"] == "startup"
        assert "writable by group or others" in engine.log_text()
    finally:
        engine.close()
