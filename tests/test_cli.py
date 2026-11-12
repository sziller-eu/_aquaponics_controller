"""The ``ape`` client, in one process, against canned servers: what it sends, what it prints, its exit status.

The client never produces an answer itself. These tests give it answers a real Engine could
(or could never) produce and check how it reports them. A server thread is a test fixture only.
"""

import json
import socket
import threading
import time

import pytest

from aquaponics_controller import cli
from aquaponics_controller import ipc_protocol as protocol

PING_RESULT = {
    "reply": "pong",
    "execution_id": "0123abcd",
    "engine_pid": 4242,
    "cycles_completed": 7,
    "engine_time_utc": "2026-11-12T06:00:00.000000Z",
}


class CannedServer:
    """Accepts one connection, remembers the request it read, and answers with ``reply(request)``."""

    def __init__(self, directory, reply):
        self.path = directory / "canned.sock"
        self.request = None
        self._reply = reply
        self._release = threading.Event()
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(self.path))
        self._server.listen(1)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        try:
            connection, _ = self._server.accept()
        except OSError:
            return
        with connection:
            line = b""
            while b"\n" not in line:
                chunk = connection.recv(4096)
                if not chunk:
                    return
                line += chunk
            self.request = json.loads(line)
            answer = self._reply(self.request)
            if answer is not None:
                connection.sendall(answer)
            else:
                self._release.wait(timeout=5)  # stay silent but connected, like an Engine that never answers

    def close(self):
        self._release.set()
        self._server.close()
        self._thread.join(timeout=5)


@pytest.fixture
def serve(short_directory):
    servers = []

    def start(reply):
        servers.append(CannedServer(short_directory, reply))
        return servers[-1]

    yield start
    for server in servers:
        server.close()


def ok(**fields):
    """A reply function: a successful answer that carries the request's own correlation identifier."""

    def reply(request):
        return json.dumps({"v": 1, "ok": True, "id": request.get("id"), **fields}).encode() + b"\n"

    return reply


def refuse(code, message="refused", *, with_id=True):
    def reply(request):
        body = {"v": 1, "ok": False, "error": code, "message": message}
        if with_id:
            body["id"] = request.get("id")
        return json.dumps(body).encode() + b"\n"

    return reply


# ---------------------------------------------------------------- what is sent


def test_ping_sends_a_ping_with_a_fresh_correlation_identifier_and_no_arguments(serve, capsys):
    server = serve(ok(result=PING_RESULT))

    assert cli.main(["--ping", "--socket", str(server.path)]) == cli.EXIT_OK

    assert server.request["v"] == 1 and server.request["op"] == "ping"
    assert 1 <= len(server.request["id"]) <= protocol.MAX_CORRELATION_ID_LENGTH
    assert "args" not in server.request


def test_each_invocation_uses_a_different_correlation_identifier(short_directory, capsys):
    identifiers = []
    for _ in range(2):
        path = short_directory / f"c{len(identifiers)}.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(1)

        def answer(listener=listener):
            connection, _ = listener.accept()
            with connection:
                request = json.loads(connection.recv(4096))
                identifiers.append(request["id"])
                connection.sendall(json.dumps({"v": 1, "ok": True, "id": request["id"], "result": PING_RESULT}).encode() + b"\n")

        thread = threading.Thread(target=answer, daemon=True)
        thread.start()
        assert cli.main(["--ping", "--socket", str(path)]) == cli.EXIT_OK
        thread.join(timeout=5)
        listener.close()

    assert len(set(identifiers)) == 2


def test_echo_sends_the_text_as_an_argument_unchanged(serve, capsys):
    text = 'héllo "wörld" \\ \U0001f600'
    server = serve(ok(result={"text": text}))

    assert cli.main(["--echo", text, "--socket", str(server.path)]) == cli.EXIT_OK

    assert server.request["op"] == "echo"
    assert server.request["args"] == {"text": text}
    assert capsys.readouterr().out == text + "\n"


def test_stop_still_sends_a_stop_without_arguments(serve, capsys):
    server = serve(ok(status="shutdown_initiated", audit_recorded=True))

    assert cli.main(["-x", "--socket", str(server.path)]) == cli.EXIT_OK

    assert server.request["op"] == "stop" and "args" not in server.request
    assert "shutdown initiated" in capsys.readouterr().out


def test_the_client_does_not_check_text_length_that_is_the_engines_decision(serve, capsys):
    server = serve(refuse(protocol.ERROR_INVALID_ARGUMENTS, "too long"))

    status = cli.main(["--echo", "x" * 300, "--socket", str(server.path)])

    assert status == cli.EXIT_REFUSED
    assert server.request["args"] == {"text": "x" * 300}  # it reached the Engine, which refused it


def test_a_request_beyond_the_protocol_frame_limit_is_not_sent_at_all(short_directory, capsys):
    started = time.monotonic()

    status = cli.main(["--echo", "x" * 5000, "--socket", str(short_directory / "no.sock")])

    assert status == cli.EXIT_USAGE
    assert "4096-byte" in capsys.readouterr().err
    assert time.monotonic() - started < 2


# ---------------------------------------------------------------- what is shown


def test_ping_shows_what_the_engine_reported(serve, capsys):
    server = serve(ok(result=PING_RESULT))

    assert cli.main(["--ping", "--socket", str(server.path)]) == cli.EXIT_OK

    output = capsys.readouterr().out
    assert "pong" in output and "0123abcd" in output and "4242" in output
    assert "7" in output and "2026-11-12T06:00:00.000000Z" in output


def test_text_the_terminal_cannot_show_is_escaped_instead_of_crashing(serve, capsys):
    server = serve(ok(result={"text": "ok \ud800 end"}))  # an unpaired surrogate can never be encoded to UTF-8

    assert cli.main(["--echo", "x", "--socket", str(server.path)]) == cli.EXIT_OK

    assert capsys.readouterr().out == "ok \\ud800 end\n"


# ---------------------------------------------------------------- how refusals and bad answers are reported


@pytest.mark.parametrize(
    "code, expected_status",
    [
        (protocol.ERROR_PEER_NOT_AUTHORIZED, cli.EXIT_PERMISSION_DENIED),
        (protocol.ERROR_INVALID_ARGUMENTS, cli.EXIT_REFUSED),
        (protocol.ERROR_UNSUPPORTED_OPERATION, cli.EXIT_REFUSED),
        (protocol.ERROR_MALFORMED_REQUEST, cli.EXIT_REFUSED),
    ],
)
@pytest.mark.parametrize("arguments", [["--ping"], ["--echo", "hi"], ["-x"]], ids=["ping", "echo", "stop"])
def test_refusals_map_to_exit_statuses_and_explain_themselves(serve, capsys, code, expected_status, arguments):
    server = serve(refuse(code, "explained"))

    assert cli.main([*arguments, "--socket", str(server.path)]) == expected_status

    error = capsys.readouterr().err
    assert code in error and "explained" in error


def test_a_refusal_that_could_not_carry_the_identifier_is_still_a_refusal(serve):
    server = serve(refuse(protocol.ERROR_REQUEST_TIMEOUT, with_id=False))

    assert cli.main(["--ping", "--socket", str(server.path)]) == cli.EXIT_REFUSED


def test_a_success_for_another_request_is_not_accepted(serve, capsys):
    server = serve(lambda request: json.dumps({"v": 1, "ok": True, "id": "somebody-else", "result": PING_RESULT}).encode() + b"\n")

    assert cli.main(["--ping", "--socket", str(server.path)]) == cli.EXIT_COMMUNICATION_FAILURE

    assert "correlation" in capsys.readouterr().err


def test_a_success_without_an_identifier_is_not_accepted(serve):
    server = serve(lambda request: json.dumps({"v": 1, "ok": True, "result": PING_RESULT}).encode() + b"\n")

    assert cli.main(["--ping", "--socket", str(server.path)]) == cli.EXIT_COMMUNICATION_FAILURE


@pytest.mark.parametrize(
    "result",
    [None, "pong", {}, {**PING_RESULT, "reply": "nope"}, {**PING_RESULT, "engine_pid": "4242"}, {**PING_RESULT, "cycles_completed": None}],
)
def test_a_ping_answer_that_is_not_a_valid_ping_result_is_reported(serve, capsys, result):
    fields = {} if result is None else {"result": result}
    server = serve(ok(**fields))

    assert cli.main(["--ping", "--socket", str(server.path)]) == cli.EXIT_COMMUNICATION_FAILURE

    assert "unintelligible" in capsys.readouterr().err


@pytest.mark.parametrize("result", [None, {}, {"text": 5}, {"text": None}, [1]])
def test_an_echo_answer_without_text_is_reported(serve, capsys, result):
    fields = {} if result is None else {"result": result}
    server = serve(ok(**fields))

    assert cli.main(["--echo", "x", "--socket", str(server.path)]) == cli.EXIT_COMMUNICATION_FAILURE


@pytest.mark.parametrize("arguments", [["--ping"], ["--echo", "hi"], ["-x"]], ids=["ping", "echo", "stop"])
def test_every_operation_gives_up_after_its_timeout_when_the_engine_never_answers(serve, arguments):
    server = serve(lambda request: None)  # reads the request, never answers
    started = time.monotonic()

    status = cli.main([*arguments, "--socket", str(server.path), "--timeout", "0.4"])

    assert status == cli.EXIT_TIMEOUT
    assert time.monotonic() - started < 3


@pytest.mark.parametrize("arguments", [["--ping"], ["--echo", "hi"], ["-x"]], ids=["ping", "echo", "stop"])
def test_every_operation_reports_an_absent_engine(short_directory, capsys, arguments):
    status = cli.main([*arguments, "--socket", str(short_directory / "absent" / "e.sock")])

    assert status == cli.EXIT_UNREACHABLE
    assert "no Engine is listening" in capsys.readouterr().err


# ---------------------------------------------------------------- usage


@pytest.mark.parametrize(
    "arguments",
    [[], ["--ping", "--echo", "x"], ["--ping", "-x"], ["--echo", "x", "-x"], ["--echo"], ["--timeout", "5"]],
    ids=["none", "ping+echo", "ping+stop", "echo+stop", "echo-without-text", "no-operation"],
)
def test_exactly_one_operation_must_be_chosen(short_directory, arguments, capsys):
    with pytest.raises(SystemExit) as raised:
        cli.main([*arguments, "--socket", str(short_directory / "x.sock")])

    assert raised.value.code == cli.EXIT_USAGE


def test_an_empty_echo_text_is_a_valid_request(serve, capsys):
    server = serve(ok(result={"text": ""}))

    assert cli.main(["--echo", "", "--socket", str(server.path)]) == cli.EXIT_OK

    assert server.request["args"] == {"text": ""}
