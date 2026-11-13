"""Diagnostic queries and responsive idle waiting, with the Engine and clients as separate processes.

Bounds are generous (seconds, where the real behaviour is milliseconds): they prove "answered
promptly instead of after the 30-second cycle interval", not performance. Every wait has a timeout
and every Engine process is killed at teardown.

Not covered here: a request from a different Linux user (needs a second account). The authorization
decision itself is tested in test_engine_lifecycle.py, and the client's reaction to it in test_cli.py.
"""

import json
import os
import socket
import threading
import time

import pytest

from aquaponics_controller import cli

from helpers import EngineProcess, events_of, parse_response, raw_request, read_audit, run_ape


@pytest.fixture
def engines(short_directory):
    started = []

    def start(**options):
        options.setdefault("name", f"engine{len(started)}")
        engine = EngineProcess(short_directory, **options)
        started.append(engine)
        engine.wait_until_running()
        return engine

    yield start
    for engine in started:
        engine.close()


@pytest.fixture
def engine(engines):
    return engines(cycle_interval=0.1)


@pytest.fixture
def slow_cycle_engine(engines):
    return engines(cycle_interval=30)  # one operating cycle every 30 seconds


def request_line(operation, *, identifier=None, arguments=None, version=1) -> bytes:
    payload = {"v": version, "op": operation}
    if identifier is not None:
        payload["id"] = identifier
    if arguments is not None:
        payload["args"] = arguments
    return json.dumps(payload).encode("utf-8") + b"\n"


def ask(endpoint, operation, **fields) -> dict:
    return parse_response(raw_request(endpoint, request_line(operation, **fields), timeout=5))


def ping(endpoint) -> dict:
    answer = ask(endpoint, "ping", identifier="p")
    assert answer["ok"] is True, answer
    return answer["result"]


# ---------------------------------------------------------------- ape --ping and ape --echo


def test_ape_ping_shows_information_only_the_running_engine_can_provide(engine):
    result = run_ape("--ping", "--socket", str(engine.endpoint))

    assert result.returncode == cli.EXIT_OK
    assert "pong" in result.stdout
    assert engine.execution_id() in result.stdout  # the identifier of this Engine execution, from the Engine
    assert f"pid {engine.process.pid}" in result.stdout  # the Engine's process, not the CLI's
    assert "operating cycles completed:" in result.stdout


def test_the_ping_answer_describes_the_engine_state(engine):
    first = ping(engine.endpoint)
    time.sleep(0.5)
    second = ping(engine.endpoint)

    assert first["reply"] == "pong"
    assert first["execution_id"] == engine.execution_id()
    assert first["engine_pid"] == engine.process.pid != os.getpid()
    assert second["cycles_completed"] > first["cycles_completed"]  # the Engine kept looping in between
    assert first["engine_time_utc"] < second["engine_time_utc"] and first["engine_time_utc"].endswith("Z")


def test_ape_echo_returns_the_text_through_the_engine(engine):
    result = run_ape("--echo", "hello engine", "--socket", str(engine.endpoint))

    assert (result.returncode, result.stdout) == (cli.EXIT_OK, "hello engine\n")
    assert "IPC request processed by the Engine: op='echo'" in engine.log_text()  # the Engine did the work


@pytest.mark.parametrize(
    "text",
    ["", "plain", 'quotes " and \\ backslashes', "tab\tnewline\nbell\x07", "h\u00e9llo w\u00f6rld", "\u65e5\u672c\u8a9e", "\U0001f600" * 3, "x" * 256, "\U0001f600" * 256],
    ids=["empty", "plain", "json-escapes", "control-characters", "latin", "cjk", "emoji", "ascii-256", "emoji-256"],
)
def test_echo_returns_any_valid_text_exactly(engine, text):
    answer = ask(engine.endpoint, "echo", identifier="e-1", arguments={"text": text})

    assert (answer["ok"], answer["result"], answer["id"]) == (True, {"text": text}, "e-1")


def test_the_same_text_is_returned_whether_escaped_or_sent_as_raw_utf8(engine):
    raw = b'{"v":1,"op":"echo","args":{"text":"\xc3\xa9\xf0\x9f\x98\x80"}}\n'
    escaped = b'{"v":1,"op":"echo","args":{"text":"\\u00e9\\ud83d\\ude00"}}\n'

    answers = [parse_response(raw_request(engine.endpoint, payload)) for payload in (raw, escaped)]

    assert [a["result"] for a in answers] == [{"text": "\u00e9\U0001f600"}] * 2


@pytest.mark.parametrize("length", [257, 300])
def test_echo_text_beyond_256_characters_is_refused_by_the_engine(engine, length):
    answer = ask(engine.endpoint, "echo", identifier="long", arguments={"text": "x" * length})

    assert (answer["ok"], answer["error"], answer["id"]) == (False, "invalid_arguments", "long")
    assert "result" not in answer and "x" * 20 not in json.dumps(answer)  # the text is not repeated back


def test_ape_reports_refused_echo_text_and_the_engine_runs_on(engine):
    result = run_ape("--echo", "x" * 257, "--socket", str(engine.endpoint))

    assert result.returncode == cli.EXIT_REFUSED
    assert "invalid_arguments" in result.stderr
    assert engine.process.poll() is None and ping(engine.endpoint)["reply"] == "pong"


@pytest.mark.parametrize(
    "payload, expected_error",
    [
        (request_line("echo"), "invalid_arguments"),
        (request_line("echo", arguments={}), "invalid_arguments"),
        (request_line("echo", arguments={"text": 5}), "invalid_arguments"),
        (request_line("echo", arguments={"text": ["a"]}), "invalid_arguments"),
        (request_line("echo", arguments={"text": "a", "more": 1}), "invalid_arguments"),
        (b'{"v":1,"op":"echo","args":[1]}\n', "invalid_arguments"),
        (b'{"v":1,"op":"echo","args":"text"}\n', "invalid_arguments"),
        (request_line("ping", arguments={"x": 1}), "invalid_arguments"),
        (request_line("stop", arguments={"x": 1}), "invalid_arguments"),
        (request_line("ping", identifier="i" * 65), "invalid_correlation_id"),
        (b'{"v":1,"op":"ping","id":7}\n', "invalid_correlation_id"),
        (request_line("ping", version=2), "unsupported_protocol_version"),
        (request_line("reboot"), "unsupported_operation"),
        (request_line("PING"), "unsupported_operation"),
        (b'{"v":1,"op":"echo","args":{"text":"\\ud800"}}\n', "invalid_arguments"),  # an unpaired surrogate
        (b"[1,2,3]\n", "malformed_request"),
        (b"not json\n", "malformed_request"),
        (b"\xff\xfe\n", "malformed_request"),
        (b"[" * 2000 + b"]" * 2000 + b"\n", "malformed_request"),  # deep nesting, within the size limit
        (b"x" * 5000, "request_too_large"),
    ],
    ids=lambda value: value[:30].decode("latin-1") if isinstance(value, bytes) else value,
)
def test_invalid_requests_get_a_specific_error_and_do_not_disturb_the_engine(engine, payload, expected_error):
    answer = parse_response(raw_request(engine.endpoint, payload))

    assert (answer["ok"], answer["error"]) == (False, expected_error)
    assert answer["message"]  # intelligible, not just a code
    assert len(json.dumps(answer)) < 1000  # bounded
    assert engine.process.poll() is None
    assert ping(engine.endpoint)["reply"] == "pong"


def test_a_refusal_can_be_matched_to_its_request(engine):
    answer = ask(engine.endpoint, "reboot", identifier="match-me")

    assert (answer["error"], answer["id"]) == ("unsupported_operation", "match-me")


def test_the_original_stop_request_without_the_new_fields_still_works(engine):
    answer = parse_response(raw_request(engine.endpoint, b'{"v":1,"op":"stop"}\n'))

    assert answer["ok"] is True and answer["status"] == "shutdown_initiated" and "id" not in answer
    assert engine.wait() == 0


# ---------------------------------------------------------------- queries are not commands

def test_queries_leave_no_trace_in_the_audit_or_on_disk(engine, short_directory):
    files_before = sorted(str(p.relative_to(short_directory)) for p in short_directory.rglob("*") if p.name != "engine0.log")
    audit_before = engine.audit_file.read_bytes()

    for _ in range(20):
        ping(engine.endpoint)
        ask(engine.endpoint, "echo", arguments={"text": "persist-me?"})
    time.sleep(0.3)

    assert engine.audit_file.read_bytes() == audit_before
    assert events_of(read_audit(engine.audit_file)) == ["ENGINE_STARTING", "ENGINE_RUNNING"]
    assert sorted(str(p.relative_to(short_directory)) for p in short_directory.rglob("*") if p.name != "engine0.log") == files_before


def test_queries_are_not_replayed_after_a_restart(engines, engine):
    for _ in range(5):
        ping(engine.endpoint)
        ask(engine.endpoint, "echo", arguments={"text": "once"})
    assert run_ape("-x", "--socket", str(engine.endpoint)).returncode == 0
    assert engine.wait() == 0

    restarted = engines(cycle_interval=0.1, name="restarted")
    time.sleep(0.5)

    log = restarted.log_text()
    assert "IPC request received" not in log  # nothing from the previous execution was processed again
    assert ping(restarted.endpoint)["cycles_completed"] >= 1


def test_the_engine_traces_receipt_processing_and_answer_with_the_identifier_but_not_the_text(engine):
    secret = "plain-text-that-must-not-reach-the-log"
    ask(engine.endpoint, "echo", identifier="trace-42", arguments={"text": secret})
    time.sleep(0.2)

    log = engine.log_text()
    assert "IPC request received: op='echo' id='trace-42'" in log
    assert "IPC request processed by the Engine: op='echo' id='trace-42' outcome=ok" in log
    assert "IPC request answered: id='trace-42' ok=True delivered" in log
    assert secret not in log


def test_diagnostic_queries_are_not_lifecycle_audit_events_but_stop_still_is(engine):
    ping(engine.endpoint)
    assert run_ape("-x", "--socket", str(engine.endpoint)).returncode == 0
    assert engine.wait() == 0

    assert events_of(read_audit(engine.audit_file)) == [
        "ENGINE_STARTING",
        "ENGINE_RUNNING",
        "STOP_REQUEST_RECEIVED",
        "SHUTDOWN_INITIATED",
        "SHUTDOWN_COMPLETED",
    ]


# ---------------------------------------------------------------- a long cycle interval no longer means slow answers


def test_a_30_second_cycle_interval_does_not_mean_a_30_second_answer(slow_cycle_engine):
    for _ in range(5):
        started = time.monotonic()
        result = run_ape("--ping", "--socket", str(slow_cycle_engine.endpoint), "--timeout", "5")
        assert result.returncode == cli.EXIT_OK, result.stderr
        assert time.monotonic() - started < 3


def test_requests_during_the_idle_wait_do_not_run_extra_operating_cycles(slow_cycle_engine):
    first = ping(slow_cycle_engine.endpoint)
    for _ in range(10):
        ping(slow_cycle_engine.endpoint)
        ask(slow_cycle_engine.endpoint, "echo", arguments={"text": "x"})
    last = ping(slow_cycle_engine.endpoint)

    assert first["cycles_completed"] <= 1  # the first cycle may still have been running
    assert last["cycles_completed"] == first["cycles_completed"] or last["cycles_completed"] == 1
    assert last["cycles_completed"] <= 1  # twenty-two requests, still not a second cycle


def test_ape_stop_ends_an_engine_with_a_30_second_interval_promptly(slow_cycle_engine):
    started = time.monotonic()

    result = run_ape("-x", "--socket", str(slow_cycle_engine.endpoint))

    assert result.returncode == cli.EXIT_OK
    assert slow_cycle_engine.wait(timeout=5) == 0
    assert time.monotonic() - started < 3  # not up to 30 seconds
    assert not slow_cycle_engine.endpoint.exists()


def test_a_signal_still_ends_a_busy_idle_wait_promptly(slow_cycle_engine):
    import signal

    for _ in range(5):
        ping(slow_cycle_engine.endpoint)
    started = time.monotonic()
    slow_cycle_engine.send_signal(signal.SIGTERM)

    assert slow_cycle_engine.wait(timeout=5) == 0
    assert time.monotonic() - started < 3


def test_an_idle_engine_with_a_long_interval_uses_almost_no_cpu(slow_cycle_engine):
    ping(slow_cycle_engine.endpoint)
    before = slow_cycle_engine.cpu_seconds()

    time.sleep(3)

    assert slow_cycle_engine.cpu_seconds() - before < 0.1


def test_an_engine_that_is_answering_requests_still_uses_little_cpu_between_them(slow_cycle_engine):
    before = slow_cycle_engine.cpu_seconds()
    for _ in range(30):
        ping(slow_cycle_engine.endpoint)
        time.sleep(0.05)

    assert slow_cycle_engine.cpu_seconds() - before < 0.5


def test_requests_do_not_leak_descriptors_in_the_engine(slow_cycle_engine):
    def descriptors():
        return len(os.listdir(f"/proc/{slow_cycle_engine.process.pid}/fd"))

    ping(slow_cycle_engine.endpoint)
    before = descriptors()
    for _ in range(100):
        ping(slow_cycle_engine.endpoint)
    raw_request(slow_cycle_engine.endpoint, b"garbage\n")
    socket.socket(socket.AF_UNIX, socket.SOCK_STREAM).close()

    assert descriptors() == before


# ---------------------------------------------------------------- bounded behaviour under load


def test_the_cycles_keep_their_pace_while_the_engine_answers_a_stream_of_requests(engine):
    start_cycles = ping(engine.endpoint)["cycles_completed"]
    started = time.monotonic()
    answered = 0
    while time.monotonic() - started < 2.0:
        ping(engine.endpoint)
        answered += 1
    elapsed = time.monotonic() - started
    cycles = ping(engine.endpoint)["cycles_completed"] - start_cycles

    assert answered > 50  # a real stream of requests...
    assert cycles >= 0.6 * elapsed / 0.1  # ...and the 0.1 s cycles did not starve (nominally ~20 in 2 s)
    assert cycles <= elapsed / 0.1 + 2  # nor did requests make them run faster than the timer


def test_a_burst_of_simultaneous_clients_is_served_and_the_engine_stays_healthy(engine):
    """40 clients at once: more than the 8 served per call and more than the backlog admits.

    Clients use the same request function as ``ape``. One whose connection finds the queue full
    retries connecting (only connecting, nothing was sent) within its timeout, so every one of them
    is answered. Every outcome is recorded; a thread never ends with an unhandled exception.
    """
    outcomes = []

    def client(number):
        try:
            response = cli.send_request(engine.endpoint, "echo", 5.0, {"text": f"burst {number}"})
            outcomes.append(("answered", response.ok, response.result))
        except cli.ClientError as exc:
            outcomes.append(("failed", exc.exit_status, str(exc)))
        except Exception as exc:  # noqa: BLE001 - recorded, so that the assertion below reports it
            outcomes.append(("exception", type(exc).__name__, str(exc)))

    threads = [threading.Thread(target=client, args=(number,)) for number in range(40)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert len(outcomes) == 40
    assert sorted(outcomes, key=str) == sorted(
        (("answered", True, {"text": f"burst {number}"}) for number in range(40)), key=str
    )
    assert engine.process.poll() is None
    assert ping(engine.endpoint)["reply"] == "pong"


def test_stalled_clients_delay_the_engine_by_a_bounded_time_only(engine):
    stalled = []
    try:
        for _ in range(6):  # connect and send nothing
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(str(engine.endpoint))
            stalled.append(client)
        started = time.monotonic()
        answer = ask(engine.endpoint, "ping", identifier="behind-stalled")
        waited = time.monotonic() - started
        cycles_before = ping(engine.endpoint)["cycles_completed"]
        time.sleep(1.5)
        cycles_after = ping(engine.endpoint)["cycles_completed"]
    finally:
        for client in stalled:
            client.close()

    assert answer["ok"] is True and waited < 3  # behind six stalled clients, still answered promptly
    assert cycles_after - cycles_before >= 8  # and the cycles kept running (nominally ~15 in 1.5 s)
    assert engine.process.poll() is None


def test_stop_is_still_honoured_while_clients_stall(engine):
    stalled = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stalled.connect(str(engine.endpoint))
    try:
        result = run_ape("-x", "--socket", str(engine.endpoint))
        assert result.returncode == cli.EXIT_OK
        assert engine.wait(timeout=5) == 0
    finally:
        stalled.close()


# ---------------------------------------------------------------- the client against a missing or silent Engine


@pytest.mark.parametrize("arguments", [["--ping"], ["--echo", "x"]])
def test_ape_reports_a_missing_engine_for_queries_too(short_directory, arguments):
    started = time.monotonic()

    result = run_ape(*arguments, "--socket", str(short_directory / "nothing" / "engine.sock"))

    assert result.returncode == cli.EXIT_UNREACHABLE and "no Engine is listening" in result.stderr
    assert time.monotonic() - started < 5


def test_ape_ping_times_out_against_an_engine_that_never_answers(short_directory):
    silent = short_directory / "silent.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(silent))
    server.listen(1)
    try:
        started = time.monotonic()
        result = run_ape("--ping", "--socket", str(silent), "--timeout", "0.5")
    finally:
        server.close()

    assert result.returncode == cli.EXIT_TIMEOUT
    assert time.monotonic() - started < 5


def test_the_default_client_timeout_is_still_five_seconds():
    assert cli.DEFAULT_TIMEOUT_SECONDS == 5.0
