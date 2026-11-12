"""The readiness-driven idle wait, with real sockets, in one process.

Timing assertions use generous bounds: they prove "returns promptly instead of waiting out the
timeout", not performance.
"""

import os
import socket
import threading
import time

import pytest

from aquaponics_controller.idle_wait import IdleWait


@pytest.fixture
def idle_wait():
    waiter = IdleWait()
    yield waiter
    waiter.close()


@pytest.fixture
def listener(short_directory):
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(short_directory / "s.sock"))
    server.listen(4)
    server.setblocking(False)
    yield server
    server.close()


def timed(call):
    started = time.monotonic()
    result = call()
    return result, time.monotonic() - started


def test_without_anything_happening_the_wait_lasts_for_its_timeout(idle_wait):
    idle_wait.open(None)

    ready, elapsed = timed(lambda: idle_wait.wait(0.2))

    assert ready is False
    assert 0.15 <= elapsed < 1.0


def test_a_wake_up_sent_before_the_wait_begins_is_not_lost(idle_wait):
    idle_wait.open(None)
    idle_wait.wake()  # for example a signal arriving just before the Engine starts waiting

    ready, elapsed = timed(lambda: idle_wait.wait(30))

    assert ready is False
    assert elapsed < 1.0


def test_a_wake_up_from_another_thread_ends_a_running_wait(idle_wait):
    idle_wait.open(None)
    timer = threading.Timer(0.2, idle_wait.wake)
    timer.start()

    try:
        _, elapsed = timed(lambda: idle_wait.wait(30))
    finally:
        timer.cancel()

    assert elapsed < 2.0


def test_a_wake_up_is_consumed_so_the_next_wait_lasts_again(idle_wait):
    idle_wait.open(None)
    idle_wait.wake()
    idle_wait.wait(30)

    _, elapsed = timed(lambda: idle_wait.wait(0.2))

    assert elapsed >= 0.15  # no stale byte keeps ending waits at once (that would be a busy loop)


def test_many_wake_ups_do_not_block_the_caller_and_are_consumed_together(idle_wait):
    idle_wait.open(None)

    for _ in range(200_000):  # more than the socket buffer holds: wake() must never block or raise
        idle_wait.wake()
    idle_wait.wait(30)

    _, elapsed = timed(lambda: idle_wait.wait(0.2))
    assert elapsed >= 0.15


def test_a_client_waiting_on_the_listening_socket_ends_the_wait_with_ingress_ready(idle_wait, listener, short_directory):
    idle_wait.open(listener)
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(short_directory / "s.sock"))
    try:
        ready, elapsed = timed(lambda: idle_wait.wait(30))
    finally:
        client.close()

    assert ready is True
    assert elapsed < 1.0


def test_the_ingress_is_ignored_on_request_while_a_client_stays_waiting(idle_wait, listener, short_directory):
    idle_wait.open(listener)
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(short_directory / "s.sock"))
    try:
        ready, elapsed = timed(lambda: idle_wait.wait(0.2, watch_ingress=False))
        resumed, _ = timed(lambda: idle_wait.wait(30, watch_ingress=True))
    finally:
        client.close()

    assert (ready, resumed) == (False, True)  # the listener was ignored, then watched again
    assert elapsed >= 0.15


def test_a_wake_up_wins_over_nothing_but_does_not_hide_a_waiting_client(idle_wait, listener, short_directory):
    idle_wait.open(listener)
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(short_directory / "s.sock"))
    idle_wait.wake()
    try:
        ready, _ = timed(lambda: idle_wait.wait(30))
    finally:
        client.close()

    assert ready is True


def test_waiting_before_open_is_a_programming_error():
    with pytest.raises(RuntimeError):
        IdleWait().wait(0.1)


def test_wake_before_open_and_after_close_is_harmless():
    waiter = IdleWait()
    waiter.wake()
    waiter.open(None)
    waiter.close()
    waiter.wake()


def test_close_is_repeatable_and_releases_every_descriptor(listener):
    before = len(os.listdir("/proc/self/fd"))

    for _ in range(25):
        waiter = IdleWait()
        waiter.open(listener)
        waiter.close()
        waiter.close()

    assert len(os.listdir("/proc/self/fd")) == before
    assert listener.fileno() != -1  # the ingress socket belongs to the ingress, not to the wait
