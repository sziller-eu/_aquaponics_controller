"""Interruptible idle wait for the Engine: socket readiness plus a wake-up channel.

Between two operating cycles the Engine has nothing to do. Instead of sleeping for
the whole interval, it waits here for whichever happens first:

- the cycle deadline is reached (the wait times out);
- the IPC listening socket becomes readable (a client is waiting to be served);
- ``wake()`` is called (a stop was requested, possibly from a signal handler).

This is still one thread and one loop. Nothing here runs an operating cycle, and
the deadline belongs to the caller: readiness only lets the Engine answer a
request earlier, it never makes a cycle start earlier. External communication is
serviced by the Engine; it never becomes the clock that drives the Engine (see
"Bounded External Ingress" in the architecture documentation).

Wake-up channel: a connected socket pair. ``wake()`` writes one byte to one end;
the other end is watched by the selector. Readiness is level-triggered, so a byte
written at ANY moment, even just before the Engine starts waiting, ends the next
wait at once: there is no window in which a wake-up can be lost. ``wake()`` is safe
to call from a signal handler or another thread. It is best-effort by design: the
caller's stop flag, not the byte, is the source of truth, and the byte only
shortens the wait.
"""

import logging
import selectors
import socket

logger = logging.getLogger(__name__)

_WAKE = "wake"
_INGRESS = "ingress"


class IdleWait:
    def __init__(self) -> None:
        self._selector: selectors.BaseSelector | None = None
        self._wake_reader: socket.socket | None = None
        self._wake_writer: socket.socket | None = None
        self._ingress_source: socket.socket | None = None
        self._watching_ingress = False

    def open(self, ingress_source: socket.socket | None) -> None:
        """Create the wake-up channel. ``ingress_source`` is the listening socket to watch, or None.

        Without an ingress source the wait is a plain interruptible timer.
        """
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.setblocking(False)  # a full buffer means "already woken", never a blocked signal handler
        self._wake_reader = reader
        self._wake_writer = writer
        self._selector = selectors.DefaultSelector()
        self._selector.register(reader, selectors.EVENT_READ, _WAKE)
        self._ingress_source = ingress_source
        self._set_watching_ingress(ingress_source is not None)

    def close(self) -> None:
        """Release every descriptor this object created. Safe when never opened and when repeated.

        Everything is attempted; the first failure is raised afterwards.
        """
        first_error: OSError | None = None
        writer, self._wake_writer = self._wake_writer, None  # first: a late wake() then finds nothing to write to
        selector, self._selector = self._selector, None
        reader, self._wake_reader = self._wake_reader, None
        self._ingress_source = None
        self._watching_ingress = False
        for resource in (writer, selector, reader):
            if resource is None:
                continue
            try:
                resource.close()
            except OSError as exc:
                first_error = first_error or exc
        if first_error is not None:
            raise first_error

    def wake(self) -> None:
        """End the current or next wait. Callable from a signal handler or another thread; never blocks."""
        writer = self._wake_writer
        if writer is None:
            return  # not open: the caller's stop flag is checked before every wait anyway
        try:
            writer.send(b"x")
        except OSError:
            pass  # buffer already full (a wake-up is pending) or channel closed: nothing more to do

    def wait(self, timeout_seconds: float, *, watch_ingress: bool = True) -> bool:
        """Wait up to ``timeout_seconds``. Return True if the ingress socket is ready to be serviced.

        Returns False on timeout or wake-up; the caller re-checks its stop flag and deadline.
        ``watch_ingress=False`` ignores the ingress socket, which the caller uses to back off when
        readiness produced no progress (otherwise a socket that stays readable would make the
        Engine spin).
        """
        if self._selector is None:
            raise RuntimeError("the idle wait is not open")
        self._set_watching_ingress(watch_ingress and self._ingress_source is not None)
        ingress_ready = False
        for key, _ in self._selector.select(max(timeout_seconds, 0.0)):
            if key.data == _WAKE:
                self._drain_wake_channel()
            else:
                ingress_ready = True
        return ingress_ready

    def _drain_wake_channel(self) -> None:
        assert self._wake_reader is not None
        try:
            while self._wake_reader.recv(64):
                pass
        except BlockingIOError:
            pass

    def _set_watching_ingress(self, watch: bool) -> None:
        if watch == self._watching_ingress or self._selector is None or self._ingress_source is None:
            return
        if watch:
            self._selector.register(self._ingress_source, selectors.EVENT_READ, _INGRESS)
        else:
            self._selector.unregister(self._ingress_source)
        self._watching_ingress = watch
