"""Time access for the Engine.

Wall-clock and monotonic time are obtained here, not scattered through the code
(see "Time Handling" in the architecture documentation). Code that needs the time
receives a clock object, so tests can supply a deterministic one.

- ``utc_now()``  absolute, timezone-aware UTC time, for timestamps in records.
- ``monotonic()`` elapsed-time source, for durations, deadlines and timeouts.
"""

import time
from datetime import datetime, timezone


class SystemClock:
    """The real clock."""

    def monotonic(self) -> float:
        return time.monotonic()

    def utc_now(self) -> datetime:
        return datetime.now(timezone.utc)


def format_utc(moment: datetime) -> str:
    """Render a timezone-aware moment as an ISO 8601 UTC string, e.g. 2026-11-11T12:00:00.000000Z."""
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
