"""The phases of one Engine operating cycle, and the time each may take.

An operating cycle (the documentation also says "control cycle"; both mean the same
Engine process) passes through these phases, in this order. The order itself is
written out in ``Engine.run_operating_cycle``, which reads as the process map; this
module only gives each phase a stable name and a time limit.

See "Operating-Cycle Phases" in the architecture documentation.

Time limits are COOPERATIVE BUDGETS, not pre-emptive deadlines. The Engine runs in
one thread and cannot interrupt a phase. A phase must not start further work once
its budget is used up, and each operation inside it must bound its own duration.
The Engine measures every phase with the monotonic clock and logs a WARNING when a
phase took longer than its limit; an overrun does not stop the Engine. A phase that
blocks forever cannot be detected by the thread that is blocked in it: that is left
to future external supervision and the watchdog.
"""

import enum
from types import MappingProxyType


class CyclePhase(enum.StrEnum):
    """Stable phase identifiers, listed in execution order. Used in logs and in ENGINE_FAILURE records."""

    ACQUIRE_OBSERVATIONS = "ACQUIRE_OBSERVATIONS"
    RECONCILE_STATE = "RECONCILE_STATE"
    INGEST_EXTERNAL_INTENTS = "INGEST_EXTERNAL_INTENTS"
    EVALUATE_SCHEDULED_WORK = "EVALUATE_SCHEDULED_WORK"
    EVALUATE_CONTROL_POLICIES = "EVALUATE_CONTROL_POLICIES"
    EXECUTE_READY_COMMANDS = "EXECUTE_READY_COMMANDS"
    FINALIZE_CYCLE = "FINALIZE_CYCLE"


# Seconds. Code constants by decision (Issue #6), not command-line configuration.
# The ingest limit covers the existing ingress bounds: 0.5 s of admission budget, after which no
# further connection is started, plus one connection's 0.2 s read and 0.2 s write deadlines.
# The total (1.5 s) must stay well below the CLI's 5 s default timeout, because a request that
# arrives while a cycle runs waits for that cycle to end.
PHASE_TIME_LIMITS_SECONDS = MappingProxyType(
    {
        CyclePhase.ACQUIRE_OBSERVATIONS: 0.1,
        CyclePhase.RECONCILE_STATE: 0.1,
        CyclePhase.INGEST_EXTERNAL_INTENTS: 0.9,
        CyclePhase.EVALUATE_SCHEDULED_WORK: 0.1,
        CyclePhase.EVALUATE_CONTROL_POLICIES: 0.1,
        CyclePhase.EXECUTE_READY_COMMANDS: 0.1,
        CyclePhase.FINALIZE_CYCLE: 0.1,
    }
)
