# AGENTS.md — Shared AI Development Instructions

These instructions apply to every AI coding assistant working in this repository,
regardless of tool. This file is a **summary and entry point**. It does not define
policy of its own.

The maintained AsciiDoc documents under `documentation/` are the authority. Where this
summary and those documents differ, the documents win, and the difference must be reported.
Consult the relevant documents for your task rather than relying on this summary alone.

## Project context

- This software controls a **real, physical aquaponics installation**: pumps, aeration,
  mains-voltage relays, fish and plants. Physical safety and operational reliability come first.
- The **Engine** is the authoritative controller. It must remain **autonomous and
  offline-capable**: no Internet, API, GitHub or cloud dependency for physical control.
- This is a **new implementation**. The legacy controller may be consulted for historical
  context, physical operating experience and useful ideas. No legacy functionality, class or
  module structure, or behavioural compatibility is required; the documented project scope
  and architecture decide what is implemented.

## Authoritative documentation

The `ap_*.adoc` files are the sources. The `_aquaponics_*.adoc` files are thin wrappers that
include them, and the generated `*.html` files are not authoritative.

| Document | Covers |
|---|---|
| `documentation/ap_project_scope_and_goals.adoc` | Purpose, scope, autonomy requirements, design priority order |
| `documentation/ap_architecture_and_design.adoc` | Engine authority, command path, persistence, API boundary, reliability |
| `documentation/ap_development_and_operations.adoc` | Branching, testing, release, deployment, operations |
| `documentation/ap_contributing_and_governance.adoc` | Contribution rules, safety-critical changes, AI context priority |
| `documentation/ap_ai_code_and_logic_guidelines.adoc` | How code is structured, named, logged and documented; AI session initialization |
| `documentation/ap_it_system_reproduction.adoc` | Rebuilding the production controller on replacement hardware |
| `documentation/ap_physical_infrastructure.adoc` | The real installation: water circuit, relays, sensors, electrical, fail-safe behaviour |

For code work, read `ap_ai_code_and_logic_guidelines.adoc` ("AI Development Session
Initialization" and "AI Implementation Procedure") before implementing.

## Session initialization

At the start of a session:

1. Inspect the active Git branch and the working-tree state (`git status -sb`).
2. Identify existing modifications and untracked files.
3. Read the documentation relevant to your assignment.
4. **Preserve existing user work.** Do not discard, reset, overwrite or stage changes or
   untracked files you did not create (for example `documentation/media/`) without
   explicit authorization.
5. Identify contradictions or unresolved architectural questions, and report them.

Do not assume `production` (the GitHub default branch) contains the latest development
decisions. Previous conversations and retained memory are supplementary only and never
override the current repository documentation.

## Development governance

Branching model (details in the development and governance documents):

- `production`: approved production code. No direct development.
- `staging`: integration and validation. Direct development normally avoided.
- `feature/<short-description>`: development without a dedicated issue.
- `feature/<issue-number>-<short-description>`: development associated with a GitHub issue.

Feature branches start from `staging` and enter it through pull requests. `staging` is
promoted to `production` through pull requests.

GitHub issues are recommended for substantial work but not mandatory.

**Do not commit, push, merge, create pull requests or create GitHub issues without
explicit authorization.** Never commit secrets (keys, tokens, passwords).

## Architecture and safety

Summarized from the documentation; consult it for the full rules:

- **Engine authority.** Only the Engine controls physical hardware. API, CLI, web,
  mobile, SHMC and AI clients are not managers and never access hardware directly.
- **Unified command path.** Every state-changing action reaches hardware through the
  Engine command path, whatever its origin, however trusted the caller.
- **Hardware abstraction.** Hardware stays behind adapter boundaries. Fake hardware must
  not be casually enabled in production composition.
- **Offline operation.** The Engine must start, sense, schedule, control and recover
  without the API, Internet or any external service.
- **Safety-critical changes** (pumps, aeration, relays, heating, water refill, startup,
  shutdown, persistence, watchdog, recovery, scheduling, authentication) require explicit
  failure-path analysis and tests. A nominal-path test is not enough.
- **Readable logic.** Code must expose the engineering process it implements: explicit,
  inspectable and readable by the human maintainer, as defined in the AI code and logic
  guidelines.

## Instruction precedence

- Current task instructions define the work being performed.
- Repository architecture, safety rules and governance constrain how it may be implemented.
- An implementation instruction does not silently override an architectural constraint.
  Conflicts, contradictions and ambiguities, between an instruction and the documentation
  or within the documentation, must be **reported, not silently resolved**.
- Only an explicitly authorized architectural change may alter established requirements,
  and it must then be recorded in the repository documentation.
- Older code that violates the desired architecture is not permission to change it.
- Stay within the assigned scope. Do not redesign adjacent architecture.

## Development philosophy

Human understanding of the implementation is essential. Correctness, reliability and
maintainability take precedence over implementation speed. Development proceeds
incrementally, with independently verifiable results where practical.

The assistant handles implementation details but does not independently make significant
architectural decisions; these remain subject to explicit human review. Avoid unnecessary
abstractions and speculative functionality.

(Recorded in `ap_ai_code_and_logic_guidelines.adoc`, "AI Implementation Procedure".)
