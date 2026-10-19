# _aquaponics_controller
Open-source, offline-first Aquaponics controller for autonomous monitoring and control of scalable physical aquaponics systems.

## Development

Requires Python 3.11 or newer (provisional baseline; the production version is chosen together with the Raspberry Pi OS).

```bash
python3 -m venv .venv
source .venv/bin/activate

python -m pip install -e ".[dev]"
python -m pytest
```

The authoritative architecture, development and operations documentation is in `documentation/`
(AsciiDoc sources `ap_*.adoc`, assembled by `documentation/_aquaponics@sziller.eu.adoc`).

## Branching model

- `production` - approved, stable, production-ready code (GitHub default branch). No direct development.
- `staging` - integration and testing of completed work before promotion. Direct development normally avoided.
- `feature/<short-description>` or `feature/<issue-number>-<short-description>` - development branches created from `staging` (an issue number is needed only when the branch has a dedicated issue).

Workflow: `feature/...` -> pull request -> `staging` -> pull request -> `production`.

See `documentation/` (Development and Operations, Contributing and Governance) for details.
