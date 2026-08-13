# Testing Patterns

## Core Sections (Required)

### 1) Test Stack and Commands

- Primary test framework: Python standard-library `unittest` (Python 3.11 runtime constraint).
- Assertion/mocking tools: `unittest.TestCase` assertions; small pandas fixtures and `tempfile`. No third-party mocking dependency is declared.
- Commands:

```bash
# All discovered tests
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'

# One test module
PYTHONPATH=src .venv/bin/python -m unittest tests.test_broker_branch_accumulation

# Integration/e2e
# [TODO] No dedicated command or suite is defined.

# Coverage
# [TODO] No coverage tool or command is configured.
```

Current local evidence on 2026-08-13: discovery ran 27 tests in 0.099 seconds and returned `OK`.

### 2) Test Layout

- Tests are centralized under `tests/`; filenames follow `test_<subject>.py`.
- Classes inherit `unittest.TestCase`; methods follow `test_<behavior>`.
- No shared setup/fixture file was found. Tests build data inline and use temporary directories for filesystem isolation.
- Existing suites focus on analysis modules, especially broker-flow and exit-strategy calculations.

### 3) Test Scope Matrix

| Scope | Covered? | Typical target | Notes |
|-------|----------|----------------|-------|
| Unit | Yes | Pure/dataframe analysis functions and event rules | 27 discovered tests across eight files |
| Integration | No dedicated suite found | ETL, filesystem contracts, GCP clients, market APIs | `[TODO]` distinguish any manually run integration checks from automated tests |
| E2E | No suite found | Scheduler-to-crawler-to-GCS or ETL-to-dashboard flow | External services/browser/GCP are not covered by discovered tests |

### 4) Mocking and Isolation Strategy

- Main approach: construct deterministic pandas DataFrames and dataclass configs; use `tempfile.TemporaryDirectory` for path-dependent behavior.
- External HTTP, browser, GCS, Firestore, Cloud Tasks, Cloud Run, and Dash interactions are not mocked in the discovered suite.
- Isolation guarantee: temporary directories are removed by context managers; tests do not share a repository fixture.
- Common failure mode: `[TODO]` no flaky-test record or CI history is available in the repository.

### 5) Coverage and Quality Signals

- Coverage tool + threshold: `[TODO]` none configured.
- Current reported coverage: `[TODO]` no report exists.
- Positive signal: all 27 discovered tests passed locally on 2026-08-13.
- Known gaps: crawler parsing/retries, deployment handlers, ETL manifests/archive operations, report builders, Dash callbacks, Docker entrypoints, and full data-contract compatibility have no discovered automated coverage.
- CI enforcement: `[ASK USER]` Is testing intentionally local-only, or is CI configured outside this checkout?

### 6) Evidence

- `tests/test_broker_branch_accumulation.py`
- `tests/test_general_broker_flow_anomaly.py`
- `tests/test_single_branch_persistent_accumulation.py`
- `pyproject.toml`
- Terminal: `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'` on 2026-08-13
