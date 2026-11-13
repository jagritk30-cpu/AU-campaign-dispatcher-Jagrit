# Solution: Durable Campaign Dispatcher (Assignment B)

This directory contains the complete implementation of the Durable Campaign Dispatcher for Assignment B.

For detailed architecture, state machine diagrams, design decisions, failure handling, and benchmark results, see [`../SUBMISSION.md`](../SUBMISSION.md).

## Module Layout

- **`run.py`**: Entrypoint script that orchestrates the FastAPI webhook receiver and engine loop concurrently.
- **`dispatcher/`**: Core library:
  - `config.py`: Loads and parses campaign fixtures and lead CSV files.
  - `clock.py`: `SimClock` (provider-synced) and `FakeClock` (deterministic unit testing).
  - `db.py`: SQLite WAL-mode persistence layer, atomic `record_call_accepted`, and `process_call_outcome`.
  - `scheduler.py`: Window calculations, regulatory bounds (09:00–21:00 IST), retry cooldowns, and priority ordering.
  - `webhook.py`: FastAPI server receiving and verifying carrier webhooks, exposing `/status` and `/health`.
  - `engine.py`: Main scheduling loop, 504 ambiguity resolution, silent-call recovery, and `final_state.csv` generation.
- **`tests/`**: Unit test suite (`pytest -v`):
  - `test_scheduler.py`: Time math and priority ordering.
  - `test_webhook.py`: HMAC signature verification, duplicate idempotency, early webhooks, and `/status`.
  - `test_idempotency.py`: 503/504 response handling and attempt counting.

## Quick Start

```bash
# Start the dispatcher (expects mock telephony running on http://127.0.0.1:8099)
python run.py

# Run all unit tests
pytest ../solution/tests -v
```
