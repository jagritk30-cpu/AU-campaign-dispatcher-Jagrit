# AI Usage Disclosure

## Tools used

Antigravity (Google DeepMind's agentic AI assistant)

## What I used them for

- Scaffolded the solution module layout (`solution/dispatcher/`, `solution/tests/`, `solution/run.py`)
- Generated the initial SQLite schema, FastAPI webhook receiver, scheduler helpers, and asyncio engine loop
- Generated the unit test suite (`test_scheduler.py`, `test_webhook.py`, `test_idempotency.py`) and an E2E verification script that runs `mock_telephony` with `kill -9` crash injection
- Drafted and formatted `SUBMISSION.md`

## What I wrote or decided myself

- **State machine & crash-safety model:** Chose SQLite WAL + pre-HTTP `request_id` commit (`dialling` state) as the idempotency anchor rather than Temporal
- **At-most-once attempt & webhook race design:** Added `recorded_calls` keyed by `request_id` so early webhooks (arriving before `201` is read) and delayed `201` responses never double-count attempts or revert state
- **Window & precedence rules:** Chose strict `grace=0` with a speed-scaled end buffer to absorb local HTTP transit latency without violating R1, and immediate `exhausted` transition on final attempts to preserve `completed > exhausted > dnd_blocked`

## Where the AI was wrong or unhelpful

1. **Webhook-before-201 race bug:** The initial `webhook.py` checked `if call_sid not in active_calls` and discarded the webhook while still marking `call_sid` in `processed_webhooks`, which would drop results whenever a webhook arrived before the `201` response was processed. Fixed via `recorded_calls` and matching `dialling` leads on `request_id`.
2. **In-flight `dialling` concurrency bug:** The initial `engine.py` only counted `active_calls` toward tenant concurrency and phone uniqueness, ignoring leads in `dialling` state (such as calls awaiting `504` resolution), which risked `429`/`409` violations. Fixed by counting both `active_calls` and `dialling` leads.
3. **2s `504` blocking & N+1 SQLite queries:** Initial `engine.py` used `asyncio.gather` on dial batches and queried `phone_is_dnd` per lead per tick, stalling the loop for 2+ real seconds on `504`s and advancing the simulated clock past window boundaries. Fixed by batching `get_dnd_phones()`, spawning non-blocking `dial_lead` tasks, and adding a fresh `clock.now()` window check right before `POST /v1/calls`.
4. **`run.py` hang on exit & `pytest-asyncio` fixture bug:** `run.py` originally awaited `asyncio.gather(server.serve(), run_engine(...))` before setting `server.should_exit = True`, causing it to hang after writing `final_state.csv`; tests also used `asyncio.get_event_loop()` in sync fixtures (failing in Python 3.12). Both were caught by running `pytest` and the E2E harness and fixed.

## How I verified AI-generated code

- Audited all state transitions and business rules (R1–R9) against `assignment-b-workflow/README.md` and `mock_telephony/server.py`
- Ran `pytest solution/tests -v` (29/29 tests passing)
- Ran full E2E chaos tests against `mock_telephony` (`--speed 3600 --failure-scale 1.0`) across seeds 7, 42, 99, 1234 with two `kill -9` crashes mid-run, confirming `violation_count: 0` in `/admin/report` and `0` mismatches between `final_state.csv` and `/admin/leads`
