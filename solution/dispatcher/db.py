"""
db.py – SQLite schema definition and all database queries.

All writes go through an asyncio.Lock (per running event loop) to avoid
"database is locked" errors when the engine loop and webhook server run
concurrently in the same process.
"""
from __future__ import annotations

import asyncio
import weakref
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiosqlite

# Per-loop lock so multiple asyncio.run() / pytest-asyncio tests never share a bound lock.
_loop_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)

TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}
FINAL_STATES = ("completed", "exhausted", "dnd_blocked")


def _get_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _loop_locks.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _loop_locks[loop] = lock
    return lock


SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_after TEXT,
    callback_at TEXT,
    last_call_sid TEXT,
    last_request_id TEXT,
    last_status TEXT,
    last_disposition TEXT,
    PRIMARY KEY (campaign_id, lead_id)
);

CREATE TABLE IF NOT EXISTS active_calls (
    call_sid TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    started_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recorded_calls (
    request_id TEXT PRIMARY KEY,
    call_sid TEXT NOT NULL UNIQUE,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dnd_phones (
    phone TEXT PRIMARY KEY,
    blocked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_webhooks (
    call_sid TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);
"""


async def init_db(db_path: str) -> None:
    """Create tables if they don't exist."""
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.executescript(SCHEMA)
            await db.commit()


async def load_leads_idempotent(
    db_path: str, leads_by_campaign: Dict[str, List[Dict[str, str]]]
) -> None:
    """Insert leads into DB only if not already present (idempotent)."""
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            for campaign_id, leads in leads_by_campaign.items():
                for lead in leads:
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO leads (campaign_id, lead_id, phone, state, attempts)
                        VALUES (?, ?, ?, 'pending', 0)
                        """,
                        (campaign_id, lead["lead_id"], lead["phone"]),
                    )
            await db.commit()


async def get_lead(
    db_path: str, campaign_id: str, lead_id: str
) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM leads WHERE campaign_id=? AND lead_id=?",
            (campaign_id, lead_id),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_system_status(db_path: str) -> Dict[str, Any]:
    """Return campaign progress breakdown and active calls summary."""
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        campaigns: Dict[str, Dict[str, int]] = {}
        async with db.execute(
            "SELECT campaign_id, state, COUNT(*) as cnt FROM leads GROUP BY campaign_id, state"
        ) as cur:
            for row in await cur.fetchall():
                cid, st, cnt = row["campaign_id"], row["state"], row["cnt"]
                campaigns.setdefault(cid, {})[st] = cnt

        async with db.execute("SELECT * FROM active_calls") as cur:
            active_calls = [dict(r) for r in await cur.fetchall()]

        return {
            "campaigns": campaigns,
            "active_calls_count": len(active_calls),
            "active_calls": active_calls,
        }


async def get_all_leads(db_path: str) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM leads") as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_leads_by_state(db_path: str, state: str) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM leads WHERE state=?", (state,)) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_leads_in_states(db_path: str, states: List[str]) -> List[Dict[str, Any]]:
    placeholders = ",".join("?" * len(states))
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"SELECT * FROM leads WHERE state IN ({placeholders})", states
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def update_lead_state(
    db_path: str,
    campaign_id: str,
    lead_id: str,
    state: str,
    *,
    attempts: Optional[int] = None,
    retry_after: Optional[str] = None,
    callback_at: Optional[str] = None,
    last_call_sid: Optional[str] = None,
    last_request_id: Optional[str] = None,
    last_status: Optional[str] = None,
    last_disposition: Optional[str] = None,
) -> None:
    fields = ["state=?"]
    vals: List[Any] = [state]
    if attempts is not None:
        fields.append("attempts=?")
        vals.append(attempts)
    if retry_after is not None:
        fields.append("retry_after=?")
        vals.append(retry_after)
    if callback_at is not None:
        fields.append("callback_at=?")
        vals.append(callback_at)
    if last_call_sid is not None:
        fields.append("last_call_sid=?")
        vals.append(last_call_sid)
    if last_request_id is not None:
        fields.append("last_request_id=?")
        vals.append(last_request_id)
    if last_status is not None:
        fields.append("last_status=?")
        vals.append(last_status)
    if last_disposition is not None:
        fields.append("last_disposition=?")
        vals.append(last_disposition)
    vals.extend([campaign_id, lead_id])
    sql = f"UPDATE leads SET {', '.join(fields)} WHERE campaign_id=? AND lead_id=?"
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute(sql, vals)
            await db.commit()


async def set_lead_request_id(
    db_path: str, campaign_id: str, lead_id: str, request_id: str, state: str = "dialling"
) -> None:
    """Commit request_id BEFORE making the HTTP call (crash safety)."""
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute(
                "UPDATE leads SET state=?, last_request_id=? WHERE campaign_id=? AND lead_id=?",
                (state, request_id, campaign_id, lead_id),
            )
            await db.commit()


async def record_call_accepted(
    db_path: str,
    call_sid: str,
    request_id: str,
    campaign_id: str,
    lead_id: str,
    phone: str,
    tenant_id: str,
    started_at: str,
) -> bool:
    """
    Atomically record that a call was accepted by the provider (200/201/504-found).

    Idempotent on request_id:
    - Increments leads.attempts at most once per request_id.
    - If a webhook for call_sid already arrived and finished before 201 was read,
      does NOT revert lead.state back to 'active' or re-insert into active_calls.
    Returns True if this call newly transitioned the lead to 'active'.
    """
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                "SELECT 1 FROM recorded_calls WHERE request_id=?", (request_id,)
            ) as cur:
                already_recorded = await cur.fetchone() is not None

            if already_recorded:
                return False

            await db.execute(
                """
                INSERT INTO recorded_calls (request_id, call_sid, campaign_id, lead_id)
                VALUES (?, ?, ?, ?)
                """,
                (request_id, call_sid, campaign_id, lead_id),
            )

            async with db.execute(
                "SELECT 1 FROM processed_webhooks WHERE call_sid=?", (call_sid,)
            ) as cur:
                webhook_already_done = await cur.fetchone() is not None

            if webhook_already_done:
                # Webhook beat us here and already updated attempts/state.
                return False

            await db.execute(
                """
                UPDATE leads
                SET state='active',
                    attempts=attempts + 1,
                    last_call_sid=?,
                    last_request_id=?
                WHERE campaign_id=? AND lead_id=?
                """,
                (call_sid, request_id, campaign_id, lead_id),
            )
            await db.execute(
                """
                INSERT OR REPLACE INTO active_calls
                    (call_sid, campaign_id, lead_id, phone, tenant_id, request_id, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (call_sid, campaign_id, lead_id, phone, tenant_id, request_id, started_at),
            )
            await db.commit()
            return True


async def add_active_call(
    db_path: str,
    call_sid: str,
    campaign_id: str,
    lead_id: str,
    phone: str,
    tenant_id: str,
    request_id: str,
    started_at: str,
) -> None:
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO active_calls
                    (call_sid, campaign_id, lead_id, phone, tenant_id, request_id, started_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (call_sid, campaign_id, lead_id, phone, tenant_id, request_id, started_at),
            )
            await db.commit()


async def remove_active_call(db_path: str, call_sid: str) -> None:
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute("DELETE FROM active_calls WHERE call_sid=?", (call_sid,))
            await db.commit()


async def get_active_calls(db_path: str) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM active_calls") as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def count_active_calls_for_tenant(db_path: str, tenant_id: str) -> int:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM active_calls WHERE tenant_id=?", (tenant_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def phone_in_active_calls(db_path: str, phone: str) -> bool:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        async with db.execute(
            "SELECT 1 FROM active_calls WHERE phone=?", (phone,)
        ) as cur:
            row = await cur.fetchone()
            return row is not None


async def add_dnd_phone(db_path: str, phone: str, blocked_at: str) -> None:
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute(
                "INSERT OR IGNORE INTO dnd_phones (phone, blocked_at) VALUES (?, ?)",
                (phone, blocked_at),
            )
            await db.commit()


async def phone_is_dnd(db_path: str, phone: str) -> bool:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        async with db.execute("SELECT 1 FROM dnd_phones WHERE phone=?", (phone,)) as cur:
            row = await cur.fetchone()
            return row is not None


async def get_dnd_phones(db_path: str) -> set[str]:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        async with db.execute("SELECT phone FROM dnd_phones") as cur:
            rows = await cur.fetchall()
            return {r[0] for r in rows}


async def apply_dnd_to_all_leads(db_path: str, phone: str, now_iso: str) -> None:
    """Mark all non-terminal, non-in-flight leads with this phone as dnd_blocked."""
    excluded = ("completed", "exhausted", "dnd_blocked", "active", "dialling")
    placeholders = ",".join("?" * len(excluded))
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute(
                f"""
                UPDATE leads SET state='dnd_blocked'
                WHERE phone=? AND state NOT IN ({placeholders})
                """,
                (phone, *excluded),
            )
            await db.commit()


async def is_webhook_processed(db_path: str, call_sid: str) -> bool:
    async with aiosqlite.connect(db_path, timeout=30.0) as db:
        async with db.execute(
            "SELECT 1 FROM processed_webhooks WHERE call_sid=?", (call_sid,)
        ) as cur:
            row = await cur.fetchone()
            return row is not None


async def mark_webhook_processed(db_path: str, call_sid: str, processed_at: str) -> None:
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            await db.execute(
                "INSERT OR IGNORE INTO processed_webhooks (call_sid, processed_at) VALUES (?, ?)",
                (call_sid, processed_at),
            )
            await db.commit()


async def process_call_outcome(
    db_path: str,
    call_sid: str,
    request_id: Optional[str],
    campaign_id: str,
    lead_id: str,
    phone: str,
    status: str,
    disposition: Optional[str],
    callback_at: Optional[str],
    ended_at: str,
    retry_no_answer_minutes: int,
    retry_busy_minutes: int,
    retry_failed_minutes: int,
    max_attempts: int,
    processed_at: str,
) -> bool:
    """
    Atomically process a call result (from webhook or polling recovery).

    Handles:
    - Idempotency on call_sid (processed_webhooks)
    - Race where webhook arrives before the 201 HTTP response was processed
      (records request_id and increments attempts once if not yet recorded)
    - Global DND propagation (with precedence completed > exhausted > dnd_blocked)
    - Immediate transition to exhausted when attempts >= max_attempts and non-terminal

    Returns True if newly processed, False if already processed or unknown.
    """
    async with _get_lock():
        async with aiosqlite.connect(db_path, timeout=30.0) as db:
            db.row_factory = aiosqlite.Row

            # 1. Idempotency check
            async with db.execute(
                "SELECT 1 FROM processed_webhooks WHERE call_sid=?", (call_sid,)
            ) as cur:
                if await cur.fetchone() is not None:
                    return False

            # 2. Load lead
            async with db.execute(
                "SELECT * FROM leads WHERE campaign_id=? AND lead_id=?",
                (campaign_id, lead_id),
            ) as cur:
                lead_row = await cur.fetchone()
                if lead_row is None:
                    return False
                lead = dict(lead_row)

            # 3. Check if call is known (either in active_calls or matches dialling request_id)
            async with db.execute(
                "SELECT request_id FROM active_calls WHERE call_sid=?", (call_sid,)
            ) as cur:
                ac_row = await cur.fetchone()

            effective_request_id = request_id or (ac_row["request_id"] if ac_row else None) or lead.get("last_request_id")

            is_known_active = ac_row is not None
            is_dialling_match = (
                lead["state"] == "dialling"
                and effective_request_id is not None
                and lead.get("last_request_id") == effective_request_id
            )

            if not is_known_active and not is_dialling_match:
                # Unknown / stale call that does not match our active or dialling state
                return False

            # 4. Ensure attempt is counted at most once per request_id
            attempts = lead["attempts"]
            if effective_request_id:
                async with db.execute(
                    "SELECT 1 FROM recorded_calls WHERE request_id=?",
                    (effective_request_id,),
                ) as cur:
                    recorded = await cur.fetchone() is not None
                if not recorded:
                    await db.execute(
                        """
                        INSERT INTO recorded_calls (request_id, call_sid, campaign_id, lead_id)
                        VALUES (?, ?, ?, ?)
                        """,
                        (effective_request_id, call_sid, campaign_id, lead_id),
                    )
                    attempts += 1

            # 5. Determine next lead state (precedence: completed > exhausted > dnd_blocked)
            retry_after: Optional[str] = None
            cb_at: Optional[str] = None

            if disposition in TERMINAL_DISPOSITIONS:
                new_state = "completed"
            elif attempts >= max_attempts:
                new_state = "exhausted"
            elif disposition == "callback_requested":
                new_state = "callback_wait"
                cb_at = callback_at
            elif status in ("no_answer", "busy"):
                new_state = "retry_wait"
                ended_dt = datetime.fromisoformat(ended_at)
                mins = retry_no_answer_minutes if status == "no_answer" else retry_busy_minutes
                retry_after = (ended_dt + timedelta(minutes=mins)).isoformat()
            elif status == "failed":
                new_state = "failed_wait"
                ended_dt = datetime.fromisoformat(ended_at)
                retry_after = (ended_dt + timedelta(minutes=retry_failed_minutes)).isoformat()
            else:
                new_state = "completed"

            # Check if phone became DND while this call was in flight (and lead is not terminal/exhausted)
            if disposition == "dnd":
                await db.execute(
                    "INSERT OR IGNORE INTO dnd_phones (phone, blocked_at) VALUES (?, ?)",
                    (phone, ended_at),
                )
            elif new_state not in ("completed", "exhausted"):
                async with db.execute(
                    "SELECT 1 FROM dnd_phones WHERE phone=?", (phone,)
                ) as cur:
                    if await cur.fetchone() is not None:
                        new_state = "dnd_blocked"

            # 6. Mark webhook processed, delete from active_calls, update lead
            await db.execute(
                "INSERT OR IGNORE INTO processed_webhooks (call_sid, processed_at) VALUES (?, ?)",
                (call_sid, processed_at),
            )
            await db.execute("DELETE FROM active_calls WHERE call_sid=?", (call_sid,))
            await db.execute(
                """
                UPDATE leads SET
                    state=?,
                    attempts=?,
                    retry_after=?,
                    callback_at=?,
                    last_status=?,
                    last_disposition=?,
                    last_call_sid=?
                WHERE campaign_id=? AND lead_id=?
                """,
                (
                    new_state,
                    attempts,
                    retry_after,
                    cb_at,
                    status,
                    disposition,
                    call_sid,
                    campaign_id,
                    lead_id,
                ),
            )

            # 7. Propagate DND to all other eligible leads with the same phone
            if disposition == "dnd":
                excluded = ("completed", "exhausted", "dnd_blocked", "active", "dialling")
                placeholders = ",".join("?" * len(excluded))
                await db.execute(
                    f"""
                    UPDATE leads SET state='dnd_blocked'
                    WHERE phone=? AND state NOT IN ({placeholders})
                    """,
                    (phone, *excluded),
                )

            await db.commit()
            return True


async def process_webhook_full(
    db_path: str,
    call_sid: str,
    campaign_id: str,
    lead_id: str,
    phone: str,
    status: str,
    disposition: Optional[str],
    callback_at: Optional[str],
    ended_at: str,
    retry_no_answer_minutes: int,
    retry_busy_minutes: int,
    retry_failed_minutes: int,
    attempts: int,
    processed_at: str,
    max_attempts: int = 3,
    request_id: Optional[str] = None,
) -> None:
    """Backward-compatible wrapper around process_call_outcome."""
    await process_call_outcome(
        db_path=db_path,
        call_sid=call_sid,
        request_id=request_id,
        campaign_id=campaign_id,
        lead_id=lead_id,
        phone=phone,
        status=status,
        disposition=disposition,
        callback_at=callback_at,
        ended_at=ended_at,
        retry_no_answer_minutes=retry_no_answer_minutes,
        retry_busy_minutes=retry_busy_minutes,
        retry_failed_minutes=retry_failed_minutes,
        max_attempts=max_attempts,
        processed_at=processed_at,
    )
