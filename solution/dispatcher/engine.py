"""
engine.py – Main dispatch loop.

Runs every ~0.05 real seconds:
1. Check dialling-timeout leads (> 30 real seconds with no active 504 task)
2. Check active calls that may have missed a webhook
3. Activate retry_wait/failed_wait/callback_wait leads whose timer expired
4. Apply DND & exhaustion to pending leads (precedence: completed > exhausted > dnd_blocked)
5. Pick eligible leads in priority order, commit request_id, and spawn non-blocking dial tasks
6. Check if all leads are in final states → write final_state.csv and exit
"""
from __future__ import annotations

import asyncio
import csv
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import httpx

from . import db as database
from .clock import SimClock
from .config import AppConfig
from .scheduler import (
    TERMINAL_STATES,
    is_eligible_now,
    sort_eligible_leads,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}

# Real-time timeout before an orphaned "dialling" lead triggers a recovery poll
DIALLING_TIMEOUT_REAL_SECONDS = 30.0
# Sim-time before we poll an active call that hasn't received a webhook (max call ~390 sim sec)
ACTIVE_CALL_SIM_TIMEOUT_SECONDS = 420.0

_in_flight_504: Set[str] = set()
_background_tasks: Set[asyncio.Task[Any]] = set()


def _track_task(task: asyncio.Task[Any]) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def make_request_id(campaign_id: str, lead_id: str, attempt_no: int) -> str:
    suffix = uuid.uuid4().hex[:8]
    return f"{campaign_id}-{lead_id}-{attempt_no}-{suffix}"


def _window_end_buffer_seconds(speed: float) -> float:
    return max(30.0, speed * 0.1)


async def poll_by_request_id(
    provider_url: str,
    request_id: str,
    max_retries: int = 5,
) -> Optional[Dict[str, Any]]:
    """
    Poll GET /v1/calls?request_id=...
    Returns call data dict on 200, None on confirmed 404.
    """
    delay = 0.2
    not_found_count = 0
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(max_retries):
            try:
                resp = await client.get(
                    f"{provider_url}/v1/calls",
                    params={"request_id": request_id},
                )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    not_found_count += 1
                    if not_found_count >= 2 or attempt == max_retries - 1:
                        return None
                    await asyncio.sleep(0.05)
                    continue
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
            except httpx.HTTPError as exc:
                logger.warning("poll_by_request_id error: %s", exc)
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 10.0)
        return None


async def poll_by_call_sid(
    provider_url: str,
    call_sid: str,
) -> Optional[Dict[str, Any]]:
    """GET /v1/calls/{call_sid} – return dict, {'not_found': True} on 404, or None on network error."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(f"{provider_url}/v1/calls/{call_sid}")
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return {"not_found": True}
        except httpx.HTTPError as exc:
            logger.warning("poll_by_call_sid error: %s", exc)
    return None


async def apply_call_result(
    db_path: str,
    call_data: Dict[str, Any],
    config: AppConfig,
) -> None:
    """Apply an ended call's result as if we got a webhook."""
    result = call_data.get("result", {})
    call_sid = result.get("call_sid") or call_data.get("call_sid", "")
    request_id = result.get("request_id") or call_data.get("request_id")
    campaign_id = result.get("campaign_id") or call_data.get("campaign_id", "")
    lead_id = result.get("lead_id") or call_data.get("lead_id", "")
    phone = result.get("phone", "")
    status = result.get("status") or ""
    disposition = result.get("disposition")
    callback_at_raw = result.get("callback_at")
    ended_at_raw = result.get("ended_at")
    processed_at = datetime.now(tz=timezone.utc).isoformat()
    ended_at = ended_at_raw or processed_at

    if not phone:
        lead = await database.get_lead(db_path, campaign_id, lead_id)
        if lead:
            phone = lead["phone"]

    rp = config.retry_policy
    max_attempts = (
        config.campaigns[campaign_id].max_attempts
        if campaign_id in config.campaigns
        else 3
    )

    await database.process_call_outcome(
        db_path=db_path,
        call_sid=call_sid,
        request_id=request_id,
        campaign_id=campaign_id,
        lead_id=lead_id,
        phone=phone,
        status=status,
        disposition=disposition,
        callback_at=callback_at_raw,
        ended_at=ended_at,
        retry_no_answer_minutes=rp["no_answer_minutes"],
        retry_busy_minutes=rp.get("busy_minutes", rp["no_answer_minutes"]),
        retry_failed_minutes=rp["failed_minutes"],
        max_attempts=max_attempts,
        processed_at=processed_at,
    )


async def handle_504_background(
    db_path: str,
    provider_url: str,
    campaign_id: str,
    lead_id: str,
    request_id: str,
    config: AppConfig,
) -> None:
    """
    Background task for 504 / ambiguous-timeout resolution.
    Poll until we know if the call was placed or not.
    """
    _in_flight_504.add(request_id)
    try:
        logger.info("504 recovery: polling for request_id=%s", request_id)
        result = await poll_by_request_id(provider_url, request_id, max_retries=5)

        if result is not None:
            call_sid = result.get("call_sid", "")
            campaign = config.campaigns.get(campaign_id)
            tenant_id = campaign.tenant_id if campaign else ""
            lead = await database.get_lead(db_path, campaign_id, lead_id)
            if lead is None:
                return
            if lead["state"] not in ("dialling", "pending"):
                if result.get("state") == "ended":
                    await apply_call_result(db_path, result, config)
                return

            logger.info("504 resolved: call placed, call_sid=%s", call_sid)
            now_iso = datetime.now(tz=timezone.utc).isoformat()
            await database.record_call_accepted(
                db_path=db_path,
                call_sid=call_sid,
                request_id=request_id,
                campaign_id=campaign_id,
                lead_id=lead_id,
                phone=lead["phone"],
                tenant_id=tenant_id,
                started_at=now_iso,
            )

            if result.get("state") == "ended":
                await apply_call_result(db_path, result, config)
        else:
            logger.info(
                "504 resolved: call NOT placed for request_id=%s, resetting to pending",
                request_id,
            )
            lead = await database.get_lead(db_path, campaign_id, lead_id)
            if lead and lead["state"] == "dialling" and lead.get("last_request_id") == request_id:
                await database.update_lead_state(db_path, campaign_id, lead_id, "pending")
    finally:
        _in_flight_504.discard(request_id)


async def crash_recovery(
    db_path: str,
    provider_url: str,
    config: AppConfig,
) -> None:
    """On startup, resolve any leads stuck in dialling or active state."""
    dialling_leads = await database.get_leads_by_state(db_path, "dialling")
    for lead in dialling_leads:
        request_id = lead.get("last_request_id")
        if not request_id:
            await database.update_lead_state(
                db_path, lead["campaign_id"], lead["lead_id"], "pending"
            )
            continue

        logger.info(
            "Crash recovery: checking dialling lead %s/%s request_id=%s",
            lead["campaign_id"],
            lead["lead_id"],
            request_id,
        )
        result = await poll_by_request_id(provider_url, request_id, max_retries=5)
        if result is not None:
            call_sid = result.get("call_sid", "")
            campaign = config.campaigns.get(lead["campaign_id"])
            tenant_id = campaign.tenant_id if campaign else ""
            now_iso = datetime.now(tz=timezone.utc).isoformat()

            await database.record_call_accepted(
                db_path=db_path,
                call_sid=call_sid,
                request_id=request_id,
                campaign_id=lead["campaign_id"],
                lead_id=lead["lead_id"],
                phone=lead["phone"],
                tenant_id=tenant_id,
                started_at=now_iso,
            )
            if result.get("state") == "ended":
                await apply_call_result(db_path, result, config)
        else:
            await database.update_lead_state(
                db_path, lead["campaign_id"], lead["lead_id"], "pending"
            )

    active_calls = await database.get_active_calls(db_path)
    for call in active_calls:
        call_sid = call["call_sid"]
        logger.info("Crash recovery: checking active call %s", call_sid)
        result = await poll_by_call_sid(provider_url, call_sid)
        if result and result.get("state") == "ended":
            await apply_call_result(db_path, result, config)
        elif result and result.get("not_found"):
            await database.remove_active_call(db_path, call_sid)
            await database.update_lead_state(
                db_path, call["campaign_id"], call["lead_id"], "pending"
            )


async def write_final_state_csv(db_path: str, output_path: str) -> None:
    """Write final_state.csv sorted by campaign_id, lead_id."""
    all_leads = await database.get_all_leads(db_path)
    final_states = []
    for lead in all_leads:
        final_states.append(
            {
                "campaign_id": lead["campaign_id"],
                "lead_id": lead["lead_id"],
                "final_state": lead["state"],
                "attempts": lead["attempts"],
                "last_status": lead.get("last_status") or "",
                "last_disposition": lead.get("last_disposition") or "",
            }
        )

    final_states.sort(key=lambda r: (r["campaign_id"], r["lead_id"]))

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "campaign_id",
                "lead_id",
                "final_state",
                "attempts",
                "last_status",
                "last_disposition",
            ],
        )
        writer.writeheader()
        writer.writerows(final_states)

    logger.info("Wrote final_state.csv with %d rows to %s", len(final_states), output_path)


def all_leads_final(leads: List[Dict[str, Any]]) -> bool:
    """Return True when every lead is in a terminal state."""
    return all(lead["state"] in TERMINAL_STATES for lead in leads)


async def dial_lead(
    db_path: str,
    provider_url: str,
    callback_url: str,
    lead: Dict[str, Any],
    campaign_id: str,
    tenant_id: str,
    config: AppConfig,
    request_id: Optional[str] = None,
    clock: Optional[SimClock] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> None:
    """
    Attempt to dial one lead.
    - Commit request_id BEFORE the HTTP call (if not already committed by caller)
    - Re-verify calling window right before POST if clock is provided
    - Handle 201/200/503/504 responses atomically
    """
    lead_id = lead["lead_id"]
    phone = lead["phone"]

    if request_id is None:
        attempts_so_far = lead.get("attempts", 0)
        attempt_no = attempts_so_far + 1
        request_id = make_request_id(campaign_id, lead_id, attempt_no)
        await database.set_lead_request_id(
            db_path, campaign_id, lead_id, request_id, state="dialling"
        )

    # Fresh window check right before network call so stale loop timestamps never violate R1
    if clock is not None:
        campaign = config.campaigns.get(campaign_id)
        if campaign is not None:
            now_check = clock.now()
            end_probe = now_check + timedelta(seconds=_window_end_buffer_seconds(clock.speed))
            # Check lead eligibility assuming it's pending right now
            probe_lead = {**lead, "state": "pending"}
            if not (
                is_eligible_now(probe_lead, campaign, config, now_check, grace_seconds=0)
                and is_eligible_now(probe_lead, campaign, config, end_probe, grace_seconds=0)
            ):
                logger.info(
                    "Window closed before POST for %s/%s at %s – resetting to pending",
                    campaign_id,
                    lead_id,
                    now_check.isoformat(),
                )
                current = await database.get_lead(db_path, campaign_id, lead_id)
                if current and current["state"] == "dialling" and current.get("last_request_id") == request_id:
                    await database.update_lead_state(db_path, campaign_id, lead_id, "pending")
                return

    payload = {
        "request_id": request_id,
        "campaign_id": campaign_id,
        "lead_id": lead_id,
        "phone": phone,
        "callback_url": callback_url,
    }

    logger.info(
        "Dialling %s/%s phone=%s request_id=%s", campaign_id, lead_id, phone, request_id
    )

    try:
        if client is not None:
            resp = await client.post(f"{provider_url}/v1/calls", json=payload)
        else:
            async with httpx.AsyncClient(timeout=15.0) as local_client:
                resp = await local_client.post(f"{provider_url}/v1/calls", json=payload)
    except httpx.ConnectError as exc:
        logger.warning("ConnectError dialling %s/%s: %s", campaign_id, lead_id, exc)
        current = await database.get_lead(db_path, campaign_id, lead_id)
        if current and current["state"] == "dialling" and current.get("last_request_id") == request_id:
            await database.update_lead_state(db_path, campaign_id, lead_id, "pending")
        return
    except httpx.HTTPError as exc:
        logger.warning(
            "Ambiguous HTTP error dialling %s/%s (%s) – resolving via poll",
            campaign_id,
            lead_id,
            exc,
        )
        task = asyncio.create_task(
            handle_504_background(
                db_path, provider_url, campaign_id, lead_id, request_id, config
            )
        )
        _track_task(task)
        return

    status_code = resp.status_code

    if status_code in (200, 201):
        data = resp.json()
        call_sid = data.get("call_sid", "")
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        activated = await database.record_call_accepted(
            db_path=db_path,
            call_sid=call_sid,
            request_id=request_id,
            campaign_id=campaign_id,
            lead_id=lead_id,
            phone=phone,
            tenant_id=tenant_id,
            started_at=now_iso,
        )
        if activated:
            logger.info("Call accepted: call_sid=%s request_id=%s", call_sid, request_id)
        else:
            logger.info(
                "Call accepted (already resolved by webhook): call_sid=%s", call_sid
            )

        if data.get("state") == "ended":
            await apply_call_result(db_path, data, config)

    elif status_code == 503:
        logger.info(
            "503 from provider for %s/%s – resetting to pending", campaign_id, lead_id
        )
        current = await database.get_lead(db_path, campaign_id, lead_id)
        if current and current["state"] == "dialling" and current.get("last_request_id") == request_id:
            await database.update_lead_state(db_path, campaign_id, lead_id, "pending")

    elif status_code == 504:
        logger.info(
            "504 from provider for %s/%s – starting background poll",
            campaign_id,
            lead_id,
        )
        task = asyncio.create_task(
            handle_504_background(
                db_path, provider_url, campaign_id, lead_id, request_id, config
            )
        )
        _track_task(task)

    elif status_code in (422, 429, 409):
        logger.error(
            "Rule violation %d for %s/%s: %s",
            status_code,
            campaign_id,
            lead_id,
            resp.text,
        )
        current = await database.get_lead(db_path, campaign_id, lead_id)
        if current and current["state"] == "dialling" and current.get("last_request_id") == request_id:
            await database.update_lead_state(db_path, campaign_id, lead_id, "pending")

    else:
        logger.warning(
            "Unexpected status %d for %s/%s", status_code, campaign_id, lead_id
        )
        current = await database.get_lead(db_path, campaign_id, lead_id)
        if current and current["state"] == "dialling" and current.get("last_request_id") == request_id:
            await database.update_lead_state(db_path, campaign_id, lead_id, "pending")


async def run_engine(
    db_path: str,
    provider_url: str,
    callback_url: str,
    config: AppConfig,
    clock: SimClock,
    output_csv: str = "final_state.csv",
) -> None:
    """Main engine loop."""
    logger.info("Engine starting, provider=%s", provider_url)

    await crash_recovery(db_path, provider_url, config)

    dialling_seen_at: Dict[str, float] = {}
    last_active_poll_at: Dict[str, float] = {}
    end_buffer_sim = _window_end_buffer_seconds(clock.speed)

    async with httpx.AsyncClient(timeout=15.0) as http_client:
        while True:
            await asyncio.sleep(0.05)

            sim_now = clock.now()
            now_real = time.time()

            # Load snapshot once per tick
            all_leads = await database.get_all_leads(db_path)
            if all_leads and all_leads_final(all_leads):
                for t in list(_background_tasks):
                    t.cancel()
                logger.info("All leads in final state. Writing final_state.csv and exiting.")
                await write_final_state_csv(db_path, output_csv)
                return

            # --- Step 1: dialling timeout (> 30 real seconds with no active 504 task) ---
            dialling_leads = [l for l in all_leads if l["state"] == "dialling"]
            active_rids = set()
            for lead in dialling_leads:
                request_id = lead.get("last_request_id")
                if not request_id:
                    await database.update_lead_state(
                        db_path, lead["campaign_id"], lead["lead_id"], "pending"
                    )
                    continue
                active_rids.add(request_id)
                if request_id not in dialling_seen_at:
                    dialling_seen_at[request_id] = now_real
                elif (
                    request_id not in _in_flight_504
                    and now_real - dialling_seen_at[request_id] > DIALLING_TIMEOUT_REAL_SECONDS
                ):
                    logger.warning(
                        "Dialling lead %s/%s stuck > %.0fs, triggering recovery poll",
                        lead["campaign_id"],
                        lead["lead_id"],
                        DIALLING_TIMEOUT_REAL_SECONDS,
                    )
                    dialling_seen_at[request_id] = now_real
                    task = asyncio.create_task(
                        handle_504_background(
                            db_path,
                            provider_url,
                            lead["campaign_id"],
                            lead["lead_id"],
                            request_id,
                            config,
                        )
                    )
                    _track_task(task)

            for stale_rid in list(dialling_seen_at):
                if stale_rid not in active_rids:
                    del dialling_seen_at[stale_rid]

            # --- Step 2: active calls that may have missed a webhook ---
            active_calls = await database.get_active_calls(db_path)
            timeout_real = ACTIVE_CALL_SIM_TIMEOUT_SECONDS / max(clock.speed, 1.0)
            poll_cooldown_real = max(0.25, 60.0 / max(clock.speed, 1.0))

            for call in active_calls:
                call_sid = call["call_sid"]
                started_real = datetime.fromisoformat(call["started_at"]).timestamp()
                if now_real - started_real > timeout_real:
                    last_polled = last_active_poll_at.get(call_sid, 0.0)
                    if now_real - last_polled >= poll_cooldown_real:
                        last_active_poll_at[call_sid] = now_real
                        logger.info(
                            "Active call %s exceeded sim timeout, polling provider", call_sid
                        )
                        result = await poll_by_call_sid(provider_url, call_sid)
                        if result and result.get("state") == "ended":
                            await apply_call_result(db_path, result, config)

            # --- Step 3: Activate timer-expired leads ---
            waiting_leads = [
                l for l in all_leads if l["state"] in ("retry_wait", "failed_wait", "callback_wait")
            ]
            for lead in waiting_leads:
                campaign = config.campaigns.get(lead["campaign_id"])
                if campaign is None:
                    continue
                if lead.get("attempts", 0) >= campaign.max_attempts:
                    await database.update_lead_state(
                        db_path, lead["campaign_id"], lead["lead_id"], "exhausted"
                    )
                    lead["state"] = "exhausted"
                    continue

                timer_field = (
                    "retry_after"
                    if lead["state"] in ("retry_wait", "failed_wait")
                    else "callback_at"
                )
                timer_val = lead.get(timer_field)
                if not timer_val or sim_now >= datetime.fromisoformat(timer_val):
                    await database.update_lead_state(
                        db_path, lead["campaign_id"], lead["lead_id"], "pending"
                    )
                    lead["state"] = "pending"

            # --- Step 4 & 5: Apply exhausted > dnd_blocked precedence to pending leads ---
            dnd_phones = await database.get_dnd_phones(db_path)
            pending_leads = [l for l in all_leads if l["state"] == "pending"]
            for lead in pending_leads:
                campaign = config.campaigns.get(lead["campaign_id"])
                if campaign and lead.get("attempts", 0) >= campaign.max_attempts:
                    await database.update_lead_state(
                        db_path, lead["campaign_id"], lead["lead_id"], "exhausted"
                    )
                    lead["state"] = "exhausted"
                elif lead["phone"] in dnd_phones:
                    await database.update_lead_state(
                        db_path, lead["campaign_id"], lead["lead_id"], "dnd_blocked"
                    )
                    lead["state"] = "dnd_blocked"

            # --- Step 6: Pick eligible leads in priority order ---
            sim_now = clock.now()
            safe_end_probe = sim_now + timedelta(seconds=end_buffer_sim)
            eligible = []
            for lead in pending_leads:
                if lead["state"] != "pending":
                    continue
                campaign = config.campaigns.get(lead["campaign_id"])
                if campaign is None:
                    continue
                if is_eligible_now(lead, campaign, config, sim_now, grace_seconds=0) and is_eligible_now(
                    lead, campaign, config, safe_end_probe, grace_seconds=0
                ):
                    eligible.append(lead)

            eligible = sort_eligible_leads(eligible)

            # Count BOTH active_calls AND dialling leads toward tenant & phone limits!
            active_calls_now = await database.get_active_calls(db_path)
            dialling_now = [l for l in all_leads if l["state"] == "dialling"]

            tenant_active: Dict[str, int] = {}
            phones_active: Set[str] = set()
            for ac in active_calls_now:
                tenant_active[ac["tenant_id"]] = tenant_active.get(ac["tenant_id"], 0) + 1
                phones_active.add(ac["phone"])
            for dl in dialling_now:
                camp = config.campaigns.get(dl["campaign_id"])
                if camp:
                    tenant_active[camp.tenant_id] = tenant_active.get(camp.tenant_id, 0) + 1
                phones_active.add(dl["phone"])

            for lead in eligible:
                campaign = config.campaigns.get(lead["campaign_id"])
                if campaign is None:
                    continue
                tenant_id = campaign.tenant_id
                tenant_cfg = config.tenants.get(tenant_id)
                if tenant_cfg is None:
                    continue

                current_concurrency = tenant_active.get(tenant_id, 0)
                if current_concurrency >= tenant_cfg.max_concurrent_calls:
                    continue

                if lead["phone"] in phones_active:
                    continue

                if lead.get("attempts", 0) >= campaign.max_attempts:
                    await database.update_lead_state(
                        db_path, lead["campaign_id"], lead["lead_id"], "exhausted"
                    )
                    continue

                tenant_active[tenant_id] = current_concurrency + 1
                phones_active.add(lead["phone"])

                # Commit request_id + state=dialling synchronously before spawning task
                attempt_no = lead.get("attempts", 0) + 1
                req_id = make_request_id(lead["campaign_id"], lead["lead_id"], attempt_no)
                await database.set_lead_request_id(
                    db_path, lead["campaign_id"], lead["lead_id"], req_id, state="dialling"
                )

                task = asyncio.create_task(
                    dial_lead(
                        db_path=db_path,
                        provider_url=provider_url,
                        callback_url=callback_url,
                        lead=lead,
                        campaign_id=lead["campaign_id"],
                        tenant_id=tenant_id,
                        config=config,
                        request_id=req_id,
                        clock=clock,
                        client=http_client,
                    )
                )
                _track_task(task)
