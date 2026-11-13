"""
scheduler.py – Window math, eligibility checks, priority ordering.

All time comparisons use IST (Asia/Kolkata = UTC+5:30).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .config import AppConfig, CampaignConfig
from .clock import IST

DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
TERMINAL_STATES = {"completed", "exhausted", "dnd_blocked"}
FINAL_STATES = TERMINAL_STATES  # alias


def _hm(s: str) -> Tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def in_window(
    t: datetime,
    start: str,
    end: str,
    days: Optional[List[str]],
    grace_seconds: float = 0,
) -> bool:
    """
    Return True if `t` is inside the [start, end) window on an allowed day.
    Grace allows up to `grace_seconds` outside the window on either edge.
    Matches the provider's own in_window logic.
    """
    probes = (t,) if grace_seconds <= 0 else (t, t - timedelta(seconds=grace_seconds), t + timedelta(seconds=grace_seconds))
    for probe in probes:
        lt = probe.astimezone(IST)
        if days is not None and DAYS[lt.weekday()] not in days:
            continue
        sh, sm = _hm(start)
        eh, em = _hm(end)
        s_dt = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        e_dt = lt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if s_dt <= lt < e_dt:
            return True
    return False


def next_window_open(
    t: datetime,
    start: str,
    end: str,
    days: List[str],
) -> datetime:
    """
    Return the earliest datetime >= t that is inside the window.
    Checks up to 9 days ahead to cover any weekly schedule.
    """
    lt = t.astimezone(IST)
    for d in range(0, 9):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in days:
            continue
        sh, sm = _hm(start)
        eh, em = _hm(end)
        s_dt = day.replace(hour=sh, minute=sm)
        e_dt = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s_dt:
                return s_dt
            if lt < e_dt:
                return lt
            continue  # past window for today
        return s_dt
    return lt  # fallback: shouldn't happen


def compute_retry_after(ended_at: datetime, status: str, retry_policy: Dict[str, int]) -> datetime:
    """Compute the earliest retry time from ended_at based on call status."""
    if status == "no_answer":
        mins = retry_policy.get("no_answer_minutes", 60)
    elif status == "busy":
        mins = retry_policy.get("busy_minutes", retry_policy.get("no_answer_minutes", 60))
    elif status == "failed":
        mins = retry_policy.get("failed_minutes", 15)
    else:
        mins = 0
    return ended_at + timedelta(minutes=mins)


def is_eligible_now(
    lead: Dict[str, Any],
    campaign: CampaignConfig,
    config: AppConfig,
    sim_now: datetime,
    grace_seconds: float,
) -> bool:
    """
    Check all scheduling rules for a single lead at sim_now.
    Returns True if the lead can be dialled right now.
    """
    state = lead["state"]
    if state in TERMINAL_STATES:
        return False
    if state != "pending":
        return False

    # R1: regulatory window
    rw = config.regulatory_window
    if not in_window(sim_now, rw["start"], rw["end"], None, grace_seconds):
        return False

    # R1: campaign window
    w = campaign.window
    if not in_window(sim_now, w.start, w.end, w.days, grace_seconds):
        return False

    # R6: callback_at guard
    if lead.get("callback_at"):
        cb_at = datetime.fromisoformat(lead["callback_at"])
        if sim_now < cb_at - timedelta(seconds=grace_seconds):
            return False

    # R5: retry_after guard
    if lead.get("retry_after") and lead.get("last_disposition") != "callback_requested":
        ra_dt = datetime.fromisoformat(lead["retry_after"])
        if sim_now < ra_dt - timedelta(seconds=grace_seconds):
            return False

    return True


def next_eligible_time(
    lead: Dict[str, Any],
    campaign: CampaignConfig,
    config: AppConfig,
    sim_now: datetime,
) -> Optional[datetime]:
    """
    For a non-terminal lead, return when it will next be eligible (considering window + timers).
    Returns None if the lead is in a terminal state.
    """
    if lead["state"] in TERMINAL_STATES:
        return None

    t = sim_now
    if lead.get("callback_at") and lead.get("last_disposition") == "callback_requested":
        cb_dt = datetime.fromisoformat(lead["callback_at"])
        if cb_dt > t:
            t = cb_dt
    elif lead.get("retry_after"):
        ra_dt = datetime.fromisoformat(lead["retry_after"])
        if ra_dt > t:
            t = ra_dt

    rw = config.regulatory_window
    w = campaign.window

    reg_next = next_window_open(t, rw["start"], rw["end"], DAYS)
    camp_next = next_window_open(t, w.start, w.end, w.days)

    return max(reg_next, camp_next)


def priority_key(lead: Dict[str, Any]) -> int:
    """
    Lower = higher priority.
    0: callback leads (overdue or due)
    1: retry/failed-wait leads (back to pending after wait)
    2: fresh leads (never attempted)
    """
    if lead.get("last_disposition") == "callback_requested":
        return 0
    if lead.get("attempts", 0) > 0:
        return 1
    return 2


def sort_eligible_leads(leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort leads by priority: callbacks (earliest first) > retries (earliest first) > fresh."""
    def _sort_tuple(lead: Dict[str, Any]) -> Tuple[int, str, str, str]:
        prio = priority_key(lead)
        if prio == 0:
            due = lead.get("callback_at") or ""
        elif prio == 1:
            due = lead.get("retry_after") or ""
        else:
            due = ""
        return (prio, due, lead.get("campaign_id", ""), lead.get("lead_id", ""))

    return sorted(leads, key=_sort_tuple)
