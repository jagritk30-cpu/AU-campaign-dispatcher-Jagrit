"""
test_scheduler.py – Window math and priority ordering tests.

Uses FakeClock (no real waits), Thursday 2026-11-12T17:30:00+05:30 as base.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dispatcher.clock import FakeClock, IST
from dispatcher.config import AppConfig, CampaignConfig, CampaignWindow, TenantConfig
from dispatcher.scheduler import (
    compute_retry_after,
    in_window,
    is_eligible_now,
    next_window_open,
    priority_key,
    sort_eligible_leads,
)

# -------------------------------------------------------------------------
# Fixtures / helpers
# -------------------------------------------------------------------------

SIM_START = datetime(2026, 11, 12, 17, 30, 0, tzinfo=IST)  # Thursday


def make_config(
    camp_start="10:00",
    camp_end="19:00",
    camp_days=None,
    reg_start="09:00",
    reg_end="21:00",
    grace=120,
) -> tuple[AppConfig, CampaignConfig]:
    if camp_days is None:
        camp_days = ["mon", "tue", "wed", "thu", "fri", "sat"]
    tenant = TenantConfig(tenant_id="acme_insurance", max_concurrent_calls=4)
    campaign = CampaignConfig(
        campaign_id="c_acme_renewals",
        tenant_id="acme_insurance",
        leads_file="",
        window=CampaignWindow(start=camp_start, end=camp_end, days=camp_days),
        max_attempts=3,
    )
    cfg = AppConfig(
        sim_start=SIM_START.isoformat(),
        timezone="Asia/Kolkata",
        regulatory_window={"start": reg_start, "end": reg_end},
        window_grace_seconds=grace,
        retry_policy={"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15},
        webhook_secret="secret",
        tenants={"acme_insurance": tenant},
        campaigns={"c_acme_renewals": campaign},
        leads={},
    )
    return cfg, campaign


def make_lead(state="pending", attempts=0, last_disposition=None, callback_at=None):
    return {
        "campaign_id": "c_acme_renewals",
        "lead_id": "ren_0001",
        "phone": "+919999999999",
        "state": state,
        "attempts": attempts,
        "last_disposition": last_disposition,
        "callback_at": callback_at,
    }


# -------------------------------------------------------------------------
# test_in_window (direct)
# -------------------------------------------------------------------------

def test_in_window_true():
    t = datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)  # 15:00 IST Thursday
    assert in_window(t, "10:00", "19:00", ["thu"], grace_seconds=0) is True


def test_in_window_false_hour():
    t = datetime(2026, 11, 12, 20, 0, 0, tzinfo=IST)  # 20:00 IST
    assert in_window(t, "10:00", "19:00", ["thu"], grace_seconds=0) is False


def test_in_window_false_day():
    t = datetime(2026, 11, 15, 15, 0, 0, tzinfo=IST)  # Sunday
    assert in_window(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri"], grace_seconds=0) is False


# -------------------------------------------------------------------------
# test_in_window eligibility checks via is_eligible_now
# -------------------------------------------------------------------------

def test_in_window_eligible():
    """Lead IS in window → eligible."""
    cfg, campaign = make_config()
    t = datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)  # Thursday 15:00
    clock = FakeClock(t)
    lead = make_lead()
    assert is_eligible_now(lead, campaign, cfg, clock.now(), 120) is True


def test_out_of_window_time():
    """Lead is outside window hours → not eligible."""
    cfg, campaign = make_config(camp_start="10:00", camp_end="19:00")
    t = datetime(2026, 11, 12, 20, 0, 0, tzinfo=IST)  # 20:00 IST – after campaign end
    clock = FakeClock(t)
    lead = make_lead()
    assert is_eligible_now(lead, campaign, cfg, clock.now(), 0) is False


def test_out_of_window_day():
    """Lead is on wrong day → not eligible."""
    cfg, campaign = make_config(camp_days=["mon", "tue", "wed"])
    t = datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)  # Thursday
    clock = FakeClock(t)
    lead = make_lead()
    assert is_eligible_now(lead, campaign, cfg, clock.now(), 0) is False


def test_regulatory_window():
    """Even if campaign window is open, 08:59 IST → not eligible (regulatory 09:00–21:00)."""
    cfg, campaign = make_config(camp_start="08:00", camp_end="21:00", reg_start="09:00", reg_end="21:00")
    t = datetime(2026, 11, 12, 8, 59, 0, tzinfo=IST)  # 08:59 IST
    clock = FakeClock(t)
    lead = make_lead()
    # With grace=0, 08:59 is outside regulatory 09:00
    assert is_eligible_now(lead, campaign, cfg, clock.now(), grace_seconds=0) is False


# -------------------------------------------------------------------------
# next_window_open tests
# -------------------------------------------------------------------------

def test_next_window_open_today():
    """sim_now before window → next_eligible is window start today."""
    t = datetime(2026, 11, 12, 9, 0, 0, tzinfo=IST)  # Thursday 09:00 (before 10:00 campaign start)
    expected = datetime(2026, 11, 12, 10, 0, 0, tzinfo=IST)
    result = next_window_open(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"])
    assert result.astimezone(IST).hour == 10
    assert result.astimezone(IST).day == 12


def test_next_window_open_tomorrow():
    """sim_now after window close → next_eligible is next valid day."""
    t = datetime(2026, 11, 12, 20, 0, 0, tzinfo=IST)  # Thursday 20:00 (after 19:00 close)
    result = next_window_open(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"])
    result_ist = result.astimezone(IST)
    assert result_ist.day == 13  # Friday
    assert result_ist.hour == 10


# -------------------------------------------------------------------------
# Priority ordering
# -------------------------------------------------------------------------

def test_priority_callbacks_first():
    """Callback lead + retry lead + fresh lead → callback comes first."""
    callback_lead = make_lead(attempts=1, last_disposition="callback_requested")
    retry_lead = make_lead(attempts=1, last_disposition="no_answer")
    fresh_lead = make_lead(attempts=0, last_disposition=None)

    leads = [fresh_lead, retry_lead, callback_lead]
    sorted_leads = sort_eligible_leads(leads)
    assert sorted_leads[0] is callback_lead


def test_priority_retries_before_fresh():
    """Retry lead + fresh lead → retry comes first."""
    retry_lead = make_lead(attempts=1, last_disposition="no_answer")
    fresh_lead = make_lead(attempts=0, last_disposition=None)

    leads = [fresh_lead, retry_lead]
    sorted_leads = sort_eligible_leads(leads)
    assert sorted_leads[0] is retry_lead


def test_priority_key_callback():
    lead = make_lead(attempts=1, last_disposition="callback_requested")
    assert priority_key(lead) == 0


def test_priority_key_retry():
    lead = make_lead(attempts=1, last_disposition="no_answer")
    assert priority_key(lead) == 1


def test_priority_key_fresh():
    lead = make_lead(attempts=0, last_disposition=None)
    assert priority_key(lead) == 2


# -------------------------------------------------------------------------
# Retry timing
# -------------------------------------------------------------------------

def test_retry_after_no_answer():
    """retry_after = ended_at + 60 min."""
    ended_at = datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    result = compute_retry_after(ended_at, "no_answer", retry_policy)
    expected = ended_at + timedelta(minutes=60)
    assert result == expected


def test_retry_after_busy():
    """retry_after = ended_at + 60 min for busy."""
    ended_at = datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    result = compute_retry_after(ended_at, "busy", retry_policy)
    expected = ended_at + timedelta(minutes=60)
    assert result == expected


def test_retry_after_failed():
    """retry_after = ended_at + 15 min for failed."""
    ended_at = datetime(2026, 11, 12, 15, 0, 0, tzinfo=IST)
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    result = compute_retry_after(ended_at, "failed", retry_policy)
    expected = ended_at + timedelta(minutes=15)
    assert result == expected
