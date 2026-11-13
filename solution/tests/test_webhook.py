"""
test_webhook.py – HMAC verification, duplicate handling, DND side-effects, race safety.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from dispatcher import db as database
from dispatcher.webhook import make_webhook_app

IST = timezone(timedelta(hours=5, minutes=30))
WEBHOOK_SECRET = "whsec_test"
NOW_ISO = datetime(2026, 11, 12, 17, 30, 0, tzinfo=timezone.utc).isoformat()


@pytest_asyncio.fixture
async def db_path(tmp_path):
    path = str(tmp_path / "test.db")
    await database.init_db(path)
    return path


@pytest.fixture
def app(db_path):
    return make_webhook_app(
        db_path=db_path,
        webhook_secret=WEBHOOK_SECRET,
        retry_no_answer_minutes=60,
        retry_busy_minutes=60,
        retry_failed_minutes=15,
        max_attempts_map={"c_acme_renewals": 3, "c_acme_feedback": 3},
    )


def sign_body(body: bytes) -> str:
    return hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()


def make_payload(
    call_sid: str = "CA_test_001",
    request_id: str = "req_001",
    campaign_id: str = "c_acme_renewals",
    lead_id: str = "ren_0001",
    phone: str = "+919999999999",
    status: str = "completed",
    disposition: str | None = "interested",
) -> dict:
    return {
        "event": "call.completed",
        "call_sid": call_sid,
        "request_id": request_id,
        "tenant_id": "acme_insurance",
        "campaign_id": campaign_id,
        "lead_id": lead_id,
        "phone": phone,
        "status": status,
        "disposition": disposition,
        "callback_at": None,
        "started_at": NOW_ISO,
        "ended_at": NOW_ISO,
        "duration_sec": 120,
    }


async def _seed_lead(db_path: str, campaign_id: str, lead_id: str, phone: str) -> None:
    """Insert a lead + active_call so webhook processing succeeds."""
    await database.load_leads_idempotent(
        db_path, {campaign_id: [{"lead_id": lead_id, "phone": phone}]}
    )
    await database.record_call_accepted(
        db_path=db_path,
        call_sid="CA_test_001",
        request_id="req_001",
        campaign_id=campaign_id,
        lead_id=lead_id,
        phone=phone,
        tenant_id="acme_insurance",
        started_at=NOW_ISO,
    )


@pytest.mark.asyncio
async def test_valid_hmac_accepted(app, db_path):
    """Correct signature → 200."""
    await _seed_lead(db_path, "c_acme_renewals", "ren_0001", "+919999999999")
    payload = make_payload()
    body = json.dumps(payload).encode()
    sig = sign_body(body)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_invalid_hmac_rejected(app, db_path):
    """Wrong signature → 400."""
    payload = make_payload()
    body = json.dumps(payload).encode()
    bad_sig = "deadbeef" * 8

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": bad_sig},
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_duplicate_webhook_idempotent(app, db_path):
    """Same call_sid processed twice → second is no-op (200)."""
    await _seed_lead(db_path, "c_acme_renewals", "ren_0001", "+919999999999")
    payload = make_payload()
    body = json.dumps(payload).encode()
    sig = sign_body(body)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r1 = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
        await asyncio.sleep(0.1)

        r2 = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r2.json().get("status") == "already_processed"


@pytest.mark.asyncio
async def test_dnd_blocks_other_leads_with_exhausted_precedence(app, db_path):
    """DND blocks pending leads with same phone, but does not override exhausted leads."""
    phone = "+919111111111"
    await database.load_leads_idempotent(
        db_path,
        {
            "c_acme_renewals": [
                {"lead_id": "ren_dnd1", "phone": phone},
                {"lead_id": "ren_exh", "phone": phone},
            ],
            "c_acme_feedback": [{"lead_id": "fb_dnd1", "phone": phone}],
        },
    )
    # Mark ren_exh as exhausted already
    await database.update_lead_state(db_path, "c_acme_renewals", "ren_exh", "exhausted", attempts=3)
    # Seed ren_dnd1 as active call
    await database.record_call_accepted(
        db_path, "CA_dnd_001", "req_dnd_1", "c_acme_renewals", "ren_dnd1", phone, "acme_insurance", NOW_ISO
    )

    payload = make_payload(
        call_sid="CA_dnd_001",
        request_id="req_dnd_1",
        campaign_id="c_acme_renewals",
        lead_id="ren_dnd1",
        phone=phone,
        status="completed",
        disposition="dnd",
    )
    body = json.dumps(payload).encode()
    sig = sign_body(body)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
    assert resp.status_code == 200
    await asyncio.sleep(0.1)

    lead1 = await database.get_lead(db_path, "c_acme_renewals", "ren_dnd1")
    lead2 = await database.get_lead(db_path, "c_acme_feedback", "fb_dnd1")
    lead_exh = await database.get_lead(db_path, "c_acme_renewals", "ren_exh")
    assert lead1["state"] == "completed"
    assert lead2["state"] == "dnd_blocked"
    assert lead_exh["state"] == "exhausted"


@pytest.mark.asyncio
async def test_terminal_disposition_completes_lead(app, db_path):
    """interested disposition → state=completed."""
    await _seed_lead(db_path, "c_acme_renewals", "ren_0001", "+919999999999")
    payload = make_payload(status="completed", disposition="interested")
    body = json.dumps(payload).encode()
    sig = sign_body(body)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
    assert resp.status_code == 200
    await asyncio.sleep(0.1)

    lead = await database.get_lead(db_path, "c_acme_renewals", "ren_0001")
    assert lead["state"] == "completed"
    assert lead["last_disposition"] == "interested"


@pytest.mark.asyncio
async def test_webhook_before_201_response(app, db_path):
    """
    Webhook arriving while lead is still in 'dialling' (before 201 is read)
    must be processed cleanly and never reverted by a subsequent record_call_accepted.
    """
    await database.load_leads_idempotent(
        db_path, {"c_acme_renewals": [{"lead_id": "ren_early", "phone": "+919888888888"}]}
    )
    req_id = "c_acme_renewals-ren_early-1-abcd1234"
    await database.set_lead_request_id(db_path, "c_acme_renewals", "ren_early", req_id, "dialling")

    payload = make_payload(
        call_sid="CA_early_001",
        request_id=req_id,
        campaign_id="c_acme_renewals",
        lead_id="ren_early",
        phone="+919888888888",
        status="completed",
        disposition="interested",
    )
    body = json.dumps(payload).encode()
    sig = sign_body(body)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/telephony",
            content=body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
    assert resp.status_code == 200
    await asyncio.sleep(0.1)

    # Simulate delayed 201 arriving AFTER webhook
    activated = await database.record_call_accepted(
        db_path=db_path,
        call_sid="CA_early_001",
        request_id=req_id,
        campaign_id="c_acme_renewals",
        lead_id="ren_early",
        phone="+919888888888",
        tenant_id="acme_insurance",
        started_at=NOW_ISO,
    )
    assert activated is False

    lead = await database.get_lead(db_path, "c_acme_renewals", "ren_early")
    assert lead["state"] == "completed"
    assert lead["attempts"] == 1
    assert await database.get_active_calls(db_path) == []


@pytest.mark.asyncio
async def test_status_endpoint(app, db_path):
    """GET /status returns campaign breakdown and active calls count."""
    await _seed_lead(db_path, "c_acme_renewals", "ren_0001", "+919999999999")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "campaigns" in data
    assert "c_acme_renewals" in data["campaigns"]
    assert data["active_calls_count"] == 1
