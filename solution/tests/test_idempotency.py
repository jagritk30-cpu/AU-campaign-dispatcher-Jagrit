"""
test_idempotency.py – 504 handling, 503 attempt counting, state transitions.

Mocks the provider HTTP client to test engine logic without network.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from dispatcher import db as database
from dispatcher.config import AppConfig, CampaignConfig, CampaignWindow, TenantConfig
from dispatcher.engine import (
    apply_call_result,
    dial_lead,
    handle_504_background,
)

IST = timezone(timedelta(hours=5, minutes=30))
NOW_ISO = datetime(2026, 11, 12, 17, 30, 0, tzinfo=timezone.utc).isoformat()


def make_config() -> AppConfig:
    tenant = TenantConfig(tenant_id="acme_insurance", max_concurrent_calls=4)
    campaign = CampaignConfig(
        campaign_id="c_acme_renewals",
        tenant_id="acme_insurance",
        leads_file="",
        window=CampaignWindow(start="10:00", end="19:00", days=["mon", "tue", "wed", "thu", "fri", "sat"]),
        max_attempts=3,
    )
    return AppConfig(
        sim_start=NOW_ISO,
        timezone="Asia/Kolkata",
        regulatory_window={"start": "09:00", "end": "21:00"},
        window_grace_seconds=120,
        retry_policy={"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15},
        webhook_secret="secret",
        tenants={"acme_insurance": tenant},
        campaigns={"c_acme_renewals": campaign},
        leads={},
    )


def make_lead(attempts=0):
    return {
        "campaign_id": "c_acme_renewals",
        "lead_id": "ren_0001",
        "phone": "+919999999999",
        "state": "pending",
        "attempts": attempts,
        "last_disposition": None,
        "callback_at": None,
        "last_request_id": None,
        "last_call_sid": None,
    }


@pytest_asyncio.fixture
async def db_path(tmp_path):
    path = str(tmp_path / "test.db")
    await database.init_db(path)
    await database.load_leads_idempotent(
        path, {"c_acme_renewals": [{"lead_id": "ren_0001", "phone": "+919999999999"}]}
    )
    return path


@pytest.mark.asyncio
async def test_504_poll_found(db_path):
    """504 → poll → found as active → state transitions to active."""
    config = make_config()
    request_id = "c_acme_renewals-ren_0001-1-abcd1234"

    await database.set_lead_request_id(db_path, "c_acme_renewals", "ren_0001", request_id, "dialling")

    mock_call_data = {
        "call_sid": "CA_test_504",
        "request_id": request_id,
        "campaign_id": "c_acme_renewals",
        "lead_id": "ren_0001",
        "state": "active",
        "created_at": NOW_ISO,
    }

    with patch("dispatcher.engine.poll_by_request_id", new=AsyncMock(return_value=mock_call_data)):
        await handle_504_background(
            db_path=db_path,
            provider_url="http://localhost:8099",
            campaign_id="c_acme_renewals",
            lead_id="ren_0001",
            request_id=request_id,
            config=config,
        )

    lead_after = await database.get_lead(db_path, "c_acme_renewals", "ren_0001")
    assert lead_after["state"] == "active"
    assert lead_after["attempts"] == 1
    assert lead_after["last_call_sid"] == "CA_test_504"


@pytest.mark.asyncio
async def test_504_poll_not_found(db_path):
    """504 → poll → 404 → safe to reset to pending."""
    config = make_config()
    request_id = "c_acme_renewals-ren_0001-1-notfound0"

    await database.set_lead_request_id(db_path, "c_acme_renewals", "ren_0001", request_id, "dialling")

    with patch("dispatcher.engine.poll_by_request_id", new=AsyncMock(return_value=None)):
        await handle_504_background(
            db_path=db_path,
            provider_url="http://localhost:8099",
            campaign_id="c_acme_renewals",
            lead_id="ren_0001",
            request_id=request_id,
            config=config,
        )

    lead_after = await database.get_lead(db_path, "c_acme_renewals", "ren_0001")
    assert lead_after["state"] == "pending"
    assert lead_after["attempts"] == 0


@pytest.mark.asyncio
async def test_503_not_counted_as_attempt(db_path):
    """503 response → attempts unchanged."""
    config = make_config()
    lead = make_lead(attempts=0)

    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_response.text = '{"error": "service_unavailable"}'

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        await dial_lead(
            db_path=db_path,
            provider_url="http://localhost:8099",
            callback_url="http://localhost:8200/webhooks/telephony",
            lead=lead,
            campaign_id="c_acme_renewals",
            tenant_id="acme_insurance",
            config=config,
        )

    lead_after = await database.get_lead(db_path, "c_acme_renewals", "ren_0001")
    assert lead_after["attempts"] == 0
    assert lead_after["state"] == "pending"


@pytest.mark.asyncio
async def test_201_counts_as_attempt(db_path):
    """201 response → attempts incremented."""
    config = make_config()
    lead = make_lead(attempts=0)

    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.json = MagicMock(
        return_value={
            "call_sid": "CA_201_test",
            "state": "active",
            "request_id": "req_201",
            "campaign_id": "c_acme_renewals",
            "lead_id": "ren_0001",
            "created_at": NOW_ISO,
        }
    )

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        await dial_lead(
            db_path=db_path,
            provider_url="http://localhost:8099",
            callback_url="http://localhost:8200/webhooks/telephony",
            lead=lead,
            campaign_id="c_acme_renewals",
            tenant_id="acme_insurance",
            config=config,
        )

    lead_after = await database.get_lead(db_path, "c_acme_renewals", "ren_0001")
    assert lead_after["attempts"] == 1
    assert lead_after["state"] == "active"
    assert lead_after["last_call_sid"] == "CA_201_test"


@pytest.mark.asyncio
async def test_third_attempt_non_terminal_exhausts_immediately(db_path):
    """On attempt 3, even callback_requested transitions immediately to exhausted."""
    config = make_config()
    await database.update_lead_state(db_path, "c_acme_renewals", "ren_0001", "pending", attempts=2)
    req_id = "c_acme_renewals-ren_0001-3-final000"
    await database.record_call_accepted(
        db_path, "CA_3rd", req_id, "c_acme_renewals", "ren_0001", "+919999999999", "acme_insurance", NOW_ISO
    )

    await apply_call_result(
        db_path,
        {
            "call_sid": "CA_3rd",
            "request_id": req_id,
            "campaign_id": "c_acme_renewals",
            "lead_id": "ren_0001",
            "state": "ended",
            "result": {
                "call_sid": "CA_3rd",
                "request_id": req_id,
                "campaign_id": "c_acme_renewals",
                "lead_id": "ren_0001",
                "phone": "+919999999999",
                "status": "completed",
                "disposition": "callback_requested",
                "callback_at": NOW_ISO,
                "ended_at": NOW_ISO,
            },
        },
        config,
    )

    lead_after = await database.get_lead(db_path, "c_acme_renewals", "ren_0001")
    assert lead_after["attempts"] == 3
    assert lead_after["state"] == "exhausted"
