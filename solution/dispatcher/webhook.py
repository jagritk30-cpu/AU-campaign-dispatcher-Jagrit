"""
webhook.py – FastAPI webhook receiver on port 8200.

Verifies HMAC signatures, handles idempotency, and processes call results.
Responds 200 quickly (background processing after ack) to avoid redelivery.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import JSONResponse

from . import db as database

logger = logging.getLogger(__name__)


def make_webhook_app(
    db_path: str,
    webhook_secret: str,
    retry_no_answer_minutes: int,
    retry_busy_minutes: int,
    retry_failed_minutes: int,
    max_attempts_map: dict,  # campaign_id -> max_attempts
) -> FastAPI:
    app = FastAPI(title="Dispatcher Webhook Receiver")

    async def _process_webhook(payload: dict) -> None:
        """Background: process a validated, non-duplicate webhook."""
        call_sid = payload.get("call_sid", "")
        request_id = payload.get("request_id")
        campaign_id = payload.get("campaign_id", "")
        lead_id = payload.get("lead_id", "")
        phone = payload.get("phone", "")
        status = payload.get("status") or ""
        disposition = payload.get("disposition")
        callback_at_raw = payload.get("callback_at")
        ended_at_raw = payload.get("ended_at")
        processed_at = datetime.now(tz=timezone.utc).isoformat()
        ended_at = ended_at_raw or processed_at

        max_attempts = max_attempts_map.get(campaign_id, 3)

        processed = await database.process_call_outcome(
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
            retry_no_answer_minutes=retry_no_answer_minutes,
            retry_busy_minutes=retry_busy_minutes,
            retry_failed_minutes=retry_failed_minutes,
            max_attempts=max_attempts,
            processed_at=processed_at,
        )

        if processed:
            logger.info(
                "webhook processed: call_sid=%s campaign=%s lead=%s status=%s disposition=%s",
                call_sid,
                campaign_id,
                lead_id,
                status,
                disposition,
            )
        else:
            logger.debug(
                "webhook skipped (already processed or unknown): call_sid=%s campaign=%s lead=%s",
                call_sid,
                campaign_id,
                lead_id,
            )

    @app.post("/webhooks/telephony")
    async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
        body = await request.body()

        expected_sig = hmac.new(
            webhook_secret.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
        received_sig = request.headers.get("X-AU-Signature", "")
        if not hmac.compare_digest(expected_sig, received_sig):
            logger.warning("Invalid webhook signature received")
            return JSONResponse({"error": "invalid_signature"}, status_code=400)

        try:
            payload = json.loads(body)
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)

        call_sid = payload.get("call_sid", "")

        if await database.is_webhook_processed(db_path, call_sid):
            logger.debug("Duplicate webhook for call_sid=%s, ignoring", call_sid)
            return JSONResponse({"status": "already_processed"}, status_code=200)

        background_tasks.add_task(_process_webhook, payload)
        return JSONResponse({"status": "accepted"}, status_code=200)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/status")
    async def status():
        """Observability endpoint returning per-campaign lead counts and active calls."""
        return await database.get_system_status(db_path)

    return app
