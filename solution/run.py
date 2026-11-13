"""
run.py – Entrypoint: runs engine + webhook server together via asyncio.

Usage:
  python run.py [OPTIONS]

Options:
  --dispatcher-port  Port for webhook server (default: 8200)
  --provider-url     Provider base URL (default: http://127.0.0.1:8099)
  --db               SQLite DB path (default: dispatcher.db)
  --fixtures         Path to campaigns.json (default: ../assignment-b-workflow/fixtures/campaigns.json)
  --output-csv       Path for final_state.csv (default: final_state.csv)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import uvicorn

# Allow running from solution/ directory
sys.path.insert(0, os.path.dirname(__file__))

from dispatcher.clock import sync_clock
from dispatcher.config import load_config
from dispatcher.db import init_db, load_leads_idempotent
from dispatcher.engine import run_engine
from dispatcher.webhook import make_webhook_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agentic Universe Dispatcher")
    parser.add_argument(
        "--dispatcher-port",
        type=int,
        default=8200,
        help="Port for the webhook receiver (default: 8200)",
    )
    parser.add_argument(
        "--provider-url",
        default="http://127.0.0.1:8099",
        help="Mock telephony provider base URL",
    )
    parser.add_argument(
        "--db",
        default="dispatcher.db",
        help="SQLite database file path",
    )
    parser.add_argument(
        "--fixtures",
        default=os.path.join(
            os.path.dirname(__file__),
            "..",
            "assignment-b-workflow",
            "fixtures",
            "campaigns.json",
        ),
        help="Path to campaigns.json fixtures file",
    )
    parser.add_argument(
        "--output-csv",
        default="final_state.csv",
        help="Output path for final_state.csv",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    fixtures_path = os.path.abspath(args.fixtures)
    db_path = os.path.abspath(args.db)
    output_csv = os.path.abspath(args.output_csv)
    provider_url = args.provider_url.rstrip("/")
    dispatcher_port = args.dispatcher_port

    logger.info("Loading config from %s", fixtures_path)
    config = load_config(fixtures_path)

    logger.info("Initialising database at %s", db_path)
    await init_db(db_path)

    logger.info("Loading leads (idempotent)")
    await load_leads_idempotent(db_path, config.leads)

    logger.info("Syncing simulated clock from %s", provider_url)
    clock = await sync_clock(provider_url)
    logger.info(
        "Clock synced: sim_now=%s speed=%.1fx",
        clock.now().isoformat(),
        clock.speed,
    )

    callback_url = f"http://127.0.0.1:{dispatcher_port}/webhooks/telephony"
    max_attempts_map = {cid: c.max_attempts for cid, c in config.campaigns.items()}

    webhook_app = make_webhook_app(
        db_path=db_path,
        webhook_secret=config.webhook_secret,
        retry_no_answer_minutes=config.retry_policy["no_answer_minutes"],
        retry_busy_minutes=config.retry_policy.get(
            "busy_minutes", config.retry_policy["no_answer_minutes"]
        ),
        retry_failed_minutes=config.retry_policy["failed_minutes"],
        max_attempts_map=max_attempts_map,
    )

    uvicorn_config = uvicorn.Config(
        webhook_app,
        host="0.0.0.0",
        port=dispatcher_port,
        log_level="warning",
    )
    server = uvicorn.Server(uvicorn_config)

    logger.info("Starting webhook server on port %d", dispatcher_port)
    server_task = asyncio.create_task(server.serve())

    # Wait until uvicorn is listening before placing any calls
    while not server.started:
        if server_task.done():
            await server_task
            return
        await asyncio.sleep(0.05)

    logger.info("Starting engine loop")
    try:
        await run_engine(
            db_path=db_path,
            provider_url=provider_url,
            callback_url=callback_url,
            config=config,
            clock=clock,
            output_csv=output_csv,
        )
    finally:
        logger.info("Dispatcher finished. Stopping webhook server.")
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    asyncio.run(main())
