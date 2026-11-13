"""
config.py – Load campaigns.json and lead CSVs.
Produces a fully-resolved config dict and a list of lead dicts.
"""
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class TenantConfig:
    tenant_id: str
    max_concurrent_calls: int


@dataclass
class CampaignWindow:
    start: str   # "HH:MM"
    end: str     # "HH:MM"
    days: List[str]  # e.g. ["mon","tue",...]


@dataclass
class CampaignConfig:
    campaign_id: str
    tenant_id: str
    leads_file: str
    window: CampaignWindow
    max_attempts: int


@dataclass
class AppConfig:
    sim_start: str
    timezone: str
    regulatory_window: Dict[str, str]
    window_grace_seconds: int
    retry_policy: Dict[str, int]
    webhook_secret: str
    tenants: Dict[str, TenantConfig]
    campaigns: Dict[str, CampaignConfig]
    # raw leads per campaign
    leads: Dict[str, List[Dict[str, str]]] = field(default_factory=dict)


def load_config(fixtures_path: str) -> AppConfig:
    """Load campaigns.json and all lead CSVs from the same directory."""
    with open(fixtures_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    base_dir = os.path.dirname(os.path.abspath(fixtures_path))

    tenants: Dict[str, TenantConfig] = {}
    for t in raw["tenants"]:
        tenants[t["tenant_id"]] = TenantConfig(
            tenant_id=t["tenant_id"],
            max_concurrent_calls=t["max_concurrent_calls"],
        )

    campaigns: Dict[str, CampaignConfig] = {}
    leads: Dict[str, List[Dict[str, str]]] = {}
    for c in raw["campaigns"]:
        win = c["window"]
        campaigns[c["campaign_id"]] = CampaignConfig(
            campaign_id=c["campaign_id"],
            tenant_id=c["tenant_id"],
            leads_file=c["leads_file"],
            window=CampaignWindow(
                start=win["start"],
                end=win["end"],
                days=win["days"],
            ),
            max_attempts=c["max_attempts"],
        )
        leads_path = os.path.join(base_dir, c["leads_file"])
        with open(leads_path, "r", encoding="utf-8") as lf:
            reader = csv.DictReader(lf)
            leads[c["campaign_id"]] = [
                {"lead_id": row["lead_id"], "phone": row["phone"]}
                for row in reader
            ]

    return AppConfig(
        sim_start=raw["sim_start"],
        timezone=raw.get("timezone", "Asia/Kolkata"),
        regulatory_window=raw["regulatory_window"],
        window_grace_seconds=raw.get("window_grace_seconds", 120),
        retry_policy=raw["retry_policy"],
        webhook_secret=raw["webhook_secret"],
        tenants=tenants,
        campaigns=campaigns,
        leads=leads,
    )
