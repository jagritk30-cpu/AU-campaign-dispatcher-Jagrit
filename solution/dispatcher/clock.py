"""
clock.py – SimClock: call /v1/clock ONCE at startup, then compute locally.

sim_now = sim_epoch + (time.time() - real_epoch) * speed
"""
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Protocol

IST = timezone(timedelta(hours=5, minutes=30))


class ClockProtocol(Protocol):
    def now(self) -> datetime: ...


class SimClock:
    """Tracks simulated time computed locally from a single clock sync."""

    def __init__(self, sim_epoch: datetime, real_epoch: float, speed: float):
        self._sim_epoch = sim_epoch
        self._real_epoch = real_epoch
        self._speed = speed

    @property
    def speed(self) -> float:
        return self._speed

    def now(self) -> datetime:
        elapsed_real = time.time() - self._real_epoch
        elapsed_sim = elapsed_real * self._speed
        return self._sim_epoch + timedelta(seconds=elapsed_sim)

    def now_ist(self) -> datetime:
        return self.now().astimezone(IST)


class FakeClock:
    """Deterministic clock for tests – set time explicitly."""

    def __init__(self, t: datetime):
        self._t = t

    def now(self) -> datetime:
        return self._t

    def now_ist(self) -> datetime:
        return self._t.astimezone(IST)

    def set(self, t: datetime) -> None:
        self._t = t

    @property
    def speed(self) -> float:
        return 1.0


async def sync_clock(provider_url: str) -> SimClock:
    """Call /v1/clock once and return a SimClock."""
    import httpx

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{provider_url}/v1/clock")
        resp.raise_for_status()
        data = resp.json()

    sim_epoch = datetime.fromisoformat(data["sim_epoch"])
    real_epoch: float = data["real_epoch"]
    speed: float = float(data["speed"])
    return SimClock(sim_epoch=sim_epoch, real_epoch=real_epoch, speed=speed)
