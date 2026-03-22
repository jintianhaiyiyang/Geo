"""Async rate limiter with global + per-domain constraints and jitter."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Dict
from urllib.parse import urlsplit


@dataclass
class _Slot:
    lock: asyncio.Lock
    next_allowed: float = 0.0


class AsyncRateLimiter:
    def __init__(
        self,
        global_rps: float,
        per_domain_rps: float,
        jitter_ms_min: int = 50,
        jitter_ms_max: int = 180,
    ):
        self.global_rps = float(global_rps)
        self.per_domain_rps = float(per_domain_rps)
        self.jitter_ms_min = int(jitter_ms_min)
        self.jitter_ms_max = int(jitter_ms_max)

        self._global_slot = _Slot(lock=asyncio.Lock())
        self._domain_slots: Dict[str, _Slot] = {}
        self._domain_map_lock = asyncio.Lock()

        self._stats = {
            "acquires": 0,
            "global_wait_seconds": 0.0,
            "domain_wait_seconds": 0.0,
            "jitter_seconds": 0.0,
        }

    @staticmethod
    def _domain_key(url: str) -> str:
        return (urlsplit(url).netloc or "").lower() or "_unknown"

    async def _acquire_slot(self, slot: _Slot, rps: float) -> float:
        if rps <= 0:
            return 0.0

        interval = 1.0 / rps
        async with slot.lock:
            now = time.monotonic()
            wait_seconds = max(0.0, slot.next_allowed - now)
            slot.next_allowed = max(now, slot.next_allowed) + interval

        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        return wait_seconds

    async def _domain_slot(self, domain: str) -> _Slot:
        async with self._domain_map_lock:
            slot = self._domain_slots.get(domain)
            if slot is None:
                slot = _Slot(lock=asyncio.Lock())
                self._domain_slots[domain] = slot
            return slot

    async def acquire(self, url: str) -> None:
        self._stats["acquires"] += 1
        global_wait = await self._acquire_slot(self._global_slot, self.global_rps)
        self._stats["global_wait_seconds"] += global_wait

        domain = self._domain_key(url)
        slot = await self._domain_slot(domain)
        domain_wait = await self._acquire_slot(slot, self.per_domain_rps)
        self._stats["domain_wait_seconds"] += domain_wait

        low = min(self.jitter_ms_min, self.jitter_ms_max)
        high = max(self.jitter_ms_min, self.jitter_ms_max)
        if high > 0:
            delay = random.uniform(max(0, low), high) / 1000.0
            self._stats["jitter_seconds"] += delay
            await asyncio.sleep(delay)

    def stats(self) -> Dict[str, float]:
        return {
            "acquires": int(self._stats["acquires"]),
            "global_wait_seconds": round(float(self._stats["global_wait_seconds"]), 6),
            "domain_wait_seconds": round(float(self._stats["domain_wait_seconds"]), 6),
            "jitter_seconds": round(float(self._stats["jitter_seconds"]), 6),
            "global_rps": self.global_rps,
            "per_domain_rps": self.per_domain_rps,
            "jitter_ms_min": self.jitter_ms_min,
            "jitter_ms_max": self.jitter_ms_max,
        }
