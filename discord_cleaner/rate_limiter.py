from __future__ import annotations

import asyncio
import random
import time


class AdaptivePacer:
    """A minimum-delay gate plus bounded retry/cooldown calculations.

    discord.py-self's HTTP client remains the authority for Discord route/global
    rate limits. This gate only prevents this application from producing bursts.
    """

    def __init__(self, delay: float, max_backoff: float = 120.0):
        self.delay = delay
        self.max_backoff = max_backoff
        self.cooldown_until = 0.0
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            sleep_for = max(0.0, self._last + self.delay - now, self.cooldown_until - now)
            if sleep_for:
                await asyncio.sleep(sleep_for)
            self._last = time.monotonic()

    def backoff(self, attempt: int, retry_after: float | None = None) -> float:
        base = retry_after if retry_after is not None else min(self.max_backoff, 2 ** max(0, attempt - 1))
        return min(self.max_backoff, max(0.0, base) + random.uniform(0, min(1.0, base * 0.2)))

    def impose_cooldown(self, seconds: float) -> None:
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + min(self.max_backoff, seconds))

    @property
    def cooldown_remaining(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())
