from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from .models import WorkItem
from .rate_limiter import AdaptivePacer

log = logging.getLogger(__name__)


class AuthenticationStopped(RuntimeError): pass


class DeletionRunner:
    def __init__(self, client: Any, db: Any, *, account_id: int, delay: float,
                 max_retries: int, concurrency: int, dashboard: Any | None = None):
        self.client, self.db, self.account_id = client, db, account_id
        self.max_retries, self.dashboard = max_retries, dashboard
        self.pacer = AdaptivePacer(delay)
        self.concurrency = concurrency
        self._channels: dict[int, Any] = {}
        self._inaccessible_channels: set[int] = set()
        self._channel_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._stop = asyncio.Event()

    def stop(self) -> None: self._stop.set()

    async def run(self, items: list[WorkItem]) -> None:
        queue: asyncio.Queue[WorkItem] = asyncio.Queue(maxsize=max(1, self.concurrency * 2))
        async def producer() -> None:
            for item in items:
                if self._stop.is_set(): break
                await queue.put(item)
            for _ in range(self.concurrency): await queue.put(None)  # type: ignore[arg-type]
        async def consumer() -> None:
            while True:
                item = await queue.get()
                try:
                    if item is None: return
                    if not self._stop.is_set():
                        try: await self._process(item)
                        except AuthenticationStopped: self._stop.set()
                finally: queue.task_done()
        tasks = [asyncio.create_task(consumer()) for _ in range(self.concurrency)]
        producer_task = asyncio.create_task(producer())
        try:
            await asyncio.gather(producer_task, *tasks)
        finally:
            self._stop.set()
            for task in [producer_task, *tasks]: task.cancel()
            await asyncio.gather(producer_task, *tasks, return_exceptions=True)

    async def _resolve_channel(self, channel_id: int) -> Any:
        if channel_id in self._channels: return self._channels[channel_id]
        channel = self.client.get_channel(channel_id)
        if channel is None: channel = await self.client.fetch_channel(channel_id)
        self._channels[channel_id] = channel
        return channel

    async def _process(self, item: WorkItem) -> None:
        if self.dashboard: self.dashboard.update(item=item, status="Deleting")
        # Serialize a channel even when limited cross-channel concurrency is enabled.
        async with self._channel_locks[item.channel_id]:
            if item.channel_id in self._inaccessible_channels:
                return
            for local_attempt in range(1, self.max_retries + 1):
                if self._stop.is_set(): return
                await self.pacer.wait()
                stage = "channel"
                try:
                    channel = await self._resolve_channel(item.channel_id)
                    stage = "message"
                    message = await channel.fetch_message(item.message_id)
                    if int(message.author.id) != self.account_id:
                        self.db.record(item, "skipped", "author does not match authenticated account")
                        return
                    stage = "delete"
                    await message.delete()
                    self.db.record(item, "deleted")
                    return
                except asyncio.CancelledError: raise
                except Exception as exc:
                    status = getattr(exc, "status", None)
                    code = getattr(exc, "code", None)
                    text = f"{type(exc).__name__}: {exc}"[:1000]
                    if status == 404:
                        if stage == "channel":
                            self._mark_channel_inaccessible(item, text)
                        else:
                            self.db.record(item, "already_missing", text)
                        return
                    if status == 403 or code in {50001, 50013}:
                        if stage in {"channel", "message"}:
                            self._mark_channel_inaccessible(item, text)
                        else:
                            self.db.record(item, "inaccessible", text)
                        return
                    if status in {401} or code in {40001, 40002}:
                        self.db.record(item, "failed", text); self.stop(); raise AuthenticationStopped(text)
                    transient = status == 429 or (isinstance(status, int) and status >= 500) or isinstance(exc, (OSError, asyncio.TimeoutError))
                    if transient and local_attempt < self.max_retries:
                        retry_after = getattr(exc, "retry_after", None)
                        wait = self.pacer.backoff(local_attempt, float(retry_after) if retry_after else None)
                        if status == 429: self.pacer.impose_cooldown(wait * min(3, local_attempt))
                        await asyncio.sleep(wait); continue
                    self.db.record(item, "failed", text); return

    def _mark_channel_inaccessible(self, item: WorkItem, error: str) -> None:
        self._inaccessible_channels.add(item.channel_id)
        affected = self.db.mark_channel_inaccessible(item, error)
        log.info("Marked %s pending messages in channel %s inaccessible after one API denial",
                 affected, item.channel_id)
