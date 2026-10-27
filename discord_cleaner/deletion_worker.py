from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any

from .metrics import PerformanceMetrics
from .models import ChannelWorkload, WorkItem
from .rate_limiter import AdaptivePacer

log = logging.getLogger(__name__)
DISCORD_OPERATION_ALLOWLIST = frozenset({"fetch_channel", "fetch_message", "delete"})
DELETE_CHANNEL_TYPE_ALLOWLIST = frozenset({"DM"})


class AuthenticationStopped(RuntimeError): pass


class DeletionRunner:
    """Bounded, channel-aware deletion scheduler.

    Direct mode uses discord.py-self partial objects and therefore issues one
    message-specific request on the successful path. Fetch mode performs an
    ownership GET before DELETE and is intended for uncertain provenance.
    """

    def __init__(self, client: Any, db: Any, *, account_id: int, delay: float,
                 max_retries: int, concurrency: int, dashboard: Any | None = None,
                 verification_mode: str = "fetch", batch_size: int = 100,
                 metrics: PerformanceMetrics | None = None):
        if verification_mode not in {"direct", "fetch"}: raise ValueError("verification_mode must be direct or fetch")
        self.client, self.db, self.account_id = client, db, account_id
        self.max_retries, self.dashboard = max_retries, dashboard
        self.pacer = AdaptivePacer(delay); self.concurrency = concurrency
        self.verification_mode = verification_mode; self.batch_size = batch_size
        self.metrics = metrics or PerformanceMetrics()
        self._channels: dict[int, Any] = {}; self._partial_channels: dict[int, Any] = {}
        self._blocked_channels: set[int] = set(); self._stop = asyncio.Event()

    def stop(self) -> None: self._stop.set()

    async def run(self, items: list[WorkItem]) -> None:
        """Compatibility entry point used by focused tests; memory is caller-owned."""
        grouped: dict[int, list[WorkItem]] = {}
        for item in items: grouped.setdefault(item.channel_id, []).append(item)
        workloads = deque((channel_id, rows) for channel_id, rows in grouped.items())
        while workloads and not self._stop.is_set():
            group = [workloads.popleft() for _ in range(min(self.concurrency, len(workloads)))]
            await asyncio.gather(*(self._process_rows(rows) for _, rows in group))
        self.db.flush() if hasattr(self.db, "flush") else None

    async def run_scheduled(self, *, channel_id: int | None = None, channel_types: set[str] | None = None,
                            exclude_channels: set[int] | None = None, exclude_guilds: set[int] | None = None) -> None:
        workloads = deque(self.db.channel_workloads(channel_id=channel_id, channel_types=channel_types,
                                                     exclude_channels=exclude_channels, exclude_guilds=exclude_guilds))
        while workloads and not self._stop.is_set():
            current: list[ChannelWorkload] = [workloads.popleft() for _ in range(min(self.concurrency, len(workloads)))]
            async def process(workload: ChannelWorkload) -> None:
                rows = self.db.pending_for_channel(workload.channel_id, self.batch_size)
                await self._process_rows(rows)
                if workload.channel_id not in self._blocked_channels and len(rows) == self.batch_size and not self._stop.is_set():
                    workloads.append(workload)
            await asyncio.gather(*(process(workload) for workload in current))
            self.db.flush()

    async def _process_rows(self, rows: list[WorkItem]) -> None:
        for item in rows:
            if self._stop.is_set() or item.channel_id in self._blocked_channels: return
            try: await self._process(item)
            except AuthenticationStopped: self._stop.set(); return

    async def _resolve_channel(self, item: WorkItem) -> Any:
        if item.channel_id in self._channels: return self._channels[item.channel_id]
        started=time.monotonic()
        channel=self.client.get_channel(item.channel_id)
        if channel is None:
            self.metrics.api_requests += 1
            try: channel=await self.client.fetch_channel(item.channel_id)
            finally: self.metrics.channel_resolve_seconds += time.monotonic()-started
        self._channels[item.channel_id]=channel; return channel

    def _partial_message(self, item: WorkItem) -> Any:
        channel=self._partial_channels.get(item.channel_id)
        if channel is None:
            channel=self.client.get_partial_messageable(item.channel_id,guild_id=item.guild_id)
            self._partial_channels[item.channel_id]=channel
        return channel.get_partial_message(item.message_id)

    async def _process(self, item: WorkItem) -> None:
        if item.channel_type not in DELETE_CHANNEL_TYPE_ALLOWLIST:
            log.error("Safety policy refused deletion from non-DM channel %s (%s)",item.channel_id,item.channel_type)
            return
        if self.dashboard: self.dashboard.update(item=item,status="Deleting")
        for attempt in range(1,self.max_retries+1):
            if self._stop.is_set(): return
            before=time.monotonic(); await self.pacer.wait(); self.metrics.application_wait_seconds += time.monotonic()-before
            stage="direct_delete" if self.verification_mode=="direct" else "channel_resolve"
            try:
                if self.verification_mode=="fetch":
                    channel=await self._resolve_channel(item); stage="message_fetch"
                    started=time.monotonic(); self.metrics.api_requests+=1
                    try: message=await channel.fetch_message(item.message_id)
                    finally: self.metrics.message_fetch_seconds += time.monotonic()-started
                    if int(message.author.id)!=self.account_id:
                        self._record(item,"skipped","author does not match authenticated account",stage=stage,source="ownership_mismatch")
                        return
                else:
                    message=self._partial_message(item)
                stage="delete"; started=time.monotonic(); self.metrics.api_requests+=1
                try: await message.delete()
                finally:
                    self.metrics.message_delete_seconds += time.monotonic()-started; self.metrics.delete_samples+=1
                self._record(item,"deleted",stage=stage); self.metrics.deleted+=1; self.metrics.classified+=1; self.metrics.network_outcomes+=1
                return
            except asyncio.CancelledError: raise
            except Exception as exc:
                status=getattr(exc,"status",None); code=getattr(exc,"code",None)
                self.metrics.http_status(status); text=f"{type(exc).__name__}: {exc}"[:1000]
                if status==404:
                    self._record(item,"already_missing",text,http_status=status,discord_code=code,stage=stage)
                    self.metrics.already_missing+=1; self.metrics.classified+=1; self.metrics.network_outcomes+=1; return
                if status==403 or code in {50001,50013}:
                    if stage in {"direct_delete","delete","message_fetch","channel_resolve"}:
                        await self._investigate_forbidden(item,text,status,code,stage)
                    return
                if status==401 or code in {40001,40002}:
                    self._record(item,"failed",text,http_status=status,discord_code=code,stage=stage)
                    self.metrics.failed+=1; self.stop(); raise AuthenticationStopped(text)
                transient=status==429 or (isinstance(status,int) and status>=500) or isinstance(exc,(OSError,asyncio.TimeoutError))
                if transient and attempt<self.max_retries:
                    retry_after=getattr(exc,"retry_after",None)
                    wait=self.pacer.backoff(attempt,float(retry_after) if retry_after is not None else None)
                    if status==429: self.pacer.impose_cooldown(wait)
                    started=time.monotonic(); await asyncio.sleep(wait); self.metrics.api_wait_seconds+=time.monotonic()-started
                    continue
                self._record(item,"failed",text,http_status=status,discord_code=code,stage=stage)
                self.metrics.failed+=1; self.metrics.classified+=1; self.metrics.network_outcomes+=1; return

    async def _investigate_forbidden(self,item:WorkItem,error:str,http_status:int|None,code:int|None,stage:str)->None:
        """Use one channel-level probe; never infer a channel from one message failure."""
        try:
            started=time.monotonic(); self.metrics.api_requests+=1
            try: await self.client.fetch_channel(item.channel_id)
            finally: self.metrics.channel_resolve_seconds += time.monotonic()-started
        except Exception as probe:
            probe_status=getattr(probe,"status",None); probe_code=getattr(probe,"code",None)
            self.metrics.http_status(probe_status)
            reason=f"message {stage} failed ({error}); channel probe: {type(probe).__name__}: {probe}"[:1000]
            if probe_status==404:
                affected=self.db.mark_channel(item,"deleted_channel","inaccessible",reason,http_status=404,discord_code=probe_code,source="channel_confirmed")
                self._blocked_channels.add(item.channel_id); self.metrics.confirmed_inaccessible+=affected; self.metrics.classified+=affected; self.metrics.network_outcomes+=1; return
            if probe_status==403 or probe_code in {50001,50013}:
                affected=self.db.mark_channel(item,"inaccessible_confirmed","inaccessible",reason,http_status=403,discord_code=probe_code,source="channel_confirmed")
                self._blocked_channels.add(item.channel_id); self.metrics.confirmed_inaccessible+=affected; self.metrics.classified+=affected; self.metrics.network_outcomes+=1; return
            self.db.update_channel_probe(item.channel_id,"temporarily_unavailable",http_status=probe_status,discord_code=probe_code,reason=reason)
            self._record(item,"failed",reason,http_status=probe_status,discord_code=probe_code,stage="channel_probe",source="temporary_probe_failure")
            self.metrics.failed+=1; self.metrics.classified+=1; self.metrics.network_outcomes+=1; return
        self.db.update_channel_probe(item.channel_id,"needs_recheck",http_status=http_status,discord_code=code,reason=error)
        self._record(item,"inaccessible",error,http_status=http_status,discord_code=code,stage=stage,source="message_forbidden_unverified")
        self.metrics.unverified_inaccessible+=1; self.metrics.classified+=1; self.metrics.network_outcomes+=1

    def _record(self,item:WorkItem,status:str,error:str|None=None,**evidence:Any)->None:
        started=time.monotonic(); self.db.record(item,status,error,**evidence)
        self.metrics.sqlite_seconds += time.monotonic()-started
