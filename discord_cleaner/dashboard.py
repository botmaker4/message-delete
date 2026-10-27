from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table

from .metrics import PerformanceMetrics


@dataclass(slots=True)
class Dashboard:
    db: object
    mode: str
    account: str
    delay: float
    pacer: object
    concurrency: int = 1
    performance: PerformanceMetrics | None = None
    scope_channel_types: set[str] | None = None
    started: float = field(default_factory=time.monotonic)
    channel_id: int | None = None
    channel_type: str = "-"
    current_status: str = "Starting"
    deleted_start: int = 0
    pending_start: int = 0
    _live: Live | None = field(default=None, init=False)
    _console: Console = field(default_factory=Console, init=False)
    _last_plain_update: float = field(default=0.0, init=False)
    _last_render: float = field(default=0.0, init=False)
    _last_reconcile: float = field(default=0.0, init=False)
    _cached_stats: dict[str, int] = field(default_factory=dict, init=False)
    _perf_checkpoint: dict[str, int] = field(default_factory=dict, init=False)
    _report_task: asyncio.Task[None] | None = field(default=None, init=False)

    def __enter__(self) -> "Dashboard":
        self._cached_stats = self.db.stats(channel_types=self.scope_channel_types)
        self.deleted_start = self._cached_stats["deleted"]
        self.pending_start = self._cached_stats["pending"]
        if self.performance is None: self.performance = PerformanceMetrics()
        self._perf_checkpoint = self._performance_counts()
        if self._console.is_terminal:
            self._live = Live(self.render(), refresh_per_second=2, console=self._console)
            self._live.__enter__()
        else:
            self._print_plain()
            try: loop=asyncio.get_running_loop()
            except RuntimeError: loop=None
            if loop is not None: self._report_task=loop.create_task(self._report_periodically())
        return self

    def __exit__(self, *args: object) -> None:
        if self._live: self._live.__exit__(*args)
        if self._report_task:
            self._report_task.cancel()
            self._report_task.add_done_callback(_consume_task_result)
            self._print_plain()

    def update(self, *, item: object | None = None, status: str | None = None) -> None:
        if item:
            self.channel_id = item.channel_id; self.channel_type = item.channel_type
        if status: self.current_status = status
        now = time.monotonic()
        if self._live and now - self._last_render >= 0.5:
            self._last_render = now
            self._live.update(self.render())
        # Non-interactive output is emitted by an independent timer so long API
        # waits and cooldowns still produce useful Coolify logs.

    async def _report_periodically(self) -> None:
        while True:
            await asyncio.sleep(30)
            self._print_plain()

    def _metrics(self) -> tuple[dict[str, int], float, float]:
        now = time.monotonic()
        if not self._cached_stats or now - self._last_reconcile >= 5:
            self._cached_stats = self.db.stats(channel_types=self.scope_channel_types); self._last_reconcile = now
            self._perf_checkpoint = self._performance_counts()
        stats = dict(self._cached_stats)
        perf = self.performance or PerformanceMetrics()
        delta = {key: value - self._perf_checkpoint.get(key, 0) for key, value in self._performance_counts().items()}
        stats["deleted"] = self._cached_stats["deleted"] + delta["deleted"]
        stats["already_missing"] = self._cached_stats["already_missing"] + delta["already_missing"]
        stats["failed"] = self._cached_stats["failed"] + delta["failed"]
        stats["confirmed_inaccessible"] = self._cached_stats.get("confirmed_inaccessible", 0) + delta["confirmed_inaccessible"]
        stats["unverified_inaccessible"] = self._cached_stats.get("unverified_inaccessible", 0) + delta["unverified_inaccessible"]
        stats["inaccessible"] = stats["confirmed_inaccessible"] + stats["unverified_inaccessible"]
        stats["pending"] = max(0, self._cached_stats["pending"] - delta["classified"])
        stats["remaining"] = stats["pending"] + stats["failed"]
        elapsed = max(0.001, time.monotonic() - self.started)
        rate = perf.network_outcomes / elapsed
        eta = stats["pending"] / rate if rate > 0 else float("inf")
        return stats, elapsed, eta

    def _performance_counts(self) -> dict[str, int]:
        perf = self.performance or PerformanceMetrics()
        return {key: int(getattr(perf, key)) for key in (
            "deleted", "already_missing", "failed", "confirmed_inaccessible",
            "unverified_inaccessible", "classified",
        )}

    def _print_plain(self) -> None:
        stats, elapsed, eta = self._metrics()
        self._last_plain_update = time.monotonic()
        perf = self.performance or PerformanceMetrics()
        eta_text = _duration(eta) if eta != float("inf") else "unknown (insufficient data)"
        self._console.print(
            "PROGRESS "
            f"deleted={stats['deleted']:,} missing={stats['already_missing']:,} "
            f"inaccessible={stats['inaccessible']:,} failed={stats['failed']:,} "
            f"pending={stats['pending']:,} channel={self.channel_id or '-'} "
            f"requests/min={perf.requests_per_minute:.1f} deletes/min={perf.deletes_per_minute:.1f} "
            f"classified/min={perf.classifications_per_minute:.1f} "
            f"429={perf.http_429} 403={perf.http_403} 404={perf.http_404} "
            f"requests={perf.api_requests} cooldown={self.pacer.cooldown_remaining:.1f}s "
            f"elapsed={_duration(elapsed)} api_eta={eta_text}",
            highlight=False,
        )

    def render(self) -> Panel:
        stats, elapsed, eta_seconds = self._metrics()
        table = Table.grid(padding=(0, 2)); table.add_column(style="bold cyan"); table.add_column(justify="right")
        for label, key in (("Total Exported", "total"), ("Successfully Deleted", "deleted"),
                           ("Already Missing", "already_missing"), ("Confirmed Inaccessible", "confirmed_inaccessible"),
                           ("Unverified Inaccessible", "unverified_inaccessible"),
                           ("Failed", "failed"), ("Remaining", "remaining")):
            table.add_row(label, f"{stats[key]:,}")
        table.add_row("Current Channel", str(self.channel_id or "-")); table.add_row("Channel Type", self.channel_type)
        table.add_row("Channels Completed", f"{stats.get('channels_completed',0):,} / {stats.get('channels_total',0):,}")
        table.add_row("Active Channels", f"{stats.get('active_channels',0):,}")
        table.add_row("Current Status", self.current_status)
        perf = self.performance or PerformanceMetrics()
        table.add_row("Successful Deletes/min", f"{perf.deletes_per_minute:.2f}")
        table.add_row("API Requests/min", f"{perf.requests_per_minute:.2f}")
        table.add_row("Overall Classifications/min", f"{perf.classifications_per_minute:.2f}")
        table.add_row("HTTP 429 / 403 / 404", f"{perf.http_429} / {perf.http_403} / {perf.http_404}")
        table.add_row("Average Delete Latency", f"{perf.average_delete_ms:.1f} ms")
        table.add_row("Channel Resolve / Message Fetch", f"{perf.channel_resolve_seconds:.1f}s / {perf.message_fetch_seconds:.1f}s")
        table.add_row("API / App Wait", f"{perf.api_wait_seconds:.1f}s / {perf.application_wait_seconds:.1f}s")
        table.add_row("SQLite Time", f"{perf.sqlite_seconds:.2f}s")
        table.add_row("Current Delay", f"{self.delay:.2f}s")
        table.add_row("Rate Limit Cooldown", f"{self.pacer.cooldown_remaining:.1f}s")
        table.add_row("Elapsed Time", f"{elapsed:.0f}s")
        table.add_row("API-bound Estimate", _duration(eta_seconds) if eta_seconds != float("inf") else "Unreliable: insufficient data")
        progress = Progress(TextColumn("Overall"), BarColumn(), TextColumn("{task.completed:,}/{task.total:,}"))
        progress.add_task("overall", total=max(1, stats["total"]), completed=stats["total"] - stats["remaining"])
        channel_progress = Progress(TextColumn("Channel"), BarColumn(), TextColumn("{task.completed:,}/{task.total:,}"))
        done, total = self.db.channel_stats(self.channel_id) if self.channel_id else (0, 0)
        channel_progress.add_task("channel", total=max(1, total), completed=done)
        return Panel(Group(table, progress, channel_progress), title="DISCORD MESSAGE CLEANER", subtitle=f"{self.account} - {self.mode}")


def _duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days: return f"{days}d {hours:02d}h {minutes:02d}m"
    if hours: return f"{hours}h {minutes:02d}m {seconds:02d}s"
    return f"{minutes}m {seconds:02d}s"


def _consume_task_result(task: asyncio.Task[None]) -> None:
    try: task.result()
    except asyncio.CancelledError: pass
