from __future__ import annotations

import time
from dataclasses import dataclass, field

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table


@dataclass(slots=True)
class Dashboard:
    db: object
    mode: str
    account: str
    delay: float
    pacer: object
    concurrency: int = 1
    started: float = field(default_factory=time.monotonic)
    channel_id: int | None = None
    channel_type: str = "-"
    current_status: str = "Starting"
    deleted_start: int = 0
    pending_start: int = 0
    _live: Live | None = field(default=None, init=False)
    _console: Console = field(default_factory=Console, init=False)
    _last_plain_update: float = field(default=0.0, init=False)

    def __enter__(self) -> "Dashboard":
        self.deleted_start = self.db.stats()["deleted"]
        self.pending_start = self.db.stats()["pending"]
        if self._console.is_terminal:
            self._live = Live(self.render(), refresh_per_second=2, console=self._console)
            self._live.__enter__()
        else:
            self._print_plain()
        return self

    def __exit__(self, *args: object) -> None:
        if self._live: self._live.__exit__(*args)

    def update(self, *, item: object | None = None, status: str | None = None) -> None:
        if item:
            self.channel_id = item.channel_id; self.channel_type = item.channel_type
        if status: self.current_status = status
        if self._live:
            self._live.update(self.render())
        elif time.monotonic() - self._last_plain_update >= 15:
            self._print_plain()

    def _metrics(self) -> tuple[dict[str, int], float, float]:
        stats = self.db.stats()
        elapsed = max(0.001, time.monotonic() - self.started)
        processed = max(0, self.pending_start - stats["pending"])
        rate = processed / elapsed
        eta = stats["pending"] / rate if rate > 0 else stats["pending"] * self.delay / max(1, self.concurrency)
        return stats, elapsed, eta

    def _print_plain(self) -> None:
        stats, elapsed, eta = self._metrics()
        self._last_plain_update = time.monotonic()
        self._console.print(
            "PROGRESS "
            f"deleted={stats['deleted']:,} missing={stats['already_missing']:,} "
            f"inaccessible={stats['inaccessible']:,} failed={stats['failed']:,} "
            f"pending={stats['pending']:,} channel={self.channel_id or '-'} "
            f"elapsed={_duration(elapsed)} eta={_duration(eta)}",
            highlight=False,
        )

    def render(self) -> Panel:
        stats, elapsed, eta_seconds = self._metrics()
        table = Table.grid(padding=(0, 2)); table.add_column(style="bold cyan"); table.add_column(justify="right")
        for label, key in (("Total Exported", "total"), ("Deleted", "deleted"),
                           ("Already Missing", "already_missing"), ("Inaccessible", "inaccessible"),
                           ("Failed", "failed"), ("Remaining", "remaining")):
            table.add_row(label, f"{stats[key]:,}")
        table.add_row("Current Channel", str(self.channel_id or "-")); table.add_row("Channel Type", self.channel_type)
        table.add_row("Current Status", self.current_status)
        table.add_row("Successful Deletes/min", f"{(stats['deleted']-self.deleted_start)*60/elapsed:.2f}")
        table.add_row("Current Delay", f"{self.delay:.2f}s")
        table.add_row("Rate Limit Cooldown", f"{self.pacer.cooldown_remaining:.1f}s")
        table.add_row("Elapsed Time", f"{elapsed:.0f}s")
        table.add_row("Estimated Time Remaining", _duration(eta_seconds))
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
