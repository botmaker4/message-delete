from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import logging
import signal
import sys

from rich.console import Console
from rich.table import Table

from discord_cleaner.config import Settings
from discord_cleaner.dashboard import Dashboard
from discord_cleaner.deletion_worker import AuthenticationStopped, DeletionRunner
from discord_cleaner.package_parser import DataPackage, PackageError
from discord_cleaner.progress_db import ProgressDB

console = Console()


def csv_ints(value: str) -> set[int]:
    try: return {int(part.strip()) for part in value.split(",") if part.strip()}
    except ValueError as exc: raise argparse.ArgumentTypeError("must be comma-separated numeric IDs") from exc


def csv_types(value: str) -> set[str]:
    return {part.strip().upper() for part in value.split(",") if part.strip()}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Delete your own messages listed in a Discord Data Package")
    action = p.add_mutually_exclusive_group()
    action.add_argument("--scan", action="store_true", help="import package records into SQLite (never deletes)")
    action.add_argument("--stats", action="store_true", help="show saved progress")
    action.add_argument("--retry-failed", action="store_true", help="return failed entries to pending")
    p.add_argument("--resume", action="store_true", help="document intent to continue pending work")
    p.add_argument("--channel-id", type=int)
    p.add_argument("--channel-type", type=csv_types, help="comma-separated package channel types")
    p.add_argument("--exclude-channels", type=csv_ints, default=set())
    p.add_argument("--exclude-guilds", type=csv_ints, default=set())
    p.add_argument("--env-file", default=".env")
    p.add_argument("--verbose", action="store_true")
    return p


def show_stats(db: ProgressDB) -> None:
    stats = db.stats(); table = Table(title="Saved progress")
    table.add_column("Status"); table.add_column("Count", justify="right")
    for key in ("total", "pending", "deleted", "already_missing", "inaccessible", "failed", "skipped", "remaining"):
        table.add_row(key.replace("_", " ").title(), f"{stats[key]:,}")
    console.print(table)


def verify_self_library() -> None:
    try: version = importlib.metadata.version("discord.py-self")
    except importlib.metadata.PackageNotFoundError as exc:
        raise RuntimeError("discord.py-self is not installed; run: pip install -r requirements.txt") from exc
    logging.getLogger(__name__).debug("discord.py-self %s", version)


async def execute(settings: Settings, db: ProgressDB, args: argparse.Namespace) -> None:
    verify_self_library()
    if not settings.token: raise RuntimeError("DISCORD_USER_TOKEN is required")
    if settings.expected_account_id is None: raise RuntimeError("EXPECTED_ACCOUNT_ID is required")
    items = db.select_work(channel_id=args.channel_id, channel_types=args.channel_type,
                           exclude_channels=args.exclude_channels, exclude_guilds=args.exclude_guilds)
    if not items:
        console.print("No pending messages match the filters."); return
    import discord  # imported only for an authenticated execution
    client = discord.Client()
    completed = False

    @client.event
    async def on_ready() -> None:
        nonlocal completed
        if completed: return
        completed = True
        if client.user is None or int(client.user.id) != settings.expected_account_id:
            console.print("Authenticated account does not match EXPECTED_ACCOUNT_ID. Stopping.")
            await client.close(); return
        runner = DeletionRunner(client, db, account_id=int(client.user.id), delay=settings.delete_delay,
                                max_retries=settings.max_retries, concurrency=settings.max_concurrent)
        label = f"{client.user} ({client.user.id})"
        dashboard = Dashboard(db, "EXECUTE", label, settings.delete_delay, runner.pacer, settings.max_concurrent)
        runner.dashboard = dashboard
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.add_signal_handler(sig, runner.stop)
            except (NotImplementedError, RuntimeError): pass
        try:
            with dashboard: await runner.run(items)
        except AuthenticationStopped as exc:
            console.print(f"Authentication stopped: {exc}")
        finally:
            await client.close()

    try:
        try:
            await client.start(settings.token)
        except discord.LoginFailure as exc:
            raise RuntimeError("Discord rejected the user token; no deletions were attempted") from exc
        except discord.HTTPException as exc:
            if getattr(exc, "status", None) in {401, 403}:
                raise RuntimeError("Discord authentication/session was rejected; processing stopped") from exc
            raise
    finally:
        if not client.is_closed(): await client.close()


def main() -> int:
    args = parser().parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try: settings = Settings.load(args.env_file)
    except ValueError as exc: console.print(f"[red]Configuration error:[/red] {exc}"); return 2
    try:
        with ProgressDB(settings.database_path) as db:
            if args.scan:
                with DataPackage(settings.messages_path) as package:
                    found, added = db.import_records(package.records())
                console.print(f"Scanned {found:,} unique records; added {added:,} new records."); show_stats(db); return 0
            if args.retry_failed:
                console.print(f"Returned {db.retry_failed():,} failed records to pending."); return 0
            if args.stats:
                show_stats(db); return 0
            # Normal mode: refresh the database from the package, then process all pending rows.
            with DataPackage(settings.messages_path) as package:
                found, added = db.import_records(package.records())
            console.print(f"Scanned {found:,} unique records; added {added:,} new records.")
            asyncio.run(execute(settings, db, args)); show_stats(db); return 0
    except (PackageError, OSError, RuntimeError) as exc:
        console.print(f"[red]Error:[/red] {exc}"); return 1
    except KeyboardInterrupt:
        console.print("Interrupted. Confirmed results are saved; run python main.py to continue."); return 130


if __name__ == "__main__": raise SystemExit(main())
