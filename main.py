from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import logging
import signal
import sqlite3
import sys

from rich.console import Console
from rich.table import Table

from discord_cleaner.config import Settings
from discord_cleaner.dashboard import Dashboard
from discord_cleaner.deletion_worker import AuthenticationStopped, DeletionRunner
from discord_cleaner.metrics import PerformanceMetrics
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
    action.add_argument("--diagnose", action="store_true", help="analyze classifications and historical evidence")
    action.add_argument("--audit-inaccessible", action="store_true", help="report inaccessible channels without network calls")
    action.add_argument("--recheck-inaccessible", action="store_true", help="probe inaccessible channels once; does not retry messages")
    action.add_argument("--authorize-inaccessible-retry", action="store_true", help="reset historically inaccessible rows in channels proven accessible")
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
    for key in ("total", "pending", "deleted", "already_missing", "confirmed_inaccessible", "unverified_inaccessible", "failed", "skipped", "remaining"):
        table.add_row(key.replace("_", " ").title(), f"{stats[key]:,}")
    console.print(table)


def show_audit(db: ProgressDB) -> None:
    rows=db.audit_inaccessible(); table=Table(title="Inaccessible channel audit")
    for column in ("Channel", "Type", "Channel status", "Messages", "Evidence", "HTTP", "Code", "Stage"): table.add_column(column)
    for row in rows[:50]:
        table.add_row(str(row["channel_id"]),row["channel_type"],row["accessibility_status"],f"{row['message_count']:,}",
                      str(row["evidence"] or "historical_unverified"),str(row["last_http_status"] or "-"),
                      str(row["last_discord_code"] or "-"),str(row["last_stage"] or "-"))
    console.print(table); console.print(f"Distinct inaccessible channels: {len(rows):,}. Showing top {min(50,len(rows)):,}.")


def show_diagnosis(db: ProgressDB) -> None:
    report=db.diagnosis(); totals=report["totals"]; total=max(1,int(totals["total"] or 0))
    table=Table(title="Messages by channel type");
    for column in ("Type","Total","Pending","Inaccessible"): table.add_column(column)
    for row in report["by_type"]: table.add_row(row["channel_type"],f"{row['total']:,}",f"{row['pending'] or 0:,}",f"{row['inaccessible'] or 0:,}")
    console.print(table)
    console.print(f"Distinct inaccessible channels: {totals['distinct_inaccessible_channels']:,}")
    console.print(f"Network-attempted messages: {int(totals['attempted'] or 0):,} ({int(totals['attempted'] or 0)*100/total:.2f}%)")
    console.print(f"Channel-inferred inaccessible: {int(totals['inferred'] or 0):,} ({int(totals['inferred'] or 0)*100/total:.2f}%)")
    console.print(f"Successful deletion rate: {int(totals['deleted'] or 0)*100/total:.2f}%")
    errors=Table(title="HTTP/code/stage distribution");
    for column in ("HTTP","Discord code","Stage","Count"): errors.add_column(column)
    for row in report["errors"][:30]: errors.add_row(str(row["http_status"]),str(row["discord_code"]),row["stage"],f"{row['count']:,}")
    console.print(errors)
    top=Table(title="Top inaccessible channels");
    for column in ("Channel","Count","Evidence"): top.add_column(column)
    for row in report["top"]: top.add_row(str(row["channel_id"]),f"{row['count']:,}",str(row["source"] or "historical_unverified"))
    console.print(top)


def show_startup_report(db: ProgressDB,settings: Settings)->None:
    metrics=db.startup_metrics(); table=Table(title="Startup database report")
    table.add_column("Metric"); table.add_column("Value",justify="right")
    rows=(
        ("Total exported",metrics["total"]),("Successfully deleted",metrics["deleted"]),
        ("Pending",metrics["pending"]),("Confirmed inaccessible",metrics["confirmed_inaccessible"]),
        ("Historically unverified inaccessible",metrics["unverified_inaccessible"]),
        ("Already missing",metrics["already_missing"]),("Failed",metrics["failed"]),
        ("Channels remaining",metrics["active_channels"]),
        ("Current run deletes/min","0.00 (not started)"),
        ("Recent deletes/min (5m)",f"{metrics['recent_5m_per_min']:.2f}"),
        ("Recent deletes/min (30m)",f"{metrics['recent_30m_per_min']:.2f}"),
        ("Recorded terminal HTTP 429 rows",metrics["recorded_429"]),
        ("Application minimum delay",f"{settings.delete_delay:.2f}s"),
        ("Concurrency",settings.max_concurrent),
    )
    for label,value in rows: table.add_row(label,f"{value:,}" if isinstance(value,int) else str(value))
    console.print(table)
    console.print("Rate limits: discord.py-self route/global buckets remain authoritative; surfaced 429 Retry-After values are honored.")


def selected_mode(args: argparse.Namespace,settings: Settings)->str:
    for attribute,mode in (
        ("scan","scan"),("retry_failed","retry_failed"),("stats","stats"),
        ("diagnose","diagnose"),("audit_inaccessible","audit"),
        ("recheck_inaccessible","recheck"),("authorize_inaccessible_retry","authorize"),
    ):
        if getattr(args,attribute,False): return mode
    if args.resume: return "execute"
    return settings.cleaner_mode


def verify_self_library() -> None:
    try: version = importlib.metadata.version("discord.py-self")
    except importlib.metadata.PackageNotFoundError as exc:
        raise RuntimeError("discord.py-self is not installed; run: pip install -r requirements.txt") from exc
    logging.getLogger(__name__).debug("discord.py-self %s", version)


async def execute(settings: Settings, db: ProgressDB, args: argparse.Namespace, package_fingerprint: str,
                  provenance_verified: bool, provenance_reason: str) -> None:
    verify_self_library()
    if not settings.token: raise RuntimeError("DISCORD_USER_TOKEN is required")
    if settings.expected_account_id is None: raise RuntimeError("EXPECTED_ACCOUNT_ID is required")
    workloads = db.channel_workloads(channel_id=args.channel_id, channel_types=args.channel_type,
                                     exclude_channels=args.exclude_channels, exclude_guilds=args.exclude_guilds)
    pending_count=sum(workload.pending_count for workload in workloads)
    if not pending_count:
        console.print("No pending messages match the filters."); return
    import discord  # imported only for an authenticated execution
    client = discord.Client()
    completed = False
    ready = asyncio.Event()

    @client.event
    async def on_ready() -> None:
        nonlocal completed
        ready.set()
        if completed: return
        completed = True
        if client.user is None or int(client.user.id) != settings.expected_account_id:
            console.print("Authenticated account does not match EXPECTED_ACCOUNT_ID. Stopping.")
            await client.close(); return
        db.register_package(package_fingerprint,int(client.user.id),provenance_verified)
        if settings.verification_mode=="direct" and not db.package_verified_for(int(client.user.id),package_fingerprint):
            console.print(f"Package provenance is not verified for direct deletion: {provenance_reason}")
            console.print("Set VERIFICATION_MODE=fetch to verify the author of every message.")
            await client.close(); return
        metrics=PerformanceMetrics()
        runner = DeletionRunner(client, db, account_id=int(client.user.id), delay=settings.delete_delay,
                                max_retries=settings.max_retries, concurrency=settings.max_concurrent,
                                verification_mode=settings.verification_mode,batch_size=settings.channel_batch_size,
                                metrics=metrics)
        label = f"{client.user} ({client.user.id})"
        console.print(f"Authenticated as {label}. Starting {pending_count:,} pending records in {len(workloads):,} channels ({settings.verification_mode} mode).")
        dashboard = Dashboard(db, "EXECUTE", label, settings.delete_delay, runner.pacer, settings.max_concurrent,metrics)
        runner.dashboard = dashboard
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.add_signal_handler(sig, runner.stop)
            except (NotImplementedError, RuntimeError): pass
        try:
            with dashboard:
                await runner.run_scheduled(channel_id=args.channel_id,channel_types=args.channel_type,
                                           exclude_channels=args.exclude_channels,exclude_guilds=args.exclude_guilds)
        except AuthenticationStopped as exc:
            console.print(f"Authentication stopped: {exc}")
        finally:
            await client.close()

    async def ready_watchdog() -> None:
        try:
            await asyncio.wait_for(ready.wait(), timeout=90)
        except TimeoutError:
            console.print("Discord gateway did not become ready within 90 seconds. Check the token, outbound network access, and Discord account session.")
            await client.close()

    console.print(f"Connecting to Discord; {pending_count:,} pending records selected...")
    watchdog = asyncio.create_task(ready_watchdog())
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
        watchdog.cancel()
        await asyncio.gather(watchdog, return_exceptions=True)
        if not client.is_closed(): await client.close()


async def recheck_inaccessible(settings: Settings,db: ProgressDB)->None:
    verify_self_library()
    if not settings.token or settings.expected_account_id is None: raise RuntimeError("Discord credentials are required for recheck")
    import discord
    client=discord.Client(); completed=False
    @client.event
    async def on_ready()->None:
        nonlocal completed
        if completed: return
        completed=True
        if client.user is None or int(client.user.id)!=settings.expected_account_id:
            console.print("Authenticated account does not match EXPECTED_ACCOUNT_ID. Stopping."); await client.close(); return
        rows=db.audit_inaccessible(); console.print(f"Rechecking {len(rows):,} inaccessible channels with at most one probe each...")
        for number,row in enumerate(rows,1):
            channel_id=int(row["channel_id"])
            try:
                await client.fetch_channel(channel_id)
                db.update_channel_probe(channel_id,"accessible",reason="channel-level recheck succeeded")
            except discord.NotFound as exc:
                db.update_channel_probe(channel_id,"deleted_channel",http_status=404,discord_code=getattr(exc,"code",None),reason="channel-level recheck returned 404")
            except discord.Forbidden as exc:
                db.update_channel_probe(channel_id,"inaccessible_confirmed",http_status=403,discord_code=getattr(exc,"code",None),reason="channel-level recheck returned 403")
            except discord.HTTPException as exc:
                db.update_channel_probe(channel_id,"temporarily_unavailable",http_status=getattr(exc,"status",None),discord_code=getattr(exc,"code",None),reason=f"temporary recheck failure: {exc}"[:1000])
            if number%50==0: console.print(f"Rechecked {number:,}/{len(rows):,} channels")
        console.print("Recheck complete. Messages were not retried or reset. Review --audit-inaccessible first.")
        await client.close()
    try: await client.start(settings.token)
    except discord.LoginFailure as exc: raise RuntimeError("Discord rejected the user token") from exc
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
            mode=selected_mode(args,settings)
            console.print(f"Cleaner mode: {mode}")
            if mode=="scan":
                with DataPackage(settings.messages_path) as package:
                    fingerprint=package.fingerprint()
                    found, added = db.import_records(package.records())
                db.register_package(fingerprint,settings.expected_account_id,False,found)
                console.print(f"Scanned {found:,} unique records; added {added:,} new records."); show_stats(db); return 0
            if mode=="retry_failed":
                console.print(f"Returned {db.retry_failed():,} failed records to pending."); return 0
            if mode=="stats":
                show_stats(db); return 0
            if mode=="diagnose":
                show_diagnosis(db); return 0
            if mode=="audit":
                show_audit(db); return 0
            if mode=="recheck":
                asyncio.run(recheck_inaccessible(settings,db)); show_audit(db); return 0
            if mode=="authorize":
                console.print(f"Reset {db.apply_rechecked_channels():,} historical rows from channels proven accessible."); return 0
            # Execute mode fingerprints first and only reparses an unindexed package.
            with DataPackage(settings.messages_path) as package:
                fingerprint=package.fingerprint()
                indexed_count=db.indexed_package_count(fingerprint)
                already_verified=bool(settings.expected_account_id and db.package_verified_for(settings.expected_account_id,fingerprint))
                if already_verified:
                    provenance_verified,provenance_reason=True,"unchanged package fingerprint is already bound to EXPECTED_ACCOUNT_ID"
                else:
                    provenance_verified,provenance_reason=package.ownership_evidence(settings.expected_account_id) if settings.expected_account_id else (False,"EXPECTED_ACCOUNT_ID is unset")
                if indexed_count is None:
                    found,added=db.import_records(package.records())
                    db.register_package(fingerprint,settings.expected_account_id,False,found)
                    console.print(f"Indexed {found:,} unique records; added {added:,} new records.")
                else:
                    console.print(f"Package unchanged and already indexed ({indexed_count:,} records); skipped full ZIP rescan.")
            console.print(f"Package provenance: {provenance_reason}")
            show_startup_report(db,settings)
            asyncio.run(execute(settings, db, args, fingerprint,provenance_verified,provenance_reason)); show_stats(db); return 0
    except (PackageError, OSError, RuntimeError, sqlite3.Error) as exc:
        console.print(f"[red]Error:[/red] {exc}"); return 1
    except KeyboardInterrupt:
        console.print("Interrupted. Confirmed results are saved; run python main.py to continue."); return 130


if __name__ == "__main__": raise SystemExit(main())
