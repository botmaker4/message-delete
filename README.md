# Discord Message Cleaner

A resumable Python 3.11+ worker that deletes message IDs from an official Discord Data Package using `discord.py-self`.

> Automating a user account violates Discord's platform rules and can lead to restrictions or termination. This tool cannot eliminate that risk.

## Install and run

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python main.py
```

Normal operation scans the package, resumes SQLite progress, verifies the authenticated ID, and processes every pending matching row. There is no execute flag, limit, or confirmation prompt. Ctrl+C stops scheduling and flushes recorded results.

### Environment-controlled operation

Coolify always starts the container with `python main.py`. Set `CLEANER_MODE` to choose the operation without changing that command:

| Value | Behavior |
|---|---|
| `execute` | Default. Check/index the package, print the startup diagnostic summary, and resume pending deletion. |
| `diagnose` | Print the complete database/classification report and exit without Discord access. |
| `audit` | Print the inaccessible-channel audit and exit without Discord access. |
| `recheck` | Authenticate and make at most one read-only channel probe per inaccessible channel; never retry messages. |
| `stats` | Print progress and exit without Discord access. |

Explicit local CLI actions continue to override `CLEANER_MODE`. Neither diagnostics nor recheck ever authorizes inaccessible messages. The authorization command remains CLI-only by design.

Useful commands:

```powershell
python main.py --scan
python main.py --stats
python main.py --diagnose
python main.py --audit-inaccessible
python main.py --recheck-inaccessible
python main.py --authorize-inaccessible-retry
python main.py --retry-failed
```

`--recheck-inaccessible` makes at most one channel-level probe per recorded inaccessible channel and never retries its messages. Review the audit afterward. Only the separate `--authorize-inaccessible-retry` command returns legacy `historical_unverified` rows from channels proven accessible to pending.

## Request and ownership model

In the default `VERIFICATION_MODE=direct`, the worker uses the supported partial-object path `get_partial_messageable(...).get_partial_message(...).delete()`. A successful record therefore needs one DELETE, not a GET plus DELETE, and remains inside `discord.py-self`'s rate-limit machinery.

Direct mode is enabled only after all of these checks succeed:

- the authenticated ID equals `EXPECTED_ACCOUNT_ID`;
- the package passes its schema and ID validation;
- numeric DM/group recipient metadata consistently contains that account ID;
- the exact package fingerprint is bound to that authenticated account in SQLite.

Discord exports do not include an author field or cryptographic signature in each message row. If the package has no usable ownership evidence or its provenance is uncertain, set `VERIFICATION_MODE=fetch`. Fetch mode performs the message GET and author check before every DELETE. Never construct or mix package records manually.

## Scheduling, failures, and rate limits

Work is grouped by channel. The scheduler loads a bounded batch (100 by default), rotates channels, favors channels with demonstrated success, and never places the entire message backlog in an asyncio queue. Channel objects are reused.

The ZIP is SHA-256 fingerprinted on startup. If that exact package is already completely indexed, execution skips reparsing every channel and message JSON file. A new or incompletely indexed fingerprint is scanned and added with `INSERT OR IGNORE`; existing progress is never reset.

A message DELETE 404 is `already_missing`. A 403 is not automatically treated as proof that the channel is inaccessible: the worker performs one channel-level probe. Only an independent channel 403 or 404 bulk-classifies the remaining channel rows. An accessible probe leaves the message as unverified inaccessible and the channel as `needs_recheck`; transient probes are deferred/failed with their HTTP status, Discord code, operation stage, and sanitized reason recorded.

`discord.py-self` remains authoritative for route and global limits. The default application delay is zero, avoiding duplicate arbitrary sleeps; a nonzero `DELETE_DELAY_SECONDS` remains available as a conservative minimum. Surfaced 429 Retry-After values and transient 5xx/network errors receive bounded backoff with jitter. Concurrency stays configurable and capped at four.

## Progress database and migration

SQLite uses WAL, indexes for channel/status scheduling, and commits result batches of 25. Results are changed only after an API outcome. A crash after remote success but before the next commit safely reconciles as `already_missing` on restart. Bulk channel results are a single durable transaction.

Opening a legacy database creates a timestamped sibling backup before schema changes, preserves every existing result, and labels old inaccessible rows `historical_unverified`. Never delete `progress.sqlite3`, its WAL/SHM files while running, or migration backups.

The dashboard uses in-memory counters, renders at most twice per second, and reconciles SQLite every five seconds. An independent reporter gives Coolify a compact progress line every 30 seconds even during cooldowns. It includes actual successful deletions/minute, classification speed, total/current request rate, 429/403/404 counts, cooldown, elapsed time, and API-bound ETA. API-bound ETA excludes bulk local classifications and remains an estimate, not a guarantee.

## Safety boundary

The explicit Discord operation allowlist is: resolve/probe a channel, fetch one exact message in verification mode, and delete one exact package-listed message. The project contains no guild join/leave, membership management, channel/thread deletion, DM/group closure, permission/role changes, history scraping, or message sending.

## Tests and benchmarks

```powershell
pip install -r requirements-dev.txt
pytest -q
python benchmarks/benchmark_simulated.py
```

Tests and benchmarks use fake Discord objects and never authenticate. See [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md) and [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md). The reference archive contains 2,191 channel folders and 174,812 unique valid IDs.

## Coolify

Use the `Dockerfile` build strategy, no domain/port/HTTP health check, and mount the server directory `/srv/discord-cleaner/data` at `/data`. Place `package.zip` in that server directory; never commit it or `.env`.

```dotenv
DISCORD_USER_TOKEN=your-secret-token
EXPECTED_ACCOUNT_ID=your-numeric-account-id
MESSAGES_PATH=/data/package.zip
PROGRESS_DB=/data/progress.sqlite3
DELETE_DELAY_SECONDS=0.0
MAX_CONCURRENT_DELETES=1
MAX_RETRIES=5
VERIFICATION_MODE=direct
CHANNEL_BATCH_SIZE=100
CLEANER_MODE=execute
```

After updating an existing deployment, preserve `/data`, change `DELETE_DELAY_SECONDS` from `1.0` to `0.0` if you want adaptive/library-managed pacing, and redeploy. The first start backs up and migrates the database automatically. The container exposes no port and exits normally when no pending work remains.

For `diagnose`, `audit`, or `stats`, the process intentionally exits successfully after printing the report. Configure Coolify's container restart policy as **on-failure** or **no**, not **always**, so exit code 0 does not create a restart loop. For a one-off report, stop the execute worker first, temporarily set `CLEANER_MODE`, deploy a separate copy of the resource pointing at the same `/data` mount, collect its logs, then remove/stop that copy before restarting execute mode. Never run two containers against the same SQLite files. Coolify scheduled tasks run inside an already-running container and record their output, but a separate stopped-worker diagnostic resource is safer when the cleaner normally exits after completion.
