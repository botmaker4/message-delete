from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from .models import ExportedMessage, WorkItem


STATUSES = ("pending", "deleted", "already_missing", "inaccessible", "failed", "skipped")


class ProgressDB:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                channel_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                channel_type TEXT NOT NULL, guild_id INTEGER, label TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN
                  ('pending','deleted','already_missing','inaccessible','failed','skipped')),
                attempt_count INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                last_attempt_at TEXT, deleted_at TEXT,
                PRIMARY KEY(channel_id, message_id)
            );
            CREATE INDEX IF NOT EXISTS ix_messages_status ON messages(status);
            CREATE INDEX IF NOT EXISTS ix_messages_channel ON messages(channel_id, status);
        """)

    def __enter__(self) -> "ProgressDB": return self
    def __exit__(self, *args: object) -> None: self.close()
    def close(self) -> None: self.connection.close()

    def import_records(self, records: Iterable[ExportedMessage], batch_size: int = 2000) -> tuple[int, int]:
        before = self.connection.total_changes
        count = 0
        batch: list[tuple[object, ...]] = []
        sql = """INSERT OR IGNORE INTO messages
            (channel_id,message_id,channel_type,guild_id,label) VALUES (?,?,?,?,?)"""
        for record in records:
            count += 1
            batch.append((record.channel_id, record.message_id, record.channel_type, record.guild_id, record.label))
            if len(batch) >= batch_size:
                self.connection.executemany(sql, batch); self.connection.commit(); batch.clear()
        if batch:
            self.connection.executemany(sql, batch); self.connection.commit()
        return count, self.connection.total_changes - before

    def select_work(self, *, limit: int | None = None, channel_id: int | None = None,
                    channel_types: set[str] | None = None, exclude_channels: set[int] | None = None,
                    exclude_guilds: set[int] | None = None) -> list[WorkItem]:
        clauses, params = ["status = 'pending'"], []
        if channel_id is not None: clauses.append("channel_id = ?"); params.append(channel_id)
        if channel_types:
            clauses.append("channel_type IN (%s)" % ",".join("?" * len(channel_types))); params.extend(sorted(channel_types))
        for column, values in (("channel_id", exclude_channels), ("guild_id", exclude_guilds)):
            if values:
                clauses.append(f"({column} IS NULL OR {column} NOT IN ({','.join('?' * len(values))}))"); params.extend(sorted(values))
        sql = "SELECT channel_id,message_id,channel_type,guild_id,attempt_count AS attempts FROM messages WHERE " + " AND ".join(clauses) + " ORDER BY channel_id,message_id"
        if limit is not None: sql += " LIMIT ?"; params.append(limit)
        return [WorkItem(**dict(row)) for row in self.connection.execute(sql, params)]

    def record(self, item: WorkItem, status: str, error: str | None = None) -> None:
        if status not in STATUSES: raise ValueError(status)
        now = datetime.now(timezone.utc).isoformat()
        self.connection.execute("""UPDATE messages SET status=?, attempt_count=attempt_count+1,
            last_error=?, last_attempt_at=?, deleted_at=? WHERE channel_id=? AND message_id=?""",
            (status, (error or None), now, now if status == "deleted" else None, item.channel_id, item.message_id))
        self.connection.commit()

    def mark_channel_inaccessible(self, item: WorkItem, error: str) -> int:
        """Finish every pending row for a channel after a channel-wide denial.

        Only the message that triggered the API response counts as an attempt;
        the remaining rows are classified locally without further requests.
        """
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.connection.execute("""UPDATE messages
            SET status='inaccessible',
                attempt_count=attempt_count + CASE WHEN message_id=? THEN 1 ELSE 0 END,
                last_error=?, last_attempt_at=?
            WHERE channel_id=? AND status='pending'""",
            (item.message_id, error[:1000], now, item.channel_id))
        self.connection.commit()
        return cursor.rowcount

    def retry_failed(self) -> int:
        cursor = self.connection.execute("UPDATE messages SET status='pending', last_error=NULL WHERE status='failed'")
        self.connection.commit(); return cursor.rowcount

    def stats(self) -> dict[str, int]:
        result = {status: 0 for status in STATUSES}
        for row in self.connection.execute("SELECT status,COUNT(*) AS n FROM messages GROUP BY status"):
            result[row["status"]] = row["n"]
        result["total"] = sum(result.values())
        result["remaining"] = result["pending"] + result["failed"]
        return result

    def channel_stats(self, channel_id: int) -> tuple[int, int]:
        row = self.connection.execute("SELECT COUNT(*) total, SUM(status!='pending') done FROM messages WHERE channel_id=?", (channel_id,)).fetchone()
        return int(row["done"] or 0), int(row["total"] or 0)
