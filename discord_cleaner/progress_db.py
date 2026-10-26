from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from .models import ChannelWorkload, ExportedMessage, WorkItem

STATUSES = ("pending", "deleted", "already_missing", "inaccessible", "failed", "skipped")
CHANNEL_STATUSES = ("unknown", "accessible", "temporarily_unavailable", "inaccessible_confirmed", "deleted_channel", "needs_recheck")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProgressDB:
    def __init__(self, path: str | Path, commit_batch_size: int = 25):
        self.path = Path(path); self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.name != ":memory:"
        self.connection = sqlite3.connect(self.path); self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL"); self.connection.execute("PRAGMA synchronous=FULL"); self.connection.execute("PRAGMA busy_timeout=5000")
        self.sqlite_seconds = 0.0; self.commit_count = 0; self._uncommitted = 0; self._commit_batch_size = commit_batch_size
        self._migrate(existed)

    def _migrate(self, existed: bool) -> None:
        has_messages = self.connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages'").fetchone() is not None
        old = {row[1] for row in self.connection.execute("PRAGMA table_info(messages)")} if has_messages else set()
        if existed and has_messages and "error_stage" not in old:
            backup = self.path.with_name(f"{self.path.name}.backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
            with sqlite3.connect(backup) as destination: self.connection.backup(destination)
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                channel_id INTEGER NOT NULL,message_id INTEGER NOT NULL,channel_type TEXT NOT NULL,
                guild_id INTEGER,label TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending','deleted','already_missing','inaccessible','failed','skipped')),
                attempt_count INTEGER NOT NULL DEFAULT 0,last_error TEXT,last_attempt_at TEXT,deleted_at TEXT,
                http_status INTEGER,discord_code INTEGER,error_stage TEXT,classification_source TEXT,
                network_attempted INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(channel_id,message_id));
            CREATE TABLE IF NOT EXISTS channels (
                channel_id INTEGER PRIMARY KEY,channel_type TEXT NOT NULL,guild_id INTEGER,
                accessibility_status TEXT NOT NULL DEFAULT 'unknown',last_attempt_at TEXT,
                next_eligible_at REAL NOT NULL DEFAULT 0,success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,last_http_status INTEGER,last_discord_code INTEGER,
                last_stage TEXT,last_reason TEXT);
            CREATE TABLE IF NOT EXISTS package_imports (
                fingerprint TEXT PRIMARY KEY,account_id INTEGER,verified INTEGER NOT NULL DEFAULT 0,imported_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        if has_messages:
            for name, declaration in {"http_status":"INTEGER","discord_code":"INTEGER","error_stage":"TEXT","classification_source":"TEXT","network_attempted":"INTEGER NOT NULL DEFAULT 0"}.items():
                if name not in old: self.connection.execute(f"ALTER TABLE messages ADD COLUMN {name} {declaration}")
            self.connection.execute("UPDATE messages SET classification_source='historical_unverified' WHERE status='inaccessible' AND classification_source IS NULL")
            self.connection.execute("INSERT OR IGNORE INTO channels(channel_id,channel_type,guild_id) SELECT channel_id,MIN(channel_type),MIN(guild_id) FROM messages GROUP BY channel_id")
            self.connection.execute("""UPDATE channels SET accessibility_status='needs_recheck' WHERE channel_id IN
                (SELECT DISTINCT channel_id FROM messages WHERE status='inaccessible' AND classification_source='historical_unverified')""")
        self.connection.executescript("""
            CREATE INDEX IF NOT EXISTS ix_messages_status_channel ON messages(status,channel_id);
            CREATE INDEX IF NOT EXISTS ix_messages_channel_status ON messages(channel_id,status,message_id);
            CREATE INDEX IF NOT EXISTS ix_messages_error ON messages(http_status,discord_code,error_stage);
            CREATE INDEX IF NOT EXISTS ix_channels_schedule ON channels(accessibility_status,next_eligible_at);
        """)
        self.connection.execute("INSERT OR REPLACE INTO metadata(key,value) VALUES('schema_version','2')"); self.connection.commit()

    def __enter__(self) -> "ProgressDB": return self
    def __exit__(self, *args: object) -> None: self.close()
    def close(self) -> None: self.flush(); self.connection.close()
    def flush(self) -> None:
        if self._uncommitted:
            started=time.monotonic(); self.connection.commit(); self.sqlite_seconds += time.monotonic()-started
            self.commit_count += 1; self._uncommitted=0

    def import_records(self, records: Iterable[ExportedMessage], batch_size: int = 2000) -> tuple[int,int]:
        before=self.connection.total_changes; count=0; batch=[]; channels={}
        sql="INSERT OR IGNORE INTO messages(channel_id,message_id,channel_type,guild_id,label) VALUES(?,?,?,?,?)"
        for record in records:
            count+=1; batch.append((record.channel_id,record.message_id,record.channel_type,record.guild_id,record.label)); channels[record.channel_id]=(record.channel_id,record.channel_type,record.guild_id)
            if len(batch)>=batch_size: self.connection.executemany(sql,batch); self.connection.commit(); self.commit_count+=1; batch.clear()
        if batch: self.connection.executemany(sql,batch); self.connection.commit(); self.commit_count+=1
        message_changes=self.connection.total_changes-before
        self.connection.executemany("""INSERT INTO channels(channel_id,channel_type,guild_id) VALUES(?,?,?)
            ON CONFLICT(channel_id) DO UPDATE SET channel_type=excluded.channel_type,guild_id=COALESCE(excluded.guild_id,channels.guild_id)""",channels.values())
        self.connection.commit(); self.commit_count+=1
        return count,message_changes

    def register_package(self,fingerprint:str,account_id:int|None,verified:bool)->None:
        self.connection.execute("INSERT OR REPLACE INTO package_imports VALUES(?,?,?,?)",(fingerprint,account_id,int(verified),_now())); self.connection.commit(); self.commit_count+=1
    def package_verified_for(self,account_id:int,fingerprint:str|None=None)->bool:
        if fingerprint is None:
            return self.connection.execute("SELECT 1 FROM package_imports WHERE account_id=? AND verified=1 LIMIT 1",(account_id,)).fetchone() is not None
        return self.connection.execute("SELECT 1 FROM package_imports WHERE account_id=? AND fingerprint=? AND verified=1 LIMIT 1",(account_id,fingerprint)).fetchone() is not None

    def _filters(self,channel_id,channel_types,exclude_channels,exclude_guilds,alias="m."):
        clauses=[]; params=[]
        if channel_id is not None: clauses.append(f" AND {alias}channel_id=?"); params.append(channel_id)
        if channel_types: clauses.append(f" AND {alias}channel_type IN ({','.join('?'*len(channel_types))})"); params.extend(sorted(channel_types))
        for column,values in (("channel_id",exclude_channels),("guild_id",exclude_guilds)):
            if values: clauses.append(f" AND ({alias}{column} IS NULL OR {alias}{column} NOT IN ({','.join('?'*len(values))}))"); params.extend(sorted(values))
        return clauses,params

    def select_work(self,*,limit=None,channel_id=None,channel_types=None,exclude_channels=None,exclude_guilds=None)->list[WorkItem]:
        clauses,params=self._filters(channel_id,channel_types,exclude_channels,exclude_guilds,"")
        sql="SELECT channel_id,message_id,channel_type,guild_id,attempt_count AS attempts FROM messages WHERE status='pending'"+"".join(clauses)+" ORDER BY channel_id,message_id"
        if limit is not None: sql+=" LIMIT ?"; params.append(limit)
        return [WorkItem(**dict(row)) for row in self.connection.execute(sql,params)]

    def channel_workloads(self,*,channel_id=None,channel_types=None,exclude_channels=None,exclude_guilds=None)->list[ChannelWorkload]:
        clauses,params=self._filters(channel_id,channel_types,exclude_channels,exclude_guilds)
        sql="""SELECT m.channel_id,MIN(m.channel_type) channel_type,MIN(m.guild_id) guild_id,COUNT(*) pending_count,
            c.accessibility_status,c.success_count,c.failure_count FROM messages m JOIN channels c ON c.channel_id=m.channel_id
            WHERE m.status='pending' AND c.accessibility_status NOT IN ('inaccessible_confirmed','deleted_channel')"""+"".join(clauses)+"""
            GROUP BY m.channel_id ORDER BY (c.success_count>0) DESC,c.failure_count ASC,c.last_attempt_at IS NOT NULL,c.last_attempt_at"""
        return [ChannelWorkload(**dict(row)) for row in self.connection.execute(sql,params)]

    def pending_for_channel(self,channel_id:int,limit:int)->list[WorkItem]:
        return [WorkItem(**dict(row)) for row in self.connection.execute("""SELECT channel_id,message_id,channel_type,guild_id,
            attempt_count AS attempts FROM messages WHERE channel_id=? AND status='pending' ORDER BY message_id LIMIT ?""",(channel_id,limit))]

    def record(self,item:WorkItem,status:str,error:str|None=None,*,http_status=None,discord_code=None,stage=None,source="direct_result",network_attempted=True)->None:
        if status not in STATUSES: raise ValueError(status)
        started=time.monotonic(); now=_now()
        self.connection.execute("""UPDATE messages SET status=?,attempt_count=attempt_count+?,last_error=?,last_attempt_at=?,deleted_at=?,
            http_status=?,discord_code=?,error_stage=?,classification_source=?,network_attempted=MAX(network_attempted,?)
            WHERE channel_id=? AND message_id=?""",(status,int(network_attempted),error or None,now,now if status=="deleted" else None,http_status,discord_code,stage,source,int(network_attempted),item.channel_id,item.message_id))
        self.connection.execute("""UPDATE channels SET last_attempt_at=?,success_count=success_count+?,failure_count=failure_count+?,
            accessibility_status=CASE WHEN ? IN ('deleted','already_missing') THEN 'accessible' ELSE accessibility_status END,
            last_http_status=?,last_discord_code=?,last_stage=?,last_reason=? WHERE channel_id=?""",(now,int(status=="deleted"),int(status in {"failed","inaccessible"}),status,http_status,discord_code,stage,error or None,item.channel_id))
        self.sqlite_seconds+=time.monotonic()-started; self._uncommitted+=1
        if self._uncommitted>=self._commit_batch_size: self.flush()

    def mark_channel(self,item:WorkItem,channel_status:str,message_status:str,error:str,*,http_status=None,discord_code=None,stage="channel_probe",source="channel_confirmed")->int:
        if channel_status not in CHANNEL_STATUSES: raise ValueError(channel_status)
        self.flush(); now=_now(); started=time.monotonic()
        cursor=self.connection.execute("""UPDATE messages SET status=?,attempt_count=attempt_count+CASE WHEN message_id=? THEN 1 ELSE 0 END,
            last_error=?,last_attempt_at=?,http_status=?,discord_code=?,error_stage=?,classification_source=?,
            network_attempted=MAX(network_attempted,CASE WHEN message_id=? THEN 1 ELSE 0 END) WHERE channel_id=? AND status='pending'""",
            (message_status,item.message_id,error[:1000],now,http_status,discord_code,stage,source,item.message_id,item.channel_id))
        self.connection.execute("""UPDATE channels SET accessibility_status=?,last_attempt_at=?,last_http_status=?,last_discord_code=?,last_stage=?,last_reason=? WHERE channel_id=?""",
            (channel_status,now,http_status,discord_code,stage,error[:1000],item.channel_id))
        self.connection.commit(); self.commit_count+=1; self.sqlite_seconds+=time.monotonic()-started
        return cursor.rowcount

    def mark_channel_inaccessible(self,item:WorkItem,error:str)->int:
        return self.mark_channel(item,"inaccessible_confirmed","inaccessible",error,http_status=403)

    def update_channel_probe(self,channel_id:int,status:str,*,http_status=None,discord_code=None,reason=None)->None:
        self.flush(); self.connection.execute("""UPDATE channels SET accessibility_status=?,last_attempt_at=?,last_http_status=?,
            last_discord_code=?,last_stage='channel_probe',last_reason=? WHERE channel_id=?""",(status,_now(),http_status,discord_code,reason,channel_id)); self.connection.commit(); self.commit_count+=1

    def retry_failed(self)->int:
        self.flush(); cursor=self.connection.execute("UPDATE messages SET status='pending',last_error=NULL WHERE status='failed'"); self.connection.commit(); self.commit_count+=1; return cursor.rowcount
    def apply_rechecked_channels(self)->int:
        self.flush(); cursor=self.connection.execute("""UPDATE messages SET status='pending',last_error=NULL,classification_source='recheck_authorized'
            WHERE status='inaccessible' AND classification_source='historical_unverified' AND channel_id IN
            (SELECT channel_id FROM channels WHERE accessibility_status='accessible')"""); self.connection.commit(); self.commit_count+=1; return cursor.rowcount

    def stats(self)->dict[str,int]:
        result={status:0 for status in STATUSES}
        for row in self.connection.execute("SELECT status,COUNT(*) n FROM messages GROUP BY status"): result[row["status"]]=row["n"]
        result["total"]=sum(result.values()); result["remaining"]=result["pending"]+result["failed"]
        result["unverified_inaccessible"]=self.connection.execute("SELECT COUNT(*) FROM messages WHERE status='inaccessible' AND classification_source='historical_unverified'").fetchone()[0]
        result["confirmed_inaccessible"]=result["inaccessible"]-result["unverified_inaccessible"]
        channel_row=self.connection.execute("""SELECT COUNT(*) total,
            SUM(NOT EXISTS(SELECT 1 FROM messages m WHERE m.channel_id=c.channel_id AND m.status='pending')) completed,
            SUM(EXISTS(SELECT 1 FROM messages m WHERE m.channel_id=c.channel_id AND m.status='pending')) active FROM channels c""").fetchone()
        result["channels_total"]=int(channel_row["total"] or 0); result["channels_completed"]=int(channel_row["completed"] or 0); result["active_channels"]=int(channel_row["active"] or 0)
        return result
    def channel_stats(self,channel_id:int)->tuple[int,int]:
        row=self.connection.execute("SELECT COUNT(*) total,SUM(status!='pending') done FROM messages WHERE channel_id=?",(channel_id,)).fetchone(); return int(row["done"] or 0),int(row["total"] or 0)
    def audit_inaccessible(self):
        return list(self.connection.execute("""SELECT c.channel_id,c.channel_type,c.accessibility_status,COUNT(*) message_count,
            MIN(m.classification_source) evidence,c.last_http_status,c.last_discord_code,c.last_stage FROM messages m JOIN channels c USING(channel_id)
            WHERE m.status='inaccessible' GROUP BY c.channel_id ORDER BY message_count DESC"""))
    def diagnosis(self)->dict[str,object]:
        by_type=list(self.connection.execute("SELECT channel_type,COUNT(*) total,SUM(status='pending') pending,SUM(status='inaccessible') inaccessible FROM messages GROUP BY channel_type ORDER BY total DESC"))
        errors=list(self.connection.execute("""SELECT COALESCE(http_status,-1) http_status,COALESCE(discord_code,-1) discord_code,
            COALESCE(error_stage,'unknown') stage,COUNT(*) count FROM messages WHERE last_error IS NOT NULL GROUP BY 1,2,3 ORDER BY count DESC"""))
        top=list(self.connection.execute("SELECT channel_id,COUNT(*) count,MIN(classification_source) source FROM messages WHERE status='inaccessible' GROUP BY channel_id ORDER BY count DESC LIMIT 20"))
        totals=dict(self.connection.execute("""SELECT COUNT(*) total,SUM(network_attempted>0) attempted,SUM(status='deleted') deleted,
            SUM(status='inaccessible' AND classification_source IN ('channel_confirmed','historical_unverified')) inferred FROM messages""").fetchone())
        totals["distinct_inaccessible_channels"]=self.connection.execute("SELECT COUNT(DISTINCT channel_id) FROM messages WHERE status='inaccessible'").fetchone()[0]
        return {"by_type":by_type,"errors":errors,"top":top,"totals":totals}
