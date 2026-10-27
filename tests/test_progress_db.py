from discord_cleaner.models import ExportedMessage
from discord_cleaner.progress_db import ProgressDB


def test_resume_and_retry(tmp_path):
    path = tmp_path / "p.db"
    record = ExportedMessage(123456, 777777, "DM")
    with ProgressDB(path) as db:
        assert db.import_records([record]) == (1, 1)
        item = db.select_work()[0]
        db.record(item, "failed", "temporary")
        assert db.select_work() == []
        assert db.retry_failed() == 1
    with ProgressDB(path) as db:
        assert len(db.select_work()) == 1
        item = db.select_work()[0]; db.record(item, "deleted")
    with ProgressDB(path) as db:
        assert db.stats()["deleted"] == 1
        assert db.select_work() == []


def test_filters(tmp_path):
    with ProgressDB(tmp_path / "p.db") as db:
        db.import_records([ExportedMessage(123456, 777777, "DM"), ExportedMessage(234567, 888888, "GUILD_TEXT", 999999)])
        assert [x.channel_id for x in db.select_work(channel_types={"DM"})] == [123456]
        assert [x.channel_id for x in db.select_work(exclude_guilds={999999})] == [123456]


def test_dm_only_workload_selection(tmp_path):
    with ProgressDB(tmp_path/"dm-only.db") as db:
        db.import_records([
            ExportedMessage(123456,700001,"DM"),
            ExportedMessage(234567,700002,"GROUP_DM"),
            ExportedMessage(345678,700003,"GUILD_TEXT",999999),
        ])
        workloads=db.channel_workloads(channel_types={"DM"})
        assert [(row.channel_id,row.channel_type) for row in workloads] == [(123456,"DM")]
        assert db.stats(channel_types={"DM"})["pending"] == 1


def test_channel_wide_inaccessible_is_bulk_and_resumable(tmp_path):
    with ProgressDB(tmp_path / "p.db") as db:
        db.import_records([ExportedMessage(123456, 700000 + n, "DM") for n in range(100)])
        first = db.select_work()[0]
        assert db.mark_channel_inaccessible(first, "HTTP 403") == 100
        assert db.stats()["inaccessible"] == 100
        assert db.select_work() == []
        attempts = db.connection.execute(
            "SELECT SUM(attempt_count) FROM messages WHERE channel_id=123456"
        ).fetchone()[0]
        assert attempts == 1


def test_v1_migration_preserves_progress_and_labels_old_inaccessible(tmp_path):
    import sqlite3
    path=tmp_path/"legacy.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE messages(channel_id INTEGER,message_id INTEGER,channel_type TEXT,guild_id INTEGER,
            label TEXT,status TEXT,attempt_count INTEGER,last_error TEXT,last_attempt_at TEXT,deleted_at TEXT,
            PRIMARY KEY(channel_id,message_id))""")
        conn.execute("INSERT INTO messages VALUES(123456,777777,'DM',NULL,'','deleted',1,NULL,NULL,'now')")
        conn.execute("INSERT INTO messages VALUES(123456,888888,'DM',NULL,'','inaccessible',1,'old 403',NULL,NULL)")
    with ProgressDB(path) as db:
        assert db.stats()["deleted"] == 1
        assert db.stats()["unverified_inaccessible"] == 1
        assert db.connection.execute("SELECT accessibility_status FROM channels").fetchone()[0] == "needs_recheck"
    assert list(tmp_path.glob("legacy.sqlite3.backup-*"))


def test_batched_commits_and_interrupted_resume(tmp_path):
    path=tmp_path/"batch.sqlite3"
    records=[ExportedMessage(123456,700000+n,"DM") for n in range(100)]
    with ProgressDB(path,commit_batch_size=25) as db:
        db.import_records(records); before=db.commit_count
        for item in db.select_work()[:30]: db.record(item,"deleted")
        db.flush()
        assert db.commit_count-before == 2
    with ProgressDB(path) as db:
        assert db.stats()["deleted"] == 30
        assert len(db.select_work()) == 70
