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
