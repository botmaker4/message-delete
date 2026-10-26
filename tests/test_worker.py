import asyncio
from types import SimpleNamespace

import pytest

from discord_cleaner.deletion_worker import DeletionRunner
from discord_cleaner.models import WorkItem


class HTTPError(Exception):
    def __init__(self, status, retry_after=None):
        super().__init__(f"HTTP {status}"); self.status=status; self.retry_after=retry_after


class DB:
    def __init__(self): self.rows=[]
    def record(self, item, status, error=None, **evidence): self.rows.append((status,error,evidence))
    def mark_channel(self, item, channel_status, message_status, error, **evidence):
        self.rows.append((message_status,error,evidence)); return 100
    def update_channel_probe(self, *args, **kwargs): pass
    def flush(self): pass


class Message:
    def __init__(self, author=42, errors=()): self.author=SimpleNamespace(id=author); self.errors=list(errors); self.deleted=0
    async def delete(self):
        if self.errors: raise self.errors.pop(0)
        self.deleted += 1


class Channel:
    def __init__(self, message=None, error=None): self.message=message; self.error=error; self.calls=0
    async def fetch_message(self, _):
        self.calls += 1
        if self.error: raise self.error
        return self.message


class Client:
    def __init__(self, channel): self.channel=channel
    def get_channel(self, _): return self.channel
    async def fetch_channel(self, _): return self.channel

    def get_partial_messageable(self, channel_id, **kwargs):
        return PartialChannel(self.channel.message)


class PartialChannel:
    def __init__(self,message): self.message=message
    def get_partial_message(self,_): return self.message


class ProbeFailureClient(Client):
    def __init__(self, channel, probe_error): super().__init__(channel); self.probe_error=probe_error
    async def fetch_channel(self, _): raise self.probe_error


def item(): return WorkItem(123456, 777777, "DM", None, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [(403,"inaccessible"),(404,"already_missing")])
async def test_permanent_http_results(status, expected):
    db=DB(); runner=DeletionRunner(Client(Channel(error=HTTPError(status))),db,account_id=42,delay=0,max_retries=2,concurrency=1)
    await runner.run([item()]); assert db.rows[0][0] == expected


@pytest.mark.asyncio
async def test_429_and_5xx_are_retried(monkeypatch):
    async def no_sleep(_): pass
    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    message=Message(errors=[HTTPError(429, 0.01), HTTPError(503)])
    db=DB(); runner=DeletionRunner(Client(Channel(message)),db,account_id=42,delay=0,max_retries=3,concurrency=1)
    await runner.run([item()]); assert message.deleted == 1; assert db.rows[-1][0] == "deleted"


@pytest.mark.asyncio
async def test_author_must_match():
    message=Message(author=99); db=DB()
    await DeletionRunner(Client(Channel(message)),db,account_id=42,delay=0,max_retries=1,concurrency=1).run([item()])
    assert message.deleted == 0; assert db.rows[0][0] == "skipped"


@pytest.mark.asyncio
async def test_one_confirmed_channel_403_short_circuits_the_entire_channel():
    channel = Channel(error=HTTPError(403)); db = DB()
    items = [WorkItem(123456, 700000 + number, "DM", None, 0) for number in range(100)]
    runner = DeletionRunner(ProbeFailureClient(channel,HTTPError(403)), db, account_id=42, delay=0, max_retries=2,concurrency=1)
    await runner.run(items)
    assert channel.calls == 1
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_ambiguous_message_403_does_not_bulk_mark_channel():
    channel=Channel(error=HTTPError(403)); db=DB(); items=[WorkItem(123456,700000+n,"DM",None,0) for n in range(3)]
    await DeletionRunner(Client(channel),db,account_id=42,delay=0,max_retries=1,concurrency=1).run(items)
    assert channel.calls == 3
    assert [row[0] for row in db.rows] == ["inaccessible"]*3


@pytest.mark.asyncio
async def test_direct_mode_uses_one_delete_without_fetch():
    message=Message(); channel=Channel(message); db=DB()
    runner=DeletionRunner(Client(channel),db,account_id=42,delay=0,max_retries=1,concurrency=1,verification_mode="direct")
    await runner.run([item()])
    assert message.deleted == 1
    assert channel.calls == 0
    assert runner.metrics.api_requests == 1


def test_no_unrelated_destructive_calls_exist():
    import pathlib
    source="\n".join(p.read_text(encoding="utf-8") for p in pathlib.Path("discord_cleaner").glob("*.py"))
    forbidden=("guild.leave(", ".delete_channel(", ".create_guild(", ".leave_group(", ".close_dm(")
    assert not any(term in source for term in forbidden)
    from discord_cleaner.deletion_worker import DISCORD_OPERATION_ALLOWLIST
    assert DISCORD_OPERATION_ALLOWLIST == {"fetch_channel","fetch_message","delete"}


def test_eta_duration_formatting():
    from discord_cleaner.dashboard import _duration
    assert _duration(65) == "1m 05s"
    assert _duration(3661) == "1h 01m 01s"
    assert _duration(90000) == "1d 01h 00m"
