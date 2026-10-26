from io import StringIO
from types import SimpleNamespace

from rich.console import Console

from discord_cleaner.dashboard import Dashboard
from discord_cleaner.metrics import PerformanceMetrics


class DB:
    def __init__(self): self.calls=0
    def stats(self):
        self.calls+=1
        return {"total":100_000,"pending":100_000,"deleted":0,"already_missing":0,
                "inaccessible":0,"confirmed_inaccessible":0,"unverified_inaccessible":0,
                "failed":0,"skipped":0,"remaining":100_000,"channels_total":100,
                "channels_completed":0,"active_channels":100}
    def channel_stats(self,_): return (0,1000)


def test_100k_dashboard_updates_do_not_query_sql_per_message():
    db=DB(); metrics=PerformanceMetrics(); dashboard=Dashboard(db,"TEST","account",0,SimpleNamespace(cooldown_remaining=0),1,metrics)
    dashboard._console=Console(file=StringIO(),force_terminal=False)
    with dashboard:
        for number in range(100_000):
            dashboard.update(item=SimpleNamespace(channel_id=123456,channel_type="DM"))
    assert db.calls <= 2
