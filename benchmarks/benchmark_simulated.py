"""Controlled benchmark; never connects to Discord.

Run: python benchmarks/benchmark_simulated.py
"""
from __future__ import annotations

import asyncio
import sys
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from discord_cleaner.deletion_worker import DeletionRunner
from discord_cleaner.models import ExportedMessage
from discord_cleaner.progress_db import ProgressDB


class HTTPError(Exception):
    def __init__(self,status,retry_after=None): super().__init__(f"HTTP {status}"); self.status=status; self.retry_after=retry_after


class Message:
    def __init__(self,client,channel_id,message_id): self.client=client; self.channel_id=channel_id; self.id=message_id
    async def delete(self):
        self.client.requests+=1; outcome=self.client.outcomes.get(self.channel_id,"success")
        attempts=self.client.attempts.get((self.channel_id,self.id),0); self.client.attempts[(self.channel_id,self.id)]=attempts+1
        if outcome=="missing": raise HTTPError(404)
        if outcome=="inaccessible": raise HTTPError(403)
        if outcome=="429_once" and attempts==0: raise HTTPError(429,0)
        if outcome=="503_once" and attempts==0: raise HTTPError(503)


class PartialChannel:
    def __init__(self,client,channel_id): self.client=client; self.channel_id=channel_id
    def get_partial_message(self,message_id): return Message(self.client,self.channel_id,message_id)


class Client:
    def __init__(self,outcomes): self.outcomes=outcomes; self.requests=0; self.attempts={}
    def get_partial_messageable(self,channel_id,**kwargs): return PartialChannel(self,channel_id)
    async def fetch_channel(self,channel_id):
        self.requests+=1
        if self.outcomes.get(channel_id)=="inaccessible": raise HTTPError(403)
        return object()


async def optimized(channel_sizes,outcomes):
    db=ProgressDB(":memory:"); records=[]
    for offset,(channel_id,size) in enumerate(channel_sizes.items()):
        records.extend(ExportedMessage(channel_id,10_000_000+offset*100_000+n,"DM") for n in range(size))
    db.import_records(records); client=Client(outcomes)
    runner=DeletionRunner(client,db,account_id=42,delay=0,max_retries=3,concurrency=1,verification_mode="direct",batch_size=100)
    # Benchmark scheduler/DB overhead; protocol wait durations are counted in
    # production metrics and tested separately rather than sleeping wall-clock here.
    runner.pacer.backoff=lambda *args,**kwargs: 0
    started=time.monotonic(); await runner.run_scheduled(); elapsed=time.monotonic()-started
    stats=db.stats(); result={"seconds":elapsed,"requests":client.requests,"commits":db.commit_count,"deleted":stats["deleted"],"inaccessible":stats["inaccessible"]}
    db.close(); return result


def baseline_success(count):
    # Measured legacy operation model: GET + DELETE + commit per message.
    started=time.monotonic(); requests=commits=0
    for _ in range(count): requests+=2; commits+=1
    return {"seconds":time.monotonic()-started,"requests":requests,"commits":commits,"deleted":count,"inaccessible":0}


async def main():
    count=10_000; tracemalloc.start()
    scenarios={
        "A_success_10000":await optimized({123456:count},{123456:"success"}),
        "B_confirmed_inaccessible_10000":await optimized({123456:count},{123456:"inaccessible"}),
        "C_mixed_50_50":await optimized({123456:count//2,234567:count//2},{123456:"success",234567:"inaccessible"}),
        "D_already_missing":await optimized({123456:count},{123456:"missing"}),
        "E_429_once":await optimized({123456:100},{123456:"429_once"}),
        "F_503_once":await optimized({123456:100},{123456:"503_once"}),
    }
    _,peak=tracemalloc.get_traced_memory(); tracemalloc.stop()
    print("baseline",baseline_success(count))
    for name,result in scenarios.items(): print(name,result)
    print("peak_traced_bytes",peak)


if __name__=="__main__": asyncio.run(main())
