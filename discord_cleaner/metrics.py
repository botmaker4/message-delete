from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class PerformanceMetrics:
    started: float = field(default_factory=time.monotonic)
    api_requests: int = 0
    http_429: int = 0
    http_403: int = 0
    http_404: int = 0
    deleted: int = 0
    already_missing: int = 0
    confirmed_inaccessible: int = 0
    unverified_inaccessible: int = 0
    failed: int = 0
    classified: int = 0
    network_outcomes: int = 0
    channel_resolve_seconds: float = 0.0
    message_fetch_seconds: float = 0.0
    message_delete_seconds: float = 0.0
    api_wait_seconds: float = 0.0
    application_wait_seconds: float = 0.0
    sqlite_seconds: float = 0.0
    delete_samples: int = 0

    @property
    def elapsed(self) -> float:
        return max(0.001, time.monotonic() - self.started)

    @property
    def deletes_per_minute(self) -> float:
        return self.deleted * 60 / self.elapsed

    @property
    def requests_per_minute(self) -> float:
        return self.api_requests * 60 / self.elapsed

    @property
    def classifications_per_minute(self) -> float:
        return self.classified * 60 / self.elapsed

    @property
    def average_delete_ms(self) -> float:
        return self.message_delete_seconds * 1000 / max(1, self.delete_samples)

    def http_status(self, status: int | None) -> None:
        if status == 429: self.http_429 += 1
        elif status == 403: self.http_403 += 1
        elif status == 404: self.http_404 += 1
