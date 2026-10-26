from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return value


def _positive_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    token: str
    expected_account_id: int | None
    messages_path: Path
    database_path: Path
    delete_delay: float
    max_concurrent: int
    max_retries: int
    verification_mode: str
    channel_batch_size: int
    cleaner_mode: str

    @classmethod
    def load(cls, env_file: str | Path = ".env") -> "Settings":
        load_dotenv(env_file)
        account = os.getenv("EXPECTED_ACCOUNT_ID", "").strip()
        if account and not account.isdigit():
            raise ValueError("EXPECTED_ACCOUNT_ID must contain digits only")
        # Concurrency is deliberately capped: extra workers rarely improve Discord throughput.
        workers = _positive_int("MAX_CONCURRENT_DELETES", 1)
        if workers > 4:
            raise ValueError("MAX_CONCURRENT_DELETES cannot exceed the safety cap of 4")
        mode=os.getenv("VERIFICATION_MODE", "direct").strip().lower()
        if mode not in {"direct","fetch"}: raise ValueError("VERIFICATION_MODE must be direct or fetch")
        cleaner_mode=os.getenv("CLEANER_MODE", "execute").strip().lower()
        if cleaner_mode not in {"execute","diagnose","audit","recheck","stats"}:
            raise ValueError("CLEANER_MODE must be execute, diagnose, audit, recheck, or stats")
        return cls(
            token=os.getenv("DISCORD_USER_TOKEN", "").strip(),
            expected_account_id=int(account) if account else None,
            messages_path=Path(os.getenv("MESSAGES_PATH", "./package.zip")),
            database_path=Path(os.getenv("PROGRESS_DB", "./progress.sqlite3")),
            delete_delay=_positive_float("DELETE_DELAY_SECONDS", 0.0),
            max_concurrent=workers,
            max_retries=_positive_int("MAX_RETRIES", 5),
            verification_mode=mode,
            channel_batch_size=_positive_int("CHANNEL_BATCH_SIZE", 100),
            cleaner_mode=cleaner_mode,
        )
