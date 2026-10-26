from __future__ import annotations

from dataclasses import dataclass


VALID_CHANNEL_TYPES = frozenset(
    {"DM", "GROUP_DM", "GUILD_TEXT", "PUBLIC_THREAD", "PRIVATE_THREAD", "GUILD_VOICE"}
)


@dataclass(frozen=True, slots=True)
class ExportedMessage:
    channel_id: int
    message_id: int
    channel_type: str
    guild_id: int | None = None
    label: str = ""


@dataclass(slots=True)
class WorkItem:
    channel_id: int
    message_id: int
    channel_type: str
    guild_id: int | None
    attempts: int


@dataclass(slots=True)
class ChannelWorkload:
    channel_id: int
    channel_type: str
    guild_id: int | None
    pending_count: int
    accessibility_status: str
    success_count: int
    failure_count: int
