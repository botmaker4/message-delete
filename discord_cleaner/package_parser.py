from __future__ import annotations

import json
import hashlib
import re
import zipfile
from collections.abc import Iterator
from contextlib import AbstractContextManager
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from .models import ExportedMessage


CHANNEL_DIR = re.compile(r"^c([1-9][0-9]{5,24})$")
SNOWFLAKE = re.compile(r"^[1-9][0-9]{5,24}$")


class PackageError(ValueError):
    pass


class DataPackage(AbstractContextManager["DataPackage"]):
    """Read a Discord Messages directory or ZIP without extracting it."""

    def __init__(self, source: str | Path):
        self.source = Path(source)
        self._zip: zipfile.ZipFile | None = None
        self._root = self.source
        if self.source.is_file():
            if not zipfile.is_zipfile(self.source):
                raise PackageError(f"Not a ZIP archive: {self.source}")
            self._zip = zipfile.ZipFile(self.source)
            names = self._zip.namelist()
            roots = [n[: -len("index.json")] for n in names if n.endswith("Messages/index.json")]
            if not roots and "index.json" in names:
                roots = [""]
            if len(roots) != 1:
                self.close()
                raise PackageError("ZIP must contain exactly one Messages/index.json")
            self._zip_prefix = roots[0]
        else:
            if self.source.name != "Messages" and (self.source / "Messages").is_dir():
                self._root = self.source / "Messages"
            if not (self._root / "index.json").is_file():
                raise PackageError(f"Messages/index.json not found under {self.source}")

    def close(self) -> None:
        if self._zip is not None:
            self._zip.close()

    def fingerprint(self) -> str:
        """Return a stable local fingerprint used to bind an import to an account."""
        digest = hashlib.sha256()
        if self.source.is_file():
            with self.source.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        for path in sorted(self._root.rglob("*.json")):
            digest.update(path.relative_to(self._root).as_posix().encode())
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        return digest.hexdigest()

    def __exit__(self, *args: object) -> None:
        self.close()

    def _open(self, relative: str) -> BinaryIO:
        if self._zip is not None:
            return self._zip.open(self._zip_prefix + relative)
        return (self._root / relative).open("rb")

    def _json(self, relative: str) -> object:
        try:
            with self._open(relative) as handle:
                return json.load(handle)
        except (OSError, KeyError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PackageError(f"Cannot read valid JSON from {relative}: {exc}") from exc

    def _channel_dirs(self) -> list[str]:
        if self._zip is not None:
            result = set()
            for name in self._zip.namelist():
                p = PurePosixPath(name)
                if name.startswith(self._zip_prefix) and p.name == "messages.json":
                    dirname = p.parent.name
                    if CHANNEL_DIR.fullmatch(dirname):
                        result.add(dirname)
            return sorted(result, key=lambda value: int(value[1:]))
        return sorted(
            (p.name for p in self._root.iterdir() if p.is_dir() and CHANNEL_DIR.fullmatch(p.name)),
            key=lambda value: int(value[1:]),
        )

    def records(self) -> Iterator[ExportedMessage]:
        index = self._json("index.json")
        if not isinstance(index, dict):
            raise PackageError("index.json must be an object")
        seen: set[tuple[int, int]] = set()
        for dirname in self._channel_dirs():
            folder_id = dirname[1:]
            channel = self._json(f"{dirname}/channel.json")
            messages = self._json(f"{dirname}/messages.json")
            if not isinstance(channel, dict) or not isinstance(messages, list):
                raise PackageError(f"Invalid schema in {dirname}")
            raw_id = str(channel.get("id", ""))
            if not SNOWFLAKE.fullmatch(raw_id) or raw_id != folder_id:
                raise PackageError(f"Channel ID mismatch in {dirname}")
            channel_id = int(raw_id)
            kind = str(channel.get("type", "UNKNOWN")).upper()
            guild = channel.get("guild")
            guild_id_raw = guild.get("id") if isinstance(guild, dict) else None
            guild_id = int(guild_id_raw) if str(guild_id_raw).isdigit() else None
            label = str(index.get(raw_id, ""))
            for offset, item in enumerate(messages):
                if not isinstance(item, dict) or not SNOWFLAKE.fullmatch(str(item.get("ID", ""))):
                    raise PackageError(f"Invalid message at {dirname}/messages.json index {offset}")
                message_id = int(item["ID"])
                key = (channel_id, message_id)
                if key in seen:
                    continue
                seen.add(key)
                yield ExportedMessage(channel_id, message_id, kind, guild_id, label)

    def ownership_evidence(self, account_id: int) -> tuple[bool, str]:
        """Validate account evidence present in official DM recipient metadata.

        Message rows do not contain authors, so this is provenance evidence rather
        than a cryptographic guarantee. Packages without evidence must use fetch mode.
        """
        evidenced = 0; mismatches = 0
        for dirname in self._channel_dirs():
            channel = self._json(f"{dirname}/channel.json")
            if not isinstance(channel, dict) or str(channel.get("type", "")).upper() not in {"DM", "GROUP_DM"}:
                continue
            recipients = channel.get("recipients")
            numeric = {int(value) for value in recipients or [] if str(value).isdigit()}
            if numeric:
                evidenced += 1
                if account_id not in numeric: mismatches += 1
        if mismatches:
            return False, f"{mismatches} DM/group channel recipient lists do not contain EXPECTED_ACCOUNT_ID"
        if not evidenced:
            return False, "package has no numeric DM/group recipient evidence; use VERIFICATION_MODE=fetch"
        return True, f"EXPECTED_ACCOUNT_ID appears consistently in {evidenced} DM/group recipient lists"
