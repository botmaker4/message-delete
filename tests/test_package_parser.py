import json
import zipfile

import pytest

from discord_cleaner.package_parser import DataPackage, PackageError


def make_package(tmp_path, messages, channel=None):
    path = tmp_path / "data.zip"
    channel = channel or {"id": "123456", "type": "DM"}
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("Messages/index.json", json.dumps({"123456": "label"}))
        z.writestr("Messages/c123456/channel.json", json.dumps(channel))
        z.writestr("Messages/c123456/messages.json", json.dumps(messages))
    return path


def test_zip_parsing_and_deduplication(tmp_path):
    path = make_package(tmp_path, [{"ID": 777777}, {"ID": "777777"}])
    with DataPackage(path) as package: records = list(package.records())
    assert [(r.channel_id, r.message_id, r.channel_type, r.label) for r in records] == [(123456, 777777, "DM", "label")]


@pytest.mark.parametrize("kind", ["DM", "GROUP_DM", "GUILD_TEXT", "PUBLIC_THREAD", "PRIVATE_THREAD", "GUILD_VOICE"])
def test_channel_classification(tmp_path, kind):
    path = make_package(tmp_path, [{"ID": "777777"}], {"id": "123456", "type": kind, "guild": {"id": "888888"}})
    with DataPackage(path) as package: record = next(package.records())
    assert record.channel_type == kind
    assert record.guild_id == 888888


@pytest.mark.parametrize("messages", [[{"ID": "bad"}], {"ID": "777777"}])
def test_malformed_entries_rejected(tmp_path, messages):
    path = make_package(tmp_path, messages)
    with DataPackage(path) as package:
        with pytest.raises(PackageError): list(package.records())


def test_extracted_messages_directory(tmp_path):
    root = tmp_path / "Messages"; channel = root / "c123456"; channel.mkdir(parents=True)
    (root / "index.json").write_text('{"123456":"x"}', encoding="utf-8")
    (channel / "channel.json").write_text('{"id":"123456","type":"DM"}', encoding="utf-8")
    (channel / "messages.json").write_text('[{"ID":"777777"}]', encoding="utf-8")
    with DataPackage(root) as package: assert len(list(package.records())) == 1
