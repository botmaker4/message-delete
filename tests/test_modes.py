import argparse

import pytest

from discord_cleaner.config import Settings
from discord_cleaner.models import ExportedMessage
from discord_cleaner.progress_db import ProgressDB
from main import selected_mode


def namespace(**overrides):
    values={"scan":False,"retry_failed":False,"stats":False,"diagnose":False,
            "audit_inaccessible":False,"recheck_inaccessible":False,
            "authorize_inaccessible_retry":False,"resume":False}
    values.update(overrides); return argparse.Namespace(**values)


@pytest.mark.parametrize("mode",["execute","diagnose","audit","recheck","stats"])
def test_cleaner_mode_environment_dispatch(monkeypatch,tmp_path,mode):
    monkeypatch.setenv("CLEANER_MODE",mode)
    settings=Settings.load(tmp_path/"missing.env")
    assert settings.cleaner_mode==mode
    assert selected_mode(namespace(),settings)==mode


def test_explicit_cli_action_overrides_environment(monkeypatch,tmp_path):
    monkeypatch.setenv("CLEANER_MODE","diagnose")
    settings=Settings.load(tmp_path/"missing.env")
    assert selected_mode(namespace(stats=True),settings)=="stats"
    assert selected_mode(namespace(resume=True),settings)=="execute"


def test_invalid_cleaner_mode_is_rejected(monkeypatch,tmp_path):
    monkeypatch.setenv("CLEANER_MODE","delete-everything")
    with pytest.raises(ValueError,match="CLEANER_MODE"):
        Settings.load(tmp_path/"missing.env")


def test_unchanged_indexed_package_does_not_reset_progress(tmp_path):
    with ProgressDB(tmp_path/"progress.sqlite3") as db:
        records=[ExportedMessage(123456,700000+n,"DM") for n in range(3)]
        db.import_records(records); db.register_package("abc",42,True,3)
        first=db.select_work()[0]; db.record(first,"deleted"); db.flush()
        assert db.indexed_package_count("abc")==3
        assert db.stats()["deleted"]==1
        assert db.package_verified_for(42,"abc") is True


@pytest.mark.parametrize("mode",["stats","diagnose","audit"])
def test_nonnetwork_environment_modes_run_from_plain_main(monkeypatch,tmp_path,mode):
    import main
    monkeypatch.setattr("sys.argv",["main.py"])
    monkeypatch.setenv("CLEANER_MODE",mode)
    monkeypatch.setenv("PROGRESS_DB",str(tmp_path/f"{mode}.sqlite3"))
    monkeypatch.delenv("DISCORD_USER_TOKEN",raising=False)
    assert main.main()==0


def test_recheck_environment_mode_dispatches_without_cli_flag(monkeypatch,tmp_path):
    import main
    called=[]
    async def fake_recheck(settings,db): called.append(settings.cleaner_mode)
    monkeypatch.setattr(main,"recheck_inaccessible",fake_recheck)
    monkeypatch.setattr("sys.argv",["main.py"])
    monkeypatch.setenv("CLEANER_MODE","recheck")
    monkeypatch.setenv("PROGRESS_DB",str(tmp_path/"recheck.sqlite3"))
    assert main.main()==0
    assert called==["recheck"]


def test_execute_environment_mode_scans_then_dispatches(monkeypatch,tmp_path):
    import main
    called=[]
    class Package:
        def __init__(self,path): pass
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def fingerprint(self): return "fingerprint"
        def ownership_evidence(self,account_id): return True,"test evidence"
        def records(self): return iter([ExportedMessage(123456,777777,"DM")])
    async def fake_execute(settings,db,args,*provenance): called.append((settings.cleaner_mode,db.stats()["pending"]))
    monkeypatch.setattr(main,"DataPackage",Package); monkeypatch.setattr(main,"execute",fake_execute)
    monkeypatch.setattr("sys.argv",["main.py"]); monkeypatch.setenv("CLEANER_MODE","execute")
    monkeypatch.setenv("PROGRESS_DB",str(tmp_path/"execute.sqlite3")); monkeypatch.setenv("MESSAGES_PATH","unused.zip")
    monkeypatch.setenv("EXPECTED_ACCOUNT_ID","42")
    assert main.main()==0
    assert called==[("execute",1)]
