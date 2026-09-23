"""media 物化(contracts §5.3 / §6 父进程侧):零网络本地判定(file_plan / skip_reason / describe_files)、
子进程协议映射(假 worker:rc 0/2/3/4/5/124/125/异常 rc/结果不一致/符号链接/挂起)、
只有父进程发布(tmp → 原子 rename)、幂等复用、路径安全(id/realpath/symlink)、配额。"""
import os
import pathlib
import time

import pytest

from lib import constants, media
from lib.media import MediaError
from tests.conftest import CHAT
from tests.helpers import slack_file

FAKE_WORKER = str(pathlib.Path(__file__).resolve().parent / "fake_worker.py")
BID = "bind-1"
MID = "%s:1700000000.000100" % CHAT
TOKENS = {"bot_token": "xoxb-test"}


def f_ok(id="F1", name="a.pdf", n=4, **kw):
    return slack_file(id=id, name=name, url_private_download="https://files.slack.com/ok/%d" % n, **kw)


def f_rc(code, id="F2", name="b.bin"):
    return slack_file(id=id, name=name, url_private_download="https://files.slack.com/rc/%d" % code)


def mat(env, files, **kw):
    kw.setdefault("worker_path", FAKE_WORKER)
    kw.setdefault("binding_id", BID)
    kw.setdefault("message_id", MID)
    return media.materialize(TOKENS, env.media_root, kw.pop("binding_id"), kw.pop("message_id"),
                             files, **kw)


def tmp_leftovers(env, bid=BID):
    d = env.media_root / bid
    return [x for x in os.listdir(d) if x.startswith(".tmp")] if d.exists() else []


# ---------------------------------------------------------------- 零网络本地判定
class TestFilePlan:
    def test_skip_reasons_matrix(self):
        assert media.skip_reason(slack_file(mode="tombstone")) == "tombstone"
        assert media.skip_reason(slack_file(hidden_by_limit=True)) == "hidden_by_limit"
        assert media.skip_reason(dict(slack_file(), file_access="check_file_info")) == "check_file_info"
        nourl = slack_file()
        nourl.pop("url_private")
        nourl.pop("url_private_download")
        assert media.skip_reason(nourl) == "no_url"
        assert media.skip_reason(slack_file(size=constants.MEDIA_FILE_MAX_BYTES + 1)) == "too_large"
        assert media.skip_reason(slack_file()) is None
        assert media.skip_reason("garbage") == "no_url"
        for r in ("tombstone", "hidden_by_limit", "check_file_info", "no_url", "too_large"):
            assert r in constants.FILE_SKIP_REASONS

    def test_plan_quota_and_names(self):
        big = constants.MEDIA_MSG_QUOTA_BYTES // 2 + 1
        files = [slack_file(id="A", name="x.bin", size=big), slack_file(id="B", name="x.bin", size=big),
                 slack_file(id="C", name="../evil", size=1), slack_file(id="D", name=".hidden", size=1),
                 slack_file(id="E", name="", size=1)]
        plan = media.file_plan(files)
        by = {e["id"]: e for e in plan}
        assert by["A"]["skip"] is None and by["B"]["skip"] == "too_large"   # 累计超配额
        for k in ("C", "D"):   # 拍平:无分隔符、不以点开头(与 .tmp 约定冲突)
            assert "/" not in by[k]["dest_name"] and not by[k]["dest_name"].startswith(".")
        assert by["D"]["dest_name"] == "hidden"
        assert by["E"]["dest_name"] == "file4"
        assert media.needs_download(files) is True
        assert media.needs_download([slack_file(mode="tombstone")]) is False
        assert media.needs_download([]) is False

    def test_dedupe_names(self):
        plan = media.file_plan([slack_file(id="A", name="a.pdf"), slack_file(id="B", name="a.pdf")])
        assert [e["dest_name"] for e in plan] == ["a.pdf", "1-a.pdf"]

    def test_describe_files_maps_paths_and_skips(self, env):
        files = [slack_file(id="A", name="a.pdf", size=10), slack_file(id="B", name="z.zip", mode="tombstone")]
        out = media.describe_files(files, ["/x/y/a.pdf"])
        assert out[0] == {"id": "A", "name": "a.pdf", "mimetype": "application/pdf", "size": 10,
                          "local_path": "/x/y/a.pdf"}
        assert out[1]["skipped_reason"] == "tombstone" and "local_path" not in out[1]
        # 找不到对应路径(不应发生)→ 保守记 skipped
        assert media.describe_files(files, [])[0]["skipped_reason"] == "no_url"


# ---------------------------------------------------------------- 子进程协议映射
class TestMaterialize:
    def test_success_publishes_atomically_only_parent(self, env):
        paths, skipped = mat(env, [f_ok(n=4), f_ok(id="F2", name="b.txt", n=1),
                                   slack_file(id="F3", name="gone", mode="tombstone")])
        assert [os.path.basename(p) for p in paths] == ["a.pdf", "b.txt"]
        for p in paths:
            assert os.path.isabs(p) and os.path.isfile(p) and not os.path.islink(p)
            assert str(env.media_root / BID / MID) + os.sep in p
        assert os.path.getsize(paths[0]) == 4
        assert skipped == [{"id": "F3", "name": "gone", "skipped_reason": "tombstone"}]
        assert tmp_leftovers(env) == []                       # 无 .tmp 残留

    def test_empty_or_all_skipped_needs_no_worker(self, env):
        assert mat(env, []) == ([], [])
        paths, skipped = mat(env, [slack_file(id="H", hidden_by_limit=True)], worker_path="/nonexistent")
        assert paths == [] and skipped[0]["skipped_reason"] == "hidden_by_limit"
        assert not (env.media_root / BID).exists()

    def test_idempotent_reuse_no_new_download(self, env):
        p1 = mat(env, [f_ok()])
        p2 = mat(env, [f_ok()], worker_path="/nonexistent-would-fail")
        assert p1 == p2

    @pytest.mark.parametrize("code", [2, 3])
    def test_permanent_rc_raises_media_error(self, env, code):
        with pytest.raises(MediaError):
            mat(env, [f_rc(code)])
        assert not (env.media_root / BID / MID).exists() and tmp_leftovers(env) == []

    @pytest.mark.parametrize("code", [4, 5, 124, 125])
    def test_transient_rc_returns_none(self, env, code):
        stats = {}
        assert mat(env, [f_ok(), f_rc(code)], stats=stats) is None
        assert stats == {}                                    # §6 列内退出码不计 unexpected
        assert not (env.media_root / BID / MID).exists() and tmp_leftovers(env) == []

    def test_unexpected_rc_counts(self, env):
        stats = {}
        assert mat(env, [f_rc(7)], stats=stats) is None
        assert stats == {"worker_unexpected_exit": 1}

    def test_inconsistent_size_is_transient_not_published(self, env):
        stats = {}
        f = slack_file(id="S", name="s.bin", url_private_download="https://files.slack.com/badsize")
        assert mat(env, [f], stats=stats) is None
        assert stats.get("worker_unexpected_exit") == 1
        assert not (env.media_root / BID / MID).exists()

    def test_rc0_without_json_is_transient(self, env):
        f = slack_file(id="N", name="n.bin", url_private_download="https://files.slack.com/nojson")
        assert mat(env, [f]) is None

    def test_worker_symlink_rejected(self, env):
        f = slack_file(id="L", name="l.bin", url_private_download="https://files.slack.com/symlink")
        assert mat(env, [f]) is None                          # 不是普通文件 → 结果不一致 → 不发布
        assert not (env.media_root / BID / MID).exists()

    def test_parent_deadline_kills_hung_worker(self, env):
        f = slack_file(id="H", name="h.bin", url_private_download="https://files.slack.com/hang")
        logs = []
        t0 = time.monotonic()
        assert mat(env, [f], deadline_s=0.5, log=logs.append) is None
        assert time.monotonic() - t0 < 5
        assert any("parent deadline" in l for l in logs)
        assert tmp_leftovers(env) == []

    def test_no_token_is_transient(self, env):
        assert media.materialize({}, env.media_root, BID, MID, [f_ok()], worker_path=FAKE_WORKER) is None
        assert media.materialize(None, env.media_root, BID, MID, [f_ok()], worker_path=FAKE_WORKER) is None

    def test_quota_exceeded_after_download(self, env):
        with pytest.raises(MediaError):
            mat(env, [f_ok(n=1000, size=10)], quota_bytes=100)
        assert not (env.media_root / BID / MID).exists() and tmp_leftovers(env) == []

    def test_spawn_failure_is_transient(self, env):
        assert mat(env, [f_ok()], python="/nonexistent/python") is None

    def test_token_passed_via_stdin_not_argv(self, env, monkeypatch):
        import subprocess as sp
        seen = {}
        real = sp.Popen

        class Rec(real):
            def __init__(self, argv, **kw):
                seen["argv"] = list(argv)
                super().__init__(argv, **kw)

            def communicate(self, input=None, timeout=None):
                seen["stdin"] = input
                return super().communicate(input=input, timeout=timeout)

        monkeypatch.setattr(media.subprocess, "Popen", Rec)
        mat(env, [f_ok()])
        assert "xoxb-test" not in " ".join(seen["argv"])
        assert b"xoxb-test" in seen["stdin"]


# ---------------------------------------------------------------- 路径安全(继承)
class TestPathSafety:
    def test_dot_segment_ids_rejected(self, env):
        for bad in (".", "..", ".hidden", "a/b"):
            with pytest.raises(MediaError):
                media.materialize(TOKENS, env.media_root, bad, MID, [f_ok()], worker_path=FAKE_WORKER)
            with pytest.raises(MediaError):
                media.materialize(TOKENS, env.media_root, "bind-ok", bad, [f_ok()], worker_path=FAKE_WORKER)

    def test_realpath_escape_via_symlinked_binding_dir(self, env, tmp_path):
        outside = tmp_path / "outside-victim"
        outside.mkdir()
        env.media_root.mkdir(parents=True, exist_ok=True)
        os.symlink(outside, env.media_root / "bind-link")
        with pytest.raises(MediaError):
            media.materialize(TOKENS, env.media_root, "bind-link", MID, [f_ok()], worker_path=FAKE_WORKER)
        assert os.listdir(outside) == []

    def test_symlinked_dest_inside_root_rejected(self, env):
        real = env.media_root / "bind-real" / "m_real"
        real.mkdir(parents=True)
        (real / "f.bin").write_bytes(b"x")
        linkdir = env.media_root / "bind-a"
        linkdir.mkdir(parents=True)
        os.symlink(real, linkdir / "m_link")
        with pytest.raises(MediaError):
            media.materialize(TOKENS, env.media_root, "bind-a", "m_link", [f_ok()], worker_path=FAKE_WORKER)

    def test_symlinked_parent_inside_root_rejected(self, env):
        other = env.media_root / "bind-b"
        other.mkdir(parents=True)
        os.symlink(other, env.media_root / "bind-linked")
        with pytest.raises(MediaError):
            media.materialize(TOKENS, env.media_root, "bind-linked", MID, [f_ok()], worker_path=FAKE_WORKER)

    def test_write_failure_fail_closed(self, env, monkeypatch):
        real_rename = os.rename

        def boom(src, dst):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(media.os, "rename", boom)
        with pytest.raises(MediaError):
            mat(env, [f_ok()])
        monkeypatch.setattr(media.os, "rename", real_rename)
        assert not (env.media_root / BID / MID).exists() and tmp_leftovers(env) == []
