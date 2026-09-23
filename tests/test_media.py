"""media 物化(contracts §5.3 / §6 父进程侧):零网络本地判定(file_plan / skip_reason / describe_files)、
子进程协议映射(假 worker:rc 0/2/3/4/5/124/125/异常 rc/结果不一致/符号链接/挂起)、
只有父进程发布(tmp → 原子 rename)、幂等复用、路径安全(id/realpath/symlink)、配额。"""
import json
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
        assert by["C"]["dest_name"] == "f03-C-_evil"            # 序号 + id 前缀(R1-M7);前导点已剥
        assert by["D"]["dest_name"] == "f04-D-hidden"
        assert by["E"]["dest_name"] == "f05-E-file4"
        assert media.needs_download(files) is True
        assert media.needs_download([slack_file(mode="tombstone")]) is False
        assert media.needs_download([]) is False

    def test_dedupe_names(self):
        plan = media.file_plan([slack_file(id="A", name="a.pdf"), slack_file(id="B", name="a.pdf")])
        assert [e["dest_name"] for e in plan] == ["f01-A-a.pdf", "f02-B-a.pdf"]

    def test_dest_names_unique_even_when_input_names_mimic_dedupe_suffix(self):
        """R1-M7:输入 a.txt / 2-a.txt / a.txt 旧实现得 a.txt / 2-a.txt / **2-a.txt**(第三个撞第二个,
        worker O_EXCL 失败 → 永久 MediaError → 整条消息含正文永久不投递)。
        现在名字 = f<idx>-<id>-<name>:确定性、同一消息内两两不同;id 相同/为空也不撞。"""
        files = [slack_file(id="A", name="a.txt"), slack_file(id="B", name="2-a.txt"),
                 slack_file(id="C", name="a.txt")]
        names = [e["dest_name"] for e in media.file_plan(files)]
        assert names == ["f01-A-a.txt", "f02-B-2-a.txt", "f03-C-a.txt"]
        assert len(set(names)) == 3
        # 极端:id 全空 / 全相同 / 名字全相同 / 含分隔符与点 —— 仍两两不同且确定
        weird = [slack_file(id="", name="x"), slack_file(id="", name="x"), slack_file(id="F/../", name="../x"),
                 slack_file(id="F/../", name="../x")]
        for f in weird:
            f["id"] = f["id"] or None
        names = [e["dest_name"] for e in media.file_plan(weird)]
        assert len(set(names)) == 4 and names == [e["dest_name"] for e in media.file_plan(weird)]
        assert all("/" not in n and not n.startswith(".") and len(n) <= media.NAME_MAX for n in names)
        # 超长名字:前缀 + 截断后的名字仍 ≤ NAME_MAX,扩展名保留
        longn = media.dest_name_for({"id": "F" * 100, "name": "n" * 500 + ".pdf"}, 0)
        assert len(longn) <= media.NAME_MAX and longn.endswith(".pdf") and longn.startswith("f01-")
        # seen 循环:即便前缀相同(人为构造)也会追加 -2/-3
        seen = {"f01-A-a.txt", "f01-A-a-2.txt"}
        assert media.dest_name_for({"id": "A", "name": "a.txt"}, 0, seen) == "f01-A-a-3.txt"

    def test_materialize_three_files_with_colliding_names_all_published(self, env):
        """R1-M7 端到端:三个文件(a.txt / 2-a.txt / a.txt)经假 worker 全部落盘,payload 三条 local_path 各不相同。"""
        files = [f_ok(id="A", name="a.txt", n=1), f_ok(id="B", name="2-a.txt", n=2), f_ok(id="C", name="a.txt", n=3)]
        paths, skipped = mat(env, files)
        assert [os.path.basename(p) for p in paths] == ["f01-A-a.txt", "f02-B-2-a.txt", "f03-C-a.txt"]
        assert [os.path.getsize(p) for p in paths] == [1, 2, 3] and skipped == []
        desc = media.describe_files(files, paths)
        assert [d["local_path"] for d in desc] == paths

    def test_describe_files_maps_paths_and_skips(self, env):
        files = [slack_file(id="A", name="a.pdf", size=10), slack_file(id="B", name="z.zip", mode="tombstone")]
        out = media.describe_files(files, ["/x/y/f01-A-a.pdf"])
        assert out[0] == {"id": "A", "name": "a.pdf", "mimetype": "application/pdf", "size": 10,
                          "local_path": "/x/y/f01-A-a.pdf"}
        assert out[1]["skipped_reason"] == "tombstone" and "local_path" not in out[1]
        # 找不到对应路径(不应发生)→ 保守记 skipped
        assert media.describe_files(files, [])[0]["skipped_reason"] == "no_url"

    def test_describe_files_falls_back_to_legacy_published_names(self, env):
        """R2-m1:R1-M7 之前发布的目录用「净化原名 / 同名加 `<n>-`」落盘;升级后 dest_name 是
        `f<idx>-<id>-<name>`,只按新名反查会把旧目录里的文件错标 no_url(media_paths 却仍带旧路径)。
        新名缺席时回落到净化后的原名(及旧去重形态 `<n>-原名`)匹配,每个路径只认领一次。"""
        files = [slack_file(id="A", name="a.pdf", size=10), slack_file(id="B", name="a.pdf", size=11),
                 slack_file(id="C", name="c.txt", size=12), slack_file(id="D", name="z.bin", size=13)]
        legacy = ["/x/y/a.pdf", "/x/y/2-a.pdf", "/x/y/c.txt", "/x/y/z.bin"]
        out = media.describe_files(files, legacy)
        assert [d.get("local_path") for d in out] == legacy
        assert all("skipped_reason" not in d for d in out)
        # 新旧混合(部分按新名发布)各归其位:新名优先;同名文件不会都指向同一路径
        mixed = ["/x/y/f01-A-a.pdf", "/x/y/2-a.pdf", "/x/y/c.txt", "/x/y/z.bin"]
        assert [d["local_path"] for d in media.describe_files(files, mixed)] == mixed
        # 旧目录只有一份 a.pdf:第二个同名文件不能抢它,仍保守 no_url
        out = media.describe_files(files[:2], ["/x/y/a.pdf"])
        assert out[0]["local_path"] == "/x/y/a.pdf" and out[1]["skipped_reason"] == "no_url"
        # 完全无关 → 仍 no_url
        assert media.describe_files(files, ["/x/y/unrelated.bin"])[0]["skipped_reason"] == "no_url"


# ---------------------------------------------------------------- 子进程协议映射
class TestMaterialize:
    def test_success_publishes_atomically_only_parent(self, env):
        paths, skipped = mat(env, [f_ok(n=4), f_ok(id="F2", name="b.txt", n=1),
                                   slack_file(id="F3", name="gone", mode="tombstone")])
        assert [os.path.basename(p) for p in paths] == ["f01-F1-a.pdf", "f02-F2-b.txt"]
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

    def test_shared_deadline_across_files_clock_driven(self, env, monkeypatch):
        """R1-M6:deadline_s 是**整条消息**的共享绝对截止。假单调钟每次 spawn 前进 80s:
        3 个文件、deadline 90 → 第 1 个 worker 拿 90s,第 2 个只拿剩余 10s,第 3 个剩余 ≤ 0 → 不再 spawn、
        瞬态 None(走预算)。旧实现每个文件各拿完整 90s(3 次 spawn、timeout 都是 90)。
        心跳在每个文件之后都刷(含未 spawn 的第 3 个),两次 spawn 之间至少一次。"""
        import subprocess as sp
        clock = {"t": 1000.0}
        monkeypatch.setattr(media, "_monotonic", lambda: clock["t"])
        events = []
        real = sp.Popen

        class Rec(real):
            def __init__(self, argv, **kw):
                clock["t"] += 80.0                    # 这个 worker「耗时」80s
                events.append("spawn")
                super().__init__(argv, **kw)

            def communicate(self, input=None, timeout=None):
                events.append(("timeout_s", round(json.loads(input)["timeout_s"], 3)))
                return super().communicate(input=input, timeout=None)   # 真等假 worker(瞬间完成)

        monkeypatch.setattr(media.subprocess, "Popen", Rec)
        beats = []
        logs = []
        files = [f_ok(id="F1", name="a", n=1), f_ok(id="F2", name="b", n=1), f_ok(id="F3", name="c", n=1)]
        res = mat(env, files, deadline_s=90, heartbeat=lambda: beats.append(len(events)), log=logs.append)
        assert res is None                                            # 第 3 个文件超共享截止 → 瞬态
        assert events.count("spawn") == 2                             # 第 3 个根本不 spawn
        assert [e[1] for e in events if isinstance(e, tuple)] == [90.0, 10.0]   # 第 2 个只拿剩余
        assert any("deadline_before_start" in l for l in logs)
        assert len(beats) == 3 and beats[0] >= 2 and beats[1] >= 4      # 每个文件之后一次;两次 spawn 之间有心跳
        assert not (env.media_root / BID / MID).exists() and tmp_leftovers(env) == []

    def test_heartbeat_called_between_slow_files(self, env, monkeypatch):
        """真慢 worker(每个 0.15s):事件序列必须是 spawn → beat → spawn → beat(文件之间刷心跳)。"""
        import subprocess as sp
        events = []
        real = sp.Popen

        class Rec(real):
            def __init__(self, argv, **kw):
                events.append("spawn")
                super().__init__(argv, **kw)

        monkeypatch.setattr(media.subprocess, "Popen", Rec)
        files = [slack_file(id="S1", name="s1.bin", url_private_download="https://files.slack.com/slow/0.15/2"),
                 slack_file(id="S2", name="s2.bin", url_private_download="https://files.slack.com/slow/0.15/3")]
        paths, skipped = mat(env, files, deadline_s=10, heartbeat=lambda: events.append("beat"))
        assert events == ["spawn", "beat", "spawn", "beat"]
        assert [os.path.getsize(p) for p in paths] == [2, 3] and skipped == []

    def test_heartbeat_exception_does_not_break_materialize(self, env):
        def boom():
            raise RuntimeError("heartbeat exploded")
        paths, skipped = mat(env, [f_ok()], heartbeat=boom)
        assert len(paths) == 1

    def test_hung_threshold_covers_shared_deadline(self):
        """ctl.HUNG_THRESHOLD_MS 必须 > 整条消息的共享 deadline + SIGTERM→SIGKILL 宽限 + 余量:
        文件之间已刷心跳,两次心跳之间最长只有一个文件的 deadline。"""
        from lib import ctl
        worst_gap_ms = (constants.DOWNLOAD_DEADLINE_S + media.KILL_GRACE_S) * 1000
        assert ctl.HUNG_THRESHOLD_MS >= worst_gap_ms + 30_000

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
