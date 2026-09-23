"""media 物化:临时目录+原子 rename / 防符号链接 / 配额 / 幂等复用 / 写失败 fail-closed。"""
import os
import pathlib

import pytest

from tests.conftest import CHAT, OWNER
from tests.helpers import mget_snapshot, ok_envelope, err_envelope
from lib import media
from lib.media import MediaError


BID = "bind-1"
MID = "om_m"


def dl_responder(env, files, snap=None):
    snap = snap or mget_snapshot(MID, CHAT, OWNER, msg_type="file")

    def fn(args, cwd):
        d = pathlib.Path(cwd) / "lark-im-resources"
        d.mkdir(parents=True, exist_ok=True)
        for name, data in files.items():
            p = d / name
            if data is None:  # 符号链接注入
                os.symlink("/etc/hosts", p)
            else:
                p.write_bytes(data)
        return ok_envelope({"messages": [snap]})

    env.runner.on(lambda a: "--download-resources" in a, fn)


def test_materialize_success_atomic(env):
    dl_responder(env, {"a.pdf": b"x" * 10, "b.txt": b"y"})
    paths = media.materialize(env.runner, env.media_root, BID, MID)
    assert paths is not None and len(paths) == 2
    for p in paths:
        assert os.path.isabs(p) and os.path.exists(p)
        assert str(env.media_root / BID / MID) in p
    # 无临时目录残留
    leftovers = [x for x in os.listdir(env.media_root / BID) if x.startswith(".tmp")]
    assert leftovers == []


def test_materialize_idempotent_reuse(env):
    dl_responder(env, {"a.pdf": b"x"})
    p1 = media.materialize(env.runner, env.media_root, BID, MID)
    calls_before = len(env.runner.calls)
    p2 = media.materialize(env.runner, env.media_root, BID, MID)
    assert p1 == p2
    assert len(env.runner.calls) == calls_before  # 复用,不再下载


def test_symlink_rejected(env):
    dl_responder(env, {"evil": None})
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, BID, MID)
    assert not (env.media_root / BID / MID).exists()


def test_quota_exceeded(env):
    dl_responder(env, {"big.bin": b"z" * 1000})
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, BID, MID, quota_bytes=100)
    assert not (env.media_root / BID / MID).exists()


def test_transient_download_failure_returns_none(env):
    env.runner.on(lambda a: "--download-resources" in a, lambda a, c: err_envelope(500))
    assert media.materialize(env.runner, env.media_root, BID, MID) is None


def test_write_failure_fail_closed(env, monkeypatch):
    """ENOSPC 模拟:rename 失败 → MediaError,无半成品 dest。"""
    dl_responder(env, {"a.pdf": b"x"})
    real_replace = os.replace

    def boom(src, dst):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "replace", boom)
    monkeypatch.setattr(os, "rename", boom)
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, BID, MID)
    monkeypatch.setattr(os, "replace", real_replace)
    assert not (env.media_root / BID / MID).exists()


def test_path_traversal_names_sanitized(env):
    dl_responder(env, {"a.pdf": b"x"})

    # 注入带路径分隔符的文件名(下载器不会这么干,纵深防御)
    def fn(args, cwd):
        d = pathlib.Path(cwd) / "lark-im-resources" / "sub"
        d.mkdir(parents=True, exist_ok=True)
        (d / "deep.txt").write_bytes(b"k")
        return ok_envelope({"messages": []})

    env.runner.responders.insert(0, (lambda a: "--download-resources" in a, fn))
    paths = media.materialize(env.runner, env.media_root, BID, "om_m2")
    assert paths is not None
    for p in paths:
        assert pathlib.Path(p).parent == env.media_root / BID / "om_m2"  # 拍平,无穿越


def test_dot_segment_ids_rejected(env):
    for bad in (".", "..", ".hidden"):
        with pytest.raises(MediaError):
            media.materialize(env.runner, env.media_root, bad, "om_x")
        with pytest.raises(MediaError):
            media.materialize(env.runner, env.media_root, "bind-ok", bad)
    assert env.runner.calls == []  # 拒绝发生在任何下载之前


def test_realpath_escape_via_symlinked_binding_dir(env, tmp_path):
    outside = tmp_path / "outside-victim"
    outside.mkdir()
    (env.media_root).mkdir(parents=True, exist_ok=True)
    os.symlink(outside, env.media_root / "bind-link")
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, "bind-link", "om_x")
    assert env.runner.calls == []


def test_symlinked_dest_inside_root_rejected(env):
    """r2-m3:dest 已存在但是 symlink(即使指向 root 内)→ 拒绝复用。"""
    real = env.media_root / "bind-real" / "om_real"
    real.mkdir(parents=True)
    (real / "f.bin").write_bytes(b"x")
    linkdir = env.media_root / "bind-a"
    linkdir.mkdir(parents=True)
    os.symlink(real, linkdir / "om_link")
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, "bind-a", "om_link")
    assert env.runner.calls == []  # 任何下载之前拒绝


def test_symlinked_parent_inside_root_rejected(env):
    """r2-m3:binding 目录本身是 symlink(指向 root 内他处)→ 拒绝(mkdir 后 lstat 校验)。"""
    other = env.media_root / "bind-b"
    other.mkdir(parents=True)
    os.symlink(other, env.media_root / "bind-linked")
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, "bind-linked", "om_x")
    assert env.runner.calls == []


def test_materialize_transient_failure_logged(env):
    """r3-6:下载瞬态失败留原始现场。"""
    env.runner.on(lambda a: "--download-resources" in a, lambda a, c: err_envelope(500))
    logs = []
    assert media.materialize(env.runner, env.media_root, "bind-log", "om_log",
                             log=logs.append) is None
    joined = "\n".join(logs)
    assert "download" in joined and "rc=1" in joined


def test_no_resources_downloaded_logged(env):
    """r4-4:下载 ok:true 但没有资源文件 → MediaError 前记原因。"""
    from tests.helpers import ok_envelope
    logs = []

    def empty_dl(args, cwd):
        import pathlib as _pl
        (_pl.Path(cwd) / "lark-im-resources").mkdir(parents=True, exist_ok=True)
        return ok_envelope({"messages": []})

    env.runner.on(lambda a: "--download-resources" in a, empty_dl)
    with pytest.raises(MediaError):
        media.materialize(env.runner, env.media_root, "bind-x", "om_empty", log=logs.append)
    joined = "\n".join(logs)
    assert "om_empty" in joined and "no resource" in joined
