"""media 物化:每 message 独立临时目录 → 校验(防符号链接/穿越/配额)→ 原子 rename(§3)。
返回:成功=绝对路径列表;瞬态失败=None(恢复工人按 deadline 重试);确定性失败=raise MediaError。"""
import os
import pathlib
import shutil
import uuid

from . import constants, runner as runner_mod


class MediaError(Exception):
    """确定性物化失败(符号链接/配额/文件系统错误/空结果)→ fail-closed。"""


def _existing(dest):
    return sorted(str(p) for p in dest.iterdir() if p.is_file() and not p.is_symlink())


def _safe_id(x):
    """修复项8:id 只作单层目录名;拒 '.'/'..'/点前缀(与 .tmp 约定冲突)/分隔符/NUL。"""
    s = str(x)
    if (not s or s in (".", "..") or s.startswith(".")
            or "/" in s or os.sep in s or (os.altsep and os.altsep in s)
            or "\x00" in s):
        raise MediaError(f"bad media path id: {s!r}")
    return s


def _log(log, msg):
    if log is None:
        return
    try:
        log(msg)
    except Exception:
        pass


def materialize(runner, media_root, binding_id, message_id,
                quota_bytes=None, timeout_s=constants.DOWNLOAD_TIMEOUT_S, log=None):
    if quota_bytes is None:
        quota_bytes = constants.MEDIA_MSG_QUOTA_BYTES
    media_root = pathlib.Path(media_root)
    dest = media_root / _safe_id(binding_id) / _safe_id(message_id)
    # 修复项8:realpath 收容 —— 目标解析后必须仍在 media root 之下(挡 symlink 逃逸)
    root_real = os.path.realpath(media_root)
    if not os.path.realpath(dest).startswith(root_real + os.sep):
        raise MediaError("media path escapes media root")
    # r2-m3:dest/parent 为 symlink(即使指向 root 内)一律拒绝(lstat 语义)
    if os.path.islink(dest) or os.path.islink(dest.parent):
        raise MediaError("symlinked media path rejected")
    if dest.is_dir():
        return _existing(dest)  # 幂等复用(此前成功过)
    tmp = None
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        # r2-m3:O_NOFOLLOW 语义 —— mkdir 后 lstat 复验(挡 mkdir 间隙被换成 symlink)
        if os.path.islink(dest.parent):
            raise MediaError("symlinked media parent rejected")
        tmp = dest.parent / f".tmp-{message_id}-{uuid.uuid4().hex[:8]}"
        tmp.mkdir()
        res = runner.run(
            ["im", "+messages-mget", "--as", "bot", "--message-ids", str(message_id),
             "--no-reactions", "--download-resources"],
            timeout_s=timeout_s, cwd=str(tmp))
        env = runner_mod.parse_envelope(res.stdout)
        if res.rc != 0 or not runner_mod.envelope_ok(env):
            if log is not None:  # r3-6:瞬态失败留现场
                try:
                    log(f"media download {message_id} failed: rc={res.rc} "
                        f"timed_out={res.timed_out} stdout={(res.stdout or '')[:300]!r} "
                        f"stderr={(res.stderr or '')[:300]!r}")
                except Exception:
                    pass
            return None  # 瞬态:网络/信封失败 → 稍后重试
        # 收集(先全量校验再搬运)
        found = []
        total = 0
        for root, dirs, files in os.walk(tmp, followlinks=False):
            for name in files:
                src = pathlib.Path(root) / name
                if src.is_symlink():
                    raise MediaError("symlink in download tree")
                if not src.is_file():
                    continue
                total += src.stat().st_size
                if total > quota_bytes:
                    raise MediaError(f"media quota exceeded ({total} > {quota_bytes})")
                found.append(src)
        if not found:
            _log(log, f"media download {message_id}: ok but no resource files downloaded")
            raise MediaError("no resource files downloaded")
        staging = tmp / "_staged"
        staging.mkdir()
        seen = set()
        for i, src in enumerate(found):
            base = os.path.basename(src.name) or f"file{i}"
            if base in seen:
                base = f"{i}-{base}"
            seen.add(base)
            os.replace(src, staging / base)  # 同卷内移动
        os.rename(staging, dest)  # 原子露出
        return _existing(dest)
    except MediaError as e:
        _log(log, f"media materialize {message_id} MediaError: {e}")  # r4-4
        raise
    except OSError as e:
        _log(log, f"media materialize {message_id} fs error: {e}")
        raise MediaError(f"fs error: {e}") from e
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)
