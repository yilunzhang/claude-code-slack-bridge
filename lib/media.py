"""media 物化(contracts §5.3 / §6):Slack 附件按文件在子进程 `bin/download_worker.py` 下载到
`.tmp-*` 目录;父进程持**绝对**截止时刻、映射退出码、**只有父进程**把 tmp 目录原子 rename 为正式目录。

    materialize(client_tokens, media_root, binding_id, message_id, files, deadline_s=…, heartbeat=None)
        -> (paths, skipped);瞬态失败 → None(调用方走 materializing 预算);确定性失败 → raise MediaError。
        `deadline_s` 是**整条消息**的共享绝对截止(R1-M6):所有文件合计不超过它,每个 worker 只拿剩余秒数;
        每个文件下载结束后调用 `heartbeat()`(daemon 心跳),多附件不会把主循环拖过挂死阈值。

零网络的本地判定(`file_plan` / `skip_reason`)供 inbound 复用(带下载 vs 纯文本预算分类、payload files[])。
保留 feishu 版的安全检查:id 只作单层目录名、realpath 收容、symlink 一律拒绝、配额。
token 只经 stdin 传给 worker(不上 argv、不进日志)。"""
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import uuid

from . import constants, util
from .slackapi import DownloadResult

WORKER_PATH = pathlib.Path(__file__).resolve().parents[1] / "bin" / "download_worker.py"
TMP_PREFIX = ".tmp-"
KILL_GRACE_S = 2.0                 # 父进程到期:SIGTERM → 2s → SIGKILL
_monotonic = time.monotonic        # 截止时刻用的单调钟(测试可替换;不动全局 time 模块)
NAME_MAX = 200
TRANSIENT_RCS = (constants.WORKER_RC_TRANSIENT, constants.WORKER_RC_HTTP_RETRY,
                 constants.WORKER_RC_DEADLINE, constants.WORKER_RC_ORPHAN)
PERMANENT_RCS = (constants.WORKER_RC_ARGS, constants.WORKER_RC_PERMANENT)


class MediaError(Exception):
    """确定性物化失败(worker 永久拒绝 / 参数错 / 符号链接 / 配额 / 文件系统错误)→ fail-closed。"""


# ---------------------------------------------------------------- 本地判定(零网络)
def _s(v):
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return None


def file_url(f):
    """下载 URL:url_private_download 优先,其次 url_private;缺失 → None。"""
    if not isinstance(f, dict):
        return None
    return _s(f.get("url_private_download")) or _s(f.get("url_private"))


def skip_reason(f, max_bytes=None):
    """单个 Slack file 对象是否可下载:None = 可下载;否则 ∈ constants.FILE_SKIP_REASONS。"""
    if max_bytes is None:
        max_bytes = constants.MEDIA_FILE_MAX_BYTES
    if not isinstance(f, dict):
        return "no_url"
    mode = _s(f.get("mode"))
    access = _s(f.get("file_access"))
    if mode == "tombstone" or access == "file_not_found":
        return "tombstone"
    if mode == "hidden_by_limit" or access == "hidden_by_limit":
        return "hidden_by_limit"
    if access == "check_file_info":
        return "check_file_info"
    if file_url(f) is None:
        return "no_url"
    size = f.get("size")
    if isinstance(size, bool):
        size = None
    if isinstance(size, (int, float)) and size > max_bytes:
        return "too_large"
    return None


def _safe_name(name, index, max_len=NAME_MAX):
    """落盘文件名:只取 basename;去分隔符/NUL/前导点(与 .tmp 约定冲突);空 → file<i>;限长。"""
    s = name if isinstance(name, str) else ""
    s = s.replace("\x00", "")
    s = s.replace("/", "_")
    if os.sep != "/":
        s = s.replace(os.sep, "_")
    if os.altsep:
        s = s.replace(os.altsep, "_")
    s = os.path.basename(s).strip().lstrip(".")
    if not s:
        s = "file%d" % index
    if len(s) > max_len:
        stem, ext = os.path.splitext(s)
        s = stem[: max_len - len(ext)] + ext if len(ext) < max_len else s[:max_len]
    return s


_ID_SAFE_RE = re.compile(r"[^A-Za-z0-9_-]+")
ID_MAX = 32


def _safe_id_fragment(file_id, index):
    """文件 id 作为文件名片段:只留 [A-Za-z0-9_-],空 → noid<i>,限长。"""
    s = file_id if isinstance(file_id, str) else ""
    s = _ID_SAFE_RE.sub("", s)[:ID_MAX]
    return s or ("noid%d" % index)


def dest_name_for(f, index, seen=None):
    """附件落盘名(确定性、同一消息内唯一,R1-M7):`f<idx:02d>-<file_id>-<sanitized_name>`。
    1-based 序号 + 文件 id 已经使不同条目不可能同名;仍在 `seen` 集合上循环加 `-2`/`-3`… 直到不撞
    (防御截断/畸形 id 的极端情况),绝不让第三个文件撞上第二个的名字。"""
    prefix = "f%02d-%s-" % (index + 1, _safe_id_fragment(f.get("id") if isinstance(f, dict) else None, index))
    base = _safe_name((f.get("name") or f.get("title")) if isinstance(f, dict) else None, index,
                      max_len=max(NAME_MAX - len(prefix), 16))
    cand = prefix + base
    if seen is None:
        return cand
    n = 1
    stem, ext = os.path.splitext(cand)
    while cand in seen:
        n += 1
        cand = "%s-%d%s" % (stem, n, ext)
    return cand


def file_plan(files, max_bytes=None, quota_bytes=None):
    """→ [{"file", "id", "name", "url", "skip", "dest_name"}](保序)。零网络。
    skip=None 的条目才下载;累计声明大小超 quota_bytes 的后续文件 → too_large。
    dest_name 对可下载条目确定(`f<idx>-<file_id>-<name>`,`dest_name_for`),供 payload 反查 local_path。"""
    if max_bytes is None:
        max_bytes = constants.MEDIA_FILE_MAX_BYTES
    if quota_bytes is None:
        quota_bytes = constants.MEDIA_MSG_QUOTA_BYTES
    plan = []
    seen = set()
    total = 0
    for i, f in enumerate(files or []):
        if not isinstance(f, dict):
            continue
        reason = skip_reason(f, max_bytes)
        dest_name = None
        if reason is None:
            size = f.get("size")
            if isinstance(size, (int, float)) and not isinstance(size, bool):
                if total + int(size) > quota_bytes:
                    reason = "too_large"          # 装不下的跳过;更小的后续文件仍可入选
                else:
                    total += int(size)
        if reason is None:
            dest_name = dest_name_for(f, i, seen)
            seen.add(dest_name)
        plan.append({"file": f, "id": _s(f.get("id")), "name": f.get("name") or f.get("title"),
                     "url": file_url(f), "skip": reason, "dest_name": dest_name, "index": i})
    return plan


def skipped_of(plan):
    return [{"id": e["id"], "name": e["name"], "skipped_reason": e["skip"]}
            for e in plan if e["skip"] is not None]


def _lookup_published(entry, by_name, claimed):
    """按 dest_name(`fNN-<id>-<name>`)精确反查已发布路径;每个路径只认领一次。
    R4-m1:不再做任何旧命名回落 —— 数据目录是全新的(`~/.claude/data/slack-bridge`),不存在 R1-M7 之前
    的旧式目录;任何启发式回落都会在原名碰巧像新式名时错配别的附件。找不到 → None(payload 记 no_url)。"""
    p = by_name.get(entry["dest_name"])
    if p is not None and p not in claimed:
        claimed.add(p)
        return p
    return None


def describe_files(files, paths):
    """payload `files[]`(contracts §4.3):可下载条目 → local_path(按 dest_name 精确反查 paths),
    其余 → skipped_reason。paths 里找不到对应文件 → 保守按 no_url
    记为 skipped(payload 只引用确实存在于发布目录里的路径)。"""
    by_name = {}
    for p in paths or []:
        by_name[os.path.basename(p)] = p
    claimed = set()
    out = []
    for e in file_plan(files):
        f = e["file"]
        item = {"id": e["id"], "name": e["name"], "mimetype": f.get("mimetype"), "size": f.get("size")}
        if e["skip"] is not None:
            item["skipped_reason"] = e["skip"]
        else:
            p = _lookup_published(e, by_name, claimed)
            if p is not None:
                item["local_path"] = p
            else:
                item["skipped_reason"] = "no_url"
        out.append(item)
    return out


def needs_download(files):
    """预算分类:有任一可下载文件 → 带下载;否则纯文本(全部 skipped 或无 files)。"""
    return any(e["skip"] is None for e in file_plan(files))


# ---------------------------------------------------------------- 路径安全
def _existing(dest):
    return sorted(str(p) for p in dest.iterdir() if p.is_file() and not p.is_symlink())


def _safe_id(x):
    """id 只作单层目录名;拒 '.'/'..'/点前缀(与 .tmp 约定冲突)/分隔符/NUL。"""
    s = str(x)
    if (not s or s in (".", "..") or s.startswith(".")
            or "/" in s or os.sep in s or (os.altsep and os.altsep in s)
            or "\x00" in s):
        raise MediaError("bad media path id: %r" % (s,))
    return s


def _log(log, msg):
    if log is None:
        return
    try:
        log(msg)
    except Exception:
        pass


def _beat(heartbeat):
    """心跳是旁路:任何异常都不得影响物化结果。"""
    if heartbeat is None:
        return
    try:
        heartbeat()
    except Exception:
        pass


# ---------------------------------------------------------------- 子进程下载
def _parse_worker_line(out):
    try:
        text = out.decode("utf-8", "replace") if isinstance(out, bytes) else (out or "")
        lines = [l for l in text.splitlines() if l.strip()]
        if not lines:
            return None
        obj = json.loads(lines[-1])
        return obj if isinstance(obj, dict) else None
    except ValueError:
        return None


def _download_one(url, dest_tmp, token, max_bytes, deadline_at, worker_path=None, python=None,
                  log=None, allow_plain_http_hosts=None):
    """一个文件一个 worker 子进程。`deadline_at` = 调用方(materialize)持有的**绝对**截止时刻
    (`_monotonic()` 秒,整条消息共享);到期 SIGTERM → 2s → SIGKILL,视为瞬态。
    → DownloadResult(ok / permanent / rc / path)。"""
    remaining = float(deadline_at) - _monotonic()
    if remaining <= 0:
        return DownloadResult(ok=False, error="deadline_before_start", rc=None)
    req = {"url": url, "dest_tmp": str(dest_tmp), "token": token,
           "max_bytes": int(max_bytes), "timeout_s": float(remaining)}
    if allow_plain_http_hosts:
        req["allow_plain_http_hosts"] = list(allow_plain_http_hosts)   # 仅测试(本地 http.server)
    argv = [python or sys.executable, str(worker_path or WORKER_PATH)]
    try:
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    except OSError as e:
        _log(log, "download worker spawn failed: %s" % e)
        return DownloadResult(ok=False, error="spawn:%s" % e, rc=None)
    killed = False
    try:
        out, err = proc.communicate(input=json.dumps(req).encode("utf-8"), timeout=remaining)
    except subprocess.TimeoutExpired:
        killed = True
        try:
            proc.terminate()
        except OSError:
            pass
        try:
            out, err = proc.communicate(timeout=KILL_GRACE_S)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except OSError:
                pass
            out, err = proc.communicate()
    rc = proc.returncode
    resp = _parse_worker_line(out)
    # worker stderr / 错误文本进 daemon.log 前遮蔽 token(worker 自身也不打 traceback,双保险;R1-M5)
    err_text = util.redact_secrets((err or b"")[:300].decode("utf-8", "replace"), (token,)) if err else ""
    if killed:
        _log(log, "download worker killed by parent deadline rc=%s stderr=%r" % (rc, err_text))
        _unlink(dest_tmp)
        return DownloadResult(ok=False, error="parent_deadline", rc=rc)
    if rc == 0:
        nbytes = resp.get("nbytes") if resp else None
        good = (resp is not None and resp.get("ok") is True and isinstance(nbytes, int)
                and not isinstance(nbytes, bool) and os.path.isfile(str(dest_tmp))
                and not os.path.islink(str(dest_tmp)) and os.path.getsize(str(dest_tmp)) == nbytes)
        if good:
            return DownloadResult(ok=True, nbytes=nbytes, content_type=resp.get("content_type"),
                                  http_status=resp.get("http_status"), rc=0, path=str(dest_tmp))
        _log(log, "download worker rc=0 but result inconsistent resp=%r" % (resp,))
        _unlink(dest_tmp)
        return DownloadResult(ok=False, error="inconsistent_result", rc=-1000)   # 计 worker_unexpected_exit
    error = util.redact_secrets((resp or {}).get("error") or ("rc=%s" % rc), (token,))
    _unlink(dest_tmp)
    if rc in PERMANENT_RCS:
        _log(log, "download worker permanent rc=%s error=%r" % (rc, error))
        return DownloadResult(ok=False, error=str(error), rc=rc, permanent=True,
                              http_status=(resp or {}).get("http_status"))
    _log(log, "download worker transient rc=%s error=%r stderr=%r" % (rc, error, err_text))
    return DownloadResult(ok=False, error=str(error), rc=rc,
                          http_status=(resp or {}).get("http_status"))


def _unlink(path):
    try:
        os.unlink(str(path))
    except OSError:
        pass


def is_unexpected_rc(rc):
    """瞬态但**非预期**的退出码(既不在 §6 的 4/5/124/125,也不是被父进程信号杀)→ 计数 worker_unexpected_exit。"""
    if rc is None:
        return False
    if rc in TRANSIENT_RCS or rc in PERMANENT_RCS or rc == 0:
        return False
    if rc < 0 and rc != -1000:
        return False   # 被信号杀(父进程 SIGTERM/SIGKILL 或外部)
    return True


# ---------------------------------------------------------------- 主入口
def materialize(client_tokens, media_root, binding_id, message_id, files,
                deadline_s=constants.DOWNLOAD_DEADLINE_S, worker_path=None, log=None, clock=None,
                max_bytes=None, quota_bytes=None, stats=None, python=None,
                allow_plain_http_hosts=None, heartbeat=None):
    """contracts §5.3。→ (paths, skipped);瞬态 → None;确定性 → MediaError。
    - skipped 元素 {"id","name","skipped_reason"},reason ∈ FILE_SKIP_REASONS。
    - dest 已存在(此前成功发布过)→ 幂等复用。
    - stats(可选 dict):worker_unexpected_exit 计数交给调用方落 daemon_state。
    - clock 仅供调用方一致性注入;截止时刻用真实单调钟(子进程等待是真实时间)。
    - deadline_s:**整条消息**的共享绝对截止(R1-M6)——进入下载循环时记 `deadline_at`,每个 worker
      只拿剩余秒数;剩余 ≤ 0 → 瞬态 None(走预算),绝不让 N 个文件各占满一个 deadline。
    - heartbeat(可选 callable):每个文件下载结束后调用一次(异常吞掉),让 daemon 在多附件消息
      中间也能刷 last_loop_at(挂死阈值 ctl.HUNG_THRESHOLD_MS 只需覆盖单个文件的 deadline)。"""
    if max_bytes is None:
        max_bytes = constants.MEDIA_FILE_MAX_BYTES
    if quota_bytes is None:
        quota_bytes = constants.MEDIA_MSG_QUOTA_BYTES
    plan = file_plan(files, max_bytes, quota_bytes)
    skipped = skipped_of(plan)
    todo = [e for e in plan if e["skip"] is None]
    media_root = pathlib.Path(media_root)
    dest = media_root / _safe_id(binding_id) / _safe_id(message_id)
    root_real = os.path.realpath(str(media_root))
    if not os.path.realpath(str(dest)).startswith(root_real + os.sep):
        raise MediaError("media path escapes media root")
    if os.path.islink(str(dest)) or os.path.islink(str(dest.parent)):
        raise MediaError("symlinked media path rejected")
    if dest.is_dir():
        return _existing(dest), skipped          # 幂等复用(此前成功过)
    if not todo:
        return [], skipped                       # 纯文本:无需网络
    token = (client_tokens or {}).get("bot_token") if isinstance(client_tokens, dict) else None
    if not isinstance(token, str) or not token:
        _log(log, "media materialize %s: no bot token available (transient)" % message_id)
        return None
    tmp = None
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if os.path.islink(str(dest.parent)):
            raise MediaError("symlinked media parent rejected")
        tmp = dest.parent / ("%s%s-%s" % (TMP_PREFIX, message_id, uuid.uuid4().hex[:8]))
        tmp.mkdir()
        total = 0
        deadline_at = _monotonic() + float(deadline_s)   # 整条消息共享的绝对截止(覆盖全部文件)
        for e in todo:
            dest_tmp = tmp / e["dest_name"]
            r = _download_one(e["url"], dest_tmp, token, max_bytes, deadline_at,
                              worker_path=worker_path, python=python, log=log,
                              allow_plain_http_hosts=allow_plain_http_hosts)
            _beat(heartbeat)   # 文件之间刷心跳(无论成败),多附件不会把主循环拖过挂死阈值
            if r.permanent:
                raise MediaError("download rejected (%s): rc=%s %s" % (e["id"], r.rc, r.error))
            if not r.ok:
                if stats is not None and is_unexpected_rc(r.rc):
                    stats["worker_unexpected_exit"] = stats.get("worker_unexpected_exit", 0) + 1
                _log(log, "media materialize %s: transient (%s rc=%s)" % (message_id, r.error, r.rc))
                return None
            total += r.nbytes or 0
            if total > quota_bytes:
                raise MediaError("media quota exceeded (%d > %d)" % (total, quota_bytes))
        # 全部到齐:再次全量校验(无符号链接),然后**父进程**原子发布
        for p in tmp.iterdir():
            if p.is_symlink() or not p.is_file():
                raise MediaError("unexpected entry in download tree: %s" % p.name)
        os.rename(str(tmp), str(dest))
        tmp = None
        return _existing(dest), skipped
    except MediaError as e:
        _log(log, "media materialize %s MediaError: %s" % (message_id, e))
        raise
    except OSError as e:
        _log(log, "media materialize %s fs error: %s" % (message_id, e))
        raise MediaError("fs error: %s" % e) from e
    finally:
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)
