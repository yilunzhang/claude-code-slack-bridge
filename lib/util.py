"""小工具:id/nonce/marker/chunk/原子写/日志轮转/短幂等键。"""
import hashlib
import json
import os
import secrets
import uuid

from . import constants


def new_id():
    return uuid.uuid4().hex


def new_nonce():
    return secrets.token_hex(16)  # 固定长度 32 hex


def marker_for(nonce):
    return f"{constants.MARKER_PREFIX}{nonce}]"


def short_key(logical_key):
    """E4b(真机 A/B):飞书 --idempotency-key(uuid 参数)上限 ~50 字符,超长报 99992402。
    wire 短键 = 'fb:' + sha1(逻辑键)[:32],总长 35 ≤ 40(留余量);
    确定性映射:同逻辑键 → 同短键(S4 同键重试语义保持);DB 仍存逻辑键(UNIQUE 语义不变)。"""
    return "fb:" + hashlib.sha1(str(logical_key).encode("utf-8")).hexdigest()[:32]


def chunk_text(s, limit=constants.CHUNK_LIMIT):
    if not s:
        return []
    return [s[i:i + limit] for i in range(0, len(s), limit)]


def jdumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def atomic_write(path, data, mode=0o600):
    path = str(path)
    tmp = f"{path}.tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    if isinstance(data, str):
        data = data.encode("utf-8")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def append_log_line(path, line, max_bytes=constants.LOG_MAX_BYTES):
    """追加一行;超限轮转到 .1。固定文案纪律由调用方保证(不写正文)。"""
    path = str(path)
    try:
        if os.path.exists(path) and os.path.getsize(path) > max_bytes:
            os.replace(path, path + ".1")
    except OSError:
        pass
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
