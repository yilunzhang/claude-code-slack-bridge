"""小工具:id/nonce/marker/chunk/原子写/日志轮转 + Slack 文本/消息标识辅助。"""
import json
import os
import re
import secrets
import uuid

from . import constants

# Slack token 形态(xoxb-/xoxp-/xoxa-/xoxr-/xoxe-… 与 xapp-):显式 secrets 之外的兜底遮蔽。
SLACK_TOKEN_RE = re.compile(r"\bxox[a-z]-[A-Za-z0-9._-]{6,}|\bxapp-[A-Za-z0-9._-]{6,}")
REDACT_MASK = "***"
REDACT_MIN_LEN = 6   # 太短的"秘密"不做子串替换(会把日志打成筛子);token 长度远大于此


def new_id():
    return uuid.uuid4().hex


def new_nonce():
    return secrets.token_hex(16)  # 固定长度 32 hex


def marker_for(nonce):
    return f"{constants.MARKER_PREFIX}{nonce}]"


def chunk_text(s, limit=constants.CHUNK_LIMIT):
    if not s:
        return []
    return [s[i:i + limit] for i in range(0, len(s), limit)]


def chunk_text_with_footer(body, footer, limit=constants.CHUNK_LIMIT):
    """把 body 切成若干块,页脚只拼在**最后一块**,且**每一块(含页脚的那块)≤ limit**。

    规则(contracts §2.1):
    - body 空 → `[]`(页脚不单独成消息)。
    - 页脚为空 → 等价 chunk_text。
    - 先按 limit 平切;末块 + 页脚仍 ≤ limit → 直接拼。
    - 否则把末块再切一刀:前段留在原位,余下的尾部 + 页脚成为新末块(≤ limit)。
      前段缺省取 `limit-len(footer)` 字符;若这样切出的尾部与页脚同块仍 > limit(页脚超过
      limit/2 时会发生,R1-m2:body=10 / footer=8 / limit=10 曾切出 [2,16]),则改为把尾部
      **限制**在 `limit-len(footer)` 字符(前段随之变长,但前段 ≤ 末块原长 ≤ limit)。
      **正文一个字符都不丢**;页脚整体不可分。
    - 页脚本身 > limit(不可能装下)→ 丢页脚,正文照常平切;绝不丢正文。
    不变量:`len(footer) <= limit` 时,返回的**每一块**(含页脚的末块)都 ≤ limit。
    """
    if not body:
        return []
    footer = footer or ""
    chunks = chunk_text(body, limit)
    if not footer:
        return chunks
    if len(footer) > limit:
        return chunks  # 装不下:丢页脚,不丢正文
    last = chunks[-1]
    if len(last) + len(footer) <= limit:
        chunks[-1] = last + footer
        return chunks
    keep = limit - len(footer)  # 与页脚同块的正文最多只能有这么多
    if keep <= 0:
        chunks.append(footer)  # 页脚恰好占满一块
        return chunks
    # 前段缺省 keep 字符(契约文本);但尾部同样不得超过 keep,否则 tail+footer > limit。
    # 到这里 len(last) > keep(否则上面已直接拼上),所以 head_len < len(last),尾部非空。
    head_len = max(keep, len(last) - keep)
    head, tail = last[:head_len], last[head_len:]
    assert len(tail) + len(footer) <= limit and len(head) <= limit
    chunks[-1] = head
    chunks.append(tail + footer)
    return chunks


# ---- Slack 文本 / 消息标识 ----
def slack_unescape(s):
    """Slack 正文只转义三个字符:`&amp;` `&lt;` `&gt;`(先 lt/gt 再 amp,避免二次解码)。"""
    if not s:
        return "" if s is None else s
    return s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def message_id_of(channel, ts):
    """Slack 消息身份 = `channel:ts`(inbox.message_id / pendings.card_message_id / sent_message_id 同源)。"""
    if not channel or not ts:
        raise ValueError("message_id_of: channel 与 ts 均必填")
    return f"{channel}:{ts}"


def split_message_id(message_id):
    """`channel:ts` → (channel, ts)。ts 形如 `1700000000.000100`(不含 ':'),channel 也不含 ':'。
    畸形 → ValueError。"""
    if not isinstance(message_id, str) or ":" not in message_id:
        raise ValueError(f"bad message_id: {message_id!r}")
    channel, _, ts = message_id.partition(":")
    if not channel or not ts or ":" in ts:
        raise ValueError(f"bad message_id: {message_id!r}")
    return channel, ts


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


def redact_secrets(text, secrets=(), mask=REDACT_MASK):
    """把 `secrets` 里每个非空字符串在 `text` 中的出现替换为 `mask`(R1-M5):任何可能带异常文本的
    stderr / daemon.log / daemon_state.last_error / CLI JSON 行都要先过这里。
    - 同时替换其 repr 转义形态(`repr(s)[1:-1]` / `repr(s.encode())[2:-1]`):异常文本常是
      `Invalid header value b'Bearer xapp-…\\n'` 这种 bytes repr,含换行的 token 在里面是转义过的;
    - 兜底:Slack token 形态(SLACK_TOKEN_RE)一律遮蔽,调用方不知道具体 secret 时也有效;
    - text 非 str → str();None → ""。"""
    if text is None:
        return ""
    s = text if isinstance(text, str) else str(text)
    forms = []
    for sec in secrets or ():
        if not isinstance(sec, str) or len(sec) < REDACT_MIN_LEN:
            continue
        forms.append(sec)
        esc = repr(sec)[1:-1]
        if esc != sec:
            forms.append(esc)
        besc = repr(sec.encode("utf-8", "replace"))[2:-1]
        if besc not in (sec, esc):
            forms.append(besc)
    for f in sorted(set(forms), key=len, reverse=True):   # 长的先替,避免残留片段
        if f in s:
            s = s.replace(f, mask)
    return SLACK_TOKEN_RE.sub(mask, s)


def append_log_line(path, line, max_bytes=constants.LOG_MAX_BYTES):
    """追加一行;超限轮转到 .1。固定文案纪律由调用方保证(不写正文、不写 token)。"""
    path = str(path)
    try:
        if os.path.exists(path) and os.path.getsize(path) > max_bytes:
            os.replace(path, path + ".1")
    except OSError:
        pass
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
