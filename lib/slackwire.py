"""Slack 信封/事件的**纯函数**归一化 —— consumer(bin/slack_consumer.py)与 daemon
(inbound.ingest_in_tx / approval.process_in_tx)共用同一份,保证 event_key / chat_id /
交互字段提取在两端**逐字节一致**(R2-m3 / R3-m3)。

零 I/O、零 DB、零日志;所有函数对畸形输入返回 None / False,绝不抛出(除显式 assert 的编程错误)。
契约:docs/contracts.md §4.2。"""
import json
import re

from . import constants

EVENTS_API = "events_api"
INTERACTIVE = "interactive"
ENVELOPE_TYPES = (EVENTS_API, INTERACTIVE)

_MENTION_RE_CACHE = {}


def _s(v):
    """非空字符串 → 去首尾空白后的字符串;其余 → None。"""
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return None


def _d(v):
    return v if isinstance(v, dict) else None


def _l(v):
    return v if isinstance(v, list) else []


# ---------------------------------------------------------------- event_key / chat_of
def event_key(envelope_type, payload):
    """→ 'ev:<event_id>' | 'act:<team>:<channel>:<card_ts>:<user>:<action_id>:<action_ts>' | None。
    None = 任一段缺失/非法 → consumer ack 并丢弃(计数 staged_invalid)。
    interactive 键**不看 value**(value 畸形的点击仍可去重、入暂存,由 approval 判 dropped/skipped)。"""
    payload = _d(payload)
    if payload is None:
        return None
    if envelope_type == EVENTS_API:
        eid = _s(payload.get("event_id"))
        if eid is None or ":" in eid:
            return None
        return "ev:" + eid
    if envelope_type == INTERACTIVE:
        parts = _interactive_key_parts(payload)
        if parts is None:
            return None
        return "act:" + ":".join(parts)
    return None


def _interactive_key_parts(payload):
    if _s(payload.get("type")) != "block_actions":
        return None
    team = _s((_d(payload.get("team")) or {}).get("id"))
    channel = interactive_channel(payload)
    card_ts = interactive_card_ts(payload)
    user = _s((_d(payload.get("user")) or {}).get("id"))
    actions = [a for a in _l(payload.get("actions")) if isinstance(a, dict)]
    if len(actions) != 1:
        return None
    action_id = _s(actions[0].get("action_id"))
    action_ts = _s(actions[0].get("action_ts"))
    parts = (team, channel, card_ts, user, action_id, action_ts)
    if any(p is None or ":" in p for p in parts):
        return None
    return parts


def interactive_channel(payload):
    """channel = payload.channel.id **or** container.channel_id(DM 卡片有时只有后者)。"""
    ch = _s((_d(payload.get("channel")) or {}).get("id"))
    if ch is None:
        ch = _s((_d(payload.get("container")) or {}).get("channel_id"))
    return ch


def interactive_card_ts(payload):
    """card_ts = container.message_ts **or** message.ts。"""
    ts = _s((_d(payload.get("container")) or {}).get("message_ts"))
    if ts is None:
        ts = _s((_d(payload.get("message")) or {}).get("ts"))
    return ts


def chat_of(envelope_type, payload):
    """事件所属会话 id(consumer 用它钉 binding_id)。缺失 → None(仍可入暂存,binding_id=NULL)。"""
    payload = _d(payload)
    if payload is None:
        return None
    if envelope_type == EVENTS_API:
        ev = _d(payload.get("event"))
        if ev is None:
            return None
        ch = _s(ev.get("channel"))
        if ch is None:
            ch = _s((_d(ev.get("item")) or {}).get("channel"))
        return ch
    if envelope_type == INTERACTIVE:
        return interactive_channel(payload)
    return None


# ---------------------------------------------------------------- interactive
def interactive_fields(payload):
    """block_actions 全字段提取 → dict 或 None(任一缺失/非法 → None,调用方按 dropped(skipped) 处理):
      {team, channel, user, action_id, action_ts, card_ts, value:{pending_id, nonce, act}, response_url}
    校验:type=='block_actions';恰一个按钮;action_id ∈ ACTION_IDS;value 为 JSON 对象且
    pending_id/nonce 非空字符串、act ∈ {approve,reject} 且与 action_id 一致。"""
    payload = _d(payload)
    if payload is None:
        return None
    parts = _interactive_key_parts(payload)
    if parts is None:
        return None
    team, channel, card_ts, user, action_id, action_ts = parts
    if action_id not in constants.ACTION_IDS:
        return None
    action = [a for a in _l(payload.get("actions")) if isinstance(a, dict)][0]
    if _s(action.get("type")) not in (None, "button"):
        return None
    raw = action.get("value")
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except ValueError:
            return None
    else:
        value = raw
    value = _d(value)
    if value is None:
        return None
    pid, nonce, act = _s(value.get("pending_id")), value.get("nonce"), _s(value.get("act"))
    if pid is None or not isinstance(nonce, str) or not nonce:
        return None
    expected_act = "approve" if action_id == constants.ACTION_APPROVE else "reject"
    if act != expected_act:
        return None
    return {
        "team": team, "channel": channel, "user": user, "action_id": action_id,
        "action_ts": action_ts, "card_ts": card_ts,
        "value": {"pending_id": pid, "nonce": nonce, "act": act},
        "response_url": _s(payload.get("response_url")),
    }


# ---------------------------------------------------------------- events_api
def events_fields(payload):
    """events_api 信封 → 扁平 dict 或 None(缺 event / event.type):
      {team_id, api_app_id, event_id, event_time, type, channel, channel_type, user, ts, thread_ts,
       subtype, text, files, blocks, bot_id, app_id, event}"""
    payload = _d(payload)
    if payload is None:
        return None
    ev = _d(payload.get("event"))
    if ev is None or _s(ev.get("type")) is None:
        return None
    return {
        "team_id": _s(payload.get("team_id")),
        "api_app_id": _s(payload.get("api_app_id")),
        "event_id": _s(payload.get("event_id")),
        "event_time": payload.get("event_time"),
        "type": _s(ev.get("type")),
        "channel": _s(ev.get("channel")),
        "channel_type": _s(ev.get("channel_type")),
        "user": _s(ev.get("user")),
        "ts": _s(ev.get("ts")),
        "thread_ts": _s(ev.get("thread_ts")),
        "subtype": _s(ev.get("subtype")),
        "text": ev.get("text") if isinstance(ev.get("text"), str) else "",
        "files": [f for f in _l(ev.get("files")) if isinstance(f, dict)],
        "blocks": [b for b in _l(ev.get("blocks")) if isinstance(b, dict)],
        "bot_id": _s(ev.get("bot_id")),
        "app_id": _s(ev.get("app_id")),
        "event": ev,
    }


def is_dm(event):
    """channel_type=='im';缺 channel_type 时按 channel id 前缀 'D' 判。"""
    event = _d(event) or {}
    ct = _s(event.get("channel_type"))
    if ct is not None:
        return ct == "im"
    ch = _s(event.get("channel")) or ""
    return ch.startswith("D")


def is_self_event(event, cfg):
    """自身回流:三组比较(bot_id↔cfg.bot_id、user↔cfg.bot_user_id、app_id↔cfg.app_id),
    **每组都要求两边非空且相等**才算命中(None 安全:cfg 缺值绝不导致全部消息被判为自身)。"""
    event = _d(event) or {}
    cfg = cfg or {}
    pairs = (
        (_s(event.get("bot_id")), _s(cfg.get("bot_id"))),
        (_s(event.get("user")), _s(cfg.get("bot_user_id"))),
        (_s(event.get("app_id")), _s(cfg.get("app_id"))),
    )
    return any(a is not None and b is not None and a == b for a, b in pairs)


def mention_re(bot_user_id):
    bot_user_id = _s(bot_user_id)
    if bot_user_id is None:
        return None
    r = _MENTION_RE_CACHE.get(bot_user_id)
    if r is None:
        r = re.compile(r"<@" + re.escape(bot_user_id) + r"(\|[^>]*)?>")
        _MENTION_RE_CACHE[bot_user_id] = r
    return r


def _rich_text_user_ids(blocks):
    out = set()
    stack = [b for b in _l(blocks) if isinstance(b, dict)]
    while stack:
        node = stack.pop()
        if _s(node.get("type")) == "user":
            uid = _s(node.get("user_id"))
            if uid:
                out.add(uid)
        for child in _l(node.get("elements")):
            if isinstance(child, dict):
                stack.append(child)
    return out


def is_bot_mentioned(event, bot_user_id):
    """app_mention 类型 ∨ rich_text 里 user 元素 == bot ∨ 正文正则 `<@BOT(\\|name)?>`。
    bot_user_id 缺失时只认 app_mention(绝不把所有消息判为提及)。"""
    event = _d(event) or {}
    if _s(event.get("type")) == "app_mention":
        return True
    bot_user_id = _s(bot_user_id)
    if bot_user_id is None:
        return False
    if bot_user_id in _rich_text_user_ids(event.get("blocks")):
        return True
    text = event.get("text")
    if isinstance(text, str) and mention_re(bot_user_id).search(text):
        return True
    return False


def render_text(event, bot_user_id):
    """去 bot mention → slack_unescape → strip。其他人的 mention 原样保留(`<@U…>`)。"""
    from . import util
    event = _d(event) or {}
    text = event.get("text")
    if not isinstance(text, str):
        return ""
    r = mention_re(bot_user_id)
    if r is not None:
        text = r.sub("", text)
    return util.slack_unescape(text).strip()
