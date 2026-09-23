"""Test fakes 与构造器:FakeClock / FakeProber / FakeSlackClient(Slack)+ envelope/message/
block_action/slack_file 构造器;FakeRunResult/FakeRunner 与 mget 构造器为 LEGACY-FEISHU(WP5 删)。
不 mock 被测逻辑本身。"""
import itertools
import json
import time

from lib.slackapi import CallResult, InMemoryCooldownStore

# conftest 与本文件互相引用时避免环:常量在此定义,conftest 再 re-export。
TEAM = "T0TEST"
BOT_USER = "U0BOT"
BOT_ID = "B0BOT"
APP_ID = "A0APP"
OWNER = "U0OWNER"
MEMBER = "U0MEMBER"
CHAT = "C0CHAT"
DM = "D0OWNERDM"


class FakeClock:
    """wall/mono 独立可拨,支持时钟回拨测试。单位 ms。"""

    def __init__(self, wall=1_000_000_000_000, mono=50_000):
        self.wall = wall
        self.mono = mono

    def wall_ms(self):
        return self.wall

    def mono_ms(self):
        return self.mono

    def tick(self, ms):
        """正常流逝:两钟同进。"""
        self.wall += ms
        self.mono += ms

    def rewind_wall(self, ms):
        """墙钟回拨(单调钟继续走)。"""
        self.wall -= ms


class FakeProber:
    """pid → (ppid, lstart, comm);缺席=进程不存在;raising=探测异常(UNKNOWN)。"""

    def __init__(self):
        self.table = {}
        self.raising = False

    def set(self, pid, ppid, lstart, comm):
        self.table[pid] = (ppid, lstart, comm)

    def remove(self, pid):
        self.table.pop(pid, None)

    def get(self, pid):
        if self.raising:
            raise RuntimeError("simulated ps failure")
        return self.table.get(pid)


# ======================================================================
# FakeSlackClient(替代 lib.slackapi.SlackClient;零网络)
# ======================================================================
def ok(data=None, **kw):
    d = {"ok": True}
    d.update(data or {})
    d.update(kw)
    return CallResult(ok=True, data=d, http_status=200)


def err(code, http_status=200, **extra):
    d = {"ok": False, "error": code}
    d.update(extra)
    return CallResult(ok=False, data=d, error=code, http_status=http_status)


def ratelimited(retry_after=3):
    return CallResult(ok=False, data={"ok": False, "error": "ratelimited"}, error="ratelimited",
                      http_status=429, retry_after=int(retry_after))


def http5xx(status=503):
    return CallResult(ok=False, data=None, error="http_5xx", http_status=int(status))


def timeout():
    return CallResult(ok=False, error="timeout", timed_out=True)


def not_sent(reason="dns"):
    return CallResult(ok=False, error=reason, not_sent=True)


def wait(until_ms):
    return CallResult(ok=False, error="cooldown", cooldown_until=int(until_ms))


def posted(channel=CHAT, ts=None):
    """chat.postMessage 成功响应(含 channel/ts/message.ts)。"""
    ts = ts or next_ts()
    return ok({"channel": channel, "ts": ts, "message": {"ts": ts, "type": "message"}})


class FakeSlackClient:
    """`.on(method, fn)`:fn(method, params) -> CallResult(或直接给 CallResult);**首个匹配生效**;
    method 可为 '*' 通配。未注册的方法 → AssertionError(离线铁律:绝不静默放过未预期的外呼)。
    与真 SlackClient 同构:call 前检查冷却(→ wait,不记入 calls、记入 waits),
    收到 429/ratelimited 响应 → publish 冷却(取 max)。"""

    def __init__(self, cooldown_store=None, clock=None, tokens_version="fake-v1",
                 timeout_s=15):
        self.cooldown_store = cooldown_store or InMemoryCooldownStore()
        self.clock = clock
        self.timeout_s = timeout_s
        self.calls = []        # [(method, params)] 实际"发出"的调用
        self.waits = []        # [(method, cooldown_until)] 被本地冷却挡住的调用
        self.downloads = []    # media/download_worker 路径的记录(WP2 用)
        self.responders = []   # [(method, fn)]
        self._tokens_version = tokens_version
        self.reloads = []

    # ---- 编排 ----
    def on(self, method, fn):
        self.responders.append((method, fn))
        return self

    def calls_for(self, method):
        return [p for m, p in self.calls if m == method]

    # ---- SlackClient 接口 ----
    @property
    def tokens_version(self):
        return self._tokens_version

    def set_tokens_version(self, version):
        self._tokens_version = version

    def reload_tokens(self, version=None):
        self.reloads.append(version)
        if version is not None:
            self._tokens_version = version
        return self._tokens_version

    def _now_ms(self):
        if self.clock is not None:
            return self.clock.wall_ms()
        return int(time.time() * 1000)

    def call(self, method, params=None, timeout_s=None):
        params = dict(params or {})
        now = self._now_ms()
        until = self.cooldown_store.get(method)
        if until is not None and until > now:
            self.waits.append((method, until))
            return wait(until)
        self.calls.append((method, params))
        for m, fn in self.responders:
            if m == method or m == "*":
                res = fn(method, params) if callable(fn) else fn
                if not isinstance(res, CallResult):
                    raise AssertionError(
                        "FakeSlackClient responder for %s must return CallResult, got %r"
                        % (method, type(res)))
                if res.http_status == 429 or res.error == "ratelimited":
                    ra = res.retry_after if res.retry_after is not None else 1
                    self.cooldown_store.publish(method, now + int(ra) * 1000)
                return res
        raise AssertionError("unexpected Slack call in offline test: %s %r" % (method, params))


# ======================================================================
# Slack 事件构造器
# ======================================================================
_TS_COUNTER = itertools.count(100)
_EV_COUNTER = itertools.count(1)


def next_ts(base=1_700_000_000):
    return "%d.%06d" % (base, next(_TS_COUNTER))


def next_event_id():
    return "Ev%08X" % next(_EV_COUNTER)


def envelope(event, event_id=None, team_id=TEAM, api_app_id=APP_ID, event_time=None):
    """events_api 信封(SocketModeRequest.payload 形状)。"""
    return {
        "token": "verification-token",
        "team_id": team_id,
        "api_app_id": api_app_id,
        "event": event,
        "type": "event_callback",
        "event_id": event_id or next_event_id(),
        "event_time": event_time if event_time is not None else int(float(event.get("ts") or 0)),
        "authorizations": [{"enterprise_id": None, "team_id": team_id, "user_id": BOT_USER,
                            "is_bot": True, "is_enterprise_install": False}],
        "is_ext_shared_channel": False,
        "event_context": "ctx-" + (event_id or "x"),
    }


def message_event(text="hi", channel=CHAT, user=OWNER, ts=None, thread_ts=None, subtype=None,
                  files=None, blocks=None, channel_type=None, bot_id=None, app_id=None,
                  event_type="message"):
    ts = ts or next_ts()
    ev = {
        "type": event_type,
        "channel": channel,
        "user": user,
        "text": text,
        "ts": ts,
        "event_ts": ts,
        "client_msg_id": "cmid-" + ts,
        "team": TEAM,
    }
    if event_type == "message":
        ev["channel_type"] = channel_type or ("im" if str(channel).startswith("D") else "channel")
    if thread_ts is not None:
        ev["thread_ts"] = thread_ts
    if subtype is not None:
        ev["subtype"] = subtype
    if files is not None:
        ev["files"] = list(files)
        if subtype is None:
            ev["subtype"] = "file_share"
        ev["upload"] = False
    if blocks is not None:
        ev["blocks"] = list(blocks)
    if bot_id is not None:
        ev["bot_id"] = bot_id
    if app_id is not None:
        ev["app_id"] = app_id
    return ev


def app_mention_event(text=None, channel=CHAT, user=OWNER, ts=None, thread_ts=None):
    """app_mention 事件:**没有** channel_type / files / subtype(真机不对称)。"""
    ev = message_event(text=text if text is not None else "<@%s> hi" % BOT_USER,
                       channel=channel, user=user, ts=ts, thread_ts=thread_ts,
                       event_type="app_mention")
    ev.pop("client_msg_id", None)
    return ev


def rich_text_mention_blocks(user_id, trailing=" hi"):
    return [{"type": "rich_text", "block_id": "rt1", "elements": [
        {"type": "rich_text_section", "elements": [
            {"type": "user", "user_id": user_id}, {"type": "text", "text": trailing}]}]}]


def block_action(pending_id, nonce, act="approve", user=OWNER, channel=CHAT, card_ts=None,
                 action_ts=None, team=TEAM, value=None, action_id=None, api_app_id=APP_ID):
    """interactive(block_actions)信封。value 缺省 = 我们的卡片 value JSON;act 决定 action_id。"""
    card_ts = card_ts or next_ts()
    action_ts = action_ts or next_ts()
    if action_id is None:
        action_id = "sb_approve" if act == "approve" else "sb_reject"
    if value is None:
        value = json.dumps({"pending_id": pending_id, "nonce": nonce, "act": act},
                           separators=(",", ":"))
    return {
        "type": "block_actions",
        "user": {"id": user, "username": "someone", "name": "someone", "team_id": team},
        "api_app_id": api_app_id,
        "token": "verification-token",
        "container": {"type": "message", "message_ts": card_ts, "channel_id": channel,
                      "is_ephemeral": False},
        "trigger_id": "trig-" + action_ts,
        "team": {"id": team, "domain": "test"},
        "enterprise": None,
        "is_enterprise_install": False,
        "channel": {"id": channel, "name": "chat"},
        "message": {"type": "message", "subtype": "bot_message", "ts": card_ts,
                    "bot_id": BOT_ID, "app_id": api_app_id, "blocks": []},
        "state": {"values": {}},
        "response_url": "https://hooks.slack.com/actions/T/1/x",
        "actions": [{"action_id": action_id, "block_id": "sb_actions:" + str(pending_id),
                     "text": {"type": "plain_text", "text": "btn"}, "value": value,
                     "type": "button", "action_ts": action_ts}],
    }


def slack_file(id="F0000001", name="a.pdf", mimetype="application/pdf", size=1234,
               url_private=None, url_private_download=None, mode="hosted",
               hidden_by_limit=False, filetype=None):
    f = {
        "id": id, "name": name, "title": name, "mimetype": mimetype, "size": size,
        "mode": mode, "filetype": filetype or (name.rsplit(".", 1)[-1] if "." in name else ""),
        "is_external": False, "created": 1_700_000_000, "user": MEMBER,
    }
    if hidden_by_limit:
        f["mode"] = "hidden_by_limit"
    if mode not in ("tombstone", "hidden_by_limit") and not hidden_by_limit:
        f["url_private"] = url_private or "https://files.slack.com/files-pri/%s-%s/%s" % (TEAM, id, name)
        f["url_private_download"] = url_private_download or (f["url_private"] + "?download=1")
    return f


# ======================================================================
# LEGACY-FEISHU: remove in WP5(旧测试仍导入)
# ======================================================================
class FakeRunResult:  # LEGACY-FEISHU: remove in WP5
    def __init__(self, rc=0, stdout="", stderr="", timed_out=False, exc=None):
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.exc = exc


def ok_envelope(data, notice=None):  # LEGACY-FEISHU: remove in WP5
    env = {"ok": True, "data": data}
    if notice:
        env["_notice"] = notice
    return FakeRunResult(0, json.dumps(env))


def err_envelope(code, msg="err"):  # LEGACY-FEISHU: remove in WP5
    return FakeRunResult(1, json.dumps({"ok": False, "code": code, "msg": msg}))


def stderr_err_envelope(code, subtype="invalid_parameters", msg="field validation failed"):  # LEGACY-FEISHU: remove in WP5
    return FakeRunResult(
        1, "", json.dumps({"ok": False, "error": {
            "type": "api", "subtype": subtype, "code": code, "message": msg}},
            ensure_ascii=False))


def network_err_envelope(code=503, retryable=True, subtype="server_error"):  # LEGACY-FEISHU: remove in WP5
    err_ = {"type": "network", "subtype": subtype, "code": code, "message": f"HTTP {code}: "}
    if retryable is not None:
        err_["retryable"] = retryable
    return FakeRunResult(4, "", json.dumps({"ok": False, "error": err_}, ensure_ascii=False))


class FakeRunner:  # LEGACY-FEISHU: remove in WP5
    """可注入 lark-cli runner(旧模块 Inbound/Outbound/Recovery 在 WP2/WP3 改为 client 前仍构造它)。"""

    def __init__(self, profile="main"):
        self.profile = profile
        self.calls = []
        self.responders = []
        self.default = None

    def on(self, predicate, fn):
        self.responders.append((predicate, fn))
        return self

    def on_prefix(self, prefix, fn):
        prefix = list(prefix)

        def pred(args):
            return list(args[: len(prefix)]) == prefix

        return self.on(pred, fn)

    def run(self, args, timeout_s=None, cwd=None, no_profile=False):
        args = list(args)
        if not no_profile:
            args = args + ["--profile", self.profile]
        self.calls.append((args, cwd))
        for pred, fn in self.responders:
            if pred(args):
                return fn(args, cwd) if callable(fn) else fn
        if self.default is not None:
            return self.default(args, cwd) if callable(self.default) else self.default
        raise AssertionError(f"unexpected lark-cli call in offline test: {args}")

    def calls_matching(self, *prefix):
        prefix = list(prefix)
        return [c for c in self.calls if c[0][: len(prefix)] == prefix]


def mget_snapshot(message_id, chat_id, sender_id, msg_type="text", text="hi",
                  mentions=(), sender_type="user", content=None, reply_to=None):  # LEGACY-FEISHU: remove in WP5
    mentions = list(mentions)
    if content is None:
        prefix = "".join(f"@{m.get('name', '?')} " for m in mentions)
        if msg_type in ("text", "post"):
            content = prefix + text
        elif msg_type == "image":
            content = "[图片]"
        elif msg_type == "file":
            content = "(文件) a.pdf"
        else:
            content = ""
    snap = {
        "message_id": message_id, "chat_id": chat_id, "msg_type": msg_type,
        "sender": {"id": sender_id, "id_type": "open_id", "sender_type": sender_type},
        "content": content, "mentions": mentions,
    }
    if reply_to is not None:
        snap["reply_to"] = reply_to
    return snap


def raw_body_snapshot(message_id, chat_id, sender_id, msg_type="text", text="hi",
                      mentions=(), sender_type="user"):  # LEGACY-FEISHU: remove in WP5
    if msg_type == "text":
        content = {"text": text}
    elif msg_type == "image":
        content = {"image_key": "img_k1"}
    elif msg_type == "file":
        content = {"file_key": "file_k1", "file_name": "a.pdf"}
    elif msg_type == "post":
        content = {"title": "t", "content": [[{"tag": "text", "text": text}]]}
    else:
        content = {}
    return {
        "message_id": message_id, "chat_id": chat_id, "msg_type": msg_type,
        "sender": {"id": sender_id, "id_type": "open_id", "sender_type": sender_type},
        "body": {"content": json.dumps(content, ensure_ascii=False)},
        "mentions": list(mentions),
    }


def bot_mention(app_id, key="@_user_1", name="TestBot"):  # LEGACY-FEISHU: remove in WP5
    return {"key": key, "id": app_id, "id_type": "app_id", "name": name}


def user_mention(open_id, key="@_user_2", name="Some One"):  # LEGACY-FEISHU: remove in WP5
    return {"key": key, "id": open_id, "id_type": "open_id", "name": name}
