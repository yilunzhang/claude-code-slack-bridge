"""scripts/capability_probe.py:身份核对先行、markdown 回退、metadata 读回、清理、--write-config、
冷却、不打印 token。两层:FakeSlackClient(逻辑)+ monkeypatch slackapi._open(端到端 main)。"""
import importlib.util
import io
import json
import urllib.parse
import os
import pathlib
import urllib.error
from email.message import Message

import pytest

from lib import config as configmod
from lib import constants, db as dbmod, paths, slackapi
from tests.conftest import APP_ID, BOT_ID, BOT_USER, CHAT, OWNER, TEAM
from tests.helpers import FakeSlackClient, err, ok, posted, ratelimited

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load_probe():
    spec = importlib.util.spec_from_file_location("capability_probe", ROOT / "scripts" / "capability_probe.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


probe = _load_probe()

AUTH_OK = {"url": "https://t.slack.com/", "team": "t", "user": "slack-bridge", "team_id": TEAM,
           "user_id": BOT_USER, "bot_id": BOT_ID}


def _history_responder(store):
    """按已发消息生成 history/replies 读回(带 metadata),store 由 chat.postMessage 响应器填充。"""
    def fn(method, params):
        msgs = []
        for ts, p in store.items():
            if method == "conversations.history" and p.get("thread_ts"):
                continue
            if method == "conversations.replies" and ts != params["ts"] and p.get("thread_ts") != params["ts"]:
                continue
            m = {"type": "message", "ts": ts, "bot_id": BOT_ID, "text": p.get("text") or p.get("markdown_text")}
            if "metadata" in p:
                m["metadata"] = p["metadata"]
            msgs.append(m)
        return ok({"messages": msgs, "has_more": False})
    return fn


def make_client(clock=None, markdown_ok=True, with_metadata=True, auth=AUTH_OK):
    c = FakeSlackClient(clock=clock)
    sent = {}
    n = [0]

    def post(method, params):
        if "markdown_text" in params and not markdown_ok:
            return err("invalid_arguments")
        n[0] += 1
        ts = "1700000000.%06d" % n[0]
        p = dict(params)
        if not with_metadata:
            p.pop("metadata", None)
        sent[ts] = p
        return posted(channel=params["channel"], ts=ts)
    c.on("auth.test", lambda m, p: ok(auth))
    c.on("chat.postMessage", post)
    c.on("conversations.history", _history_responder(sent))
    c.on("conversations.replies", _history_responder(sent))
    c.on("chat.delete", lambda m, p: ok({"channel": p["channel"], "ts": p["ts"]}))
    c.sent = sent
    return c


# ---------------------------------------------------------------- run_probe 逻辑
def test_happy_path_all_ok(cfg, clock):
    c = make_client(clock)
    r = probe.run_probe(c, cfg, CHAT, "v1", probe_id="abc")
    assert r["identity_ok"] and r["markdown_text_ok"] and r["metadata_history"] and r["metadata_replies"]
    assert r["cleanup_ok"] and r["complete"] and r["errors"] == []
    assert r["markdown_mode"] == "markdown_text" and r["verify_capability"] == "ok"
    assert r["tokens_version"] == "v1" and r["probe_id"] == "abc"
    methods = [m for m, _ in c.calls]
    assert methods == ["auth.test", "chat.postMessage", "conversations.history", "chat.postMessage",
                       "conversations.replies", "chat.delete", "chat.delete"]
    top, thread = c.calls_for("chat.postMessage")
    assert top["metadata"] == {"event_type": "slack_bridge", "event_payload": {"probe_id": "abc", "kind": "top"}}
    assert top["unfurl_links"] is False and top["unfurl_media"] is False and "markdown_text" in top
    assert thread["thread_ts"] == "1700000000.000001" and "markdown_text" in thread
    hist = c.calls_for("conversations.history")[0]
    assert hist["include_all_metadata"] is True and hist["inclusive"] is True
    assert hist["oldest"] == hist["latest"] == "1700000000.000001"
    rep = c.calls_for("conversations.replies")[0]
    assert rep["include_all_metadata"] is True and rep["ts"] == "1700000000.000001"
    assert sorted(d["ts"] for d in c.calls_for("chat.delete")) == ["1700000000.000001", "1700000000.000002"]


def test_identity_mismatch_sends_nothing(cfg, clock):
    c = make_client(clock, auth=dict(AUTH_OK, bot_id="B_OTHER"))
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert r["identity_ok"] is False and r["identity_mismatch"] == [
        {"field": "bot_id", "config": BOT_ID, "actual": "B_OTHER"}]
    assert [m for m, _ in c.calls] == ["auth.test"]
    assert not r["complete"] and r["verify_capability"] is None


def test_identity_missing_fields_is_mismatch(cfg, clock):
    c = make_client(clock, auth={"team_id": TEAM, "user_id": BOT_USER})
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert not r["identity_ok"] and r["identity_mismatch"][0]["field"] == "bot_id"


def test_auth_failure_is_incomplete_not_mismatch(cfg, clock):
    c = FakeSlackClient(clock=clock)
    c.on("auth.test", lambda m, p: err("invalid_auth"))
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert not r["identity_ok"] and "identity_mismatch" not in r
    assert r["errors"] == ["auth.test:not_sent:invalid_auth"] and len(c.calls) == 1


def test_markdown_rejected_falls_back_to_text(cfg, clock):
    c = make_client(clock, markdown_ok=False)
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert r["markdown_text_ok"] is False and r["markdown_mode"] == "text"
    posts = c.calls_for("chat.postMessage")
    assert "markdown_text" in posts[0] and "text" in posts[1] and "text" in posts[2]
    assert "markdown_text" not in posts[2]     # 线程消息与顶层同形
    assert r["complete"] and r["verify_capability"] == "ok"


def test_metadata_absent_is_degraded(cfg, clock):
    c = make_client(clock, with_metadata=False)
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert r["metadata_history"] is False and r["metadata_replies"] is False
    assert r["complete"] and r["verify_capability"] == "degraded:metadata_history"
    assert r["cleanup_ok"]


def test_cooldown_makes_probe_incomplete_without_request(cfg, clock):
    c = make_client(clock)
    c.cooldown_store.publish("chat.postMessage", clock.wall_ms() + 30_000)
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert r["identity_ok"] and not r["complete"]
    assert r["errors"] == ["chat.postMessage:wait:cooldown_until=%d" % (clock.wall_ms() + 30_000)]
    assert c.calls_for("chat.postMessage") == [] and len(c.waits) == 1


def test_ratelimit_on_history_is_error_but_cleanup_still_runs(cfg, clock):
    c = make_client(clock)
    c.responders.insert(0, ("conversations.history", lambda m, p: ratelimited(5)))  # 首匹配生效
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert r["metadata_history"] is None and r["metadata_replies"] is True
    assert not r["complete"] and r["verify_capability"] is None
    assert any(e.startswith("conversations.history:ratelimited") for e in r["errors"])
    assert len(c.calls_for("chat.delete")) == 2


def test_delete_failure_only_affects_cleanup(cfg, clock):
    c = make_client(clock)
    c.responders.insert(0, ("chat.delete", lambda m, p: err("message_not_found")))
    r = probe.run_probe(c, cfg, CHAT, "v1")
    assert r["complete"] and r["verify_capability"] == "ok" and r["cleanup_ok"] is False


# ---------------------------------------------------------------- write_results
def test_write_results_writes_config_and_daemon_state(cfg, conn, clock):
    c = make_client(clock, markdown_ok=False)
    r = probe.run_probe(c, cfg, CHAT, "123:abcdef0123456789")
    w = probe.write_results(r, cfg, conn)
    assert w == {"markdown_mode": "text", "verify_capability": "ok",
                 "verify_capability_tokens_version": "123:abcdef0123456789"}
    assert json.loads(cfg.path.read_text())["markdown_mode"] == "text" and cfg["markdown_mode"] == "text"
    assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == "ok"
    assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_VERSION_KEY) == "123:abcdef0123456789"


def test_write_results_skips_when_identity_bad_or_incomplete(cfg, conn, clock):
    r = probe.run_probe(make_client(clock, auth=dict(AUTH_OK, team_id="T_X")), cfg, CHAT, "v")
    assert probe.write_results(r, cfg, conn) == {}
    assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == "unverified"
    c = make_client(clock)
    c.cooldown_store.publish("conversations.replies", clock.wall_ms() + 99_000)
    r = probe.run_probe(c, cfg, CHAT, "v")
    w = probe.write_results(r, cfg, conn)
    assert w == {"markdown_mode": "markdown_text"}      # 有结论的部分写,verify_capability 不写
    assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == "unverified"


# ---------------------------------------------------------------- main() 端到端(monkeypatch urlopen 层)
class _Resp:
    def __init__(self, body, status=200, headers=None):
        self.status = status
        self.headers = Message()
        for k, v in (headers or {}).items():
            self.headers[k] = v
        self._body = json.dumps(body).encode("utf-8")

    def getcode(self):
        return self.status

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _SlackSim:
    """极简 Slack 服务端:按 method 路由;记录请求;messages 带 metadata。"""

    def __init__(self, auth=AUTH_OK, markdown_ok=True):
        self.auth = auth
        self.markdown_ok = markdown_ok
        self.requests = []
        self.messages = {}
        self.n = 0

    def __call__(self, req, timeout_s):
        method = req.full_url.rsplit("/", 1)[1]
        ctype = req.get_header("Content-type") or ""
        if ctype.startswith("application/json"):
            # 真机事实(2026-09-24):读类方法不接受 JSON 请求体 → invalid_arguments(缺 channel/ts)
            if method in ("conversations.history", "conversations.replies", "conversations.list"):
                return _Resp({"ok": False, "error": "invalid_arguments",
                              "response_metadata": {"messages": ["[ERROR] missing required field: channel"]}})
            params = json.loads(req.data.decode("utf-8"))
        else:
            params = dict(urllib.parse.parse_qsl(req.data.decode("utf-8")))
        self.requests.append((method, params, req.get_header("Authorization")))
        if method == "auth.test":
            return _Resp(dict(self.auth, ok=True))
        if method == "chat.postMessage":
            if "markdown_text" in params and not self.markdown_ok:
                return _Resp({"ok": False, "error": "invalid_arguments"})
            self.n += 1
            ts = "1700000000.%06d" % self.n
            self.messages[ts] = params
            return _Resp({"ok": True, "channel": params["channel"], "ts": ts, "message": {"ts": ts}})
        if method in ("conversations.history", "conversations.replies"):
            msgs = [{"ts": ts, "bot_id": BOT_ID, "metadata": p.get("metadata")}
                    for ts, p in self.messages.items()]
            return _Resp({"ok": True, "messages": msgs, "has_more": False})
        if method == "chat.delete":
            self.messages.pop(params["ts"], None)
            return _Resp({"ok": True})
        return _Resp({"ok": False, "error": "unknown_method"})


def test_main_end_to_end_writes_and_never_prints_token(cfg, tokens, monkeypatch):
    sim = _SlackSim()
    monkeypatch.setattr(slackapi, "_open", sim)
    out = io.StringIO()
    rc = probe.main(["--chat-id", CHAT, "--write-config"], out=out)
    assert rc == 0
    text = out.getvalue()
    assert "xoxb" not in text and "xapp" not in text
    res = json.loads(text)
    assert res["identity_ok"] and res["complete"] and res["verify_capability"] == "ok"
    assert res["tokens_version"] == tokens.version
    assert res["written"]["verify_capability_tokens_version"] == tokens.version
    assert all(a == "Bearer xoxb-test-bot-token" for _, _, a in sim.requests)
    assert sim.messages == {}    # 已清理
    assert json.loads(paths.config_path().read_text())["markdown_mode"] == "markdown_text"
    conn = dbmod.connect(paths.db_path())
    try:
        assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == "ok"
        assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_VERSION_KEY) == tokens.version
        assert dbmod.get_state(conn, "schema_version") == "1"   # 建了 schema
    finally:
        conn.close()


def test_main_identity_mismatch_exit_3_no_messages(cfg, tokens, monkeypatch):
    sim = _SlackSim(auth=dict(AUTH_OK, user_id="U_OTHER"))
    monkeypatch.setattr(slackapi, "_open", sim)
    out = io.StringIO()
    rc = probe.main(["--chat-id", CHAT, "--write-config"], out=out)
    assert rc == 3
    assert [m for m, _, _ in sim.requests] == ["auth.test"]
    assert json.loads(paths.config_path().read_text()).get("markdown_mode") == "markdown_text"  # cfg fixture 原值
    conn = dbmod.connect(paths.db_path())
    try:
        assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == "unverified"
    finally:
        conn.close()


def test_main_without_write_config_does_not_create_db(cfg, tokens, monkeypatch):
    monkeypatch.setattr(slackapi, "_open", _SlackSim(markdown_ok=False))
    out = io.StringIO()
    rc = probe.main(["--chat-id", CHAT], out=out)
    assert rc == 0 and not paths.db_path().exists()
    res = json.loads(out.getvalue())
    assert res["markdown_mode"] == "text" and "written" not in res


def test_main_honours_daemon_state_cooldown(cfg, tokens, conn, monkeypatch):
    slackapi.DaemonStateCooldownStore(conn).publish("chat.postMessage", 4_000_000_000_000)  # 远未来
    sim = _SlackSim()
    monkeypatch.setattr(slackapi, "_open", sim)
    out = io.StringIO()
    rc = probe.main(["--chat-id", CHAT], out=out)
    assert rc == 4
    assert [m for m, _, _ in sim.requests] == ["auth.test"]
    assert json.loads(out.getvalue())["errors"][0].startswith("chat.postMessage:wait:")


def test_main_config_or_tokens_missing_exit_2(data_dir, cfg, monkeypatch):
    out = io.StringIO()
    assert probe.main(["--chat-id", CHAT], out=out) == 2      # 无 tokens.json
    assert json.loads(out.getvalue())["error"] == "config"
    os.unlink(paths.config_path())
    out = io.StringIO()
    assert probe.main(["--chat-id", CHAT], out=out) == 2


def test_main_env_tokens_version_env(cfg, monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-from-env")
    sim = _SlackSim()
    monkeypatch.setattr(slackapi, "_open", sim)
    out = io.StringIO()
    assert probe.main(["--chat-id", CHAT], out=out) == 0
    res = json.loads(out.getvalue())
    assert res["tokens_version"] == "env" and "from-env" not in out.getvalue()
    assert sim.requests[0][2] == "Bearer xoxb-from-env"
