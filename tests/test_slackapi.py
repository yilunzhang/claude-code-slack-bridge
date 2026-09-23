"""lib/slackapi:SlackClient.call(urllib,无重试/不跟随重定向/冷却预检/429 发布冷却/not_sent 分类)、
CooldownStore 实现、classify_send_error、凭据 reload。零真实网络:monkeypatch slackapi._open,
另加一个 127.0.0.1 上的真实 http.server 走完整 urllib 路径。"""
import http.client
import http.server
import io
import json
import socket
import threading
import urllib.error
from email.message import Message

import pytest

from lib import config as configmod
from lib import constants, slackapi
from lib.slackapi import CallResult, DaemonStateCooldownStore, InMemoryCooldownStore, SlackClient
from tests.helpers import FakeClock


# ---------------------------------------------------------------- 假响应工具
class _Resp:
    def __init__(self, status=200, body=b"", headers=None):
        self.status = status
        self.headers = Message()
        for k, v in (headers or {}).items():
            self.headers[k] = v
        self._body = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")

    def getcode(self):
        return self.status

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _http_error(status, body=None, headers=None):
    hdrs = Message()
    for k, v in (headers or {}).items():
        hdrs[k] = v
    raw = b"" if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    return urllib.error.HTTPError("https://slack.com/api/x", status, "err", hdrs, io.BytesIO(raw))


class Net:
    """记录请求 + 按脚本返回/抛出。"""

    def __init__(self, script):
        self.script = list(script)
        self.requests = []   # [(req, timeout)]

    def __call__(self, req, timeout_s):
        self.requests.append((req, timeout_s))
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


@pytest.fixture
def net(monkeypatch):
    def arm(*script):
        n = Net(script)
        monkeypatch.setattr(slackapi, "_open", n)
        return n
    return arm


def mk(clock=None, store=None, token="xoxb-secret-token", **kw):
    return SlackClient(token=token, cooldown_store=store or InMemoryCooldownStore(),
                       clock=clock or FakeClock(), **kw)


# ---------------------------------------------------------------- 请求形状
def test_request_shape_post_json_bearer(net):
    n = net(_Resp(200, {"ok": True, "ts": "1.1"}))
    c = mk()
    res = c.call("chat.postMessage", {"channel": "C1", "text": "héllo"}, timeout_s=7)
    assert res.ok and res.data["ts"] == "1.1" and res.http_status == 200
    req, to = n.requests[0]
    assert req.full_url == "https://slack.com/api/chat.postMessage"
    assert req.get_method() == "POST" and to == 7
    assert req.get_header("Authorization") == "Bearer xoxb-secret-token"
    assert req.get_header("Content-type").startswith("application/json")
    assert json.loads(req.data.decode("utf-8")) == {"channel": "C1", "text": "héllo"}


def test_default_timeout_and_no_token(net):
    n = net(_Resp(200, {"ok": True}))
    c = mk(timeout_s=3)
    c.call("auth.test")
    assert n.requests[0][1] == 3 and c.timeout_s == 3
    assert constants.SEND_TIMEOUT_S == 15
    c2 = SlackClient(token="")
    r = c2.call("auth.test")
    assert not r.ok and r.not_sent and r.error == "no_token"


def test_repr_never_contains_token():
    c = mk(token="xoxb-SUPERSECRET")
    assert "SUPERSECRET" not in repr(c) and "SUPERSECRET" not in str(c)
    with pytest.raises(ValueError):
        SlackClient()


# ---------------------------------------------------------------- 结果分类(响应)
def test_api_error_body(net):
    net(_Resp(200, {"ok": False, "error": "channel_not_found"}))
    r = mk().call("chat.postMessage", {})
    assert not r.ok and r.error == "channel_not_found" and r.http_status == 200
    assert slackapi.classify_send_error(r) == "failed"


def test_unparseable_body_is_unknown(net):
    net(_Resp(200, b"<html>gateway</html>"))
    r = mk().call("chat.postMessage", {})
    assert r.error == "unparseable" and slackapi.classify_send_error(r) == "unknown"
    net(_Resp(200, b"[1,2]"))
    assert mk().call("x").error == "unparseable"


def test_error_without_code(net):
    net(_Resp(200, {"ok": False}))
    r = mk().call("x")
    assert r.error == "error_without_code" and slackapi.classify_send_error(r) == "unknown"


def test_http_5xx_is_unknown_not_not_sent(net):
    net(_http_error(503, b"upstream"))
    r = mk().call("chat.postMessage", {})
    assert r.http_status == 503 and r.error == "http_5xx" and not r.not_sent
    assert slackapi.classify_send_error(r) == "unknown"


def test_http_4xx_with_slack_error_body(net):
    net(_http_error(400, {"ok": False, "error": "invalid_blocks"}))
    r = mk().call("chat.postMessage", {})
    assert r.http_status == 400 and r.error == "invalid_blocks"
    assert slackapi.classify_send_error(r) == "failed"
    net(_http_error(404, b"nope"))
    assert mk().call("x").error == "http_404"


def test_redirect_not_followed(net):
    n = net(_http_error(302, b"", {"Location": "https://evil.example/"}))
    r = mk().call("chat.postMessage", {})
    assert len(n.requests) == 1 and r.http_status == 302 and r.error == "http_redirect"
    assert slackapi.classify_send_error(r) == "unknown"  # 请求已写出,不能断言未发送


# ---------------------------------------------------------------- 429 / 冷却
def test_429_publishes_cooldown_with_retry_after(net):
    clock = FakeClock(wall=1_000_000)
    store = InMemoryCooldownStore()
    net(_http_error(429, {"ok": False, "error": "ratelimited"}, {"Retry-After": "7"}))
    c = mk(clock=clock, store=store)
    r = c.call("chat.postMessage", {})
    assert r.http_status == 429 and r.error == "ratelimited" and r.retry_after == 7
    assert slackapi.classify_send_error(r) == "ratelimited"
    assert store.get("chat.postMessage") == 1_000_000 + 7_000
    assert store.get("chat.update") is None  # 方法级


def test_429_without_retry_after_defaults_1s(net):
    clock, store = FakeClock(wall=0), InMemoryCooldownStore()
    net(_http_error(429, b""))
    r = mk(clock=clock, store=store).call("m")
    assert r.retry_after == 1 and store.get("m") == 1000


def test_ratelimited_in_200_body_also_publishes(net):
    clock, store = FakeClock(wall=0), InMemoryCooldownStore()
    net(_Resp(200, {"ok": False, "error": "ratelimited"}, {"Retry-After": "2"}))
    r = mk(clock=clock, store=store).call("m")
    assert slackapi.classify_send_error(r) == "ratelimited" and store.get("m") == 2000


def test_cooldown_precheck_returns_wait_without_request(net):
    clock, store = FakeClock(wall=10_000), InMemoryCooldownStore()
    store.publish("chat.postMessage", 15_000)
    n = net(_Resp(200, {"ok": True}))
    c = mk(clock=clock, store=store)
    r = c.call("chat.postMessage", {})
    assert n.requests == [] and r.cooldown_until == 15_000 and r.error == "cooldown"
    assert not r.not_sent and slackapi.classify_send_error(r) == "wait"
    # 其它方法不受影响
    assert c.call("chat.update", {}).ok and len(n.requests) == 1
    # 到期后放行
    clock.tick(5_000)
    n.script.append(_Resp(200, {"ok": True}))
    assert c.call("chat.postMessage", {}).ok


def test_publish_takes_max():
    s = InMemoryCooldownStore()
    assert s.publish("m", 500) == 500
    assert s.publish("m", 300) == 500
    assert s.get("m") == 500
    assert s.publish("m", 900) == 900


def test_daemon_state_cooldown_store(conn):
    s = DaemonStateCooldownStore(conn)
    assert s.get("chat.postMessage") is None
    assert s.publish("chat.postMessage", 5000) == 5000
    assert s.publish("chat.postMessage", 4000) == 5000   # max
    assert s.publish("chat.postMessage", 6000) == 6000
    row = conn.execute("SELECT value FROM daemon_state WHERE key='cooldown:chat.postMessage'").fetchone()
    assert row[0] == "6000"
    conn.execute("UPDATE daemon_state SET value='garbage' WHERE key='cooldown:chat.postMessage'")
    assert s.get("chat.postMessage") is None
    assert s.publish("chat.postMessage", 7000) == 7000   # 畸形旧值被覆盖


def test_two_stores_share_daemon_state(conn):
    a, b = DaemonStateCooldownStore(conn), DaemonStateCooldownStore(conn)
    a.publish("m", 100)
    assert b.get("m") == 100
    b.publish("m", 90)
    assert a.get("m") == 100


# ---------------------------------------------------------------- 本地/传输错误
def test_timeout_variants(net):
    for exc in (socket.timeout("t"), urllib.error.URLError(socket.timeout("t")), TimeoutError("t")):
        net(exc)
        r = mk().call("m")
        assert r.timed_out and r.error == "timeout" and not r.not_sent and r.exc is not None
        assert slackapi.classify_send_error(r) == "unknown"


def test_dns_and_refused_are_not_sent(net):
    net(urllib.error.URLError(socket.gaierror(8, "nodename nor servname")))
    r = mk().call("m")
    assert r.not_sent and r.error == "dns" and slackapi.classify_send_error(r) == "not_sent"
    net(urllib.error.URLError(ConnectionRefusedError(61, "refused")))
    r = mk().call("m")
    assert r.not_sent and r.error == "connection_refused"
    net(ConnectionRefusedError(61, "refused"))
    assert mk().call("m").not_sent
    net(socket.gaierror(8, "x"))
    assert mk().call("m").error == "dns"


def test_network_unreachable_is_not_sent(net):
    import errno
    net(urllib.error.URLError(OSError(errno.ENETUNREACH, "unreach")))
    r = mk().call("m")
    assert r.not_sent and r.error == "network_unreachable"


def test_post_write_transport_errors_are_unknown(net):
    for exc in (http.client.RemoteDisconnected("closed"), ConnectionResetError(54, "reset"),
                BrokenPipeError(32, "pipe"), urllib.error.URLError(ConnectionResetError(54, "r"))):
        net(exc)
        r = mk().call("m")
        assert not r.not_sent and not r.timed_out and not r.ok
        assert slackapi.classify_send_error(r) == "unknown"


def test_unknown_urlerror_reason_is_unknown(net):
    net(urllib.error.URLError("weird"))
    r = mk().call("m")
    assert not r.not_sent and r.error.startswith("urlerror:")


# ---------------------------------------------------------------- classify 表
@pytest.mark.parametrize("res,expected", [
    (CallResult(ok=True, data={"ok": True}), "sent"),
    (CallResult(cooldown_until=1), "wait"),
    (CallResult(http_status=429, error="ratelimited", retry_after=1), "ratelimited"),
    (CallResult(error="ratelimited", http_status=200), "ratelimited"),
    (CallResult(not_sent=True, error="dns"), "not_sent"),
    (CallResult(error="invalid_auth", http_status=200), "not_sent"),
    (CallResult(error="not_authed", http_status=200), "not_sent"),
    (CallResult(timed_out=True, error="timeout"), "unknown"),
    (CallResult(http_status=500, error="http_5xx"), "unknown"),
    (CallResult(http_status=200, error="internal_error"), "unknown"),
    (CallResult(http_status=200, error="fatal_error"), "unknown"),
    (CallResult(http_status=200, error="service_unavailable"), "unknown"),
    (CallResult(http_status=200, error="unparseable"), "unknown"),
    (CallResult(http_status=200, error="some_new_error"), "unknown"),
    (CallResult(http_status=200, error="channel_not_found"), "failed"),
    (CallResult(http_status=200, error="not_in_channel"), "failed"),
    (CallResult(http_status=200, error="msg_too_long"), "failed"),
    (CallResult(http_status=200, error="invalid_arguments"), "failed"),  # markdown 特判在 Outbound 层
    (CallResult(http_status=200, error="missing_scope"), "failed"),
    (CallResult(http_status=200, error="already_reacted"), "unknown"),   # 幂等已达成:Outbound 特判为 sent
])
def test_classify_table(res, expected):
    assert slackapi.classify_send_error(res) == expected


def test_error_sets_are_disjoint():
    assert not (constants.PERMANENT_SEND_ERRORS & constants.NOT_SENT_ERRORS)
    assert not (constants.PERMANENT_SEND_ERRORS & constants.AMBIGUOUS_SEND_ERRORS)
    assert not (constants.NOT_SENT_ERRORS & constants.AMBIGUOUS_SEND_ERRORS)


def test_callresult_fields():
    r = CallResult()
    assert vars(r).keys() == {"ok", "data", "error", "http_status", "retry_after", "timed_out",
                              "exc", "not_sent", "cooldown_until"}
    assert r.get("x", 3) == 3
    assert CallResult(data={"ts": "1"}).get("ts") == "1"


# ---------------------------------------------------------------- 凭据
def test_client_from_tokens_file_and_reload(tokens, net):
    n = net(_Resp(200, {"ok": True}), _Resp(200, {"ok": True}))
    c = SlackClient(tokens_path=tokens.path)
    assert c.tokens_version == tokens.version and ":" in c.tokens_version
    c.call("auth.test")
    assert n.requests[0][0].get_header("Authorization") == "Bearer xoxb-test-bot-token"
    # 换 token → reload → 版本变、请求用新 token
    v2 = configmod.save_tokens({"bot_token": "xoxb-NEW", "app_token": "xapp-NEW"},
                               path=tokens.path, overwrite=True)
    assert v2 != tokens.version
    assert c.reload_tokens(v2) == v2 and c.tokens_version == v2
    c.call("auth.test")
    assert n.requests[1][0].get_header("Authorization") == "Bearer xoxb-NEW"


def test_client_rejects_loose_tokens_file(tokens):
    import os
    os.chmod(tokens.path, 0o644)
    with pytest.raises(configmod.ConfigError):
        SlackClient(tokens_path=tokens.path)


def test_client_ignores_env_when_using_file(tokens, monkeypatch, net):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-ENV")
    n = net(_Resp(200, {"ok": True}))
    c = SlackClient(tokens_path=tokens.path)
    c.call("auth.test")
    assert n.requests[0][0].get_header("Authorization") == "Bearer xoxb-test-bot-token"
    assert c.tokens_version != "env"


def test_raw_token_client_version_env_and_reload_noop():
    c = SlackClient(token="xoxb-x")
    assert c.tokens_version == "env"
    assert c.reload_tokens("whatever") == "env"
    c2 = SlackClient(token="xoxb-x", tokens_version="v9")
    assert c2.tokens_version == "v9"


# ---------------------------------------------------------------- 真实 http.server(127.0.0.1)
class _Handler(http.server.BaseHTTPRequestHandler):
    calls = []

    def log_message(self, *a):  # 静默
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        type(self).calls.append((self.path, self.headers.get("Authorization"), body))
        if self.path.endswith("/rl"):
            self.send_response(429)
            self.send_header("Retry-After", "2")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":false,"error":"ratelimited"}')
        elif self.path.endswith("/redirect"):
            self.send_response(302)
            self.send_header("Location", "/api/chat.postMessage")
            self.end_headers()
        elif self.path.endswith("/broken"):
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"oops")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            echo = json.loads(body.decode("utf-8") or "{}")
            self.wfile.write(json.dumps({"ok": True, "echo": echo, "ts": "1.1"}).encode())


@pytest.fixture
def local_server():
    _Handler.calls = []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        yield "http://127.0.0.1:%d/api/" % srv.server_address[1]
    finally:
        srv.shutdown()
        srv.server_close()


def test_real_urllib_roundtrip(local_server):
    clock, store = FakeClock(wall=0), InMemoryCooldownStore()
    c = SlackClient(token="xoxb-real", base_url=local_server, cooldown_store=store, clock=clock,
                    timeout_s=5)
    r = c.call("chat.postMessage", {"channel": "C1", "text": "hi"})
    assert r.ok and r.data["echo"] == {"channel": "C1", "text": "hi"} and r.data["ts"] == "1.1"
    assert _Handler.calls[0][0] == "/api/chat.postMessage"
    assert _Handler.calls[0][1] == "Bearer xoxb-real"
    r = c.call("rl")
    assert r.http_status == 429 and r.retry_after == 2 and store.get("rl") == 2000
    assert c.call("rl").cooldown_until == 2000 and len(_Handler.calls) == 2  # 冷却挡住
    r = c.call("redirect")
    assert r.http_status == 302 and r.error == "http_redirect" and len(_Handler.calls) == 3
    r = c.call("broken")
    assert r.http_status == 500 and r.error == "http_5xx" and slackapi.classify_send_error(r) == "unknown"


def test_real_connection_refused_is_not_sent():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # 端口现在没人听
    c = SlackClient(token="x", base_url="http://127.0.0.1:%d/api/" % port, timeout_s=2)
    r = c.call("auth.test")
    assert r.not_sent and r.error == "connection_refused"
    assert slackapi.classify_send_error(r) == "not_sent"
