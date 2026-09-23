"""真实 slack_sdk 的**运行层**契约(R2-N3 / R3-m4):未安装则 skip;WP5 必须在 `.venv-test` 里跑通:
    python3 -m venv .venv-test && .venv-test/bin/pip install 'slack_sdk>=3.44,<4' pytest
    .venv-test/bin/python -m pytest tests/test_sdk_contract.py -q
用真实 `SocketModeClient` 类:monkeypatch 底层 Connection / issue_new_wss_url / send_message,
按 consumer 的写法关键字构造,经 sdk 自己的 `_on_message → enqueue_message → process_message` 路径注入合成
events_api / interactive 帧,断言 listener 以 `(client, SocketModeRequest)` 被调、
`send_socket_mode_response` 产生 `{"envelope_id": …}`、`close()` 干净退出。
另有一条**不依赖真 sdk** 的端到端:bin/slack_consumer.py 子进程 × tests/fakes/slack_sdk × 真实 schema。"""
import json
import threading
import time

import pytest

from lib import constants
from tests.conftest import CHAT, OWNER, TEAM
from tests.helpers import block_action, envelope, message_event
from tests.test_consumer import DONE, HELLO, ConsumerRun, assert_no_secret, counter, frame, make_binding

try:
    import slack_sdk  # noqa: F401
    import slack_sdk.socket_mode.builtin.client as builtin_client
    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse
    from slack_sdk.web import WebClient
    HAS_SDK = True
except ImportError:  # pragma: no cover - 环境相关
    HAS_SDK = False

needs_sdk = pytest.mark.skipif(not HAS_SDK, reason="slack_sdk 未安装(用 .venv-test 跑)")


class FakeConnection:
    """替代 slack_sdk.socket_mode.builtin.connection.Connection:不开 socket,把 sdk 传进来的
    on_message_listener 暴露给测试用于注入原始帧;send() 记录发出的文本。"""
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.on_message_listener = kwargs.get("on_message_listener")
        self.on_close_listener = kwargs.get("on_close_listener")
        self.session_id = "fake-session-%d" % len(FakeConnection.instances)
        self._active = False
        self.sent = []
        FakeConnection.instances.append(self)

    def connect(self):
        self._active = True

    def is_active(self):
        return self._active

    def disconnect(self):
        self._active = False

    def close(self):
        self._active = False

    def send(self, payload):
        if not self._active:
            raise RuntimeError("not active")
        self.sent.append(payload)

    def check_state(self):
        pass

    def run_until_completion(self, state):
        time.sleep(0.02)


@pytest.fixture
def real_client(monkeypatch):
    monkeypatch.setattr(builtin_client, "Connection", FakeConnection)
    FakeConnection.instances.clear()
    web = WebClient(token="xapp-fake-app-token", retry_handlers=[])
    client = SocketModeClient(app_token="xapp-fake-app-token", web_client=web,
                              auto_reconnect_enabled=True, ping_interval=10)
    monkeypatch.setattr(client, "issue_new_wss_url", lambda: "wss://fake.invalid/link/1")
    try:
        yield client, web
    finally:
        client.close()


@needs_sdk
def test_sdk_version_in_frozen_range():
    from slack_sdk.version import __version__
    major, minor = (int(x) for x in __version__.split(".")[:2])
    assert (major, minor) >= (3, 44) and major < 4


@needs_sdk
def test_keyword_construction_and_listener_wiring(real_client):
    client, web = real_client
    assert web.retry_handlers == [] and client.web_client is web
    assert client.auto_reconnect_enabled is True and client.ping_interval == 10
    assert client.socket_mode_request_listeners == [] and client.on_message_listeners == []
    assert hasattr(client, "on_close_listeners") and hasattr(client, "on_error_listeners")
    assert client.is_connected() is False


@needs_sdk
def test_request_path_via_sdk_connection_and_ack_shape(real_client):
    client, _ = real_client
    seen, raws = [], []
    done = threading.Event()

    def listener(c, req):
        seen.append((c, req))
        c.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
        if len(seen) == 2:
            done.set()
    client.socket_mode_request_listeners.append(listener)
    client.on_message_listeners.append(raws.append)
    client.connect()
    assert client.is_connected()
    conn = FakeConnection.instances[-1]
    assert conn.kwargs["url"] == "wss://fake.invalid/link/1" and conn.kwargs["ping_interval"] == 10
    ev_payload = envelope(message_event(text="hi", channel=CHAT, user=OWNER), event_id="EvSDK")
    act_payload = block_action("p", "n", user=OWNER, channel=CHAT, card_ts="1.1", action_ts="2.2")
    # 经 sdk 自己的路径:Connection.on_message_listener == client._on_message → enqueue → process → listeners
    conn.on_message_listener(json.dumps({"type": "hello", "num_connections": 1}))
    conn.on_message_listener(json.dumps({"type": "events_api", "envelope_id": "env-1", "payload": ev_payload,
                                         "accepts_response_payload": False, "retry_attempt": 0,
                                         "retry_reason": ""}))
    conn.on_message_listener(json.dumps({"type": "interactive", "envelope_id": "env-2",
                                         "payload": act_payload, "accepts_response_payload": False}))
    assert done.wait(5), "socket_mode_request_listeners never invoked"
    for c, req in seen:
        assert c is client and isinstance(req, SocketModeRequest)
        assert isinstance(req.payload, dict) and isinstance(req.envelope_id, str)
    assert [r.type for _, r in seen] == ["events_api", "interactive"]
    assert seen[0][1].payload == ev_payload and seen[1][1].payload == act_payload
    assert seen[0][1].retry_attempt == 0
    assert [json.loads(s) for s in conn.sent] == [{"envelope_id": "env-1"}, {"envelope_id": "env-2"}]
    assert any(json.loads(r).get("type") == "hello" for r in raws)   # 原始帧钩子看得到 hello
    client.close()
    assert client.closed is True and client.is_connected() is False


@needs_sdk
def test_enqueue_process_path_with_send_message_patched(real_client, monkeypatch):
    client, _ = real_client
    sent, seen = [], []
    got = threading.Event()
    monkeypatch.setattr(client, "send_message", lambda m: sent.append(m))
    client.socket_mode_request_listeners.append(lambda c, r: (seen.append(r), got.set()))
    client.enqueue_message(json.dumps({"type": "events_api", "envelope_id": "env-9",
                                       "payload": envelope(message_event(), event_id="EvQ")}))
    assert got.wait(5)
    client.send_socket_mode_response(SocketModeResponse(envelope_id=seen[0].envelope_id))
    assert json.loads(sent[0]) == {"envelope_id": "env-9"}
    assert SocketModeRequest.from_dict({"type": "x"}) is None                 # 缺字段 → 不投递给 listener
    assert SocketModeRequest.from_dict({"type": "x", "envelope_id": "e", "payload": {}}).type == "x"


@needs_sdk
def test_close_is_idempotent_and_stops_threads(real_client):
    client, _ = real_client
    client.connect()
    client.close()
    client.close()
    assert client.closed and not client.is_connected()
    assert not client.message_processor.is_alive()


# ======================================================================
# 端到端:bin/slack_consumer.py × tests/fakes/slack_sdk × 真实 schema(不需要真 sdk)
# ======================================================================
def test_consumer_end_to_end_with_fake_sdk(conn, tokens, tmp_path):
    from lib import util
    bid = make_binding(conn, CHAT)
    ev_payload = envelope(message_event(text="hi", channel=CHAT, user=OWNER), event_id="EvSDKE2E")
    act_payload = block_action("p", "n", user=OWNER, channel=CHAT, card_ts="1.1", action_ts="2.2")
    script = [HELLO,
              frame("events_api", "e1", ev_payload),
              frame("interactive", "e2", act_payload),
              frame("events_api", "e1dup", ev_payload, retry_attempt=1, retry_reason="timeout"),
              frame("events_api", "e3", {"event": {"type": "message"}}),      # 键不合法
              {"type": "disconnect", "reason": "refresh_requested"},
              DONE]
    run = ConsumerRun(tmp_path, script)
    assert run.wait_marker(), (run.finish(), run.err)
    # 每个 ack 出现时对应行必须已经提交(dup/invalid 除外:它们不落库)
    rows = {r["event_key"]: r for r in conn.execute("SELECT * FROM slack_events")}
    acked = {a["envelope_id"] for a in run.acks()}
    assert acked == {"e1", "e2", "e1dup", "e3"}
    assert "ev:EvSDKE2E" in rows and rows["ev:EvSDKE2E"]["binding_id"] == bid
    key = "act:%s:%s:1.1:%s:sb_approve:2.2" % (TEAM, CHAT, OWNER)
    assert key in rows and rows[key]["binding_id"] == bid and rows[key]["state"] == "staged"
    assert len(rows) == 2 and json.loads(rows["ev:EvSDKE2E"]["payload_json"]) == ev_payload
    assert rows["ev:EvSDKE2E"]["payload_json"] == util.jdumps(ev_payload)
    assert counter(conn, "staged_dup") == 1 and counter(conn, "staged_invalid") == 1
    run.close_stdin()
    assert run.finish() == constants.CONSUMER_RC_OK
    assert run.out == b""
    lines = run.stderr_lines()
    assert lines[1] == "[socket] ready num_connections=1"
    assert "[socket] disconnect reason=refresh_requested" in lines
    assert lines[-1] == "[socket] exit reason=stdin_eof rc=0"
    assert_no_secret(run)
