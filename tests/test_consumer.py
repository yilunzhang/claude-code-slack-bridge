"""bin/slack_consumer.py 子进程协议(contracts §9 / I6):脚本化假 slack_sdk(tests/fakes)+ 真实 SQLite。
- 提交后才 ack:持有 BEGIN IMMEDIATE 的另一连接下 → 锁超时 → **不 ack**、无行;
- 重复键仍 ack(staged_dup);event_key None → ack + staged_invalid、不落库;其它信封类型 ack 后丢弃;
- 接收时钉死 binding_id;payload 原样;stdout 不承载数据;stderr 哨兵 / 状态行;token 绝不出现在输出;
- 退出码矩阵:0 stdin EOF / SIGTERM / 看门狗;2 tokens;3 致命鉴权 / no_hello;4 缺 sdk;5 其它。"""
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time

import pytest

from lib import config as configmod
from lib import constants, db as dbmod, paths, util
from tests.conftest import CHAT, DM, OWNER, TEAM
from tests.helpers import block_action, envelope, message_event

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONSUMER = ROOT / "bin" / "slack_consumer.py"
FAKES = ROOT / "tests" / "fakes"
APP_TOKEN = "xapp-test-app-token"   # tests/conftest.tokens 写入的值
BOT_TOKEN = "xoxb-test-bot-token"

HELLO = {"type": "hello", "num_connections": 1}
DONE = {"__control": "done"}


def frame(etype, envelope_id, payload, **extra):
    d = {"type": etype, "envelope_id": envelope_id, "payload": payload,
         "accepts_response_payload": False}
    d.update(extra)
    return d


class ConsumerRun:
    """启动 consumer 子进程(假 sdk 经 PYTHONPATH 抢占),提供等待/收尾/日志解析。"""

    def __init__(self, tmp_path, script, env_extra=None, python=None, pythonpath=None):
        self.ack_log = tmp_path / "ack.log"
        env = dict(os.environ)
        env["PYTHONPATH"] = str(pythonpath if pythonpath is not None else FAKES)
        env["FAKE_SLACK_SCRIPT"] = json.dumps(script)
        env["FAKE_SLACK_ACK_LOG"] = str(self.ack_log)
        env.setdefault("SLACK_BRIDGE_CONSUMER_HELLO_TIMEOUT_S", "5")
        env.update(env_extra or {})
        self.proc = subprocess.Popen(
            [python or sys.executable, str(CONSUMER)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        self.out = self.err = None

    def events(self):
        if not self.ack_log.exists():
            return []
        out = []
        for line in self.ack_log.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(json.loads(line))
        return out

    def acks(self):
        return [e for e in self.events() if "envelope_id" in e]

    def event_names(self):
        return [e["__event"] for e in self.events() if "__event" in e]

    def wait_marker(self, marker="script_done", timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if marker in self.event_names():
                return True
            if self.proc.poll() is not None:
                return False
            time.sleep(0.02)
        return False

    def close_stdin(self):
        try:
            self.proc.stdin.close()
        except OSError:
            pass

    def finish(self, timeout=15):
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.out = self.proc.stdout.read()
        self.err = self.proc.stderr.read().decode("utf-8", "replace")
        self.proc.stdout.close()
        self.proc.stderr.close()
        try:
            self.proc.stdin.close()
        except (OSError, ValueError):
            pass
        return self.proc.returncode

    def stderr_lines(self):
        return [l for l in (self.err or "").splitlines() if l.strip()]


def assert_no_secret(run):
    blob = (run.err or "") + (run.out or b"").decode("utf-8", "replace")
    if run.ack_log.exists():
        blob += run.ack_log.read_text(encoding="utf-8")
    assert APP_TOKEN not in blob and BOT_TOKEN not in blob
    assert "xapp-" not in blob and "xoxb-" not in blob


def counter(conn, key):
    return int(dbmod.get_state(conn, key, "0") or 0)


def make_binding(conn, chat_id=CHAT, binding_id="b1", status="active"):
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,session_id,cc_pid,cc_start,status,bind_phase) "
        "VALUES(?,?,?,1,'x',?,'confirmed')", (binding_id, chat_id, "sess-" + binding_id, status))
    return binding_id


# ======================================================================
# 协议:落库 → ack;重复/无效/其它类型;钉死绑定;stdout 无数据
# ======================================================================
def test_protocol_end_to_end(conn, tokens, tmp_path):
    bid = make_binding(conn, CHAT)
    ev = message_event(text="hi", channel=CHAT, user=OWNER)
    ev_payload = envelope(ev, event_id="EvE2E")
    act_payload = block_action("p1", "n1", user=OWNER, channel=CHAT, card_ts="1.1", action_ts="2.2")
    dm_payload = envelope(message_event(text="dm", channel=DM, user=OWNER), event_id="EvDM")
    script = [
        HELLO,
        frame("events_api", "e1", ev_payload),
        frame("interactive", "e2", act_payload),
        frame("events_api", "e1dup", ev_payload, retry_attempt=1, retry_reason="timeout"),  # 重复键
        frame("events_api", "e3", {"event": {"type": "message"}}),                            # 无 event_id
        frame("slash_commands", "e4", {"command": "/x"}),                                      # 其它类型
        frame("events_api", "e5", dm_payload),                                                # 无绑定的 DM
        {"type": "disconnect", "reason": "refresh_requested"},
        DONE,
    ]
    run = ConsumerRun(tmp_path, script)
    assert run.wait_marker(), (run.finish(), run.err)
    run.close_stdin()
    rc = run.finish()
    assert rc == constants.CONSUMER_RC_OK == 0
    assert run.out == b""                                       # stdout 不承载数据
    assert_no_secret(run)
    lines = run.stderr_lines()
    ready = [l for l in lines if l.startswith(constants.CONSUMER_READY_SENTINEL)]
    assert ready and ready[0] == "[socket] ready num_connections=1"
    assert len(ready) == 2                                      # disconnect 帧后 sdk 重连 → 再次 hello
    assert "[socket] disconnect reason=refresh_requested" in lines
    assert lines[-1] == "[socket] exit reason=stdin_eof rc=0"
    assert not any("warn" in l or "fatal" in l for l in lines)
    # ---- 行 ----
    rows = {r["event_key"]: r for r in conn.execute("SELECT * FROM slack_events ORDER BY seq")}
    assert set(rows) == {"ev:EvE2E", "act:%s:%s:1.1:%s:sb_approve:2.2" % (TEAM, CHAT, OWNER), "ev:EvDM"}
    r = rows["ev:EvE2E"]
    assert r["envelope_type"] == "events_api" and r["chat_id"] == CHAT and r["binding_id"] == bid
    assert r["state"] == "staged" and r["drain_attempts"] == 0 and r["next_drain_at"] is None
    assert json.loads(r["payload_json"]) == ev_payload and r["payload_json"] == util.jdumps(ev_payload)
    assert isinstance(r["received_at"], int) and r["received_at"] > 1_600_000_000_000
    a = rows["act:%s:%s:1.1:%s:sb_approve:2.2" % (TEAM, CHAT, OWNER)]
    assert a["envelope_type"] == "interactive" and a["binding_id"] == bid and a["chat_id"] == CHAT
    assert json.loads(a["payload_json"]) == act_payload
    assert rows["ev:EvDM"]["binding_id"] is None and rows["ev:EvDM"]["chat_id"] == DM
    # ---- ack:五个信封各恰一次,只含 envelope_id ----
    acks = run.acks()
    assert sorted(x["envelope_id"] for x in acks) == ["e1", "e1dup", "e2", "e3", "e4", "e5"]
    assert all(set(x) == {"envelope_id"} for x in acks)
    assert counter(conn, "staged_dup") == 1 and counter(conn, "staged_invalid") == 1
    # ---- 构造契约(R2-N3):关键字参数、retry_handlers=[]、auto_reconnect、ping_interval=10 ----
    ctor = [e for e in run.events() if e.get("__event") == "ctor"][0]
    assert ctor["app_token_set"] and ctor["web_client_given"] and ctor["web_client_token_is_app_token"]
    assert ctor["web_client_retry_handlers"] == [] and ctor["auto_reconnect_enabled"] is True
    assert ctor["ping_interval"] == 10 and ctor["extra_kwargs"] == []
    names = run.event_names()
    assert names.index("connect") < names.index("script_done") < names.index("close")  # 退出前 close()


def test_no_ack_when_db_locked(conn, tokens, tmp_path):
    """另一连接持有 BEGIN IMMEDIATE → consumer 锁超时(CONSUMER_DB_BUSY_MS)→ 不 ack、无行;
    无效键的信封(不需要落库)照常 ack。"""
    make_binding(conn, CHAT)
    payload = envelope(message_event(channel=CHAT, user=OWNER), event_id="EvLOCK")
    script = [HELLO, frame("events_api", "e1", payload), frame("events_api", "e3", {"event": {}}), DONE]
    conn.execute("BEGIN IMMEDIATE")   # 持锁直到脚本跑完
    try:
        run = ConsumerRun(tmp_path, script)
        t0 = time.time()
        assert run.wait_marker(timeout=20), (run.finish(), run.err)
        elapsed = time.time() - t0
        assert elapsed >= constants.CONSUMER_DB_BUSY_MS / 1000.0 * 0.8   # 真等过 busy 超时
        assert [x["envelope_id"] for x in run.acks()] == ["e3"]
    finally:
        conn.execute("ROLLBACK")
    run.close_stdin()
    assert run.finish() == 0
    assert conn.execute("SELECT COUNT(*) FROM slack_events").fetchone()[0] == 0
    assert any(l.startswith("[socket] warn db_locked envelope=e1") for l in run.stderr_lines())
    assert_no_secret(run)


def test_reconnect_after_close_keeps_consuming(conn, tokens, tmp_path):
    make_binding(conn, CHAT)
    payload = envelope(message_event(channel=CHAT, user=OWNER), event_id="EvRC")
    script = [HELLO, {"__control": "close", "code": 1006, "reason": "gone", "reconnect": True,
                      "reconnect_after": 0.1},
              frame("events_api", "e9", payload), DONE]
    run = ConsumerRun(tmp_path, script)
    assert run.wait_marker(), (run.finish(), run.err)
    run.close_stdin()
    assert run.finish() == 0
    lines = run.stderr_lines()
    assert sum(1 for l in lines if l.startswith(constants.CONSUMER_READY_SENTINEL)) == 2
    assert any(l.startswith("[socket] disconnect reason=close code=1006") for l in lines)
    assert [x["envelope_id"] for x in run.acks()] == ["e9"]
    assert conn.execute("SELECT event_key FROM slack_events").fetchone()[0] == "ev:EvRC"


# ======================================================================
# 退出码矩阵
# ======================================================================
def test_rc_stdin_eof_and_sigterm(conn, tokens, tmp_path):
    run = ConsumerRun(tmp_path, [HELLO, DONE])
    assert run.wait_marker(), (run.finish(), run.err)
    run.proc.send_signal(signal.SIGTERM)
    assert run.finish() == 0
    assert run.stderr_lines()[-1] == "[socket] exit reason=sigterm rc=0"
    assert "close" in run.event_names()                        # SIGTERM 后先 client.close()


def test_rc_disconnect_watchdog(conn, tokens, tmp_path):
    script = [HELLO, {"__control": "close", "code": 1006, "reason": "network", "reconnect": False}, DONE]
    run = ConsumerRun(tmp_path, script, env_extra={"SLACK_BRIDGE_CONSUMER_DISCONNECT_EXIT_S": "0.5"})
    assert run.finish(timeout=15) == 0                          # stdin 仍开着:靠看门狗退出
    lines = run.stderr_lines()
    assert any(l.startswith("[socket] disconnect reason=close code=1006") for l in lines)
    assert any(l.startswith("[socket] disconnect reason=watchdog") for l in lines)
    assert lines[-1] == "[socket] exit reason=watchdog rc=0"


def test_rc_missing_sdk(conn, tokens, tmp_path):
    nosdk = tmp_path / "nosdk" / "slack_sdk"
    nosdk.mkdir(parents=True)
    (nosdk / "__init__.py").write_text("raise ImportError('simulated missing slack_sdk')\n")
    run = ConsumerRun(tmp_path, [HELLO], pythonpath=tmp_path / "nosdk")
    assert run.finish() == constants.CONSUMER_RC_NO_SDK == 4
    assert run.stderr_lines() == ["[socket] fatal missing_dependency slack_sdk"]


def test_rc_tokens_missing(conn, data_dir, tmp_path):
    run = ConsumerRun(tmp_path, [HELLO])
    assert run.finish() == constants.CONSUMER_RC_TOKENS == 2
    assert run.stderr_lines()[0].startswith("[socket] fatal tokens")


def test_rc_tokens_without_app_token(conn, data_dir, tmp_path):
    configmod.save_tokens({"bot_token": "xoxb-only"})
    run = ConsumerRun(tmp_path, [HELLO])
    assert run.finish() == 2
    assert run.stderr_lines() == ["[socket] fatal tokens missing app_token"]
    assert "xoxb-only" not in run.err


def test_rc_tokens_bad_permissions(conn, tokens, tmp_path):
    os.chmod(tokens.path, 0o644)
    run = ConsumerRun(tmp_path, [HELLO])
    assert run.finish() == 2
    assert "0600" in run.stderr_lines()[0]
    assert_no_secret(run)


def test_rc_tokens_env_is_not_honoured(conn, data_dir, tmp_path):
    """daemon 语义:文件是真相(allow_env=False)—— env 里有 token 也不算。"""
    run = ConsumerRun(tmp_path, [HELLO], env_extra={"SLACK_BOT_TOKEN": "xoxb-env", "SLACK_APP_TOKEN": "xapp-env"})
    assert run.finish() == 2
    assert "xapp-env" not in run.err


@pytest.mark.parametrize("error,rc", [
    ("invalid_auth", 3), ("link_disabled", 3), ("token_revoked", 3), ("not_authed", 3),
    ("missing_scope", 3), ("internal_error", 5), ("service_unavailable", 5),
])
def test_rc_connect_slack_error(conn, tokens, tmp_path, error, rc):
    run = ConsumerRun(tmp_path, [{"__control": "connect_error", "error": error}, HELLO])
    assert run.finish() == rc
    lines = run.stderr_lines()
    assert lines[0] == "[socket] connecting" and lines[1] == "[socket] fatal %s" % error
    assert "close" in run.event_names()
    assert_no_secret(run)


def test_rc_connect_network_exception(conn, tokens, tmp_path):
    run = ConsumerRun(tmp_path, [{"__control": "connect_exception", "kind": "network"}, HELLO])
    assert run.finish() == constants.CONSUMER_RC_OTHER == 5
    assert run.stderr_lines()[1].startswith("[socket] fatal connect_failed")


def test_rc_no_hello_is_fatal_auth(conn, tokens, tmp_path):
    run = ConsumerRun(tmp_path, [{"__control": "sleep", "seconds": 5}],
                      env_extra={"SLACK_BRIDGE_CONSUMER_HELLO_TIMEOUT_S": "0.3"})
    assert run.finish() == constants.CONSUMER_RC_AUTH == 3
    lines = run.stderr_lines()
    assert lines[1].startswith("[socket] fatal no_hello")
    assert lines[-1] == "[socket] exit reason=no_hello rc=3"


def test_rc_db_missing(tokens, data_dir, tmp_path):
    assert not paths.db_path().exists()
    run = ConsumerRun(tmp_path, [HELLO])
    assert run.finish() == 5
    assert run.stderr_lines() == ["[socket] fatal db_missing"]
    assert not paths.db_path().exists()                          # 不顺手建空库


def test_rc_db_schema_mismatch(tokens, data_dir, tmp_path):
    import sqlite3
    c = sqlite3.connect(str(paths.db_path()))
    c.execute("CREATE TABLE daemon_state(key TEXT PRIMARY KEY, value TEXT)")
    c.execute("INSERT INTO daemon_state VALUES('schema_version','999')")
    c.commit()
    c.close()
    run = ConsumerRun(tmp_path, [HELLO])
    assert run.finish() == 5
    assert run.stderr_lines()[0].startswith("[socket] fatal db_schema")


# ======================================================================
# 静态纪律
# ======================================================================
def test_only_consumer_imports_slack_sdk():
    pat = re.compile(r"^\s*(from|import)\s+slack_sdk\b", re.M)
    offenders = []
    for d in ("lib", "bin", "hooks", "scripts"):
        for p in (ROOT / d).rglob("*.py"):
            if pat.search(p.read_text(encoding="utf-8")):
                offenders.append(str(p.relative_to(ROOT)))
    assert offenders == ["bin/slack_consumer.py"]


def test_consumer_uses_frozen_sdk_calls_and_slackwire():
    src = CONSUMER.read_text(encoding="utf-8")
    assert "retry_handlers=[]" in src and "auto_reconnect_enabled=True" in src and "ping_interval=10" in src
    assert "slackwire.event_key(" in src and "slackwire.chat_of(" in src
    assert "ON CONFLICT(event_key) DO NOTHING" in src
    assert "allow_env=False" in src
    assert "socket_mode_request_listeners.append" in src
