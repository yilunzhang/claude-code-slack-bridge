"""notify skill(bin/notifyctl.py + lib/notify.run_notify)单测(Slack 版)。
全程离线:注入 stdin_text / environ / prober / make_client(FakeSlackClient);db+config+tokens 经
SLACK_BRIDGE_DATA_DIR 隔离。纪律:非前置(空消息/无绑定)失败一律如实返回、不吞;发送前拒绝 = 确定未发
(client 零调用);凭据版本门(tokens.json 版本 == outbound_gate_tokens_version)与 gate 缺一不可。"""
import importlib.util
import json
import os
import pathlib

import pytest

from tests.conftest import APP_ID, BOT_ID, BOT_USER, CC_PID, CC_START, CHAT, OWNER, TEAM
from tests.helpers import (FakeProber, FakeSlackClient, err, http5xx, not_sent, ok, posted,
                           ratelimited, timeout)
from lib import config as configmod
from lib import constants, db as dbmod, notify as notifymod, paths
from lib.slackapi import CallResult, DaemonStateCooldownStore


def _load_notifyctl():
    root = pathlib.Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("notifyctl_mod", root / "bin" / "notifyctl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


notifyctl = _load_notifyctl()


# --------------------------------------------------------------------------- helpers
def _claude_prober(cc_pid=CC_PID, cc_start=CC_START):
    p = FakeProber()
    p.set(cc_pid, 1, cc_start, "claude")
    return p


def _setup_bound(conn, *, session_id="sess-1", chat_id=CHAT, status="active",
                 cc_pid=CC_PID, cc_start=CC_START, gate="ok", gate_version="__file__"):
    """绑定行 + 门状态。gate_version='__file__' = 与 tokens.json 当前版本一致(daemon 已重验)。"""
    from lib import util
    bid = util.new_id()
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,cc_start,"
        "status,bind_phase,listener_epoch,bound_at,close_reason) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (bid, chat_id, "测试频道", session_id, cc_pid, cc_start, status, "confirmed", 1, 0,
         "user_unbind" if status in ("dead", "closed") else None))
    if gate is not None:
        dbmod.set_state(conn, constants.GATE_KEY, gate)
    if gate_version == "__file__":
        try:
            gate_version = configmod.load_tokens(allow_env=False)[1]
        except configmod.ConfigError:
            gate_version = None
    if gate_version is not None:
        dbmod.set_state(conn, constants.GATE_VERSION_KEY, gate_version)
    return bid


@pytest.fixture
def bound(cfg, tokens, conn):
    """完整可发环境:config + tokens.json + schema + active 绑定 + gate ok(版本 == 文件)。"""
    _setup_bound(conn)
    return conn


def _write_config(**overrides):
    paths.ensure_data_dir()
    c = {"team_id": TEAM, "bot_user_id": BOT_USER, "bot_id": BOT_ID, "app_id": APP_ID,
         "owner_user_id": OWNER}
    c.update(overrides)
    configmod.save_config(c)
    return c


def _fake_cfg(**overrides):
    c = {"team_id": TEAM, "bot_user_id": BOT_USER, "bot_id": BOT_ID, "app_id": APP_ID,
         "owner_user_id": OWNER}
    c.update(overrides)
    return c


def _ok_client(ts="1700000000.000100"):
    c = FakeSlackClient()
    c.on("chat.postMessage", lambda m, p: posted(channel=p["channel"], ts=ts))
    return c


def _res_client(result):
    c = FakeSlackClient()
    c.on("chat.postMessage", lambda m, p: result(m, p) if callable(result) else result)
    return c


def _call(*, stdin="需要授权 X", session="sess-1", prober=None, start_pid=CC_PID, client=None,
          make_client=None, captured=None):
    if prober is None:
        prober = _claude_prober()
    if make_client is None:
        cc = client if client is not None else FakeSlackClient()

        def make_client(tokens, version, store):
            if captured is not None:
                captured.append((tokens, version, store))
            cc.cooldown_store = store      # 与生产同构:冷却存储 = daemon_state
            return cc
    env = {} if session is None else {"CLAUDE_CODE_SESSION_ID": session}
    return notifyctl.run_notify(stdin_text=stdin, environ=env, prober=prober,
                                start_pid=start_pid, make_client=make_client)


def _params(client):
    assert len(client.calls) == 1 and client.calls[0][0] == "chat.postMessage"
    return client.calls[0][1]


# --------------------------------------------------------------------------- 模块 & 版本
def test_notifyctl_module_loads():
    assert hasattr(notifyctl, "run_notify") and hasattr(notifyctl, "main")


def test_plugin_marketplace_version_consistency():
    root = pathlib.Path(__file__).resolve().parents[1]
    pj = json.loads((root / ".claude-plugin" / "plugin.json").read_text())
    mj = json.loads((root / ".claude-plugin" / "marketplace.json").read_text())
    assert pj["version"] == mj["plugins"][0]["version"]


def test_extraction_reexport_identity():
    assert notifyctl.run_notify is notifymod.run_notify
    assert notifyctl.configmod is notifymod.configmod
    assert notifyctl.OWNER_RE is notifymod.OWNER_RE
    assert notifyctl.BROADCAST_RE is notifymod.BROADCAST_RE
    assert notifyctl.build_wire_params is notifymod.build_wire_params
    assert notifyctl.default_make_client is notifymod.default_make_client


# --------------------------------------------------------------------------- 正路径 / wire 形状
class TestHappyPath:
    def test_sends_block_kit_with_owner_mention_and_markdown_body(self, bound, tokens):
        client = _ok_client()
        cap = []
        obj, code = _call(stdin="需要你授权删库", client=client, captured=cap)
        assert code == 0 and obj["ok"] is True and obj["sent"] is True
        assert obj["message_id"] == "%s:1700000000.000100" % CHAT and obj["chat_id"] == CHAT
        p = _params(client)
        assert p["channel"] == CHAT
        assert p["text"] == "<@%s> 需要你授权删库" % OWNER          # 通知栏回退含 owner mention
        assert p["blocks"] == [
            {"type": "section", "text": {"type": "mrkdwn", "text": "<@%s>" % OWNER}},
            {"type": "markdown", "text": "需要你授权删库"},
        ]
        assert p["unfurl_links"] is False and p["unfurl_media"] is False
        assert "metadata" not in p
        # make_client 收到的是 tokens.json 快照 + 其版本 + daemon_state 冷却存储
        (t, v, store), = cap
        assert t == tokens.tokens and v == tokens.version
        assert isinstance(store, DaemonStateCooldownStore)

    def test_special_chars_verbatim_in_body(self, bound):
        client = _ok_client()
        payload = 'line1 $(whoami) `id` "dq" \'sq\' 中文\nline2 <@U0SOMEONE> *bold* & <b>'
        obj, code = _call(stdin=payload, client=client)
        assert code == 0 and obj["sent"] is True
        assert _params(client)["blocks"][1]["text"] == payload

    def test_exactly_one_owner_mention_block(self, bound):
        client = _ok_client()
        _call(stdin="需要授权", client=client)
        blocks = _params(client)["blocks"]
        mention_blocks = [b for b in blocks if b["type"] == "section"]
        assert len(mention_blocks) == 1 and mention_blocks[0]["text"]["text"] == "<@%s>" % OWNER
        assert [b["type"] for b in blocks] == ["section", "markdown"]

    def test_build_wire_params_pure_shape(self):
        p = notifymod.build_wire_params(CHAT, OWNER, "## 标题\n**粗体**")
        assert set(p) == {"channel", "text", "blocks", "unfurl_links", "unfurl_media"}
        assert p["blocks"][1] == {"type": "markdown", "text": "## 标题\n**粗体**"}


# --------------------------------------------------------------------------- 正文拒绝
class TestBodyRejections:
    @pytest.mark.parametrize("raw", [
        "<!channel> 看", "请 <!here> 看", "<!everyone>", "<!subteam^S123|eng> 帮忙",
        "<!date^1392734382^{date}|x>", "前文正常 <!channel>", "<!",
    ])
    def test_broadcast_prefix_rejected_any_offset(self, bound, raw):
        client = FakeSlackClient()
        obj, code = _call(stdin=raw, client=client)
        assert code == 3 and obj["reason"] == "invalid-mention" and obj["sent"] is False
        assert client.calls == []

    @pytest.mark.parametrize("raw", ["see <atlas> the map", "a < ! b", "<@U0SOMEONE> hi", "<#C0CHAT|general>", "x<! "])
    def test_non_broadcast_forms(self, bound, raw):
        client = _ok_client()
        obj, code = _call(stdin=raw, client=client)
        if raw == "x<! ":
            assert code == 3 and obj["reason"] == "invalid-mention"   # 仍含 `<!`
        else:
            assert code == 0 and obj["sent"] is True

    def test_body_with_nul_rejected(self, bound):
        client = FakeSlackClient()
        obj, code = _call(stdin="bad\x00msg", client=client)
        assert code == 3 and obj["reason"] == "invalid-input" and client.calls == []

    def test_body_with_lone_surrogate_rejected(self, bound):
        client = FakeSlackClient()
        obj, code = _call(stdin="hello \ud800 world", client=client)
        assert code == 3 and obj["reason"] == "invalid-input" and client.calls == []

    def test_too_long_rejected(self, bound):
        client = FakeSlackClient()
        obj, code = _call(stdin="x" * (constants.CHUNK_LIMIT + 1), client=client)
        assert code == 3 and obj["reason"] == "message-too-long" and client.calls == []

    def test_max_length_accepted(self, bound):
        client = _ok_client()
        obj, code = _call(stdin="x" * constants.CHUNK_LIMIT, client=client)
        assert code == 0 and obj["sent"] is True


# --------------------------------------------------------------------------- 空消息
class TestEmptyMessage:
    @pytest.mark.parametrize("raw", ["", "   \n\t  "])
    def test_empty_message(self, bound, raw):
        client = FakeSlackClient()
        obj, code = _call(stdin=raw, client=client)
        assert code == 0 and obj["reason"] == "empty-message" and obj["sent"] is False
        assert client.calls == []


# --------------------------------------------------------------------------- session 三元组
class TestSessionTriple:
    def test_missing_session_id(self, cfg, tokens, conn):
        client = FakeSlackClient()
        obj, code = _call(session=None, client=client)
        assert code == 3 and obj["reason"] == "session-unresolved" and client.calls == []

    @pytest.mark.parametrize("kw", [
        dict(session_id="other", cc_pid=CC_PID, cc_start=CC_START),
        dict(session_id="sess-1", cc_pid=9999, cc_start="Other Start 2026"),
        dict(session_id="sess-1", cc_pid=CC_PID, cc_start="Different Start 2026"),
        dict(session_id="sess-1", cc_pid=8888, cc_start=CC_START),
    ])
    def test_partial_triple_not_bound(self, cfg, tokens, conn, kw):
        _setup_bound(conn, **kw)
        client = FakeSlackClient()
        obj, code = _call(session="sess-1", client=client)
        assert code == 0 and obj["reason"] == "not-bound" and client.calls == []

    @pytest.mark.parametrize("status", ["starting", "closed", "dead"])
    def test_status_predicate_not_bound(self, cfg, tokens, conn, status):
        _setup_bound(conn, status=status)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 0 and obj["reason"] == "not-bound" and client.calls == []


# --------------------------------------------------------------------------- 前置:not-bound / instance / config
class TestPreconditions:
    def test_no_binding_not_bound(self, cfg, tokens, conn):
        dbmod.set_state(conn, constants.GATE_KEY, "ok")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 0 and obj["reason"] == "not-bound" and client.calls == []

    def test_db_missing_not_bound(self, cfg, tokens):
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 0 and obj["reason"] == "not-bound" and client.calls == []
        assert not paths.db_path().exists()

    def test_instance_unresolved(self, cfg, tokens, conn):
        p = FakeProber()
        p.set(CC_PID, 1, CC_START, "zsh")
        client = FakeSlackClient()
        obj, code = _call(prober=p, start_pid=CC_PID, client=client)
        assert code == 3 and obj["reason"] == "instance-unresolved" and client.calls == []

    def test_config_missing_file(self, data_dir):
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "config" and client.calls == []

    def test_config_missing_required_key(self, data_dir):
        paths.ensure_data_dir()
        paths.config_path().write_text(json.dumps(
            {"team_id": TEAM, "bot_user_id": BOT_USER, "bot_id": BOT_ID, "app_id": APP_ID}))
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "config" and client.calls == []

    def test_config_malformed_json_internal_error(self, data_dir):
        paths.ensure_data_dir()
        paths.config_path().write_text("{ not valid json ")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "internal-error" and obj["sent"] is False
        assert client.calls == []


# --------------------------------------------------------------------------- schema 门 / db 损坏
class TestSchemaAndDb:
    def test_schema_version_mismatch(self, bound):
        bound.execute("UPDATE daemon_state SET value='999' WHERE key='schema_version'")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "schema-mismatch" and client.calls == []

    def test_schema_version_row_missing(self, bound):
        bound.execute("DELETE FROM daemon_state WHERE key='schema_version'")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "schema-mismatch" and client.calls == []

    def test_db_corrupt_internal_error(self, cfg, tokens, data_dir):
        paths.ensure_data_dir()
        paths.db_path().write_bytes(b"this is not a sqlite database at all")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] in ("internal-error", "schema-mismatch")
        assert obj["sent"] is False and client.calls == []


# --------------------------------------------------------------------------- allowlist(类型优先后看空)
class TestAllowlist:
    def test_allowlist_none_allows(self, data_dir, tokens, conn):
        _write_config()
        _setup_bound(conn)
        obj, code = _call(client=_ok_client())
        assert code == 0 and obj["sent"] is True

    def test_allowlist_empty_list_allows(self, data_dir, tokens, conn):
        _write_config(chat_allowlist=[])
        _setup_bound(conn)
        obj, code = _call(client=_ok_client())
        assert code == 0 and obj["sent"] is True

    def test_allowlist_includes_chat_sends(self, data_dir, tokens, conn):
        _write_config(chat_allowlist=[CHAT])
        _setup_bound(conn)
        obj, code = _call(client=_ok_client())
        assert code == 0 and obj["sent"] is True

    def test_allowlist_excludes_chat(self, data_dir, tokens, conn):
        _write_config(chat_allowlist=["C0OTHER"])
        _setup_bound(conn)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "chat-not-allowed" and client.calls == []

    @pytest.mark.parametrize("bad", ["prefix-C0CHAT-suffix", "", 0, False, {"x": 1}, [CHAT, 0]])
    def test_allowlist_malformed_rejected(self, data_dir, tokens, conn, bad):
        _write_config(chat_allowlist=bad)
        _setup_bound(conn)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "invalid-config" and client.calls == []


# --------------------------------------------------------------------------- 身份门 + 凭据版本门(contracts §7)
class TestGateAndCredentials:
    def test_gate_ok_and_version_match_sends(self, bound):
        obj, code = _call(client=_ok_client())
        assert code == 0 and obj["sent"] is True

    @pytest.mark.parametrize("gate", ["degraded:auth_error", "mismatch", "degraded"])
    def test_gate_not_ok_rejected(self, cfg, tokens, conn, gate):
        _setup_bound(conn, gate=gate)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "gate-degraded" and client.calls == []

    def test_gate_missing_row_fail_closed(self, cfg, tokens, conn):
        _setup_bound(conn, gate=None)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "gate-degraded" and client.calls == []

    def test_gate_version_missing_credentials_unverified(self, cfg, tokens, conn):
        _setup_bound(conn, gate="ok", gate_version=None)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "credentials-unverified" and client.calls == []

    def test_tokens_changed_before_daemon_reverify_rejected_then_ok_after(self, cfg, tokens, conn):
        """真机步骤 12:换 tokens.json 后立刻 notify → 拒;daemon 重验(gate 版本 = 新文件版本)→ 通过。"""
        _setup_bound(conn)                                   # gate 版本 = 旧文件版本
        new_ver = configmod.save_tokens({"bot_token": "xoxb-rotated", "app_token": "xapp-x"}, overwrite=True)
        assert new_ver != tokens.version
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "credentials-unverified" and client.calls == []
        assert tokens.version in obj["detail"] and new_ver in obj["detail"]
        # daemon 重验后
        dbmod.set_state(conn, constants.GATE_VERSION_KEY, new_ver)
        client = _ok_client()
        cap = []
        obj, code = _call(client=client, captured=cap)
        assert code == 0 and obj["sent"] is True
        assert cap[0][0]["bot_token"] == "xoxb-rotated" and cap[0][1] == new_ver

    def test_gate_version_stale_vs_file_rejected(self, cfg, tokens, conn):
        _setup_bound(conn, gate="ok", gate_version="123:deadbeefdeadbeef")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "credentials-unverified" and client.calls == []

    def test_tokens_file_missing_credentials_unverified(self, cfg, tokens, conn):
        _setup_bound(conn)
        os.unlink(tokens.path)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "credentials-unverified" and client.calls == []

    def test_tokens_file_bad_perms_credentials_unverified(self, cfg, tokens, conn):
        _setup_bound(conn)
        os.chmod(tokens.path, 0o644)
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "credentials-unverified" and "0600" in obj["detail"]
        assert client.calls == []

    def test_env_token_does_not_bypass_file_truth(self, bound, tokens, monkeypatch):
        """env SLACK_BOT_TOKEN 只供 CLI/测试;notify 直发只信文件(版本门 + 发送用文件 token)。"""
        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-from-env")
        cap = []
        obj, code = _call(client=_ok_client(), captured=cap)
        assert code == 0 and obj["sent"] is True
        assert cap[0][0]["bot_token"] == tokens.tokens["bot_token"] and cap[0][1] == tokens.version


# --------------------------------------------------------------------------- owner / chat_id 校验
class TestOwnerAndChatValidation:
    @pytest.mark.parametrize("owner", ["all", "channel", 'U0X"><!channel>', "U0OWNER\n", "ou_owner", ""])
    def test_bad_owner_rejected(self, bound, monkeypatch, owner):
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(owner_user_id=owner))
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "invalid-owner" and client.calls == []

    def test_owner_regex_uses_fullmatch(self):
        assert notifyctl.OWNER_RE.fullmatch("U0ABC123") and notifyctl.OWNER_RE.fullmatch("W0ABC123")
        assert notifyctl.OWNER_RE.fullmatch("U0OWNER\n") is None
        assert notifyctl.OWNER_RE.fullmatch("all") is None

    def test_chat_id_malformed_rejected(self, cfg, tokens, conn):
        _setup_bound(conn, chat_id="bad")
        client = FakeSlackClient()
        obj, code = _call(client=client)
        assert code == 3 and obj["reason"] == "invalid-binding" and client.calls == []

    @pytest.mark.parametrize("chat", ["C0CHAT", "G0PRIV", "D0OWNERDM"])
    def test_chat_id_forms_accepted(self, cfg, tokens, conn, chat):
        _setup_bound(conn, chat_id=chat)
        client = _ok_client()
        obj, code = _call(client=client)
        assert code == 0 and obj["sent"] is True and _params(client)["channel"] == chat


# --------------------------------------------------------------------------- 结果分类矩阵(CallResult → sent 三态)
class TestSendMatrix:
    def test_ok_with_ts_sent(self, bound):
        obj, code = _call(client=_res_client(posted(channel=CHAT, ts="9.9")))
        assert code == 0 and obj["sent"] is True and obj["message_id"] == "%s:9.9" % CHAT

    def test_ok_without_ts_unknown(self, bound):
        obj, code = _call(client=_res_client(ok({"channel": CHAT})))
        assert code == 5 and obj["sent"] == "unknown" and obj["reason"] == "send-unknown"

    def test_local_cooldown_wait_not_sent_reason_cooldown(self, bound, clock):
        """此前被 429 → daemon_state cooldown:chat.postMessage 未到期 → 请求不发出,reason=cooldown。"""
        store = DaemonStateCooldownStore(bound)
        until = store.publish("chat.postMessage", int(__import__("time").time() * 1000) + 60_000)
        client = _ok_client()
        obj, code = _call(client=client)
        assert code == 4 and obj["sent"] is False and obj["reason"] == "cooldown"
        assert obj["retryable"] is True and obj["cooldown_until"] == until
        assert client.calls == [] and len(client.waits) == 1

    def test_ratelimited_publishes_cooldown(self, bound):
        client = _res_client(ratelimited(retry_after=7))
        obj, code = _call(client=client)
        assert code == 4 and obj["sent"] is False and obj["reason"] == "ratelimited"
        assert obj["retryable"] is True and obj["retry_after"] == 7
        assert DaemonStateCooldownStore(bound).get("chat.postMessage") is not None
        # 紧接着再发 → 本地冷却挡住,零请求
        client2 = _ok_client()
        obj2, code2 = _call(client=client2)
        assert code2 == 4 and obj2["reason"] == "cooldown" and client2.calls == []

    @pytest.mark.parametrize("reason", ["dns", "connection_refused", "tls", "network_unreachable"])
    def test_not_sent_local_transient_retryable(self, bound, reason):
        obj, code = _call(client=_res_client(not_sent(reason)))
        assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed"
        assert obj["retryable"] is True and obj["error"] == reason

    @pytest.mark.parametrize("code_", sorted(constants.NOT_SENT_ERRORS))
    def test_auth_family_not_sent_not_retryable(self, bound, code_):
        obj, code = _call(client=_res_client(err(code_)))
        assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed"
        assert obj["retryable"] is False and obj["error"] == code_

    @pytest.mark.parametrize("code_", ["channel_not_found", "not_in_channel", "invalid_blocks",
                                       "msg_too_long", "missing_scope", "is_archived"])
    def test_permanent_failed(self, bound, code_):
        obj, code = _call(client=_res_client(err(code_)))
        assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed"
        assert obj["error"] == code_ and obj["retryable"] is False

    @pytest.mark.parametrize("res", [timeout(), http5xx(503), http5xx(502), err("internal_error"),
                                     err("fatal_error"), err("service_unavailable"),
                                     err("totally_unknown_code"),
                                     CallResult(ok=False, error="unparseable", http_status=200),
                                     CallResult(ok=False, error="transport:RemoteDisconnected")])
    def test_unknown_family(self, bound, res):
        obj, code = _call(client=_res_client(res))
        assert code == 5 and obj["sent"] == "unknown" and obj["reason"] == "send-unknown"

    def test_post_send_exception_unknown(self, bound):
        def boom(m, p):
            raise RuntimeError("post-send explosion")
        client = _res_client(boom)
        obj, code = _call(client=client)
        assert code == 5 and obj["sent"] == "unknown" and obj["reason"] == "internal-error-after-send"
        assert len(client.calls) == 1

    def test_make_client_exception_before_send_is_internal_error(self, bound):
        def mk(tokens, version, store):
            raise RuntimeError("factory boom")
        obj, code = _call(make_client=mk)
        assert code == 3 and obj["sent"] is False and obj["reason"] == "internal-error"


# --------------------------------------------------------------------------- CLI 薄壳
def test_main_utf8_robust_no_traceback(monkeypatch):
    import io
    fake_stdout = type("S", (), {"buffer": io.BytesIO()})()
    fake_stdin = type("S", (), {"buffer": io.BytesIO(b"\xff\xfe bad")})()
    monkeypatch.setattr(notifyctl.sys, "stdout", fake_stdout)
    monkeypatch.setattr(notifyctl.sys, "stdin", fake_stdin)
    monkeypatch.setattr(notifyctl, "run_notify",
                        lambda **kw: ({"ok": False, "sent": False, "reason": "not-bound",
                                       "echo": kw["stdin_text"]}, 0))
    with pytest.raises(SystemExit) as ei:
        notifyctl.main()
    assert ei.value.code == 0
    parsed = json.loads(fake_stdout.buffer.getvalue().decode("utf-8"))
    assert parsed["reason"] == "not-bound" and "�" in parsed["echo"]


def test_main_stdin_read_failure_not_empty(monkeypatch):
    import io

    class _BadBuffer:
        def read(self):
            raise OSError("stdin closed")

    fake_stdin = type("S", (), {"buffer": _BadBuffer()})()
    fake_stdout = type("S", (), {"buffer": io.BytesIO()})()
    monkeypatch.setattr(notifyctl.sys, "stdin", fake_stdin)
    monkeypatch.setattr(notifyctl.sys, "stdout", fake_stdout)
    with pytest.raises(SystemExit) as ei:
        notifyctl.main()
    assert ei.value.code == 3
    parsed = json.loads(fake_stdout.buffer.getvalue().decode("utf-8"))
    assert parsed["reason"] == "stdin-error" and parsed["sent"] is False


def test_main_injects_default_make_client(monkeypatch):
    """生产装配:main() 传 default_make_client(不再是 lark runner)。"""
    import io
    seen = {}
    fake_stdout = type("S", (), {"buffer": io.BytesIO()})()
    fake_stdin = type("S", (), {"buffer": io.BytesIO(b"hi")})()
    monkeypatch.setattr(notifyctl.sys, "stdout", fake_stdout)
    monkeypatch.setattr(notifyctl.sys, "stdin", fake_stdin)

    def fake_run(**kw):
        seen.update(kw)
        return ({"ok": False, "sent": False, "reason": "not-bound"}, 0)
    monkeypatch.setattr(notifyctl, "run_notify", fake_run)
    with pytest.raises(SystemExit):
        notifyctl.main()
    assert seen["make_client"] is notifymod.default_make_client
    assert seen["stdin_text"] == "hi"


def test_default_make_client_uses_snapshot_token_and_version(tokens):
    from lib.slackapi import InMemoryCooldownStore, SlackClient
    store = InMemoryCooldownStore()
    c = notifymod.default_make_client(tokens.tokens, tokens.version, store)
    assert isinstance(c, SlackClient) and c.tokens_version == tokens.version
    assert c.cooldown_store is store and c.has_token
    assert "xoxb" not in repr(c)
