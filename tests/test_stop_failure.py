"""StopFailure 告警 hook 单测(Slack 版)。
覆盖:_sanitize_field(净化 + 顺序对抗;中和 `<!` 而非 `<at`)/ compose(诚实文案,永不含 `<!`)/
run_stop_failure(定向 + env 清继承 + DI)/ 真集成(经真 notify.run_notify + FakeSlackClient,定向到本
session 绑定会话,受凭据版本门)/ stop_failure_entry(fail-closed + 三态观测 + 无心跳)/ 薄壳引导
(bin/notifyctl.py 从任意 cwd)/ hooks.json 静态。全程离线。"""
import importlib.util
import json
import os
import pathlib

import pytest

from tests.conftest import CC_PID, CC_START, CHAT, OWNER
from tests.helpers import FakeProber, FakeSlackClient, posted
from lib import config as configmod
from lib import constants, db as dbmod, hooklib, notify as notifymod

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _claude_prober():
    p = FakeProber()
    p.set(CC_PID, 1, CC_START, "claude")
    return p


def _bind(conn, *, session_id="sess-1", chat_id=CHAT, status="active",
          cc_pid=CC_PID, cc_start=CC_START, gate="ok", gate_version="__file__"):
    from lib import util
    bid = util.new_id()
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,cc_start,"
        "status,bind_phase,listener_epoch,bound_at,close_reason) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (bid, chat_id, "测试频道", session_id, cc_pid, cc_start, status, "confirmed", 1, 0, None))
    if gate is not None:
        dbmod.set_state(conn, constants.GATE_KEY, gate)
    if gate_version == "__file__":
        gate_version = configmod.load_tokens(allow_env=False)[1]
    if gate_version is not None:
        dbmod.set_state(conn, constants.GATE_VERSION_KEY, gate_version)
    return bid


def _ok_client():
    c = FakeSlackClient()
    c.on("chat.postMessage", lambda m, p: posted(channel=p["channel"], ts="1.1"))
    return c


# --------------------------------------------------------------------------- _sanitize_field
class TestSanitize:
    def test_drops_nul_and_controls_collapses_ws(self):
        assert hooklib._sanitize_field("a\x00b\tc\r\nd", 100) == "ab c d"

    def test_drops_lone_surrogate(self):
        assert "\ud800" not in hooklib._sanitize_field("x\ud800y", 100)

    def test_neutralizes_broadcast_no_re_match(self):
        s = hooklib._sanitize_field('前 <!channel> 后', 200)
        assert notifymod.BROADCAST_RE.search(s) is None and "channel" in s

    @pytest.mark.parametrize("raw", ['<!here', '<!everyone>', '<!subteam^S1|eng>', '<!', '<!date^1^{date}|x>'])
    def test_neutralizes_forms(self, raw):
        assert notifymod.BROADCAST_RE.search(hooklib._sanitize_field(raw, 200)) is None

    @pytest.mark.parametrize("raw", ['<\x00!channel', '<\ud800!here', '<\x00\x00!everyone'])
    def test_ordering_poison_hidden_broadcast_still_neutralized(self, raw):
        """先删毒字符再中和 —— `<\\x00!` / `<\\ud800!` 删毒后会还原成 `<!`,须仍被中和。"""
        assert notifymod.BROADCAST_RE.search(hooklib._sanitize_field(raw, 200)) is None

    def test_user_mention_and_at_word_untouched(self):
        assert hooklib._sanitize_field("<@U0X> <atlas>", 100) == "<@U0X> <atlas>"

    def test_truncates_to_cap(self):
        s = hooklib._sanitize_field("x" * 500, 60)
        assert len(s) <= 60 and s.endswith("…")

    def test_output_never_has_nul_or_newline(self):
        s = hooklib._sanitize_field("a\x00\n\r\t<!channel b" + "z" * 300, 100)
        assert "\x00" not in s and "\n" not in s and "\r" not in s
        assert notifymod.BROADCAST_RE.search(s) is None


# --------------------------------------------------------------------------- compose
class TestCompose:
    def test_full_payload_includes_type_details_cwd(self):
        body = hooklib.compose_stop_failure_message(
            {"error": "overloaded", "error_details": "529 Service Overloaded",
             "cwd": "/Users/x/proj", "session_id": "s"})
        assert "overloaded" in body and "529" in body and "/Users/x/proj" in body
        assert body.startswith("⚠️")
        assert notifymod.BROADCAST_RE.search(body) is None

    def test_minimal_payload_generic_no_crash(self):
        body = hooklib.compose_stop_failure_message({"session_id": "s"})
        assert body.startswith("⚠️") and "unknown" in body

    def test_empty_payload_no_crash(self):
        body = hooklib.compose_stop_failure_message({})
        assert body.startswith("⚠️") and notifymod.BROADCAST_RE.search(body) is None

    def test_no_retry_exhausted_wording(self):
        body = hooklib.compose_stop_failure_message({"error": "authentication_failed"})
        assert "重试耗尽" not in body

    def test_broadcast_in_details_neutralized(self):
        body = hooklib.compose_stop_failure_message(
            {"error": "unknown", "error_details": '<!channel> boom'})
        assert notifymod.BROADCAST_RE.search(body) is None

    def test_all_dynamic_fields_bounded(self):
        body = hooklib.compose_stop_failure_message(
            {"error": "e" * 500, "error_details": "d" * 2000, "cwd": "/" + "p" * 2000})
        assert len(body) <= 800


# --------------------------------------------------------------------------- run_stop_failure(注入)
class TestRunStopFailure:
    def _capture_fn(self, captured, ret=None):
        def fn(**kw):
            captured.update(kw)
            return ret if ret is not None else ({"ok": True, "sent": True, "message_id": "C:1"}, 0)
        return fn

    def test_happy_passes_msg_and_session_env(self):
        cap = {}
        payload = {"session_id": "sess-1", "error": "overloaded", "error_details": "529"}
        res = hooklib.run_stop_failure(payload, environ={}, prober=_claude_prober(),
                                       notify_fn=self._capture_fn(cap))
        assert cap["stdin_text"] == hooklib.compose_stop_failure_message(payload)
        assert cap["environ"]["CLAUDE_CODE_SESSION_ID"] == "sess-1"
        assert cap["make_client"] is notifymod.default_make_client
        assert res["sent"] is True

    def test_missing_sid_clears_inherited_env(self):
        cap = {}
        hooklib.run_stop_failure({"error": "overloaded"},
                                 environ={"CLAUDE_CODE_SESSION_ID": "inherited-x", "FOO": "1"},
                                 prober=_claude_prober(), notify_fn=self._capture_fn(cap))
        assert "CLAUDE_CODE_SESSION_ID" not in cap["environ"]
        assert cap["environ"].get("FOO") == "1"

    def test_empty_environ_does_not_fallback_to_os(self, monkeypatch):
        monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "os-real")
        cap = {}
        hooklib.run_stop_failure({"error": "x"}, environ={}, prober=_claude_prober(),
                                 notify_fn=self._capture_fn(cap))
        assert "CLAUDE_CODE_SESSION_ID" not in cap["environ"]

    def test_non_str_sid_not_injected(self):
        cap = {}
        hooklib.run_stop_failure({"session_id": 123, "error": "x"}, environ={},
                                 prober=_claude_prober(), notify_fn=self._capture_fn(cap))
        assert "CLAUDE_CODE_SESSION_ID" not in cap["environ"]

    def test_make_client_injected_passthrough(self):
        cap = {}
        mk = lambda t, v, s: None  # noqa: E731
        hooklib.run_stop_failure({"session_id": "s", "error": "x"}, environ={},
                                 prober=_claude_prober(), notify_fn=self._capture_fn(cap), make_client=mk)
        assert cap["make_client"] is mk


# --------------------------------------------------------------------------- 真集成(真 run_notify)
class TestRealIntegration:
    def _run(self, client, payload):
        return hooklib.run_stop_failure(
            payload, environ={}, prober=_claude_prober(), start_pid=CC_PID,
            make_client=lambda t, v, s: client)

    def test_sends_to_bound_chat_of_this_session(self, cfg, tokens, conn):
        _bind(conn, session_id="sess-1")
        client = _ok_client()
        res = self._run(client, {"session_id": "sess-1", "error": "overloaded", "error_details": "529"})
        assert res["sent"] is True
        assert len(client.calls) == 1
        p = client.calls[0][1]
        assert p["channel"] == CHAT
        assert p["blocks"][0] == {"type": "section", "text": {"type": "mrkdwn", "text": "<@%s>" % OWNER}}
        assert p["blocks"][1]["type"] == "markdown" and "overloaded" in p["blocks"][1]["text"]
        assert p["text"].startswith("<@%s> " % OWNER)

    def test_sends_to_bound_rows_chat_not_hardcode(self, cfg, tokens, conn):
        alt = "C0ALTCHAT9"
        _bind(conn, session_id="sess-1", chat_id=alt)
        client = _ok_client()
        res = self._run(client, {"session_id": "sess-1", "error": "overloaded"})
        assert res["sent"] is True and client.calls[0][1]["channel"] == alt

    def test_unbound_session_does_not_send(self, cfg, tokens, conn):
        _bind(conn, session_id="sess-1")
        client = _ok_client()
        res = self._run(client, {"session_id": "ghost", "error": "overloaded"})
        assert res["sent"] is False and res["reason"] == "not-bound" and client.calls == []

    def test_credentials_unverified_blocks_alert(self, cfg, tokens, conn):
        """换 tokens.json 后 daemon 尚未重验 → StopFailure 告警也被 credentials-unverified 拒(零请求)。"""
        _bind(conn, session_id="sess-1")
        configmod.save_tokens({"bot_token": "xoxb-rotated", "app_token": "xapp-x"}, overwrite=True)
        client = _ok_client()
        res = self._run(client, {"session_id": "sess-1", "error": "overloaded"})
        assert res["sent"] is False and res["reason"] == "credentials-unverified" and client.calls == []

    def test_adversarial_details_still_one_owner_mention_no_broadcast(self, cfg, tokens, conn):
        _bind(conn, session_id="sess-1")
        client = _ok_client()
        res = self._run(client, {"session_id": "sess-1", "error": "overloaded",
                                 "error_details": '<!channel> <!here>\n\x00' + "x" * 400})
        assert res["sent"] is True
        p = client.calls[0][1]
        sections = [b for b in p["blocks"] if b["type"] == "section"]
        assert len(sections) == 1 and sections[0]["text"]["text"] == "<@%s>" % OWNER
        assert "<!" not in p["text"] and "<!" not in p["blocks"][1]["text"]


# --------------------------------------------------------------------------- entry:fail-closed + 观测 + 无心跳
class TestEntry:
    def test_notify_fn_raises_swallowed(self, data_dir):
        def boom(**kw):
            raise RuntimeError("kaboom")
        res = hooklib.stop_failure_entry({"session_id": "s", "error": "x"},
                                         prober=_claude_prober(), notify_fn=boom)
        assert res.get("suppressed") is True and res.get("reason") == "exception"

    def test_not_sent_logged_honestly(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        hooklib.stop_failure_entry({"session_id": "s", "error": "x"}, prober=_claude_prober(),
                                   notify_fn=lambda **k: ({"ok": False, "sent": False,
                                                           "reason": "credentials-unverified"}, 3))
        log = paths.hook_drops_path().read_text()
        assert "not-sent" in log and "credentials-unverified" in log and "delivery-unconfirmed" not in log

    def test_unknown_logged_as_unconfirmed(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        hooklib.stop_failure_entry({"session_id": "s", "error": "x"}, prober=_claude_prober(),
                                   notify_fn=lambda **k: ({"ok": False, "sent": "unknown",
                                                           "reason": "send-unknown"}, 5))
        log = paths.hook_drops_path().read_text()
        assert "delivery-unconfirmed" in log and "not-sent" not in log

    def test_sent_true_no_log(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        hooklib.stop_failure_entry({"session_id": "s", "error": "x"}, prober=_claude_prober(),
                                   notify_fn=lambda **k: ({"ok": True, "sent": True,
                                                           "message_id": "C:1"}, 0))
        assert not paths.hook_drops_path().exists()

    def test_log_is_payload_free(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        hooklib.stop_failure_entry(
            {"session_id": "s", "error": "SENTINEL_ERR", "error_details": "SENTINEL_DETAIL"},
            prober=_claude_prober(),
            notify_fn=lambda **k: ({"ok": False, "sent": False, "reason": "gate-degraded"}, 3))
        log = paths.hook_drops_path().read_text()
        assert "SENTINEL_ERR" not in log and "SENTINEL_DETAIL" not in log

    def test_no_heartbeat_written(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        hooklib.stop_failure_entry({"session_id": "s", "error": "x"}, prober=_claude_prober(),
                                   notify_fn=lambda **k: ({"ok": True, "sent": True}, 0))
        assert not (paths.data_dir() / "hook_heartbeat.stop_failure").exists()


# --------------------------------------------------------------------------- 薄壳引导(subprocess)
class TestBootstrap:
    def _env_scrubbed(self, tmp_path):
        env = {k: v for k, v in os.environ.items()
               if k not in ("PYTHONPATH", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN")}
        env["SLACK_BRIDGE_DATA_DIR"] = str(tmp_path / "d")
        env["SLACK_BRIDGE_SETTINGS_PATH"] = str(tmp_path / "s.json")
        return env

    def test_stop_failure_hook_bad_json_exits_0(self, tmp_path):
        import subprocess
        r = subprocess.run(
            ["python3", str(ROOT / "hooks" / "stop_failure_hook.py")],
            input=b"not json at all", capture_output=True, cwd=str(tmp_path),
            env=self._env_scrubbed(tmp_path))
        assert r.returncode == 0
        assert b"ModuleNotFoundError" not in r.stderr and b"Traceback" not in r.stderr

    def test_notifyctl_direct_from_foreign_cwd_no_import_error(self, tmp_path):
        import subprocess
        r = subprocess.run(
            ["python3", str(ROOT / "bin" / "notifyctl.py")],
            input=b"", capture_output=True, cwd=str(tmp_path),
            env=self._env_scrubbed(tmp_path))
        assert r.returncode == 0
        assert b"ModuleNotFoundError" not in r.stderr and b"Traceback" not in r.stderr
        assert json.loads(r.stdout)["reason"] == "empty-message"

    def test_hook_main_exits_0_even_if_entry_raises(self, monkeypatch):
        import io
        spec = importlib.util.spec_from_file_location(
            "sf_hook_mod", ROOT / "hooks" / "stop_failure_hook.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        def boom(*a, **k):
            raise RuntimeError("fail-closed helper itself blew up")

        monkeypatch.setattr(mod.hooklib, "stop_failure_entry", boom)
        monkeypatch.setattr(mod.sys, "stdin", io.StringIO("{}"))
        with pytest.raises(SystemExit) as ei:
            mod.main()
        assert ei.value.code == 0


# --------------------------------------------------------------------------- hooks.json 静态
def test_hooks_json_registers_stopfailure():
    hj = json.loads((ROOT / "hooks" / "hooks.json").read_text())
    sf = hj["hooks"]["StopFailure"]
    assert isinstance(sf, list) and len(sf) == 1
    grp = sf[0]
    assert "matcher" not in grp
    hk = grp["hooks"][0]
    assert hk["type"] == "command" and hk["command"] == "python3"
    assert any("stop_failure_hook.py" in a for a in hk["args"])
    assert hk["timeout"] >= 60


# --------------------------------------------------------------------------- 抽取守卫 / 无 lark 残留
def test_hooklib_has_no_lark_runner_dependency():
    import inspect
    src = inspect.getsource(hooklib)
    assert "make_runner" not in src and "LarkRunner" not in src and "<at" not in src
    assert "make_client" in src
