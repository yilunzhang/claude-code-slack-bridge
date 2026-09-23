"""StopFailure 告警 hook 单测。
覆盖:_sanitize_field(净化 + 顺序对抗)/ compose(诚实文案,永不含 <at)/ run_stop_failure(定向 +
env 清继承 + DI)/ 真集成(经真 notify.run_notify + fake runner,定向到本 session 绑定群)/
stop_failure_entry(fail-closed + 三态观测 + 无心跳)/ 薄壳引导(bin/notifyctl.py 从任意 cwd)/
hooks.json 静态 / 抽取守卫。全程离线:注入 notify_fn / make_runner / prober / environ。"""
import importlib.util
import json
import os
import pathlib

import pytest

from tests.conftest import APP_ID, BOT_OPEN_ID, CC_PID, CC_START, CHAT, OWNER, PROFILE

from lib import hooklib, notify as notifymod

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load_notifyctl():
    spec = importlib.util.spec_from_file_location("notifyctl_mod", ROOT / "bin" / "notifyctl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _claude_prober():
    from tests.helpers import FakeProber
    p = FakeProber()
    p.set(CC_PID, 1, CC_START, "claude")
    return p


def _bind(conn, *, session_id="sess-1", chat_id=CHAT, status="active",
          cc_pid=CC_PID, cc_start=CC_START, gate="ok"):
    from lib import db as dbmod, util
    bid = util.new_id()
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,cc_start,"
        "status,bind_phase,listener_epoch,bound_at,close_reason) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (bid, chat_id, "测试群", session_id, cc_pid, cc_start, status, "confirmed", 1, 0, None))
    if gate is not None:
        dbmod.set_state(conn, "outbound_gate", gate)
    return bid


# --------------------------------------------------------------------------- _sanitize_field
class TestSanitize:
    def test_drops_nul_and_controls_collapses_ws(self):
        assert hooklib._sanitize_field("a\x00b\tc\r\nd", 100) == "ab c d"

    def test_drops_lone_surrogate(self):
        assert "\ud800" not in hooklib._sanitize_field("x\ud800y", 100)

    def test_neutralizes_at_tag_no_atre_match(self):
        s = hooklib._sanitize_field('前 <at user_id="all"></at> 后', 200)
        assert notifymod.AT_RE.search(s) is None

    @pytest.mark.parametrize("raw", ['<at foo', '<AT foo', '<At>', '<at\t', '<at>'])
    def test_neutralizes_case_and_forms(self, raw):
        assert notifymod.AT_RE.search(hooklib._sanitize_field(raw, 200)) is None

    @pytest.mark.parametrize("raw", ['<\x00at foo', '<\ud800at foo', '<a\x00t\x00 '])
    def test_ordering_poison_hidden_at_still_neutralized(self, raw):
        """R2-Low3:先删毒字符再中和 —— `<\\x00at`/`<\\ud800at` 删毒后会还原成 `<at`,须仍被中和。"""
        assert notifymod.AT_RE.search(hooklib._sanitize_field(raw, 200)) is None

    def test_truncates_to_cap(self):
        s = hooklib._sanitize_field("x" * 500, 60)
        assert len(s) <= 60 and s.endswith("…")

    def test_output_never_has_nul_or_newline(self):
        s = hooklib._sanitize_field("a\x00\n\r\t<at b" + "z" * 300, 100)
        assert "\x00" not in s and "\n" not in s and "\r" not in s
        assert notifymod.AT_RE.search(s) is None


# --------------------------------------------------------------------------- compose
class TestCompose:
    def test_full_payload_includes_type_details_cwd(self):
        body = hooklib.compose_stop_failure_message(
            {"error": "overloaded", "error_details": "529 Service Overloaded",
             "cwd": "/Users/x/proj", "session_id": "s"})
        assert "overloaded" in body and "529" in body and "/Users/x/proj" in body
        assert body.startswith("⚠️")
        assert notifymod.AT_RE.search(body) is None

    def test_minimal_payload_generic_no_crash(self):
        body = hooklib.compose_stop_failure_message({"session_id": "s"})
        assert body.startswith("⚠️") and "unknown" in body
        assert notifymod.AT_RE.search(body) is None

    def test_empty_payload_no_crash(self):
        body = hooklib.compose_stop_failure_message({})
        assert body.startswith("⚠️") and notifymod.AT_RE.search(body) is None

    def test_no_retry_exhausted_wording(self):
        """R1-#6:用户选"所有错误类型"(含非重试类),文案不得断言"已重试耗尽"。"""
        body = hooklib.compose_stop_failure_message({"error": "authentication_failed"})
        assert "重试耗尽" not in body

    def test_at_in_details_neutralized(self):
        body = hooklib.compose_stop_failure_message(
            {"error": "unknown", "error_details": '<at user_id="all"></at> boom'})
        assert notifymod.AT_RE.search(body) is None

    def test_all_dynamic_fields_bounded(self):
        body = hooklib.compose_stop_failure_message(
            {"error": "e" * 500, "error_details": "d" * 2000, "cwd": "/" + "p" * 2000})
        assert len(body) <= 800


# --------------------------------------------------------------------------- run_stop_failure(注入)
class TestRunStopFailure:
    def _capture_fn(self, captured, ret=None):
        def fn(**kw):
            captured.update(kw)
            return ret if ret is not None else ({"ok": True, "sent": True, "message_id": "om_x"}, 0)
        return fn

    def test_happy_passes_msg_and_session_env(self):
        cap = {}
        payload = {"session_id": "sess-1", "error": "overloaded", "error_details": "529"}
        res = hooklib.run_stop_failure(payload, environ={}, prober=_claude_prober(),
                                       notify_fn=self._capture_fn(cap))
        assert cap["stdin_text"] == hooklib.compose_stop_failure_message(payload)
        assert cap["environ"]["CLAUDE_CODE_SESSION_ID"] == "sess-1"
        assert res["sent"] is True

    def test_missing_sid_clears_inherited_env(self):
        """R1-#2:payload 无 session_id 但 environ 里有继承的 CLAUDE_CODE_SESSION_ID → 必须清掉,
        否则会误发到当前绑定群。"""
        cap = {}
        hooklib.run_stop_failure({"error": "overloaded"},
                                 environ={"CLAUDE_CODE_SESSION_ID": "inherited-x", "FOO": "1"},
                                 prober=_claude_prober(), notify_fn=self._capture_fn(cap))
        assert "CLAUDE_CODE_SESSION_ID" not in cap["environ"]
        assert cap["environ"].get("FOO") == "1"  # 其它 env 保留

    def test_empty_environ_does_not_fallback_to_os(self, monkeypatch):
        """DI:environ={}(非 None)绝不回退 os.environ(`is None` 而非 `or`)。"""
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


# --------------------------------------------------------------------------- 真集成(真 run_notify)
class TestRealIntegration:
    def _fake_runner(self):
        from tests.helpers import FakeRunner, ok_envelope
        r = FakeRunner(profile=PROFILE)
        r.on_prefix(["im", "+messages-send"], lambda a, c: ok_envelope({"message_id": "om_x"}))
        return r

    def test_sends_to_bound_group_of_this_session(self, cfg, conn):
        _bind(conn, session_id="sess-1")
        runner = self._fake_runner()
        res = hooklib.run_stop_failure(
            {"session_id": "sess-1", "error": "overloaded", "error_details": "529"},
            environ={}, prober=_claude_prober(), start_pid=CC_PID, make_runner=lambda p: runner)
        assert res["sent"] is True
        assert len(runner.calls) == 1
        ca = runner.calls[0][0]
        assert ca[ca.index("--chat-id") + 1] == CHAT and ca[ca.index("--as") + 1] == "bot"
        # owner mention = 结构化 at 节点(不是纯文本)
        content = json.loads(ca[ca.index("--content") + 1])["zh_cn"]["content"]
        at_nodes = [n for para in content for n in para if n.get("tag") == "at"]
        assert len(at_nodes) == 1 and at_nodes[0]["user_id"] == OWNER

    def test_sends_to_bound_rows_chat_not_hardcode(self, cfg, conn):
        """mutation(c):目标 chat 必须取自命中的绑定行、而非硬编码 oc_chat1 —— 换 chat_id 应随之变。"""
        alt = "oc_altchat9"
        _bind(conn, session_id="sess-1", chat_id=alt)
        runner = self._fake_runner()
        res = hooklib.run_stop_failure(
            {"session_id": "sess-1", "error": "overloaded"},
            environ={}, prober=_claude_prober(), start_pid=CC_PID, make_runner=lambda p: runner)
        assert res["sent"] is True
        ca = runner.calls[0][0]
        assert ca[ca.index("--chat-id") + 1] == alt

    def test_unbound_session_does_not_send(self, cfg, conn):
        _bind(conn, session_id="sess-1")
        runner = self._fake_runner()
        res = hooklib.run_stop_failure(
            {"session_id": "ghost", "error": "overloaded"},
            environ={}, prober=_claude_prober(), start_pid=CC_PID, make_runner=lambda p: runner)
        assert res["sent"] is False and res["reason"] == "not-bound"
        assert runner.calls == []

    def test_adversarial_details_still_one_owner_at(self, cfg, conn):
        """对抗:details 含换行/NUL/surrogate/<AT>/超长 → 仍发出、且恰一个 owner at 节点。"""
        _bind(conn, session_id="sess-1")
        runner = self._fake_runner()
        res = hooklib.run_stop_failure(
            {"session_id": "sess-1", "error": "overloaded",
             "error_details": '<AT user_id="all"></at>\n\x00' + "x" * 400},
            environ={}, prober=_claude_prober(), start_pid=CC_PID, make_runner=lambda p: runner)
        assert res["sent"] is True
        content = json.loads(runner.calls[0][0][
            runner.calls[0][0].index("--content") + 1])["zh_cn"]["content"]
        at_nodes = [n for para in content for n in para if n.get("tag") == "at"]
        assert len(at_nodes) == 1 and at_nodes[0]["user_id"] == OWNER


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
                                                           "reason": "not-bound"}, 0))
        log = paths.hook_drops_path().read_text()
        assert "not-sent" in log and "not-bound" in log and "delivery-unconfirmed" not in log

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
                                                           "message_id": "om_x"}, 0))
        assert not paths.hook_drops_path().exists()

    def test_log_is_payload_free(self, data_dir):
        """观测行只含固定 reason 枚举,绝不含 payload/正文(此处 error_details 塞哨兵,不得入日志)。"""
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
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        env["FEISHU_BRIDGE_DATA_DIR"] = str(tmp_path / "d")
        env["FEISHU_BRIDGE_SETTINGS_PATH"] = str(tmp_path / "s.json")
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
        """R2-Med2:直接跑 bin/notifyctl.py(非经 hook 引导)从无关 cwd + 无 PYTHONPATH → 不得
        ModuleNotFoundError(证明 notifyctl 自身 sys.path 引导没被抽取误删)。空 stdin → empty-message exit 0。"""
        import subprocess
        r = subprocess.run(
            ["python3", str(ROOT / "bin" / "notifyctl.py")],
            input=b"", capture_output=True, cwd=str(tmp_path),
            env=self._env_scrubbed(tmp_path))
        assert r.returncode == 0
        assert b"ModuleNotFoundError" not in r.stderr and b"Traceback" not in r.stderr
        assert json.loads(r.stdout)["reason"] == "empty-message"

    def test_hook_main_exits_0_even_if_entry_raises(self, monkeypatch):
        """exit-0 绝对契约(codex impl Low-1):即便 hooklib.stop_failure_entry(含其 fail-closed
        兜底 _fail_closed_drop 的时间戳/stderr 写)自身抛,hook main() 也吞掉并 sys.exit(0)。"""
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
    assert "matcher" not in grp  # 用户选"所有错误类型" → 不设 matcher
    hk = grp["hooks"][0]
    assert hk["type"] == "command" and hk["command"] == "python3"
    assert any("stop_failure_hook.py" in a for a in hk["args"])
    assert hk["timeout"] >= 60  # R1-#1:覆盖内部 ~40s 发送路径


# --------------------------------------------------------------------------- 抽取守卫
def test_extraction_reexport_identity():
    notifyctl = _load_notifyctl()
    assert notifyctl.run_notify is notifymod.run_notify
    assert notifyctl.configmod is notifymod.configmod
    assert notifyctl.OWNER_RE is notifymod.OWNER_RE
    assert notifyctl._wire_argv is notifymod._wire_argv
    assert notifyctl.LARK_BIN == notifymod.LARK_BIN
