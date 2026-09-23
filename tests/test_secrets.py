"""R1-M5:凭据绝不进异常文本 / stderr / daemon.log / daemon_state / CLI JSON。
- config.load_tokens / save_tokens / ctl.bootstrap 在装载处拒绝含空白、控制字符、非 ASCII 的 token
  (含内部换行的 xapp token 会让 HTTP header 校验抛出携带 `Bearer <token>` 的 ValueError);
- util.redact_secrets:显式 secret(含 repr 转义形态)+ Slack token 形态兜底;
- SlackClient.call / download_worker / DaemonCore / notify / bridgectl / consumer:凭据相关异常只留类型名 +
  固定码,所有可能带异常文本的输出行都经遮蔽。"""
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys

import pytest

from lib import config as configmod
from lib import constants, ctl, db as dbmod, util
from lib.slackapi import SlackClient
from tests.conftest import OWNER

ROOT = pathlib.Path(__file__).resolve().parents[1]
BAD_TOKENS = [
    "xapp-abc\ndef", "xapp-abc\r\ndef", "xoxb-abc\tdef", "xoxb-abc def", "xoxb-abc\x00def",
    "xoxb-abc\x1bdef", "xoxb-abc\x7fdef", "xoxb-abcédef", "xoxb-abc​def", " xoxb-lead",
    "xoxb-trail\n",
]


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------- util.redact_secrets
class TestRedactSecrets:
    def test_explicit_secret_replaced_everywhere(self):
        out = util.redact_secrets("a s3cr3t-value-xyz b s3cr3t-value-xyz", ["s3cr3t-value-xyz"])
        assert out == "a *** b ***"

    def test_repr_escaped_forms_replaced(self):
        """异常文本常是 bytes repr:`Invalid header value b'Bearer tok\\n'` —— token 里的换行是转义过的。"""
        tok = "s3cr3t-with\nnewline"
        text = "Invalid header value %r" % (("Bearer " + tok + "\n").encode(),)
        assert "s3cr3t" in text and "\\n" in text
        out = util.redact_secrets(text, [tok])
        assert "s3cr3t" not in out and "newline" not in out and "***" in out
        out2 = util.redact_secrets("str repr: %r" % tok, [tok])
        assert "s3cr3t" not in out2 and "***" in out2

    def test_slack_token_shape_fallback_without_explicit_secret(self):
        out = util.redact_secrets("hdr Bearer xoxb-123456-abcdef and xapp-1-A1B2C3D4E5 tail", [])
        assert out == "hdr Bearer *** and *** tail"

    def test_short_or_non_str_secrets_ignored(self):
        assert util.redact_secrets("abc abc", ["abc", None, 12]) == "abc abc"
        assert util.redact_secrets(None, ["whatever-long"]) == ""
        assert util.redact_secrets(12345, []) == "12345"

    def test_longest_first_no_fragment_left(self):
        out = util.redact_secrets("k=s3cr3t-value-xyz-EXT", ["s3cr3t-value-xyz", "s3cr3t-value-xyz-EXT"])
        assert out == "k=***"


# ---------------------------------------------------------------- 装载处拒绝
class TestTokenCharValidation:
    @pytest.mark.parametrize("bad", BAD_TOKENS)
    def test_load_tokens_file_rejects_control_or_whitespace(self, data_dir, bad):
        from lib import paths
        paths.ensure_data_dir()
        p = str(paths.tokens_path())
        raw = json.dumps({"bot_token": "xoxb-good-token-123", "app_token": bad}).encode("utf-8")
        util.atomic_write(p, raw, mode=0o600)
        with pytest.raises(configmod.ConfigError) as ei:
            configmod.load_tokens(allow_env=False)
        msg = str(ei.value)
        assert "app_token" in msg and bad.strip() not in msg and "非法字符" in msg
        # bot_token 同样
        util.atomic_write(p, json.dumps({"bot_token": bad}).encode("utf-8"), mode=0o600)
        with pytest.raises(configmod.ConfigError) as ei:
            configmod.load_tokens(allow_env=False)
        assert "bot_token" in str(ei.value) and bad.strip() not in str(ei.value)

    @pytest.mark.parametrize("bad", BAD_TOKENS[:4])
    def test_load_tokens_env_rejects(self, data_dir, bad):
        env = {"SLACK_BOT_TOKEN": "xoxb-good-token-123", "SLACK_APP_TOKEN": bad}
        with pytest.raises(configmod.ConfigError):
            configmod.load_tokens(allow_env=True, environ=env)
        with pytest.raises(configmod.ConfigError):
            configmod.load_tokens(allow_env=True, environ={"SLACK_BOT_TOKEN": bad})

    def test_save_tokens_rejects(self, data_dir):
        with pytest.raises(configmod.ConfigError):
            configmod.save_tokens({"bot_token": "xoxb-ok-123456", "app_token": "xapp-a\nb"})
        from lib import paths
        assert not paths.tokens_path().exists()

    def test_good_tokens_still_load(self, tokens):
        t, v = configmod.load_tokens(allow_env=False)
        assert t == tokens.tokens and v == tokens.version

    def test_bootstrap_rejects_before_any_network_call(self, data_dir):
        calls = []

        def factory(t):
            calls.append(t)
            raise AssertionError("client must not be built for a malformed token")

        for bad in ("xoxb-abc\ndef", "xoxb-abc def"):
            with pytest.raises(configmod.ConfigError) as ei:
                ctl.bootstrap(factory, owner=OWNER, tokens={"bot_token": bad, "app_token": "xapp-ok-1234"})
            assert "abc" not in str(ei.value) or "def" not in str(ei.value)
        with pytest.raises(configmod.ConfigError):
            ctl.bootstrap(factory, owner=OWNER, tokens={"bot_token": "xoxb-ok-1234", "app_token": "xapp-a\nb"})
        assert calls == []


# ---------------------------------------------------------------- 传输层 / worker / daemon_core / notify / CLI
class TestNoLeakPaths:
    def test_slackclient_call_with_newline_token_is_not_sent_fixed_code(self):
        """真 SlackClient(直接 token,绕过 load_tokens 校验):header 校验 ValueError 不再向上抛,
        返回 not_sent + 固定码;结果对象的 repr / error 不含 token。"""
        tok = "xoxb-abc\ndef-secret"
        c = SlackClient(token=tok, base_url="https://127.0.0.1:9/")
        res = c.call("auth.test", {})
        assert res.ok is False and res.not_sent is True and res.error == "bad_request"
        assert res.exc is None
        assert "secret" not in repr(res) and "secret" not in repr(c)

    def test_download_worker_run_valueerror_is_fixed_code(self, tmp_path):
        dw = _load(ROOT / "bin" / "download_worker.py", "dw_secrets_mod")

        class Opener:
            def open(self, request, timeout=None):
                raise ValueError("Invalid header value b'Bearer xoxb-worker-secret\\n'")

        dest = tmp_path / "f.bin"
        req = {"url": "https://files.slack.com/x", "dest_tmp": str(dest), "token": "xoxb-worker-secret",
               "max_bytes": 10, "timeout_s": 5.0, "allow_plain_http_hosts": []}
        rc, out = dw.run(req, opener=Opener())
        assert rc == 3 and out == {"ok": False, "nbytes": None, "content_type": None,
                                   "http_status": None, "error": "bad_request"}
        assert not dest.exists()

    def test_download_worker_main_guard_never_prints_traceback(self, tmp_path, monkeypatch, capsys):
        dw = _load(ROOT / "bin" / "download_worker.py", "dw_secrets_mod2")
        monkeypatch.setattr(dw, "start_watchdog", lambda: None)
        monkeypatch.setattr(dw.signal, "alarm", lambda n: 0)
        monkeypatch.setattr(dw.signal, "signal", lambda *a: None)

        def boom(req, opener=None):
            raise RuntimeError("Invalid header value b'Bearer xoxb-worker-secret\\n'")

        monkeypatch.setattr(dw, "run", boom)
        req = json.dumps({"url": "https://files.slack.com/x", "dest_tmp": str(tmp_path / "f"),
                          "token": "xoxb-worker-secret", "max_bytes": 10, "timeout_s": 5.0})
        out = io.StringIO()
        rc = dw.main(stdin=io.StringIO(req), out=out)
        assert rc == 1
        line = json.loads(out.getvalue())
        assert line["error"] == "worker_exception:RuntimeError"
        assert "secret" not in out.getvalue() and "secret" not in capsys.readouterr().err

    def test_media_parent_redacts_worker_stderr_and_error(self, env, monkeypatch):
        """父进程把 worker stderr / error 文本写进 daemon.log 前遮蔽 token(worker 崩溃打 traceback 的双保险)。"""
        from lib import media
        from tests.test_media import FAKE_WORKER, f_rc
        logs = []
        tok = "xoxb-parent-secret-token"
        real = media.subprocess.Popen

        class Rec(real):
            """真跑假 worker(拿真实 rc),但把 stdout/stderr 换成带 token 的文本(模拟 worker 崩溃 traceback)。"""
            def communicate(self, input=None, timeout=None):
                super().communicate(input=input, timeout=timeout)
                return (b'{"ok": false, "error": "ValueError: Bearer %s"}' % tok.encode(),
                        b"Traceback ... ValueError: Invalid header value b'Bearer %s\\n'" % tok.encode())

        monkeypatch.setattr(media.subprocess, "Popen", Rec)
        # 瞬态 rc=4:error + stderr 都进日志 → 遮蔽
        assert media.materialize({"bot_token": tok}, env.media_root, "b-sec", "C0:1.1", [f_rc(4)],
                                 worker_path=FAKE_WORKER, log=logs.append) is None
        assert any("stderr=" in l for l in logs)
        # 永久 rc=3:error 进 MediaError 文本(会被 inbound 记日志)→ 遮蔽
        with pytest.raises(media.MediaError) as ei:
            media.materialize({"bot_token": tok}, env.media_root, "b-sec", "C0:2.2", [f_rc(3)],
                              worker_path=FAKE_WORKER, log=logs.append)
        blob = "\n".join(logs) + str(ei.value)
        assert tok not in blob and "parent-secret" not in blob and "***" in blob

    def test_daemon_core_last_error_and_drain_error_redacted(self, env):
        from lib.daemon_core import DaemonCore
        from lib import inbound as inbound_mod
        core = DaemonCore(env.conn, env.cfg, env.clock, env.inbound, env.approval, env.outbound, env.recovery)

        def boom(budget=None):
            raise RuntimeError("Invalid header value b'Bearer xoxb-core-secret-1\\n'")
        env.inbound.drive_pending_rows = boom
        assert core.run_followups() == {}
        last = dbmod.get_state(env.conn, "last_error")
        assert "core-secret" not in last and "***" in last and "RuntimeError" in last
        # drain 异常 → slack_events.error / last_error
        from lib import util as u
        env.conn.execute(
            "INSERT INTO slack_events(envelope_type,event_key,payload_json,received_at,state) "
            "VALUES('events_api','ev:E9',?,?,'staged')", (u.jdumps({"event_id": "E9"}), env.clock.wall_ms()))
        orig = inbound_mod.ingest_in_tx

        def bad_ingest(conn, row):
            raise RuntimeError("token xapp-drain-secret-9 leaked")
        inbound_mod.ingest_in_tx = bad_ingest
        try:
            core.drain_staging()
        finally:
            inbound_mod.ingest_in_tx = orig
        row = env.conn.execute("SELECT error FROM slack_events WHERE event_key='ev:E9'").fetchone()
        assert "drain-secret" not in row["error"] and "***" in row["error"]
        assert "drain-secret" not in dbmod.get_state(env.conn, "last_error")

    def test_notify_internal_error_detail_redacted(self, cfg, tokens, conn):
        from tests.test_notifyctl import _setup_bound, _claude_prober
        from lib import notify as notifymod
        _setup_bound(conn)
        bot = tokens.tokens["bot_token"]

        def make_client(t, version, store):
            raise ValueError("Invalid header value %r" % (("Bearer " + bot + "\n").encode(),))

        obj, code = notifymod.run_notify(stdin_text="x", environ={"CLAUDE_CODE_SESSION_ID": "sess-1"},
                                         prober=_claude_prober(), start_pid=4242, make_client=make_client)
        assert code == 3 and obj["reason"] == "internal-error"
        assert bot not in json.dumps(obj) and "test-bot-token" not in json.dumps(obj)
        assert obj["detail"].startswith("ValueError: ")

    def test_bridgectl_main_catch_all_no_traceback_no_token(self, data_dir, monkeypatch, capsys):
        mod = _load(ROOT / "bin" / "bridgectl.py", "bridgectl_secrets_mod")

        def boom(args):
            raise RuntimeError("Invalid header value b'Bearer xoxb-cli-secret-77\\n'")

        monkeypatch.setattr(mod, "cmd_status", boom)
        monkeypatch.setattr(sys, "argv", ["bridgectl", "status"])
        with pytest.raises(SystemExit) as ei:
            mod.main()
        assert ei.value.code == 2
        out = capsys.readouterr()
        assert json.loads(out.out) == {"ok": False, "error": "internal-error", "type": "RuntimeError"}
        assert "cli-secret" not in out.out + out.err

    def test_consumer_status_and_exc_label_in_process(self, capsys):
        cons = _load(ROOT / "bin" / "slack_consumer.py", "consumer_secrets_mod")
        cons._SECRETS[:] = ["xapp-inproc-secret-1", "xoxb-inproc-secret-2"]
        try:
            assert cons._exc_label(ValueError("Bearer xapp-inproc-secret-1")) == "ValueError"
            lab = cons._exc_label(ValueError("db locked by xapp-inproc-secret-1"), with_text=True)
            assert lab == "ValueError:db locked by ***"
            cons.status("[socket] warn something xoxb-inproc-secret-2 and xapp-other-token-zz")
            err = capsys.readouterr().err
            assert err == "[socket] warn something *** and ***\n"
        finally:
            cons._SECRETS[:] = []
