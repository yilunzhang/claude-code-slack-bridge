"""sendfile(bin/sendfilectl.py + lib/sendfile.run_sendfile)单测。全程离线:注入 environ / prober /
make_client(FakeSlackClient)/ upload;db+config+tokens 经 SLACK_BRIDGE_DATA_DIR 隔离。
纪律与 notify 相同:发送前拒绝 = 确定未发(client 零调用);①/② 失败 = sent:false;③ 不确定 = sent:"unknown"。"""
import importlib.util
import json
import pathlib

import pytest

from tests.conftest import CC_PID, CHAT
from tests.helpers import FakeSlackClient, err, http5xx, not_sent, ok, ratelimited, timeout
from tests.test_notifyctl import _claude_prober, _setup_bound, _write_config
from lib import notify as notifymod
from lib import sendfile as sendfilemod


def _load_ctl():
    root = pathlib.Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("sendfilectl_mod", root / "bin" / "sendfilectl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sendfilectl = _load_ctl()
UPLOAD_URL = "https://files.slack.com/upload/v1/ABC123"
FILE_ID = "F0FAKE1"


@pytest.fixture
def bound(cfg, tokens, conn):
    _setup_bound(conn)
    return conn


@pytest.fixture
def afile(tmp_path):
    p = tmp_path / "report.html"
    p.write_bytes(b"<html>report " + b"x" * 500 + b"</html>")
    return p


class Uploader:
    def __init__(self, status=200, exc=None):
        self.status, self.exc, self.calls = status, exc, []

    def __call__(self, url, data, timeout_s):
        self.calls.append((url, bytes(data), timeout_s))
        if self.exc:
            raise self.exc
        return self.status


def _ok_client(complete=None):
    c = FakeSlackClient()
    c.on("files.getUploadURLExternal", lambda m, p: ok({"upload_url": UPLOAD_URL, "file_id": FILE_ID}))
    c.on("files.completeUploadExternal",
         complete or (lambda m, p: ok({"files": [{"id": FILE_ID, "title": p["files"][0]["title"]}]})))
    return c


def _call(path, *, title=None, comment=None, client=None, upload=None, session="sess-1",
          start_pid=CC_PID, prober=None):
    cc = client if client is not None else _ok_client()
    up = upload if upload is not None else Uploader()

    def make_client(tokens, version, store):
        cc.cooldown_store = store
        return cc
    env = {} if session is None else {"CLAUDE_CODE_SESSION_ID": session}
    obj, code = sendfilemod.run_sendfile(
        path=str(path) if path is not None else None, title=title, comment_text=comment,
        environ=env, prober=prober or _claude_prober(), start_pid=start_pid,
        make_client=make_client, upload=up)
    return obj, code, cc, up


# --------------------------------------------------------------------------- 模块
def test_ctl_module_loads_and_reexports():
    assert sendfilectl.run_sendfile is sendfilemod.run_sendfile
    assert hasattr(sendfilectl, "main") and hasattr(sendfilectl, "parse_args")
    a = sendfilectl.parse_args(["--path", "/tmp/x", "--title", "t", "--comment", "c"])
    assert (a.path, a.title, a.comment) == ("/tmp/x", "t", "c")


# --------------------------------------------------------------------------- 成功路径
class TestSuccess:
    def test_three_steps_and_result(self, bound, afile):
        obj, code, cc, up = _call(afile, comment="看这个报告")
        assert code == 0 and obj == {"ok": True, "sent": True, "file_id": FILE_ID, "chat_id": CHAT}
        assert [m for m, _ in cc.calls] == ["files.getUploadURLExternal", "files.completeUploadExternal"]
        get_p = cc.calls[0][1]
        assert get_p == {"filename": "report.html", "length": afile.stat().st_size}
        assert up.calls == [(UPLOAD_URL, afile.read_bytes(), sendfilemod.UPLOAD_TIMEOUT_S)]
        comp = cc.calls[1][1]
        assert comp["files"] == [{"id": FILE_ID, "title": "report.html"}]   # 默认标题 = 文件名
        assert comp["channel_id"] == CHAT and comp["initial_comment"] == "看这个报告"

    def test_explicit_title_and_no_comment(self, bound, afile):
        obj, code, cc, _ = _call(afile, title=" SEO 报告 ", comment="   ")
        assert code == 0 and obj["sent"] is True
        comp = cc.calls[1][1]
        assert comp["files"][0]["title"] == "SEO 报告" and "initial_comment" not in comp

    def test_read_methods_form_encoding_contract(self):
        """①③ 都不在 JSON_BODY_METHODS(读类/带 files 数组)→ 走表单编码;files 会被序列化成 JSON 字符串。"""
        from lib import constants
        from lib.slackapi import encode_body
        import urllib.parse
        assert "files.getUploadURLExternal" not in constants.JSON_BODY_METHODS
        assert "files.completeUploadExternal" not in constants.JSON_BODY_METHODS
        body, ctype = encode_body("files.completeUploadExternal",
                                  {"files": [{"id": "F1", "title": "t"}], "channel_id": "C1"})
        assert ctype.startswith("application/x-www-form-urlencoded")
        q = dict(urllib.parse.parse_qsl(body.decode()))
        assert json.loads(q["files"]) == [{"id": "F1", "title": "t"}] and q["channel_id"] == "C1"


# --------------------------------------------------------------------------- 输入校验(确定未发,client 零调用)
class TestInputRejected:
    @pytest.mark.parametrize("bad", [None, "", "relative/path.txt"])
    def test_bad_path_shape(self, bound, bad):
        obj, code, cc, up = _call(bad)
        assert code == 3 and obj["reason"] == "invalid-input" and cc.calls == [] and up.calls == []

    def test_missing_dir_empty(self, bound, tmp_path):
        for p in (tmp_path / "nope.txt", tmp_path):
            obj, code, cc, _ = _call(p)
            assert code == 3 and obj["reason"] == "invalid-input" and cc.calls == []
        empty = tmp_path / "empty.bin"
        empty.write_bytes(b"")
        obj, code, cc, _ = _call(empty)
        assert code == 3 and obj["reason"] == "invalid-input" and "空" in obj["detail"]

    def test_too_large(self, bound, afile, monkeypatch):
        monkeypatch.setattr(sendfilemod, "MAX_FILE_BYTES", 10)
        obj, code, cc, up = _call(afile)
        assert code == 3 and obj["reason"] == "file-too-large" and obj["limit"] == 10
        assert cc.calls == [] and up.calls == []

    @pytest.mark.parametrize("raw", ["<!channel> 看", "前面 <!here>", "x<!subteam^S1>"])
    def test_comment_broadcast_rejected(self, bound, afile, raw):
        obj, code, cc, _ = _call(afile, comment=raw)
        assert code == 3 and obj["reason"] == "invalid-mention" and cc.calls == []

    def test_comment_too_long_and_title_bad(self, bound, afile):
        obj, code, cc, _ = _call(afile, comment="x" * (sendfilemod.COMMENT_MAX_CHARS + 1))
        assert code == 3 and obj["reason"] == "message-too-long" and cc.calls == []
        obj, code, cc, _ = _call(afile, title="a\nb")
        assert code == 3 and obj["reason"] == "invalid-input" and cc.calls == []
        obj, code, cc, _ = _call(afile, title="t" * (sendfilemod.TITLE_MAX_CHARS + 1))
        assert code == 3 and obj["reason"] == "invalid-input" and cc.calls == []


# --------------------------------------------------------------------------- 门(与 notify 同款)
class TestGates:
    def test_not_bound(self, cfg, tokens, conn, afile):
        obj, code, cc, up = _call(afile)
        assert code == 0 and obj["reason"] == "not-bound" and cc.calls == [] and up.calls == []

    def test_missing_session_id(self, bound, afile):
        obj, code, cc, _ = _call(afile, session=None)
        assert code == 3 and obj["reason"] == "session-unresolved" and cc.calls == []

    @pytest.mark.parametrize("gate", ["mismatch", "degraded:auth_error", None])
    def test_gate_not_ok(self, cfg, tokens, conn, afile, gate):
        _setup_bound(conn, gate=gate)
        obj, code, cc, _ = _call(afile)
        assert code == 3 and obj["reason"] == "gate-degraded" and cc.calls == []

    def test_credentials_version_mismatch(self, cfg, tokens, conn, afile):
        _setup_bound(conn, gate_version="stale-version")
        obj, code, cc, _ = _call(afile)
        assert code == 3 and obj["reason"] == "credentials-unverified" and cc.calls == []

    def test_allowlist_excludes_chat(self, data_dir, tokens, conn, afile):
        _write_config(chat_allowlist=["C0OTHER"])
        _setup_bound(conn)
        obj, code, cc, _ = _call(afile)
        assert code == 3 and obj["reason"] == "chat-not-allowed" and cc.calls == []

    def test_shared_gate_is_notify_gate(self):
        assert sendfilemod.open_gated_context is notifymod.open_gated_context


# --------------------------------------------------------------------------- 阶段失败
class TestStages:
    def test_upload_url_rejected_is_not_sent(self, bound, afile):
        c = FakeSlackClient().on("files.getUploadURLExternal", lambda m, p: err("invalid_arguments"))
        obj, code, cc, up = _call(afile, client=c)
        assert code == 4 and obj["sent"] is False and obj["stage"] == "upload-url"
        assert obj["reason"] == "send-failed" and up.calls == []

    def test_upload_url_timeout_is_not_sent_but_retryable(self, bound, afile):
        c = FakeSlackClient().on("files.getUploadURLExternal", lambda m, p: timeout())
        obj, code, cc, up = _call(afile, client=c)
        assert code == 4 and obj["sent"] is False and obj["retryable"] is True and up.calls == []

    def test_upload_url_ratelimited_and_cooldown(self, bound, afile):
        c = FakeSlackClient().on("files.getUploadURLExternal", lambda m, p: ratelimited(7))
        obj, code, cc, _ = _call(afile, client=c)
        assert code == 4 and obj["reason"] == "ratelimited" and obj["retry_after"] == 7
        obj2, code2, cc2, _ = _call(afile, client=c)       # 冷却已发布 → 第二次本地 wait,不发请求
        assert code2 == 4 and obj2["reason"] == "cooldown" and len(cc2.calls) == 1

    def test_malformed_upload_url(self, bound, afile):
        c = FakeSlackClient().on("files.getUploadURLExternal", lambda m, p: ok({"upload_url": "http://x", "file_id": "F1"}))
        obj, code, cc, up = _call(afile, client=c)
        assert code == 4 and obj["error"] == "malformed-upload-url" and up.calls == []

    def test_upload_http_error_is_not_sent(self, bound, afile):
        obj, code, cc, up = _call(afile, upload=Uploader(status=500))
        assert code == 4 and obj["sent"] is False and obj["stage"] == "upload"
        assert obj["reason"] == "upload-failed" and obj["retryable"] is True
        assert [m for m, _ in cc.calls] == ["files.getUploadURLExternal"]      # ③ 未调用

    def test_upload_exception_redacts_token(self, bound, afile):
        obj, code, cc, _ = _call(afile, upload=Uploader(exc=OSError("boom xoxb-test-bot-token")))
        assert code == 4 and obj["reason"] == "upload-failed"
        assert "xoxb-test-bot-token" not in json.dumps(obj)

    @pytest.mark.parametrize("res", [timeout(), http5xx(502), err("internal_error")])
    def test_complete_uncertain_is_unknown(self, bound, afile, res):
        obj, code, cc, _ = _call(afile, client=_ok_client(complete=res))
        assert code == 5 and obj["sent"] == "unknown" and obj["stage"] == "complete"
        assert obj["file_id"] == FILE_ID

    def test_complete_permanent_rejection(self, bound, afile):
        obj, code, cc, _ = _call(afile, client=_ok_client(complete=err("not_in_channel")))
        assert code == 4 and obj["sent"] is False and obj["stage"] == "complete"
        assert obj["reason"] == "send-failed" and obj["error"] == "not_in_channel"

    def test_complete_not_sent_auth(self, bound, afile):
        obj, code, cc, _ = _call(afile, client=_ok_client(complete=not_sent("invalid_auth")))
        assert code == 4 and obj["sent"] is False and obj["retryable"] is False


# --------------------------------------------------------------------------- 文档契约
def test_skill_docs_mention_sendfile_as_gated_exception():
    root = pathlib.Path(__file__).resolve().parents[1]
    bridge = (root / "skills" / "bridge" / "SKILL.md").read_text(encoding="utf-8")
    assert "sendfilectl" in bridge and "20" in bridge   # 用法 + 大小上限
    notify = (root / "skills" / "notify" / "SKILL.md").read_text(encoding="utf-8")
    assert "sendfilectl" in notify
