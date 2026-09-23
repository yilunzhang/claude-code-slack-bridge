"""notify skill (bin/notifyctl.py) 单测 —— 覆盖 plan v8「测试」节每一条。
全程离线:注入 stdin_text / environ / prober / make_runner;db+config 经 FEISHU_BRIDGE_DATA_DIR 隔离。
纪律:非前置(空消息/无绑定)失败一律如实返回、不吞;发送前拒绝=确定未发(runner 零调用)。"""
import importlib.util
import json
import pathlib

import pytest

from tests.conftest import APP_ID, BOT_OPEN_ID, CC_PID, CC_START, CHAT, OWNER, PROFILE
from tests.helpers import (FakeProber, FakeRunner, FakeRunResult, err_envelope,
                           ok_envelope, stderr_err_envelope)


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


def _ok_send_runner(message_id="om_sent"):
    r = FakeRunner(profile=PROFILE)
    r.on_prefix(["im", "+messages-send"], lambda a, c: ok_envelope({"message_id": message_id}))
    return r


def _res_runner(result):
    """任意 FakeRunResult(或 fn)应答 +messages-send。"""
    r = FakeRunner(profile=PROFILE)
    r.on_prefix(["im", "+messages-send"], lambda a, c: result if not callable(result) else result(a, c))
    return r


def _setup_bound(conn, *, session_id="sess-1", chat_id=CHAT, status="active",
                 cc_pid=CC_PID, cc_start=CC_START, gate="ok", bind_phase="confirmed"):
    from lib import db as dbmod, util
    bid = util.new_id()
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,cc_start,"
        "status,bind_phase,listener_epoch,bound_at,close_reason) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (bid, chat_id, "测试群", session_id, cc_pid, cc_start, status, bind_phase, 1, 0,
         "user_unbind" if status in ("dead", "closed") else None))
    if gate is not None:
        dbmod.set_state(conn, "outbound_gate", gate)
    return bid


def _write_config(**overrides):
    from lib import config as configmod, paths
    paths.ensure_data_dir()
    c = {"profile": PROFILE, "app_id": APP_ID, "bot_open_id": BOT_OPEN_ID,
         "bot_name": "TestBot", "owner_open_id": OWNER, "cli_version": "1.0.66"}
    c.update(overrides)
    configmod.save_config(c)
    return c


def _fake_cfg(**overrides):
    c = {"profile": PROFILE, "app_id": APP_ID, "bot_open_id": BOT_OPEN_ID,
         "bot_name": "TestBot", "owner_open_id": OWNER, "cli_version": "1.0.66"}
    c.update(overrides)
    return c


def _call(*, stdin="需要授权 X", session="sess-1", prober=None, start_pid=CC_PID, runner=None,
          make_runner=None, capture_profile=None):
    if prober is None:
        prober = _claude_prober()
    if make_runner is None:
        rr = runner if runner is not None else FakeRunner(profile=PROFILE)

        def make_runner(p):  # noqa: E731 —— 记录生产传入的 profile(codex impl MAJOR3:证明用 cfg 的 profile)
            if capture_profile is not None:
                capture_profile.append(p)
            return rr
    env = {} if session is None else {"CLAUDE_CODE_SESSION_ID": session}
    return notifyctl.run_notify(stdin_text=stdin, environ=env, prober=prober,
                                start_pid=start_pid, make_runner=make_runner)


def _arg(call_args, flag):
    return call_args[call_args.index(flag) + 1]


def _post_paragraphs(call_args):
    """从 send argv 取 `--content` 的 post JSON,返回**全部段落**(`content`,list[list[node]])。"""
    return json.loads(_arg(call_args, "--content"))["zh_cn"]["content"]


def _post_nodes(call_args):
    """全部段落**摊平**成节点列表 —— 断言"某节点存在"用它,别只看 content[0]
    (正文自 2026-07-27 起在**第二段落**的 md 节点里)。"""
    return [n for para in _post_paragraphs(call_args) for n in para]


# --------------------------------------------------------------------------- 模块 & 版本
def test_notifyctl_module_loads():
    assert hasattr(notifyctl, "run_notify") and hasattr(notifyctl, "main")


def test_plugin_marketplace_version_consistency():
    """durable 不变量:plugin.json 版本 == marketplace.json 版本(永不 rot,不硬编码具体值)。"""
    root = pathlib.Path(__file__).resolve().parents[1]
    pj = json.loads((root / ".claude-plugin" / "plugin.json").read_text())
    mj = json.loads((root / ".claude-plugin" / "marketplace.json").read_text())
    assert pj["version"] == mj["plugins"][0]["version"]


# --------------------------------------------------------------------------- 正路径
class TestHappyPath:
    def test_happy_path_sends_with_owner_prefix(self, cfg, conn, monkeypatch):
        _setup_bound(conn)
        runner = _ok_send_runner()
        obj, code = _call(stdin="需要你授权删库", runner=runner)
        assert code == 0 and obj["ok"] is True and obj["sent"] is True
        assert obj["message_id"] == "om_sent" and obj["chat_id"] == CHAT
        assert len(runner.calls) == 1
        ca = runner.calls[0][0]
        assert _arg(ca, "--as") == "bot" and _arg(ca, "--chat-id") == CHAT
        assert _arg(ca, "--msg-type") == "post"
        # owner mention = 结构化 at 节点(不受正文畸形标签影响);正文 = 独立 md 节点(会渲染 markdown)
        nodes = _post_nodes(ca)
        assert {"tag": "at", "user_id": OWNER} in nodes
        assert any(n.get("tag") == "md" and "需要你授权删库" in n.get("text", "") for n in nodes)
        assert _arg(ca, "--idempotency-key")

    def test_full_triple_match_sends(self, cfg, conn):
        _setup_bound(conn, session_id="sess-1", cc_pid=CC_PID, cc_start=CC_START)
        runner = _ok_send_runner()
        obj, code = _call(runner=runner)
        assert code == 0 and obj["sent"] is True and len(runner.calls) == 1

    def test_special_chars_verbatim_in_body(self, cfg, conn):
        """stdin 特殊字符($()/反引号/引号/中间换行,不含 <at/NUL)→ 逐字进 body(经 argv 非 shell)。"""
        _setup_bound(conn)
        runner = _ok_send_runner()
        payload = 'line1 $(whoami) `id` "dq" \'sq\' 中文\nline2'
        obj, code = _call(stdin=payload, runner=runner)
        assert code == 0 and obj["sent"] is True
        nodes = _post_nodes(runner.calls[0][0])
        assert {"tag": "at", "user_id": OWNER} in nodes
        # 特殊字符逐字进 md 节点(经 argv/JSON 非 shell:$()/反引号/引号/换行原样)
        assert any(n.get("tag") == "md" and payload in n.get("text", "") for n in nodes)

    def test_idempotency_key_differs_each_call(self, cfg, conn):
        _setup_bound(conn)
        r1, r2 = _ok_send_runner(), _ok_send_runner()
        _call(runner=r1)
        _call(runner=r2)
        assert _arg(r1.calls[0][0], "--idempotency-key") != _arg(r2.calls[0][0], "--idempotency-key")


# --------------------------------------------------------------------------- 正文 mention / NUL
class TestBodyRejections:
    def test_body_with_at_tag_rejected(self, cfg, conn):
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin='请 <at user_id="all"></at> 看', runner=runner)
        assert code == 3 and obj["reason"] == "invalid-mention" and obj["sent"] is False
        assert runner.calls == []

    def test_at_tag_nonzero_offset_rejected(self, cfg, conn):
        """codex r6:用 search 非 match —— <at 在非零偏移也拒。"""
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin='前文正常 <at user_id="all"></at>', runner=runner)
        assert code == 3 and obj["reason"] == "invalid-mention" and runner.calls == []

    @pytest.mark.parametrize("raw", [
        '<AT user_id="all"></at>',        # 大写 → 靠 IGNORECASE
        '<At user_id="all"></at>',        # 混合大小写
        '<at\tuser_id="all"></at>',       # Tab → 靠 \s(非字面空格)
        '<at\nuser_id="all"></at>',       # 换行 → 靠 \s
        '<at>text</at>',                  # `>` 紧跟 → 靠 [\s>] 的 > 分支
    ])
    def test_at_variants_rejected_raw_body(self, cfg, conn, raw):
        """**承重守卫的完整不变量**(codex impl r1 MEDIUM,实测出的变异漏洞)。

        正文改走 md 节点后,md **会真渲染** `<at user_id=...>` 成活 mention → AT_RE 是阻止正文
        自行 @全员的**唯一**本地闸门。此前 notify 侧只测了小写 `<at ` + 普通空格一种形态:codex
        把 AT_RE 削成 `re.compile(r"<at ")`(丢掉 IGNORECASE、`\\s`、`>` 分支)**全套 555 仍全绿**。
        本参数化逐条钉住 IGNORECASE / `\\s`(Tab、换行)/ `>` 三个维度,任一被削都变红。

        **正文从偏移 0 起**(codex impl r2:第 4 个维度)—— 其余 `<at` 用例都带前缀文本,故"从
        index 1 开始搜"这类变异**全套 560 仍全绿**,而"正文开头就是 mention"恰是最自然的写法。
        反向(非零偏移)由下面 `test_at_tag_nonzero_offset_rejected` 钉住,两头都不漏。

        注:StopFailure 侧的同名用例测的是 `_sanitize_field`(净化后再看 AT_RE),**不覆盖这条
        原始正文闸门** —— notify 正文不过净化,必须在这里测。"""
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin=raw + " 后文", runner=runner)  # 偏移 0 起
        assert code == 3 and obj["reason"] == "invalid-mention" and obj["sent"] is False
        assert runner.calls == []  # 零发送:拒在进 runner 之前

    def test_bare_at_word_not_rejected(self, cfg, conn):
        """<at[\\s>] 精确匹配真 mention 标签:'<atlas>' 这类词不误判。"""
        _setup_bound(conn)
        runner = _ok_send_runner()
        obj, code = _call(stdin="see <atlas> the map", runner=runner)
        assert code == 0 and obj["sent"] is True

    def test_body_with_nul_rejected(self, cfg, conn):
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin="bad\x00msg", runner=runner)
        assert code == 3 and obj["reason"] == "invalid-input" and runner.calls == []


# --------------------------------------------------------------------------- 空消息
class TestEmptyMessage:
    def test_empty_message(self, cfg, conn):
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin="", runner=runner)
        assert code == 0 and obj["reason"] == "empty-message" and obj["sent"] is False
        assert runner.calls == []

    def test_whitespace_only_message(self, cfg, conn):
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin="   \n\t  ", runner=runner)
        assert code == 0 and obj["reason"] == "empty-message" and runner.calls == []


# --------------------------------------------------------------------------- session 三元组
class TestSessionTriple:
    def test_missing_session_id(self, cfg, conn):
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(session=None, runner=runner)
        assert code == 3 and obj["reason"] == "session-unresolved" and runner.calls == []

    def test_same_inst_diff_session_not_bound(self, cfg, conn):
        """②同 pid/start、session_id 不同 → not-bound(不发旧 session)。"""
        _setup_bound(conn, session_id="other", cc_pid=CC_PID, cc_start=CC_START)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(session="sess-1", runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []

    def test_same_session_diff_inst_not_bound(self, cfg, conn):
        """③同 session_id、pid/start 不同 → not-bound。"""
        _setup_bound(conn, session_id="sess-1", cc_pid=9999, cc_start="Other Start 2026")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(session="sess-1", runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []

    def test_same_session_same_pid_diff_start_not_bound(self, cfg, conn):
        """④同 session_id、同 pid、start 不同(PID 复用)→ not-bound。"""
        _setup_bound(conn, session_id="sess-1", cc_pid=CC_PID, cc_start="Different Start 2026")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(session="sess-1", runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []

    def test_diff_pid_same_start_not_bound(self, cfg, conn):
        """⑤不同 pid、同 start → not-bound。"""
        _setup_bound(conn, session_id="sess-1", cc_pid=8888, cc_start=CC_START)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(session="sess-1", runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []

    @pytest.mark.parametrize("status", ["starting", "closed", "dead"])
    def test_status_predicate_not_bound(self, cfg, conn, status):
        """第四谓词 status='active' 缺一不可:精确三元组命中但非 active → not-bound。"""
        _setup_bound(conn, session_id="sess-1", cc_pid=CC_PID, cc_start=CC_START, status=status)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(session="sess-1", runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []


# --------------------------------------------------------------------------- not-bound / instance / config
class TestPreconditions:
    def test_no_binding_not_bound(self, cfg, conn):
        from lib import db as dbmod
        dbmod.set_state(conn, "outbound_gate", "ok")  # 有库、无绑定
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []

    def test_db_missing_not_bound(self, cfg):
        """db 文件不存在 → not-bound(connect 前判,不建空库)。"""
        from lib import paths
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 0 and obj["reason"] == "not-bound" and runner.calls == []
        assert not paths.db_path().exists()  # 证明没被 connect 建出来

    def test_instance_unresolved(self, cfg, conn):
        """find_cc_instance→None → instance-unresolved(≠not-bound)。"""
        p = FakeProber()
        p.set(CC_PID, 1, CC_START, "zsh")  # 无 claude 祖先
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(prober=p, start_pid=CC_PID, runner=runner)
        assert code == 3 and obj["reason"] == "instance-unresolved" and runner.calls == []

    def test_config_missing_file(self, data_dir):
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "config" and runner.calls == []

    def test_config_missing_required_key(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        paths.config_path().write_text(json.dumps(
            {"profile": PROFILE, "app_id": APP_ID, "bot_open_id": BOT_OPEN_ID,
             "cli_version": "1.0.66"}))  # 缺 owner_open_id
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "config" and runner.calls == []

    def test_config_malformed_json_internal_error(self, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        paths.config_path().write_text("{ not valid json ")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "internal-error" and obj["sent"] is False
        assert runner.calls == []


# --------------------------------------------------------------------------- schema 门 / db 损坏
class TestSchemaAndDb:
    def test_schema_version_mismatch(self, cfg, conn):
        _setup_bound(conn)
        conn.execute("UPDATE daemon_state SET value='999' WHERE key='schema_version'")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "schema-mismatch" and runner.calls == []

    def test_schema_version_row_missing(self, cfg, conn):
        """missing-row 也 fail-closed(不只测值 999)。"""
        _setup_bound(conn)
        conn.execute("DELETE FROM daemon_state WHERE key='schema_version'")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "schema-mismatch" and runner.calls == []

    def test_db_corrupt_internal_error(self, cfg, data_dir):
        from lib import paths
        paths.ensure_data_dir()
        paths.db_path().write_bytes(b"this is not a sqlite database at all")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] in ("internal-error", "schema-mismatch")
        assert obj["sent"] is False and runner.calls == []


# --------------------------------------------------------------------------- allowlist(类型优先后看空)
class TestAllowlist:
    def _bound(self, conn):
        _setup_bound(conn)

    def test_allowlist_none_allows(self, data_dir, conn):
        _write_config()  # 无 chat_allowlist
        self._bound(conn)
        runner = _ok_send_runner()
        obj, code = _call(runner=runner)
        assert code == 0 and obj["sent"] is True

    def test_allowlist_empty_list_allows(self, data_dir, conn):
        _write_config(chat_allowlist=[])
        self._bound(conn)
        runner = _ok_send_runner()
        obj, code = _call(runner=runner)
        assert code == 0 and obj["sent"] is True

    def test_allowlist_includes_chat_sends(self, data_dir, conn):
        _write_config(chat_allowlist=[CHAT])
        self._bound(conn)
        runner = _ok_send_runner()
        obj, code = _call(runner=runner)
        assert code == 0 and obj["sent"] is True and len(runner.calls) == 1

    def test_allowlist_excludes_chat(self, data_dir, conn):
        _write_config(chat_allowlist=["oc_other"])
        self._bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "chat-not-allowed" and runner.calls == []

    @pytest.mark.parametrize("bad", ["prefix-oc_chat1-suffix", "", 0, False, {"x": 1}])
    def test_allowlist_malformed_rejected(self, data_dir, conn, bad):
        """字符串子串/falsy 非 list/dict → invalid-config、零发送(类型优先,别写 if allow:)。"""
        _write_config(chat_allowlist=bad)
        self._bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-config" and runner.calls == []

    def test_allowlist_list_with_non_str_rejected(self, data_dir, conn):
        """列表含非 str 元素 → invalid-config(否则"只验外层是 list"的错实现会发)。"""
        _write_config(chat_allowlist=[CHAT, 0])
        self._bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-config" and runner.calls == []


# --------------------------------------------------------------------------- 身份门(fail-closed)
class TestGate:
    def test_gate_ok_sends(self, cfg, conn):
        _setup_bound(conn, gate="ok")
        runner = _ok_send_runner()
        obj, code = _call(runner=runner)
        assert code == 0 and obj["sent"] is True

    def test_gate_degraded(self, cfg, conn):
        _setup_bound(conn, gate="degraded")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "gate-degraded" and runner.calls == []

    def test_gate_missing_row_fail_closed(self, cfg, conn):
        """outbound_gate 行缺失 → gate-degraded、零发送(证明不被默认成 ok)。"""
        _setup_bound(conn, gate=None)  # 不写 outbound_gate 行
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "gate-degraded" and runner.calls == []


# --------------------------------------------------------------------------- owner 校验(fullmatch)
class TestOwnerValidation:
    def _bound(self, conn):
        _setup_bound(conn)

    def test_owner_all_rejected(self, conn, monkeypatch):
        """owner='all' → 真 @全员,必须拒。"""
        self._bound(conn)
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(owner_open_id="all"))
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-owner" and runner.calls == []

    def test_owner_close_tag_injection_rejected(self, conn, monkeypatch):
        self._bound(conn)
        evil = 'ou_x"></at><at user_id="all'
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(owner_open_id=evil))
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-owner" and runner.calls == []

    def test_owner_trailing_newline_rejected(self, conn, monkeypatch):
        """fullmatch 才拦得住尾换行('^..$' 的 $ 会放过 'ou_owner\\n')。"""
        self._bound(conn)
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(owner_open_id="ou_owner\n"))
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-owner" and runner.calls == []

    def test_owner_regex_uses_fullmatch(self):
        """守卫:回归检查 OWNER_RE 用 fullmatch 语义(尾换行不过)。"""
        assert notifyctl.OWNER_RE.fullmatch("ou_abcDEF-_09")
        assert notifyctl.OWNER_RE.fullmatch("ou_owner\n") is None
        assert notifyctl.OWNER_RE.fullmatch("all") is None


# --------------------------------------------------------------------------- chat_id 格式
class TestChatIdFormat:
    def test_chat_id_malformed_rejected(self, cfg, conn):
        """绑定 chat_id='bad'(bind 不验格式)→ invalid-binding、零发送。"""
        _setup_bound(conn, chat_id="bad")
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-binding" and runner.calls == []


# --------------------------------------------------------------------------- argv 编码门
class TestArgvEncodingGate:
    def _bound(self, conn):
        _setup_bound(conn)

    def test_wire_argv_includes_lark_bin_and_profile(self):
        """codex r6:校验对象 = 完整 argv(lark_bin 前缀 + 末尾 --profile <profile>)。
        codex impl MAJOR3:与真实 LarkRunner.build_argv 同构(证明 _wire_argv 忠实,含 profile 追加段)。"""
        from lib.runner import LarkRunner
        send_args, full = notifyctl._wire_argv(PROFILE, CHAT, OWNER, "body", "fb:key")
        assert full[0] == notifyctl.LARK_BIN
        assert full[-2:] == ["--profile", PROFILE]
        assert send_args == full[1:-2]  # send_args = 完整 argv 去掉 lark_bin 与 --profile
        assert full == LarkRunner(PROFILE).build_argv(send_args)  # 与真实 runner 构造一致

    def test_profile_nul_rejected(self, conn, monkeypatch):
        self._bound(conn)
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(profile="main\x00x"))
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-argv" and runner.calls == []

    def test_profile_non_str_rejected(self, conn, monkeypatch):
        self._bound(conn)
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(profile=123))
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-argv" and runner.calls == []

    def test_profile_surrogate_rejected(self, conn, monkeypatch):
        """str + 无 NUL 但 UTF-8 不可编码(孤立 surrogate)→ invalid-argv。"""
        self._bound(conn)
        monkeypatch.setattr(notifyctl.configmod, "require_config", lambda: _fake_cfg(profile="main\ud800"))
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(runner=runner)
        assert code == 3 and obj["reason"] == "invalid-argv" and runner.calls == []

    def test_msg_surrogate_rejected(self, cfg, conn):
        """正文含孤立 surrogate(过步1 的 <at/NUL 检查)→ 8d invalid-argv。"""
        _setup_bound(conn)
        runner = FakeRunner(profile=PROFILE)
        obj, code = _call(stdin="hello \ud800 world", runner=runner)
        assert code == 3 and obj["reason"] == "invalid-argv" and runner.calls == []

    def test_argv_gate_covers_appended_profile_at_send(self, cfg, conn):
        """正路径:runner 实际收到的 argv = send_args + 追加的 --profile <profile>(证明门覆盖真实追加段)。"""
        _setup_bound(conn)
        runner = _ok_send_runner()
        _call(runner=runner)
        ca = runner.calls[0][0]
        assert ca[-2:] == ["--profile", PROFILE]
        assert ca[:2] == ["im", "+messages-send"]


# --------------------------------------------------------------------------- 发送结果三态矩阵
class TestSendMatrix:
    def _bound(self, conn):
        _setup_bound(conn)

    def test_send_rc_nonzero_with_message_id(self, cfg, conn):
        """message_id 优先:rc!=0 + ok:true + message_id → sent:true exit 0。"""
        self._bound(conn)
        res = FakeRunResult(rc=1, stdout=json.dumps({"ok": True, "data": {"message_id": "om_x"}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 0 and obj["sent"] is True and obj["message_id"] == "om_x"

    def test_send_timed_out_with_message_id(self, cfg, conn):
        self._bound(conn)
        res = FakeRunResult(rc=-1, timed_out=True,
                            stdout=json.dumps({"ok": True, "data": {"message_id": "om_x"}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 0 and obj["sent"] is True and obj["message_id"] == "om_x"

    def test_send_exc_start_failure(self, cfg, conn):
        """res.exc(Popen 启动失败)→ 确定 sent:false exit 4。"""
        self._bound(conn)
        res = FakeRunResult(rc=-1, exc=FileNotFoundError("lark-cli not found"))
        obj, code = _call(runner=_res_runner(res))
        assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed"

    @pytest.mark.parametrize("etype", ["validation", "config", "authentication", "authorization"])
    def test_send_error_type_deterministic_failed(self, cfg, conn, etype):
        """本地 pre-API 失败(无 code)→ sent:false exit 4。"""
        self._bound(conn)
        res = FakeRunResult(rc=1, stderr=json.dumps({"ok": False, "error": {"type": etype, "message": "x"}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed"

    def test_send_error_type_network_unknown(self, cfg, conn):
        self._bound(conn)
        res = FakeRunResult(rc=1, stderr=json.dumps({"ok": False, "error": {"type": "network"}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 5 and obj["sent"] == "unknown" and obj["reason"] == "send-unknown"

    def test_send_keychain_api_unknown(self, cfg, conn):
        """codex r7 M1:keychain 失败=api/unknown,故意不脆弱匹配 message → unknown。"""
        self._bound(conn)
        res = FakeRunResult(rc=1, stderr=json.dumps(
            {"ok": False, "error": {"type": "api", "subtype": "unknown",
                                    "message": "keychain not initialized"}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 5 and obj["sent"] == "unknown"

    def test_send_permanent_code(self, cfg, conn):
        self._bound(conn)
        obj, code = _call(runner=_res_runner(err_envelope(230002)))
        assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed" and obj["code"] == 230002

    def test_send_permanent_code_via_stderr(self, cfg, conn):
        """错误信封打在 stderr(code 嵌套 .error.code)也归类。"""
        self._bound(conn)
        obj, code = _call(runner=_res_runner(stderr_err_envelope(99992402)))
        assert code == 4 and obj["sent"] is False and obj["code"] == 99992402

    def test_send_retryable_via_official_field(self, cfg, conn):
        """retryable 纯信 lark-cli 官方 error.retryable:true(如限频码)→ sent:false + retryable:true exit4
        (codex impl r4:不再靠本地 TRANSIENT 集)。"""
        self._bound(conn)
        res = FakeRunResult(rc=1, stderr=json.dumps(
            {"ok": False, "error": {"type": "api", "code": 230020, "retryable": True}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 4 and obj["sent"] is False and obj["retryable"] is True
        assert obj["reason"] == "send-rejected" and obj["code"] == 230020

    def test_send_any_numeric_code_sent_false(self, cfg, conn):
        """codex impl r2 MAJOR(框架重构):任何 numeric 错误码 = API 拒绝 = 消息未创建 = sent:false
        (非 unknown)。918273(未入表任意码)与 230025(正文超长,曾漏成 unknown)都应 sent:false exit4。"""
        self._bound(conn)
        for c in (918273, 230025):
            obj, code = _call(runner=_res_runner(err_envelope(c)))
            assert code == 4 and obj["sent"] is False and obj["code"] == c

    def test_send_code_99991661_not_retryable(self, cfg, conn):
        """codex impl r4 MINOR:99991661=authentication/token_missing、官方 retryable:false(省略)→
        sent:false 但**不 retryable**(别靠本地 TRANSIENT 集误标造无依据重试)。type=authentication 有 code
        → 走 code 分支 sent:false(顺序仍:type=='network' 才先于 code,其它 type 不抢 code)。"""
        self._bound(conn)
        res = FakeRunResult(rc=1, stderr=json.dumps(
            {"ok": False, "error": {"type": "authentication", "code": 99991661}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 4 and obj["sent"] is False and obj["code"] == 99991661
        assert obj.get("retryable") is not True  # 无官方 error.retryable → 不标 retryable

    def test_send_ok_no_message_id_unknown(self, cfg, conn):
        self._bound(conn)
        obj, code = _call(runner=_res_runner(ok_envelope({})))
        assert code == 5 and obj["sent"] == "unknown"

    def test_send_timeout_no_message_id_unknown(self, cfg, conn):
        self._bound(conn)
        obj, code = _call(runner=_res_runner(FakeRunResult(rc=-1, stdout="", timed_out=True)))
        assert code == 5 and obj["sent"] == "unknown"

    def test_send_unparseable_envelope_unknown(self, cfg, conn):
        self._bound(conn)
        obj, code = _call(runner=_res_runner(FakeRunResult(rc=0, stdout="garbage not json")))
        assert code == 5 and obj["sent"] == "unknown"

    def test_send_validation_type_no_code_after_valid_chat(self, cfg, conn):
        """合法 chat_id 但 lark-cli 返回 error.type=validation 无 code → sent:false exit 4(非 unknown)。"""
        self._bound(conn)
        res = FakeRunResult(rc=1, stderr=json.dumps({"ok": False, "error": {"type": "validation"}}))
        obj, code = _call(runner=_res_runner(res))
        assert code == 4 and obj["sent"] is False


# --------------------------------------------------------------------------- 线性化(may_have_sent)
class TestLinearization:
    def test_post_send_exception_unknown(self, cfg, conn):
        """fake runner 记录调用后抛异常(模拟 Popen 成功后才炸)→ may_have_sent → sent:unknown exit 5。"""
        _setup_bound(conn)

        def boom(a, c):
            raise RuntimeError("post-send explosion")

        runner = FakeRunner(profile=PROFILE)
        runner.on_prefix(["im", "+messages-send"], boom)
        obj, code = _call(runner=runner)
        assert code == 5 and obj["sent"] == "unknown" and obj["reason"] == "internal-error-after-send"
        assert len(runner.calls) == 1  # 证明 send 确已尝试


# ---------------------------------------------------------------- codex impl 复评修复
def test_send_type_authorization_with_unknown_code_sent_false(cfg, conn):
    """codex impl MAJOR1:{type:authorization, code:99991679(未知码)}→ 确定 type 应 sent:false exit4,
    不能因 code 非 None(未匹配 TRANSIENT/PERMANENT)就漏判成 unknown。"""
    _setup_bound(conn)
    res = FakeRunResult(rc=1, stderr=json.dumps(
        {"ok": False, "error": {"type": "authorization", "code": 99991679}}))
    obj, code = _call(runner=_res_runner(res))
    assert code == 4 and obj["sent"] is False and obj["reason"] == "send-failed"


def test_malformed_tag_body_keeps_structural_mention(cfg, conn):
    """codex impl MAJOR2:正文含畸形标签 `<b>`(非 <at)→ 不拒;owner mention 是独立 at 节点
    (结构化,`--text` 时会被 `<b>` 整段按原文渲染使 mention 失效,post at 节点不会)。"""
    _setup_bound(conn)
    runner = _ok_send_runner()
    obj, code = _call(stdin="需要确认 <b> 这个改动", runner=runner)
    assert code == 0 and obj["sent"] is True
    nodes = _post_nodes(runner.calls[0][0])
    assert {"tag": "at", "user_id": OWNER} in nodes  # mention 稳固,不因 <b> 失效
    assert any(n.get("tag") == "md" and "<b>" in n.get("text", "") for n in nodes)


def test_production_passes_config_profile_to_runner(cfg, conn):
    """codex impl MAJOR3:生产用 cfg 的 profile 调 make_runner —— 否则 make_runner('wrong') 假绿放行。"""
    _setup_bound(conn)
    captured = []
    obj, code = _call(runner=_ok_send_runner(), capture_profile=captured)
    assert code == 0 and obj["sent"] is True
    assert captured == [PROFILE]  # 生产传的是 cfg 的 profile,不是别的


def test_main_utf8_robust_no_traceback(monkeypatch):
    """codex impl MINOR:main() 非法 UTF-8 stdin replace 解码 + out() 直写 UTF-8 字节 → 不裸 traceback。"""
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
    """codex impl r2 MINOR:stdin **读取失败**(closed/OSError)≠ 空消息 → stdin-error exit3,
    不伪装成 empty-message exit0。"""
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


def test_post_content_exact_wire_shape(cfg, conn):
    """**精确 wire 形态**(codex plan r1 MEDIUM):at 与 md 必须在**两个段落**里,正文逐字、无前导空格。

    为什么要精确等值而不是"存在某 md 节点":宽松断言对**回退到同段落** `[[at, md]]`(变体 A)是
    绿的 —— 而飞书文档明确 md 独占整段;同段落只是服务端宽松拆分才能work,且会在正文前留一个多余
    空格。本断言同时钉住:tag 回退(md→text)、段落合并、前导空格复活、顺序颠倒、多出节点。"""
    _setup_bound(conn)
    runner = _ok_send_runner()
    payload = "## 标题\n**粗体**"
    _call(stdin=payload, runner=runner)
    assert _post_paragraphs(runner.calls[0][0]) == [
        [{"tag": "at", "user_id": OWNER}],
        [{"tag": "md", "text": payload}],
    ]


def test_post_content_exactly_one_owner_at_node(cfg, conn):
    """codex impl r2 MINOR:整个 post content(全部段落全部节点)只有 **一个** at 节点、且必须是 owner
    —— 防实现另加第二段 `{"tag":"at","user_id":"all"}` 而测试只看 content[0] 漏过(真会 @全员)。"""
    _setup_bound(conn)
    runner = _ok_send_runner()
    _call(stdin="需要授权", runner=runner)
    content = json.loads(_arg(runner.calls[0][0], "--content"))["zh_cn"]["content"]
    at_nodes = [n for para in content for n in para if n.get("tag") == "at"]
    assert len(at_nodes) == 1 and at_nodes[0]["user_id"] == OWNER


def test_send_network_5xx_unknown_not_sent_false(cfg, conn):
    """codex impl r3 MAJOR:{type:network, code:500}(HTTP 5xx,POST 已发出、可能已落库)→ unknown,
    不能因"有 code"就归 sent:false(否则用户重试造成重复@)。network 判定须先于数字码。"""
    _setup_bound(conn)
    res = FakeRunResult(rc=1, stderr=json.dumps(
        {"ok": False, "error": {"type": "network", "subtype": "server_error",
                                "code": 500, "retryable": True}}))
    obj, code = _call(runner=_res_runner(res))
    assert code == 5 and obj["sent"] == "unknown"


def test_send_honors_official_retryable_field(cfg, conn):
    """codex impl r3 MINOR:lark-cli 官方 error.retryable=true(如通用限频 99991400,不在本地 TRANSIENT 集)
    → sent:false + retryable:true(别漏可恢复重试)。"""
    _setup_bound(conn)
    res = FakeRunResult(rc=1, stderr=json.dumps(
        {"ok": False, "error": {"type": "api", "code": 99991400, "retryable": True}}))
    obj, code = _call(runner=_res_runner(res))
    assert code == 4 and obj["sent"] is False and obj["retryable"] is True and obj["code"] == 99991400
