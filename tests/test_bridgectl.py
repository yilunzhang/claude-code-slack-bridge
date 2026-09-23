"""bridgectl 逻辑层(lib/ctl.py)+ CLI(bin/bridgectl.py),Slack 版:
bootstrap 身份配方(token 只走 env/stdin、0600、存在即拒、bots.info/--app-id、--owner-email、不发消息)/
chats 分页契约(完整或 None)/ open-dm 钉 owner_dm_id / bind 拒 foreign_dm / status 形状 / doctor /
hooks 心跳(只读)/ reconcile code-identity / ensure 等认领。全程离线(FakeSlackClient)。"""
import importlib.util
import io
import json
import os
import pathlib
import sys

import pytest

from tests.conftest import APP_ID, BOT_ID, BOT_USER, CC_PID, CC_START, CHAT, DM, OWNER, TEAM
from tests.helpers import FakeSlackClient, err, not_sent, ok, posted
from lib import config as configmod
from lib import constants, ctl, db as dbmod, lifecycle, paths

ROOT = pathlib.Path(__file__).resolve().parents[1]
ZSH_PID = 8100
TOKENS = {"bot_token": "xoxb-boot-token-secret", "app_token": "xapp-boot-token-secret"}
AUTH_OK = {"url": "https://t.slack.com/", "team": "Test Team", "user": "slack-bridge",
           "team_id": TEAM, "user_id": BOT_USER, "bot_id": BOT_ID}


@pytest.fixture
def ctl_prober(prober):
    prober.set(ZSH_PID, CC_PID, "Tue Jul 15 12:00:00 2026", "zsh")
    return prober


def bootstrap_client(auth=None, bots_info=None, lookup=None):
    """bootstrap 用 fake:auth.test / bots.info / users.lookupByEmail;**不注册 chat.postMessage**
    —— 任何发消息都会 AssertionError(证明 bootstrap 不发消息)。"""
    c = FakeSlackClient()
    c.on("auth.test", auth or (lambda m, p: ok(AUTH_OK)))
    c.on("bots.info", bots_info or (lambda m, p: ok({"bot": {"id": p["bot"], "app_id": APP_ID, "name": "slack-bridge"}})))
    c.on("users.lookupByEmail", lookup or (lambda m, p: ok({"user": {"id": OWNER, "name": "owner"}})))
    return c


def _load_bridgectl():
    spec = importlib.util.spec_from_file_location("bridgectl_mod", ROOT / "bin" / "bridgectl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ============================================================================ bootstrap
class TestBootstrap:
    def test_pins_identity_writes_config_and_tokens_0600_no_messages(self, data_dir):
        c = bootstrap_client()
        cfg = ctl.bootstrap(lambda t: c, owner=OWNER, tokens=dict(TOKENS))
        assert cfg["team_id"] == TEAM and cfg["bot_user_id"] == BOT_USER and cfg["bot_id"] == BOT_ID
        assert cfg["app_id"] == APP_ID and cfg["owner_user_id"] == OWNER
        assert cfg["bot_name"] == "slack-bridge" and cfg["team_name"] == "Test Team"
        assert "bot_token" not in json.dumps(cfg) and "xoxb" not in json.dumps(cfg)
        on_disk = configmod.load_config()
        assert configmod.missing_keys(on_disk) == [] and on_disk["owner_user_id"] == OWNER
        assert "xoxb" not in paths.config_path().read_text()
        assert oct(os.stat(paths.config_path()).st_mode & 0o777) == "0o600"
        assert oct(os.stat(paths.tokens_path()).st_mode & 0o777) == "0o600"
        t, v = configmod.load_tokens(allow_env=False)
        assert t == TOKENS and v
        # 调用清单:只有 auth.test + bots.info;零 chat.postMessage
        assert [m for m, _ in c.calls] == ["auth.test", "bots.info"]
        assert c.calls_for("bots.info") == [{"bot": BOT_ID}]

    def test_refuses_overwrite_when_config_exists(self, cfg):
        c = bootstrap_client()
        with pytest.raises(configmod.ConfigError, match="已存在"):
            ctl.bootstrap(lambda t: c, owner=OWNER, tokens=dict(TOKENS))
        assert c.calls == []                       # 存在即拒,连 auth.test 都不打

    def test_refuses_when_tokens_exist_without_config(self, tokens):
        c = bootstrap_client()
        with pytest.raises(configmod.ConfigError, match="tokens.json 已存在"):
            ctl.bootstrap(lambda t: c, owner=OWNER, tokens=dict(TOKENS))
        assert configmod.load_config() is None

    def test_owner_email_path(self, data_dir):
        c = bootstrap_client()
        cfg = ctl.bootstrap(lambda t: c, owner_email="owner@example.com", tokens=dict(TOKENS))
        assert cfg["owner_user_id"] == OWNER
        assert c.calls_for("users.lookupByEmail") == [{"email": "owner@example.com"}]

    def test_owner_email_lookup_failure_refuses(self, data_dir):
        c = bootstrap_client(lookup=lambda m, p: err("users_not_found"))
        with pytest.raises(configmod.ConfigError, match="lookupByEmail"):
            ctl.bootstrap(lambda t: c, owner_email="nobody@example.com", tokens=dict(TOKENS))
        assert configmod.load_config() is None and not paths.tokens_path().exists()

    @pytest.mark.parametrize("kw", [dict(), dict(owner=OWNER, owner_email="a@b"), dict(owner="ou_bad")])
    def test_owner_args_validation(self, data_dir, kw):
        with pytest.raises(configmod.ConfigError):
            ctl.bootstrap(lambda t: bootstrap_client(), tokens=dict(TOKENS), **kw)

    def test_missing_app_id_refuses_unless_flag(self, data_dir):
        c = bootstrap_client(bots_info=lambda m, p: err("bot_not_found"))
        with pytest.raises(configmod.ConfigError, match="--app-id"):
            ctl.bootstrap(lambda t: c, owner=OWNER, tokens=dict(TOKENS))
        assert configmod.load_config() is None and not paths.tokens_path().exists()
        c2 = bootstrap_client(bots_info=lambda m, p: ok({"bot": {"id": BOT_ID}}))   # 无 app_id 字段
        cfg = ctl.bootstrap(lambda t: c2, owner=OWNER, tokens=dict(TOKENS), app_id="A0EXPLICIT")
        assert cfg["app_id"] == "A0EXPLICIT" and configmod.load_config()["app_id"] == "A0EXPLICIT"

    def test_explicit_app_id_conflicting_with_bots_info_refuses(self, data_dir):
        c = bootstrap_client()
        with pytest.raises(configmod.ConfigError, match="不一致"):
            ctl.bootstrap(lambda t: c, owner=OWNER, tokens=dict(TOKENS), app_id="A0OTHER")

    def test_auth_failure_or_missing_fields_refuses(self, data_dir):
        with pytest.raises(configmod.ConfigError, match="auth.test"):
            ctl.bootstrap(lambda t: bootstrap_client(auth=lambda m, p: err("invalid_auth")),
                          owner=OWNER, tokens=dict(TOKENS))
        with pytest.raises(configmod.ConfigError, match="auth.test"):
            ctl.bootstrap(lambda t: bootstrap_client(auth=lambda m, p: not_sent("dns")),
                          owner=OWNER, tokens=dict(TOKENS))
        no_bot = dict(AUTH_OK)
        no_bot.pop("bot_id")
        with pytest.raises(configmod.ConfigError, match="bot_id"):
            ctl.bootstrap(lambda t: bootstrap_client(auth=lambda m, p: ok(no_bot)),
                          owner=OWNER, tokens=dict(TOKENS))
        assert configmod.load_config() is None and not paths.tokens_path().exists()

    @pytest.mark.parametrize("tokens", [
        None, {}, {"bot_token": ""}, {"bot_token": "xoxp-user-token"},
        {"bot_token": "xoxb-x", "app_token": "notxapp"}, "xoxb-string",
    ])
    def test_token_shape_validation(self, data_dir, tokens):
        with pytest.raises(configmod.ConfigError):
            ctl.bootstrap(lambda t: bootstrap_client(), owner=OWNER, tokens=tokens)

    def test_app_token_optional_but_recorded(self, data_dir):
        cfg = ctl.bootstrap(lambda t: bootstrap_client(), owner=OWNER,
                            tokens={"bot_token": "xoxb-only"})
        assert cfg["owner_user_id"] == OWNER
        assert configmod.load_tokens(allow_env=False)[0] == {"bot_token": "xoxb-only", "app_token": None}

    def test_stores_allowlist(self, data_dir):
        cfg = ctl.bootstrap(lambda t: bootstrap_client(), owner=OWNER, tokens=dict(TOKENS),
                            chat_allowlist=["C0A", "C0B"])
        assert cfg["chat_allowlist"] == ["C0A", "C0B"]
        assert configmod.load_config()["chat_allowlist"] == ["C0A", "C0B"]

    def test_owner_cannot_be_bot(self, data_dir):
        with pytest.raises(configmod.ConfigError, match="bot 自己"):
            ctl.bootstrap(lambda t: bootstrap_client(), owner=BOT_USER, tokens=dict(TOKENS))


class TestBootstrapCli:
    """token 只经 env / --tokens-stdin;argv 没有任何 token 选项;输出绝不含 token。"""

    def _run(self, monkeypatch, capsys, argv, client, stdin_bytes=None):
        bridgectl = _load_bridgectl()
        monkeypatch.setattr(bridgectl, "SlackClient", lambda **kw: client)
        if stdin_bytes is not None:
            monkeypatch.setattr(bridgectl.sys, "stdin", io.StringIO(stdin_bytes))
        monkeypatch.setattr(sys, "argv", ["bridgectl", *argv])
        with pytest.raises(SystemExit) as ei:
            bridgectl.main()
        out = capsys.readouterr().out
        return (json.loads(out) if out.strip() else None), ei.value.code, out

    def test_env_tokens_success_output_has_no_token(self, data_dir, monkeypatch, capsys):
        monkeypatch.setenv("SLACK_BOT_TOKEN", TOKENS["bot_token"])
        monkeypatch.setenv("SLACK_APP_TOKEN", TOKENS["app_token"])
        res, code, raw = self._run(monkeypatch, capsys, ["bootstrap", "--owner", OWNER], bootstrap_client())
        assert code == 0 and res["ok"] is True
        assert res["config"]["owner_user_id"] == OWNER and res["tokens"]["app_token_present"] is True
        assert "xoxb" not in raw and "xapp" not in raw and "secret" not in raw
        assert configmod.load_tokens(allow_env=False)[0] == TOKENS

    def test_missing_env_token_refuses_with_guidance(self, data_dir, monkeypatch, capsys):
        res, code, raw = self._run(monkeypatch, capsys, ["bootstrap", "--owner", OWNER], bootstrap_client())
        assert code == 2 and res["ok"] is False and "SLACK_BOT_TOKEN" in res["error"]
        assert configmod.load_config() is None

    def test_tokens_stdin_json(self, data_dir, monkeypatch, capsys):
        res, code, raw = self._run(monkeypatch, capsys,
                                   ["bootstrap", "--owner", OWNER, "--tokens-stdin"],
                                   bootstrap_client(), stdin_bytes=json.dumps(TOKENS))
        assert code == 0 and res["ok"] is True and "secret" not in raw
        assert configmod.load_tokens(allow_env=False)[0] == TOKENS

    def test_no_argv_token_options_exist(self, data_dir, monkeypatch, capsys):
        for flag in ("--bot-token", "--app-token", "--token"):
            res, code, raw = self._run(monkeypatch, capsys,
                                       ["bootstrap", "--owner", OWNER, flag, "xoxb-argv"], bootstrap_client())
            assert code == 2 and res is None       # argparse 拒绝未知选项
        assert configmod.load_config() is None

    def test_missing_app_token_warns(self, data_dir, monkeypatch, capsys):
        monkeypatch.setenv("SLACK_BOT_TOKEN", TOKENS["bot_token"])
        res, code, raw = self._run(monkeypatch, capsys, ["bootstrap", "--owner", OWNER], bootstrap_client())
        assert code == 0 and "app_token" in res.get("warning", "")


# ============================================================================ chats
def _page(channels, cursor=""):
    return ok({"channels": channels, "response_metadata": {"next_cursor": cursor}})


def _chan(cid, name="general", member=True, private=False, archived=False):
    return {"id": cid, "name": name, "is_channel": not private, "is_group": private,
            "is_private": private, "is_member": member, "is_archived": archived, "is_im": False}


def _im(cid, user):
    return {"id": cid, "is_im": True, "user": user, "is_member": True}


class TestListChats:
    def test_filters_membership_and_owner_dm(self, cfg):
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: _page([
            _chan("C0A", "a"), _chan("C0B", "b", member=False), _chan("G0P", "p", private=True),
            _chan("C0ARCH", "old", archived=True), _im(DM, OWNER), _im("D0OTHER", "U0MEMBER"),
            {"id": "G0MPIM", "is_mpim": True, "is_member": True}, {"no": "id"}, "junk",
        ]))
        chats = ctl.list_chats(c, cfg)
        assert [x["chat_id"] for x in chats] == ["C0A", "G0P", DM]
        by = {x["chat_id"]: x for x in chats}
        assert by["C0A"]["type"] == "public_channel" and by["G0P"]["type"] == "private_channel"
        assert by[DM]["type"] == "owner_dm" and by[DM]["is_pinned_owner_dm"] is True
        p = c.calls_for("conversations.list")[0]
        assert p["types"] == "public_channel,private_channel,im" and p["exclude_archived"] is True
        assert p["limit"] == 200 and "cursor" not in p

    def test_follows_cursor_including_empty_page_with_cursor(self, cfg):
        pages = {None: _page([_chan("C0A", "a")], "c2"),
                 "c2": _page([], "c3"),                      # 空页但带 cursor:合法,继续
                 "c3": _page([_chan("C0B", "b")], "c4"),
                 "c4": _page([_chan("C0C", "c")], "")}
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: pages[p.get("cursor")])
        chats = ctl.list_chats(c, cfg)
        assert [x["chat_id"] for x in chats] == ["C0A", "C0B", "C0C"]
        assert [p.get("cursor") for p in c.calls_for("conversations.list")] == [None, "c2", "c3", "c4"]

    def test_cursor_loop_detected(self, cfg):
        pages = {None: _page([_chan("C0A", "a")], "c2"),
                 "c2": _page([_chan("C0B", "b")], "c3"),
                 "c3": _page([_chan("C0C", "c")], "c2")}    # 回到 c2 → 循环
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: pages[p.get("cursor")])
        assert ctl.list_chats(c, cfg) is None
        assert len(c.calls_for("conversations.list")) == 3

    def test_page_cap(self, cfg):
        n = [0]

        def resp(m, p):
            n[0] += 1
            return _page([_chan("C%05d" % n[0], "x")], "cur%d" % n[0])
        c = FakeSlackClient()
        c.on("conversations.list", resp)
        assert ctl.list_chats(c, cfg, page_cap=5) is None
        assert n[0] == 5
        assert ctl.LIST_PAGE_CAP == 50

    def test_partial_failure_returns_none_not_prefix(self, cfg):
        pages = {None: _page([_chan("C0A", "a")], "c2"), "c2": err("ratelimited", http_status=429)}
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: pages[p.get("cursor")])
        assert ctl.list_chats(c, cfg) is None

    @pytest.mark.parametrize("bad", [ok({"channels": "nope"}), ok({}), ok({"channels": [], "response_metadata": {"next_cursor": 5}})])
    def test_malformed_page_returns_none(self, cfg, bad):
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: bad)
        assert ctl.list_chats(c, cfg) is None

    def test_missing_response_metadata_means_last_page(self, cfg):
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: ok({"channels": [_chan("C0A")]}))
        assert [x["chat_id"] for x in ctl.list_chats(c, cfg)] == ["C0A"]

    def test_duplicate_ids_across_pages_kept_once(self, cfg):
        pages = {None: _page([_chan("C0A", "a")], "c2"), "c2": _page([_chan("C0A", "a"), _chan("C0B", "b")], "")}
        c = FakeSlackClient()
        c.on("conversations.list", lambda m, p: pages[p.get("cursor")])
        assert [x["chat_id"] for x in ctl.list_chats(c, cfg)] == ["C0A", "C0B"]


# ============================================================================ preflight: slack_sdk 探针(R1-m3)
def _fake_sdk_dir(tmp_path, init_src, version_src=None):
    d = tmp_path / "fakesdk"
    (d / "slack_sdk").mkdir(parents=True)
    (d / "slack_sdk" / "__init__.py").write_text(init_src, encoding="utf-8")
    if version_src is not None:
        (d / "slack_sdk" / "version.py").write_text(version_src, encoding="utf-8")
    return d


def _metadata_version_or_none():
    try:
        import importlib.metadata as m
        return m.version("slack_sdk")
    except Exception:  # noqa: BLE001
        return None


class TestPreflightSdkProbe:
    def test_probe_code_never_imports_sdk_at_module_level(self):
        import re
        src = (ROOT / "bin" / "bridgectl.py").read_text(encoding="utf-8")
        assert not re.search(r"^\s*(from|import)\s+slack_sdk\b", src, re.M)

    def test_installed_sdk_without_toplevel___version___is_importable(self, tmp_path):
        """slack_sdk 3.44.x 的形状:顶层没有 __version__,版本在 slack_sdk.version。
        旧探针 `print(slack_sdk.__version__)` → AttributeError → 误报 importable=false。"""
        mod = _load_bridgectl()
        d = _fake_sdk_dir(tmp_path, "# no __version__ here\n", "__version__ = '3.44.1-fake'\n")
        env = dict(os.environ, PYTHONPATH=str(d))
        res = mod._slack_sdk_status(sys.executable, env=env)
        assert res["importable"] is True and res["python"] == sys.executable
        meta = _metadata_version_or_none()      # 同一解释器:有 dist 元数据则优先它,否则回落到 slack_sdk.version
        if meta:
            assert (res["version"], res["source"]) == (meta, "metadata")
        else:
            assert (res["version"], res["source"]) == ("3.44.1-fake", "slack_sdk.version")

    def test_fallback_order_metadata_then_version_module_then_attr(self, tmp_path):
        mod = _load_bridgectl()
        if _metadata_version_or_none():
            pytest.skip("此解释器装了真 slack_sdk dist,元数据总是优先")
        d = _fake_sdk_dir(tmp_path, "__version__ = 'attr-only'\n")      # 只有顶层属性
        res = mod._slack_sdk_status(sys.executable, env=dict(os.environ, PYTHONPATH=str(d)))
        assert (res["importable"], res["version"], res["source"]) == (True, "attr-only", "slack_sdk.__version__")
        d2 = _fake_sdk_dir(tmp_path / "b", "__version__ = 'attr'\n", "__version__ = 'vermod'\n")
        res = mod._slack_sdk_status(sys.executable, env=dict(os.environ, PYTHONPATH=str(d2)))
        assert (res["version"], res["source"]) == ("vermod", "slack_sdk.version")   # version 模块先于顶层属性

    def test_import_failure_reported_as_not_importable(self, tmp_path):
        mod = _load_bridgectl()
        d = _fake_sdk_dir(tmp_path, "raise ImportError('broken install')\n")
        res = mod._slack_sdk_status(sys.executable, env=dict(os.environ, PYTHONPATH=str(d)))
        assert res["importable"] is False and res["version"] is None

    def test_unrunnable_interpreter_is_none_not_false(self):
        mod = _load_bridgectl()
        res = mod._slack_sdk_status("/nonexistent/python3")
        assert res["importable"] is None

    def test_real_sdk_in_venv_test_if_present(self):
        py = ROOT / ".venv-test" / "bin" / "python"
        if not py.exists():
            pytest.skip(".venv-test 未建(docs/dev.md)")
        mod = _load_bridgectl()
        res = mod._slack_sdk_status(str(py))
        assert res["importable"] is True and res["version"] and res["version"].startswith("3.")
        assert res["source"] in ("metadata", "slack_sdk.version")


# ============================================================================ open-dm
class TestOpenDm:
    def test_pins_owner_dm_id_on_disk(self, cfg):
        c = FakeSlackClient()
        c.on("conversations.open", lambda m, p: ok({"channel": {"id": "D0NEWDM"}}))
        cfg.pop("owner_dm_id", None)
        res = ctl.open_owner_dm(c, cfg)
        assert res == {"ok": True, "owner_dm_id": "D0NEWDM", "chat_id": "D0NEWDM"}
        assert c.calls_for("conversations.open") == [{"users": OWNER}]
        assert cfg["owner_dm_id"] == "D0NEWDM"
        assert configmod.load_config()["owner_dm_id"] == "D0NEWDM"           # set_persist 落盘
        assert configmod.ConfigSnapshot.load()["owner_user_id"] == OWNER        # 其它键不动

    def test_failure_or_non_dm_id(self, cfg):
        c = FakeSlackClient()
        c.on("conversations.open", lambda m, p: err("user_not_found"))
        assert ctl.open_owner_dm(c, cfg)["ok"] is False
        c2 = FakeSlackClient()
        c2.on("conversations.open", lambda m, p: ok({"channel": {"id": "C0NOTDM"}}))
        assert ctl.open_owner_dm(c2, cfg)["ok"] is False
        assert configmod.load_config()["owner_dm_id"] == DM                    # 未被改坏


# ============================================================================ bind / unbind
class TestBindUnbind:
    def test_bind_prepare_creates_rows_with_slack_marker(self, env, ctl_prober):
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                               chat_id=CHAT, chat_name="测试频道", cwd="/tmp/p", start_pid=ZSH_PID)
        assert res["marker"].startswith(constants.MARKER_PREFIX) and "[slack-bridge-bind:" in res["marker"]
        assert res["binding_id"] in res["listener_cmd"] and res["is_owner_dm"] is False
        assert res["banner"] and "unbind" in res["banner"]
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (res["binding_id"],)).fetchone()
        assert b["status"] == "starting" and b["cc_pid"] == CC_PID and b["cc_start"] == CC_START

    def test_bind_owner_dm_allowed(self, env, ctl_prober):
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                               chat_id=DM, chat_name=None, cwd=None, start_pid=ZSH_PID)
        assert res["binding_id"] and res["is_owner_dm"] is True

    def test_bind_foreign_dm_rejected(self, env, ctl_prober):
        with pytest.raises(lifecycle.BindConflict) as ei:
            ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                             chat_id="D0SOMEONE", chat_name=None, cwd=None, start_pid=ZSH_PID)
        assert ei.value.code == "foreign_dm"
        assert env.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0] == 0   # 零残留

    def test_bind_dm_rejected_when_owner_dm_not_pinned(self, env, ctl_prober):
        env.cfg.pop("owner_dm_id")
        with pytest.raises(lifecycle.BindConflict) as ei:
            ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                             chat_id=DM, chat_name=None, cwd=None, start_pid=ZSH_PID)
        assert ei.value.code == "foreign_dm" and "open-dm" in str(ei.value)

    def test_bind_conflict_surfaces(self, env, ctl_prober):
        env.make_binding(status="active", chat_id=CHAT)
        with pytest.raises(lifecycle.BindConflict):
            ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                             chat_id=CHAT, chat_name=None, cwd=None, start_pid=ZSH_PID)

    def test_allowlist_gate(self, env, ctl_prober):
        env.cfg["chat_allowlist"] = ["C0OTHER"]
        with pytest.raises(lifecycle.BindConflict) as ei:
            ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                             chat_id=CHAT, chat_name=None, cwd=None, start_pid=ZSH_PID)
        assert ei.value.code == "chat_not_allowed"
        assert env.conn.execute("SELECT COUNT(*) FROM pending_bind").fetchone()[0] == 0
        env.cfg["chat_allowlist"] = [CHAT]
        assert ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                                chat_id=CHAT, chat_name=None, cwd=None, start_pid=ZSH_PID)["binding_id"]

    def test_unbind_resolves_instance(self, env, ctl_prober):
        bid = env.make_binding(status="active")
        res = ctl.unbind(env.conn, env.clock, ctl_prober, start_pid=ZSH_PID)
        assert res["ok"] and res["binding_id"] == bid
        b = env.conn.execute("SELECT status, close_reason FROM bindings").fetchone()
        assert b[0] == "closed" and b[1] == "user_unbind"

    def test_unbind_without_binding(self, env, ctl_prober):
        assert ctl.unbind(env.conn, env.clock, ctl_prober, start_pid=ZSH_PID)["ok"] is False

    def test_listener_cmd_quotes_spaced_paths(self, env, ctl_prober, monkeypatch):
        import shlex
        spaced = pathlib.Path("/tmp/my plugins/slack-bridge")
        monkeypatch.setattr(paths, "pkg_root", lambda: spaced)
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                               chat_id=CHAT, chat_name="g", cwd="/tmp/p", start_pid=ZSH_PID)
        argv = shlex.split(res["listener_cmd"])
        assert argv[-2] == str(spaced / "bin" / "listener.py") and argv[-1] == res["binding_id"]


# ============================================================================ status
class TestStatus:
    def test_status_report_shape(self, env, tokens):
        env.make_binding(status="active")
        dbmod.set_state(env.conn, constants.GATE_KEY, "ok")
        dbmod.set_state(env.conn, constants.GATE_VERSION_KEY, tokens.version)
        dbmod.set_state(env.conn, constants.TOKENS_VERSION_SEEN_KEY, tokens.version)
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_KEY, "ok")
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_VERSION_KEY, tokens.version)
        dbmod.set_state(env.conn, "consumer_socket_ready", "ready num_connections=1")
        dbmod.set_state(env.conn, "event_dropped_foreign_team", "2")
        dbmod.set_state(env.conn, "cooldown:chat.postMessage", str(env.clock.wall_ms() + 5000))
        dbmod.set_state(env.conn, "cooldown:chat.update", str(env.clock.wall_ms() - 5000))
        env.conn.execute("INSERT INTO slack_events(envelope_type,event_key,chat_id,payload_json,received_at,state,drain_attempts,error) "
                         "VALUES('events_api','ev:q1',?, '{}', 0, 'quarantined', 5, 'boom')", (CHAT,))
        env.conn.execute("INSERT INTO slack_events(envelope_type,event_key,chat_id,payload_json,received_at,state) "
                         "VALUES('events_api','ev:s1',?, '{}', 0, 'staged')", (CHAT,))
        rep = ctl.status_report(env.conn, env.cfg, env.clock)
        assert rep["fingerprint"]["team_id"] == TEAM and rep["fingerprint"]["owner_dm_id"] == DM
        assert "profile" not in rep["fingerprint"] and "cli_version" not in rep["fingerprint"]
        assert rep["schema_version"] == "1"
        assert rep["outbound_gate"] == "ok" and rep["outbound_gate_tokens_version"] == tokens.version
        assert rep["tokens_version_seen"] == tokens.version
        assert rep["tokens_file"]["version"] == tokens.version and rep["credentials_verified"] is True
        assert rep["verify_capability"] == "ok" and rep["auto_resend_enabled"] is True
        assert rep["consumer"]["ready"] == "ready num_connections=1"
        assert rep["slack_events"] == {"quarantined": 1, "staged": 1}
        assert rep["quarantined"][0]["error"] == "boom" and rep["quarantined"][0]["drain_attempts"] == 5
        assert rep["cooldowns"]["chat.postMessage"]["active"] is True
        assert rep["cooldowns"]["chat.update"]["active"] is False
        assert rep["counters"]["event_dropped_foreign_team"] == "2"
        assert len(rep["bindings"]) == 1 and rep["bindings"][0]["status"] == "active"
        assert rep["markdown_mode"] == "markdown_text"
        assert "xoxb" not in json.dumps(rep)
        assert any("隔离" in h for h in rep["hints"])

    def test_status_hints_on_stale_credentials_and_unverified_capability(self, env, tokens):
        dbmod.set_state(env.conn, constants.GATE_KEY, "ok")
        dbmod.set_state(env.conn, constants.GATE_VERSION_KEY, "0:old")
        rep = ctl.status_report(env.conn, env.cfg, env.clock)
        assert rep["credentials_verified"] is False and rep["auto_resend_enabled"] is False
        assert any("credentials-unverified" in h for h in rep["hints"])
        assert any("probe" in h for h in rep["hints"])

    def test_status_gate_hints(self, env, tokens):
        for gate, needle in (("mismatch", "身份不符"), ("degraded:auth_error", "停摆")):
            dbmod.set_state(env.conn, constants.GATE_KEY, gate)
            rep = ctl.status_report(env.conn, env.cfg, env.clock)
            assert rep["outbound_gate"] == gate and any(needle in h for h in rep["hints"])

    def test_status_shows_allowlist(self, env):
        env.cfg["chat_allowlist"] = ["C0A"]
        assert ctl.status_report(env.conn, env.cfg, env.clock)["chat_allowlist"] == ["C0A"]
        env.cfg.pop("chat_allowlist")
        assert "全部" in ctl.status_report(env.conn, env.cfg, env.clock)["chat_allowlist"]

    def test_status_without_tokens_file(self, env):
        rep = ctl.status_report(env.conn, env.cfg, env.clock)
        assert rep["tokens_file"]["present"] is False and rep["credentials_verified"] is False


# ============================================================================ doctor(= probe + daemon 健康)
class TestDoctor:
    def _probe_client(self, **kw):
        from tests.test_capability_probe import make_client
        return make_client(**kw)

    def test_doctor_ok_writes_capability(self, cfg, tokens, conn):
        client = self._probe_client()
        res = ctl.doctor(CHAT, cfg=cfg, client_factory=lambda t, v, s: client, write_config=True, conn=conn)
        assert res["ok"] is True and res["probe"]["identity_ok"] and res["probe"]["complete"]
        assert res["probe"]["tokens_version"] == tokens.version
        assert res["written"][constants.VERIFY_CAPABILITY_KEY] == "ok"
        assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_VERSION_KEY) == tokens.version
        assert res["daemon"]["healthy"] is False and res["daemon"]["verify_capability"] == "ok"
        assert configmod.load_config()["markdown_mode"] == "markdown_text"
        assert "xoxb" not in json.dumps(res)
        assert [m for m, _ in client.calls][0] == "auth.test"

    def test_doctor_identity_mismatch_no_send(self, cfg, tokens, conn):
        client = self._probe_client(auth=dict(BOT_ID="x", team_id=TEAM, user_id=BOT_USER, bot_id="B_EVIL"))
        res = ctl.doctor(CHAT, cfg=cfg, client_factory=lambda t, v, s: client, conn=conn)
        assert res["ok"] is False and "身份" in res["error"]
        assert client.calls_for("chat.postMessage") == []
        assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == constants.VERIFY_CAP_UNVERIFIED

    def test_doctor_uses_env_or_file_tokens_and_cooldown_store(self, cfg, tokens, conn):
        from lib.slackapi import DaemonStateCooldownStore
        seen = {}

        def factory(t, v, s):
            seen.update(t=t, v=v, s=s)
            return self._probe_client()
        ctl.doctor(CHAT, cfg=cfg, client_factory=factory, conn=conn)
        assert seen["t"] == tokens.tokens and seen["v"] == tokens.version
        assert isinstance(seen["s"], DaemonStateCooldownStore)


# ============================================================================ hooks 心跳(只读)
def _write_hb(event, ts, plugin_version=None, pkg_root=None):
    from lib import util, version as versionmod
    paths.ensure_data_dir()
    root, ver = versionmod.install_identity()
    util.atomic_write(paths.hook_heartbeat_path(event), util.jdumps({
        "event": event, "ts": ts,
        "plugin_version": plugin_version if plugin_version is not None else ver,
        "pkg_root": pkg_root if pkg_root is not None else root}))


class TestHooksLiveStatus:
    def test_not_seen_when_no_heartbeat(self, data_dir):
        paths.ensure_data_dir()
        st = ctl.hooks_live_status()
        assert st["advisory"] is True and st["confirmed"] is False

    def test_confirmed_after_real_stop_hook_runs(self, data_dir):
        from lib import hooklib
        paths.ensure_data_dir()
        hooklib._touch_hook_heartbeat("stop")
        st = ctl.hooks_live_status()
        assert st["stop"]["seen"] and st["stop"]["fresh"] and st["stop"]["current"] and st["confirmed"]

    def test_session_end_only_does_not_confirm(self, data_dir):
        from lib import hooklib
        hooklib._touch_hook_heartbeat("session_end")
        st = ctl.hooks_live_status()
        assert st["session_end"]["seen"] is True and st["confirmed"] is False

    def test_stale_or_future_or_other_install_not_confirmed(self, data_dir):
        _write_hb("stop", 1000)
        assert ctl.hooks_live_status(now_ms=1000 + ctl.HOOK_HEARTBEAT_FRESH_MS + 1)["confirmed"] is False
        _write_hb("stop", 10_000)
        assert ctl.hooks_live_status(now_ms=5_000)["confirmed"] is False
        _write_hb("stop", 1_000_000, plugin_version="0.9.0", pkg_root="/other/install")
        st = ctl.hooks_live_status(now_ms=1_000_000)
        assert st["stop"]["fresh"] and st["stop"]["current"] is False and st["confirmed"] is False

    def test_foreign_stop_hooks_detected_and_own_ignored(self, data_dir):
        assert ctl.foreign_stop_hooks() == []
        p = paths.settings_json_path()
        p.write_text(json.dumps({"hooks": {"Stop": [
            {"hooks": [{"type": "command", "command": "python3 /x/other_blocking_hook.py"}]},
            {"hooks": [{"type": "command", "command": "python3 /p/slack-bridge/hooks/stop_hook.py"}]}]}}))
        assert ctl.foreign_stop_hooks() == ["python3 /x/other_blocking_hook.py"]


# ============================================================================ reconcile code-identity(原样移植)
class TestReconcileCodeIdentity:
    def _fakes(self, *, held=True, recorded="rootA|1.0.0", pid=5555, pstart="s",
               alive=True, probe_unknown=False, read_fail=False):
        from tests.helpers import FakeProber
        prober = FakeProber()
        if alive and pid is not None and not probe_unknown:
            prober.set(pid, 1, pstart, "python3")
        prober.raising = probe_unknown
        world = {"held": held, "kills": [], "ensures": 0}

        def lock_held():
            return world["held"]

        def read_state():
            if read_fail:
                return None
            return {"daemon_code_identity": recorded, "daemon_pid": pid, "daemon_proc_start": pstart}

        def kill(p, sig):
            world["kills"].append((p, sig))
            world["held"] = False

        def ensure():
            world["ensures"] += 1
            return "started"

        return world, prober, lock_held, read_state, kill, ensure

    def _run(self, world, prober, lock_held, read_state, kill, ensure, my_identity="rootNEW|1.0.0", wait_s=2):
        return ctl.reconcile_code_identity(
            my_identity=my_identity, read_state=read_state, lock_held=lock_held,
            prober=prober, kill=kill, ensure=ensure, sleep=lambda s: None, wait_s=wait_s)

    def test_match_proceeds_no_restart(self):
        f = self._fakes(recorded="rootA|1.0.0")
        r = self._run(*f, my_identity="rootA|1.0.0")
        assert r.get("error") is None and r["restarted"] is False and r["reason"] == "match"

    def test_no_daemon_respawns_this_version(self):
        f = self._fakes(held=False)
        r = self._run(*f)
        assert r.get("error") is None and r["restarted"] is True and f[0]["ensures"] == 1

    def test_mismatch_killable_restarts_then_proceeds(self):
        import signal
        f = self._fakes(recorded="rootOLD|0.9.0")
        r = self._run(*f)
        assert r.get("error") is None and r["restarted"] is True and r["state"] == "started"
        assert f[0]["kills"] == [(5555, signal.SIGTERM)]

    @pytest.mark.parametrize("kw", [dict(alive=False), dict(pstart=""), dict(probe_unknown=True), dict(read_fail=True)])
    def test_mismatch_unkillable_lock_held_fails_closed(self, kw):
        f = self._fakes(recorded="rootOLD|0.9.0", **kw)
        r = self._run(*f)
        assert "error" in r and r["reason"] == "unverified-cannot-restart"
        assert f[0]["kills"] == [] and f[0]["ensures"] == 0

    def test_mismatch_daemon_wont_exit_surfaces_error(self):
        from tests.helpers import FakeProber
        prober = FakeProber()
        prober.set(5555, 1, "s", "python3")
        ensures = {"n": 0}
        r = ctl.reconcile_code_identity(
            my_identity="rootNEW|1.0.0",
            read_state=lambda: {"daemon_code_identity": "rootOLD|0.9.0", "daemon_pid": 5555, "daemon_proc_start": "s"},
            lock_held=lambda: True, prober=prober, kill=lambda p, s: None,
            ensure=lambda: ensures.__setitem__("n", ensures["n"] + 1), sleep=lambda s: None, wait_s=1)
        assert "error" in r and r["reason"] == "no-exit" and ensures["n"] == 0


# ============================================================================ CLI:bind fail-closed / 等认领 / allow
class _Args:
    def __init__(self, chat_id, chat_name=None):
        self.chat_id = chat_id
        self.chat_name = chat_name


def test_cmd_bind_fails_closed_on_reconcile_error(env, monkeypatch):
    bridgectl = _load_bridgectl()
    monkeypatch.setattr(bridgectl.ctl, "ensure_daemon", lambda *a, **k: "running")
    monkeypatch.setattr(bridgectl.ctl, "reconcile_daemon_code_identity",
                        lambda *a, **k: {"restarted": False, "reason": "unverified-cannot-restart", "error": "旧 daemon"})
    with pytest.raises(SystemExit) as ei:
        bridgectl.cmd_bind(_Args(CHAT, "g"))
    assert ei.value.code == 6
    assert env.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0] == 0


def test_cmd_bind_fails_closed_when_restart_leaves_daemon_not_ready(env, monkeypatch):
    bridgectl = _load_bridgectl()
    monkeypatch.setattr(bridgectl.ctl, "ensure_daemon", lambda *a, **k: "running")
    monkeypatch.setattr(bridgectl.ctl, "reconcile_daemon_code_identity",
                        lambda *a, **k: {"restarted": True, "reason": "restarted", "state": "in_progress"})
    with pytest.raises(SystemExit) as ei:
        bridgectl.cmd_bind(_Args(CHAT, "g"))
    assert ei.value.code == 5
    assert env.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0] == 0


class TestWaitListenerClaim:
    def test_already_claimed_returns_without_sleep(self, env):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s1", listener_epoch=1)
        sleeps = []
        assert ctl.wait_listener_claim(env.conn, bid, env.clock, sleeps.append, 6000) is True and sleeps == []

    def test_claimed_after_second_poll(self, env):
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        calls = []

        def sleep(s):
            calls.append(s)
            env.clock.tick(int(s * 1000))
            if len(calls) == 2:
                env.conn.execute("UPDATE bindings SET listener_pid=7001, listener_start='ls', listener_epoch=1, "
                                 "listener_beat_at=? WHERE binding_id=?", (env.clock.wall_ms(), bid))
        assert ctl.wait_listener_claim(env.conn, bid, env.clock, sleep, 6000) is True and calls == [0.5, 0.5]

    def test_never_claimed_times_out(self, env):
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        start = env.clock.mono_ms()
        assert ctl.wait_listener_claim(env.conn, bid, env.clock, lambda s: env.clock.tick(int(s * 1000)), 6000) is False
        assert env.clock.mono_ms() - start == 6000


def _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys, chat_id=CHAT, claim_at_s=None):
    bridgectl = _load_bridgectl()
    monkeypatch.setattr(bridgectl.ctl, "ensure_daemon", lambda *a, **k: "running")
    monkeypatch.setattr(bridgectl.ctl, "reconcile_daemon_code_identity", lambda *a, **k: {"restarted": False, "reason": "match"})
    monkeypatch.setattr(bridgectl.procs, "SystemProber", lambda: ctl_prober)
    monkeypatch.setattr(bridgectl.os, "getppid", lambda: ZSH_PID)
    monkeypatch.setattr(bridgectl, "SystemClock", lambda: env.clock)
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        env.clock.tick(int(s * 1000))
        if claim_at_s is not None and abs(sum(sleeps) - claim_at_s) < 1e-9:
            env.conn.execute("UPDATE bindings SET listener_pid=7001, listener_start='ls', listener_epoch=1, "
                             "listener_beat_at=? WHERE status='starting'", (env.clock.wall_ms(),))
    monkeypatch.setattr(bridgectl.time, "sleep", fake_sleep)
    monkeypatch.setattr(sys, "argv", ["bridgectl", "bind", "--chat-id", chat_id, "--chat-name", "g"])
    with pytest.raises(SystemExit) as ei:
        bridgectl.main()
    return json.loads(capsys.readouterr().out), sleeps, ei.value.code


def test_cmd_bind_reports_listener_claimed_false_when_nobody_claims(env, ctl_prober, monkeypatch, capsys):
    res, sleeps, code = _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys)
    assert code == 0 and res["ok"] is True and res["listener_claimed"] is False and sum(sleeps) == 6.0
    assert res["marker"].startswith("[slack-bridge-bind:") and "listener_cmd" in res and "banner" in res


def test_cmd_bind_reports_listener_claimed_true_when_claimed_at_4s(env, ctl_prober, monkeypatch, capsys):
    res, sleeps, code = _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys, claim_at_s=4.0)
    assert code == 0 and res["listener_claimed"] is True and sum(sleeps) == 4.0


def test_cmd_bind_foreign_dm_exit_4(env, ctl_prober, monkeypatch, capsys):
    res, sleeps, code = _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys, chat_id="D0FOREIGN")
    assert code == 4 and res["ok"] is False and res["code"] == "foreign_dm"


class TestAllowCli:
    def _run(self, monkeypatch, capsys, *argv):
        bridgectl = _load_bridgectl()
        monkeypatch.setattr(sys, "argv", ["bridgectl", *argv])
        with pytest.raises(SystemExit) as ei:
            bridgectl.main()
        return json.loads(capsys.readouterr().out), ei.value.code

    def test_add_list_remove_with_user_id(self, env, monkeypatch, capsys):
        from lib import senderallow
        res, code = self._run(monkeypatch, capsys, "allow", "add", "--chat-id", CHAT, "--user-id", "U0MEMBER", "--note", "张三")
        assert code == 0 and res["added"] is True and senderallow.is_allowed(CHAT, "U0MEMBER")
        res, _ = self._run(monkeypatch, capsys, "allow", "list")
        assert len(res["entries"]) == 1 and res["entries"][0]["chat_id"] == CHAT
        res, _ = self._run(monkeypatch, capsys, "allow", "remove", "--chat-id", CHAT, "--user-id", "U0MEMBER")
        assert res["removed"] is True and not senderallow.is_allowed(CHAT, "U0MEMBER")
        res, code = self._run(monkeypatch, capsys, "allow", "add", "--chat-id", CHAT)
        assert code != 0 and res["ok"] is False and senderallow.load_entries() == []


def test_bump_drop_counter_uses_short_obs_timeout(data_dir, monkeypatch):
    from lib import hooklib
    paths.ensure_data_dir()
    captured = {}
    real_connect = dbmod.connect

    def spy_connect(dbfile, busy_timeout_ms=None, **kw):
        captured["busy"] = busy_timeout_ms
        return real_connect(dbfile, busy_timeout_ms=busy_timeout_ms, **kw)

    monkeypatch.setattr(dbmod, "connect", spy_connect)
    c = real_connect(paths.db_path())
    dbmod.init_schema(c, paths.schema_path())
    c.close()
    hooklib._bump_drop_counter()
    assert captured["busy"] == constants.BUSY_TIMEOUT_OBS_MS
    assert constants.BUSY_TIMEOUT_OBS_MS < constants.BUSY_TIMEOUT_SESSION_END_MS


def test_ctl_has_no_lark_or_thread_leftovers():
    import inspect
    src = inspect.getsource(ctl)
    for needle in ("lark", "runner_mod", "read_thread", "def thread", "cli_version", "owner_open_id", "profile"):
        assert needle not in src, needle
