"""bridgectl 逻辑层(lib/ctl.py):bootstrap 身份配方 / hooks 检查(只读)/ bind/unbind / doctor。"""
import json
import pathlib

import pytest

from tests.conftest import CC_PID, CC_START, CHAT, PROFILE
from tests.helpers import FakeRunResult, FakeRunner, err_envelope, ok_envelope
from lib import config as configmod
from lib import ctl, lifecycle, paths


ZSH_PID = 8100


@pytest.fixture
def ctl_prober(prober):
    prober.set(ZSH_PID, CC_PID, "Tue Jul 15 12:00:00 2026", "zsh")
    return prober


def auth_status_runner():
    r = FakeRunner(profile=PROFILE)
    r.on_prefix(["auth", "status"], lambda a, c: FakeRunResult(0, json.dumps({
        "appId": "cli_testapp",
        "identities": {"user": {"available": True, "openId": "ou_owner"},
                       "bot": {"available": True}}})))
    r.on_prefix(["api", "GET", "/open-apis/bot/v3/info"], lambda a, c: FakeRunResult(
        0, '{"note":"noise"}\n{"bot":{"open_id":"ou_bot","app_name":"Yilun CLI"}}\n'))
    r.on_prefix(["--version"], lambda a, c: FakeRunResult(0, "1.0.66\n"))
    return r


class TestBootstrap:
    def test_bootstrap_writes_fingerprint(self, data_dir):
        from lib.clock import SystemClock
        cfg = ctl.bootstrap(auth_status_runner(), PROFILE, SystemClock())
        assert cfg["app_id"] == "cli_testapp" and cfg["owner_open_id"] == "ou_owner"
        assert cfg["bot_open_id"] == "ou_bot" and cfg["bot_name"] == "Yilun CLI"
        assert cfg["cli_version"] == "1.0.66"
        assert configmod.load_config()["profile"] == PROFILE

    def test_bootstrap_refuses_overwrite(self, cfg):
        from lib.clock import SystemClock
        with pytest.raises(configmod.ConfigError):
            ctl.bootstrap(auth_status_runner(), "other", SystemClock())


def _write_hb(event, ts, plugin_version=None, pkg_root=None):
    """按新 schema 直接写某 event 的心跳文件(测试用)。"""
    from lib import paths as pathsmod, util, version as versionmod
    pathsmod.ensure_data_dir()
    root, ver = versionmod.install_identity()
    util.atomic_write(pathsmod.hook_heartbeat_path(event), util.jdumps({
        "event": event, "ts": ts,
        "plugin_version": plugin_version if plugin_version is not None else ver,
        "pkg_root": pkg_root if pkg_root is not None else root}))


class TestHooksLiveStatus:
    """plugin 化:hooks 由 plugin 提供,检测靠**分事件**哨兵心跳(不读 settings.json 判手装)。
    advisory only;Stop 心跳 seen∧fresh∧current = confirmed(主信号)。"""

    def test_not_seen_when_no_heartbeat(self, data_dir):
        from lib import paths as pathsmod
        pathsmod.ensure_data_dir()
        st = ctl.hooks_live_status()
        assert st["advisory"] is True and st["confirmed"] is False
        assert st["stop"]["seen"] is False and st["session_end"]["seen"] is False

    def test_confirmed_after_real_stop_hook_runs(self, data_dir):
        from lib import hooklib, paths as pathsmod
        pathsmod.ensure_data_dir()
        hooklib._touch_hook_heartbeat("stop")  # 模拟真实 Stop hook 运行
        st = ctl.hooks_live_status()
        assert st["stop"]["seen"] and st["stop"]["fresh"] and st["stop"]["current"]
        assert st["confirmed"] is True

    def test_session_end_only_does_not_confirm(self, data_dir):
        """MAJOR 2:仅 SessionEnd 心跳不该压掉警告(握手依赖 Stop)。"""
        from lib import hooklib
        hooklib._touch_hook_heartbeat("session_end")
        st = ctl.hooks_live_status()
        assert st["session_end"]["seen"] is True
        assert st["stop"]["seen"] is False and st["confirmed"] is False

    def test_stale_stop_not_fresh(self, data_dir):
        _write_hb("stop", 1000)
        st = ctl.hooks_live_status(now_ms=1000 + ctl.HOOK_HEARTBEAT_FRESH_MS + 1)
        assert st["stop"]["seen"] and st["stop"]["fresh"] is False and st["confirmed"] is False

    def test_future_timestamp_not_fresh(self, data_dir):
        """MAJOR 2:未来时间戳(age<0)不判 fresh。"""
        _write_hb("stop", 10_000)
        st = ctl.hooks_live_status(now_ms=5_000)  # now 在心跳之前 → age=-5000
        assert st["stop"]["seen"] and st["stop"]["fresh"] is False and st["confirmed"] is False

    def test_another_install_or_version_not_current(self, data_dir):
        """MAJOR 2:另一 install/旧版本的心跳不算 current → 不 confirm。"""
        now = 1_000_000
        _write_hb("stop", now, plugin_version="0.9.0", pkg_root="/other/install")
        st = ctl.hooks_live_status(now_ms=now)
        assert st["stop"]["seen"] and st["stop"]["fresh"]  # 新鲜
        assert st["stop"]["current"] is False and st["confirmed"] is False

    def test_foreign_stop_hooks_detected(self, data_dir):
        p = paths.settings_json_path()
        p.write_text(json.dumps({"hooks": {"Stop": [
            {"hooks": [{"type": "command", "command": "python3 /x/other_blocking_hook.py"}]}]}}))
        assert ctl.foreign_stop_hooks() == ["python3 /x/other_blocking_hook.py"]

    def test_foreign_stop_hooks_ignores_own_and_missing(self, data_dir):
        assert ctl.foreign_stop_hooks() == []  # 无 settings.json
        p = paths.settings_json_path()
        p.write_text(json.dumps({"hooks": {"Stop": [
            {"hooks": [{"type": "command",
                        "command": "python3 /p/feishu-bridge/hooks/stop_hook.py"}]}]}}))
        assert ctl.foreign_stop_hooks() == []  # 本 plugin 的不算 foreign


class TestBindUnbind:
    def test_bind_prepare_creates_rows(self, env, ctl_prober):
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                               chat_id=CHAT, chat_name="测试群", cwd="/tmp/p",
                               start_pid=ZSH_PID)
        assert res["marker"].startswith("[feishu-bridge-bind:")
        assert res["binding_id"] in res["listener_cmd"]
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                             (res["binding_id"],)).fetchone()
        assert b["status"] == "starting" and b["cc_pid"] == CC_PID
        assert b["cc_start"] == CC_START

    def test_bind_conflict_surfaces(self, env, ctl_prober):
        env.make_binding(status="active", chat_id=CHAT)
        with pytest.raises(lifecycle.BindConflict):
            ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                             chat_id=CHAT, chat_name=None, cwd=None, start_pid=ZSH_PID)

    def test_unbind_resolves_instance(self, env, ctl_prober):
        bid = env.make_binding(status="active")
        res = ctl.unbind(env.conn, env.clock, ctl_prober, start_pid=ZSH_PID)
        assert res["ok"] and res["binding_id"] == bid
        b = env.conn.execute("SELECT status, close_reason FROM bindings").fetchone()
        assert b[0] == "closed" and b[1] == "user_unbind"

    def test_unbind_without_binding(self, env, ctl_prober):
        res = ctl.unbind(env.conn, env.clock, ctl_prober, start_pid=ZSH_PID)
        assert res["ok"] is False


class TestStatusAndChats:
    def test_status_report_shape(self, env):
        env.make_binding(status="active")
        rep = ctl.status_report(env.conn, env.cfg, env.clock)
        assert rep["fingerprint"]["profile"] == PROFILE
        assert rep["schema_version"] == "1"
        assert len(rep["bindings"]) == 1
        assert rep["bindings"][0]["status"] == "active"

    def test_list_chats(self, env):
        env.runner.on_prefix(["im", "+chat-list"], lambda a, c: ok_envelope(
            {"items": [{"chat_id": "oc_1", "name": "群A"},
                       {"chat_id": "oc_2", "name": "群B"}],
             "has_more": False}))
        chats = ctl.list_chats(env.runner)
        assert [c["chat_id"] for c in chats] == ["oc_1", "oc_2"]

    def test_list_chats_fails_when_has_more_missing(self, env):
        """缺 has_more = 证明不了取全了,不能当"翻完了"成功返回。

        故意**带上** page_token:否则"缺 token"那条守卫会顺手返回 None,
        本用例就会因为别的原因变绿,测不到 has_more 的真值判断。
        第三次调用的 fail 兜底:守卫被整条删掉时不让测试挂死。"""
        state = {"n": 0}

        def respond(a, c):
            state["n"] += 1
            if state["n"] > 2:
                pytest.fail("has_more 缺失时仍在继续翻页(守卫回归)")
            return ok_envelope({"chats": [{"chat_id": f"oc_{state['n']}", "name": "群"}],
                                "page_token": f"tok{state['n'] + 1}"})

        env.runner.on_prefix(["im", "+chat-list"], respond)
        assert ctl.list_chats(env.runner) is None
        assert len(env.runner.calls_matching("im", "+chat-list")) == 1

    def test_list_chats_follows_pagination(self, env):
        """has_more=true 时必须翻页取全:漏页 = 用户的群"不在列表里",
        照 SKILL.md 会去新建一个重复群。"""
        pages = {
            None: {"chats": [{"chat_id": "oc_1", "name": "群A"}],
                   "has_more": True, "page_token": "tok2"},
            "tok2": {"chats": [{"chat_id": "oc_2", "name": "群B"}],
                     "has_more": True, "page_token": "tok3"},
            "tok3": {"chats": [{"chat_id": "oc_3", "name": "群C"}],
                     "has_more": False, "page_token": ""},
        }

        def respond(a, c):
            tok = a[a.index("--page-token") + 1] if "--page-token" in a else None
            return ok_envelope(pages[tok])

        env.runner.on_prefix(["im", "+chat-list"], respond)
        chats = ctl.list_chats(env.runner)
        assert [c["chat_id"] for c in chats] == ["oc_1", "oc_2", "oc_3"]

    def test_list_chats_keeps_paging_when_token_repeats_but_content_advances(self, env):
        """该接口实测会在多页间返回**同一个** page_token 而内容照常推进。
        按"重复 token 即失败"判,会把这条能正确取全的路径判成失败。"""
        pages = [
            {"chats": [{"chat_id": "oc_1", "name": "群A"}], "has_more": True,
             "page_token": "same"},
            {"chats": [{"chat_id": "oc_2", "name": "群B"}], "has_more": True,
             "page_token": "same"},
            {"chats": [{"chat_id": "oc_3", "name": "群C"}], "has_more": False,
             "page_token": ""},
        ]
        state = {"n": 0}

        def respond(a, c):
            state["n"] += 1
            return ok_envelope(pages[min(state["n"], len(pages)) - 1])

        env.runner.on_prefix(["im", "+chat-list"], respond)
        chats = ctl.list_chats(env.runner)
        assert [c["chat_id"] for c in chats] == ["oc_1", "oc_2", "oc_3"]

    def test_list_chats_fails_when_page_returns_nothing(self, env):
        """has_more=true 但这一页空手而归 = 无法证明有推进 → 整体失败,
        绝不能返回已取到的前缀(那又是一个"残缺列表伪装成全集")。
        守卫若被删除,这个 fake 会无限供页 → 第三次调用直接 fail,不让测试挂死。"""
        state = {"n": 0}

        def respond(a, c):
            state["n"] += 1
            if state["n"] > 2:
                pytest.fail("空页且 has_more=true 时仍在继续翻页(守卫回归)")
            if state["n"] == 1:
                return ok_envelope({"chats": [{"chat_id": "oc_1", "name": "群A"}],
                                    "has_more": True, "page_token": "tok2"})
            return ok_envelope({"chats": [], "has_more": True, "page_token": "tok3"})

        env.runner.on_prefix(["im", "+chat-list"], respond)
        assert ctl.list_chats(env.runner) is None
        assert len(env.runner.calls_matching("im", "+chat-list")) == 2

    def test_list_chats_fails_when_page_repeats_same_ids(self, env):
        """非空页 ≠ 有推进:服务端可能一直回同一批 id 且 has_more=true。
        只看"页非空"会在恒定 token 上无限循环 → 判据必须是**有没有新 chat_id**。"""
        state = {"n": 0}

        def respond(a, c):
            state["n"] += 1
            if state["n"] > 3:
                pytest.fail("整页都是已见 id 时仍在继续翻页(守卫回归)")
            cid = "oc_1" if state["n"] == 1 else "oc_2"
            return ok_envelope({"chats": [{"chat_id": cid, "name": "群"}],
                                "has_more": True, "page_token": "same"})

        env.runner.on_prefix(["im", "+chat-list"], respond)
        assert ctl.list_chats(env.runner) is None
        assert len(env.runner.calls_matching("im", "+chat-list")) == 3

    def test_list_chats_fails_when_page_has_only_malformed_items(self, env):
        """整页都是畸形条目(非 dict / 缺 chat_id)也不算推进。"""
        state = {"n": 0}

        def respond(a, c):
            state["n"] += 1
            if state["n"] > 2:
                pytest.fail("整页畸形条目时仍在继续翻页(守卫回归)")
            if state["n"] == 1:
                return ok_envelope({"chats": [{"chat_id": "oc_1", "name": "群A"}],
                                    "has_more": True, "page_token": "tok2"})
            return ok_envelope({"chats": [{}, {"name": "没有 chat_id"}, "不是 dict"],
                                "has_more": True, "page_token": "tok3"})

        env.runner.on_prefix(["im", "+chat-list"], respond)
        assert ctl.list_chats(env.runner) is None

    def test_list_chats_fails_when_more_pages_but_no_token(self, env):
        """has_more=true 但没给 token:同样是取不全,不能当成功。"""
        env.runner.on_prefix(["im", "+chat-list"], lambda a, c: ok_envelope(
            {"chats": [{"chat_id": "oc_1", "name": "群A"}],
             "has_more": True, "page_token": ""}))
        assert ctl.list_chats(env.runner) is None

    def test_list_chats_partial_page_failure_is_not_silent(self, env):
        """第 2 页失败时不能"就把第 1 页当全部返回" —— 那正是本 bug 的形状
        (残缺列表看起来和完整列表一模一样)。"""
        state = {"n": 0}

        def respond(a, c):
            state["n"] += 1
            if state["n"] == 1:
                return ok_envelope({"chats": [{"chat_id": "oc_1", "name": "群A"}],
                                    "has_more": True, "page_token": "tok2"})
            return err_envelope(99991400, "rate limited")

        env.runner.on_prefix(["im", "+chat-list"], respond)
        assert ctl.list_chats(env.runner) is None


class TestDoctor:
    def test_doctor_send_and_recall(self, env):
        env.runner.on_prefix(["im", "+messages-send"],
                             lambda a, c: ok_envelope({"message_id": "om_doc"}))
        env.runner.on_prefix(["api", "DELETE"], lambda a, c: ok_envelope({}))
        res = ctl.doctor(env.runner, CHAT, env.clock)
        assert res["ok"] and res["message_id"] == "om_doc" and res["recalled"]
        del_call = env.runner.calls_matching("api", "DELETE")[0]
        assert del_call[0][2] == "/open-apis/im/v1/messages/om_doc"


class TestChatAllowlistPlumbing:
    def test_bootstrap_stores_allowlist(self, data_dir):
        from lib.clock import SystemClock
        cfg = ctl.bootstrap(auth_status_runner(), PROFILE, SystemClock(),
                            chat_allowlist=["oc_a", "oc_b"])
        assert cfg["chat_allowlist"] == ["oc_a", "oc_b"]
        assert configmod.load_config()["chat_allowlist"] == ["oc_a", "oc_b"]

    def test_status_shows_allowlist(self, env):
        env.cfg["chat_allowlist"] = ["oc_a"]
        rep = ctl.status_report(env.conn, env.cfg, env.clock)
        assert rep["chat_allowlist"] == ["oc_a"]
        env.cfg.pop("chat_allowlist")
        rep = ctl.status_report(env.conn, env.cfg, env.clock)
        assert "全部" in rep["chat_allowlist"]


class TestAllowlistBindGate:
    def test_bind_out_of_list_chat_rejected(self, env, ctl_prober):
        env.cfg["chat_allowlist"] = ["oc_other"]
        with pytest.raises(lifecycle.BindConflict) as ei:
            ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                             chat_id=CHAT, chat_name=None, cwd=None, start_pid=ZSH_PID)
        assert ei.value.code == "chat_not_allowed"
        # 零残留
        assert env.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0] == 0
        assert env.conn.execute("SELECT COUNT(*) FROM pending_bind").fetchone()[0] == 0

    def test_bind_in_list_ok(self, env, ctl_prober):
        env.cfg["chat_allowlist"] = [CHAT]
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                               chat_id=CHAT, chat_name=None, cwd=None, start_pid=ZSH_PID)
        assert res["binding_id"]


class TestListenerCmdShlex:
    def test_listener_cmd_quotes_spaced_paths(self, env, ctl_prober, monkeypatch):
        """minor①:pkg_root/python 含空格 → listener_cmd 用 shlex.join 正确加引号。"""
        import shlex
        from lib import paths as pathsmod
        spaced = pathlib.Path("/tmp/my plugins/feishu-bridge")
        monkeypatch.setattr(pathsmod, "pkg_root", lambda: spaced)
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, ctl_prober,
                               chat_id=CHAT, chat_name="g", cwd="/tmp/p", start_pid=ZSH_PID)
        cmd = res["listener_cmd"]
        # shlex 可安全 round-trip 回 argv;倒数第二个 = listener.py 完整路径,末位 = binding_id
        argv = shlex.split(cmd)
        assert argv[-2] == str(spaced / "bin" / "listener.py")
        assert argv[-1] == res["binding_id"]
        assert "my plugins" in argv[-2]  # 空格保留在单一 argv 里(未被拆开)


class TestReconcileCodeIdentity:
    """MAJOR 3 换层:code-identity 检测=**bind 前置串行检查**(不在 supervisor)。
    reconcile_code_identity 全注入依赖,可测。"""

    def _fakes(self, *, held=True, recorded="rootA|1.0.0", pid=5555, pstart="s",
               alive=True, probe_unknown=False, read_fail=False):
        from tests.helpers import FakeProber
        prober = FakeProber()
        if alive and pid is not None and not probe_unknown:
            prober.set(pid, 1, pstart, "python3")
        prober.raising = probe_unknown  # probe_alive → UNKNOWN
        world = {"held": held, "kills": [], "ensures": 0}

        def lock_held():
            return world["held"]

        def read_state():
            if read_fail:
                return None  # read_state 失败(_read_daemon_state 异常时返回 None)
            return {"daemon_code_identity": recorded, "daemon_pid": pid,
                    "daemon_proc_start": pstart}

        def kill(p, sig):
            world["kills"].append((p, sig))
            world["held"] = False  # SIGTERM → daemon 退出释放 flock

        def ensure():
            world["ensures"] += 1
            return "started"

        return world, prober, lock_held, read_state, kill, ensure

    def _run(self, world, prober, lock_held, read_state, kill, ensure,
             my_identity="rootNEW|1.0.0", wait_s=2):
        return ctl.reconcile_code_identity(
            my_identity=my_identity, read_state=read_state, lock_held=lock_held,
            prober=prober, kill=kill, ensure=ensure, sleep=lambda s: None, wait_s=wait_s)

    def test_match_proceeds_no_restart(self):
        f = self._fakes(recorded="rootA|1.0.0")
        r = self._run(*f, my_identity="rootA|1.0.0")
        assert r.get("error") is None and r["restarted"] is False and r["reason"] == "match"
        assert f[0]["kills"] == []

    def test_no_daemon_respawns_this_version(self):
        """锁未持有(无 daemon)→ 拉本版本新的(满足不变式),无 error。"""
        f = self._fakes(held=False)
        r = self._run(*f)
        assert r.get("error") is None and r["restarted"] is True and r["reason"] == "no-daemon-respawn"
        assert f[0]["ensures"] == 1

    def test_mismatch_killable_restarts_then_proceeds(self):
        f = self._fakes(recorded="rootOLD|0.9.0", pid=5555, pstart="s", alive=True)
        r = self._run(*f)
        assert r.get("error") is None and r["restarted"] is True
        assert r["old"] == "rootOLD|0.9.0" and r["new"] == "rootNEW|1.0.0" and r["state"] == "started"
        assert f[0]["kills"] == [(5555, __import__("signal").SIGTERM)]  # 精确 SIGTERM

    # ---- fail-closed:identity 不一致但无法安全杀 + 锁仍持有 → error,不放行 bind ----
    def test_mismatch_dead_pid_lock_held_fails_closed(self):
        f = self._fakes(recorded="rootOLD|0.9.0", pid=5555, pstart="s", alive=False)
        r = self._run(*f)
        assert "error" in r and r["reason"] == "unverified-cannot-restart"
        assert f[0]["kills"] == [] and f[0]["ensures"] == 0

    def test_mismatch_missing_start_lock_held_fails_closed(self):
        """daemon self_identity() 失败会合法记录空 proc_start → 无法安全杀 → error。"""
        f = self._fakes(recorded="rootOLD|0.9.0", pid=5555, pstart="")  # 空 start
        r = self._run(*f)
        assert "error" in r and r["reason"] == "unverified-cannot-restart"
        assert f[0]["kills"] == [] and f[0]["ensures"] == 0

    def test_mismatch_probe_unknown_lock_held_fails_closed(self):
        """probe_alive()==UNKNOWN(探测失败)→ 不杀 → error(fail-closed)。"""
        f = self._fakes(recorded="rootOLD|0.9.0", pid=5555, pstart="s", probe_unknown=True)
        r = self._run(*f)
        assert "error" in r and r["reason"] == "unverified-cannot-restart"
        assert f[0]["kills"] == [] and f[0]["ensures"] == 0

    def test_read_state_failure_lock_held_fails_closed(self):
        """read_state 失败(recorded=None,无 pid)+ 锁持有 → 无法验证 identity → error。"""
        f = self._fakes(read_fail=True)
        r = self._run(*f)
        assert "error" in r and r["reason"] == "unverified-cannot-restart"
        assert f[0]["kills"] == [] and f[0]["ensures"] == 0

    def test_unverified_but_lock_released_respawns(self):
        """无法安全杀,但锁已释放(旧 daemon 自行退出)→ 拉本版本新的(不 error)。"""
        world = {"held": True, "kills": [], "ensures": 0}
        from tests.helpers import FakeProber
        prober = FakeProber()  # pid 已死 → 无法杀
        releases = {"n": 0}

        def lock_held():
            # 首次读(no-daemon 门)持有;第二次(can_kill 失败分支里的复检)已释放
            releases["n"] += 1
            return releases["n"] <= 1

        r = ctl.reconcile_code_identity(
            my_identity="rootNEW|1.0.0",
            read_state=lambda: {"daemon_code_identity": "rootOLD|0.9.0",
                                "daemon_pid": 5555, "daemon_proc_start": "s"},
            lock_held=lock_held, prober=prober, kill=lambda p, s: None,
            ensure=lambda: (world.__setitem__("ensures", world["ensures"] + 1), "started")[1],
            sleep=lambda s: None)
        assert r.get("error") is None and r["restarted"] is True and r["reason"] == "respawned-daemon-gone"

    def test_mismatch_daemon_wont_exit_surfaces_error(self):
        """SIGTERM 后 flock 未释放(旧 daemon 不退出)→ 明确 error,不 ensure。"""
        from tests.helpers import FakeProber
        prober = FakeProber()
        prober.set(5555, 1, "s", "python3")
        ensures = {"n": 0}
        r = ctl.reconcile_code_identity(
            my_identity="rootNEW|1.0.0",
            read_state=lambda: {"daemon_code_identity": "rootOLD|0.9.0",
                                "daemon_pid": 5555, "daemon_proc_start": "s"},
            lock_held=lambda: True, prober=prober, kill=lambda p, s: None,  # daemon 不退出
            ensure=lambda: ensures.__setitem__("n", ensures["n"] + 1),
            sleep=lambda s: None, wait_s=1)
        assert "error" in r and r["reason"] == "no-exit"
        assert ensures["n"] == 0  # 没拉新 daemon


def test_bump_drop_counter_uses_short_obs_timeout(data_dir, monkeypatch):
    """minor②:可观测计数用短等锁(BUSY_TIMEOUT_OBS_MS),不叠加拖住 CC 退出。"""
    from lib import hooklib, db as dbmod, constants, paths as pathsmod
    pathsmod.ensure_data_dir()
    captured = {}
    real_connect = dbmod.connect

    def spy_connect(dbfile, busy_timeout_ms=None, **kw):
        captured["busy"] = busy_timeout_ms
        return real_connect(dbfile, busy_timeout_ms=busy_timeout_ms, **kw)

    monkeypatch.setattr(dbmod, "connect", spy_connect)
    # 需要 db 存在
    c = real_connect(pathsmod.db_path()); dbmod.init_schema(c, pathsmod.schema_path()); c.close()
    hooklib._bump_drop_counter()
    assert captured["busy"] == constants.BUSY_TIMEOUT_OBS_MS
    assert constants.BUSY_TIMEOUT_OBS_MS < constants.BUSY_TIMEOUT_SESSION_END_MS


def _load_bridgectl():
    import importlib.util
    root = pathlib.Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("bridgectl_mod", root / "bin" / "bridgectl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Args:
    def __init__(self, chat_id, chat_name=None):
        self.chat_id = chat_id
        self.chat_name = chat_name


def test_cmd_bind_fails_closed_on_reconcile_error(env, monkeypatch):
    """fail-closed 端到端:reconcile 返回 error → cmd_bind exit 非0 且 **不建 pending_bind/bindings**。"""
    bridgectl = _load_bridgectl()
    monkeypatch.setattr(bridgectl.ctl, "ensure_daemon", lambda *a, **k: "running")
    monkeypatch.setattr(
        bridgectl.ctl, "reconcile_daemon_code_identity",
        lambda *a, **k: {"restarted": False, "reason": "unverified-cannot-restart",
                         "error": "检测到旧版本 daemon 但无法安全自动重启,请手动停止后重试"})
    with pytest.raises(SystemExit) as ei:
        bridgectl.cmd_bind(_Args(CHAT, "g"))
    assert ei.value.code == 6  # 非0
    # 不变式:未落任何绑定
    assert env.conn.execute("SELECT COUNT(*) FROM pending_bind").fetchone()[0] == 0
    assert env.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0] == 0


def test_cmd_bind_fails_closed_when_restart_leaves_daemon_not_ready(env, monkeypatch):
    """重启了旧 daemon 但新 daemon 未就绪 → 也不落绑定(不变式)。"""
    bridgectl = _load_bridgectl()
    monkeypatch.setattr(bridgectl.ctl, "ensure_daemon", lambda *a, **k: "running")
    monkeypatch.setattr(
        bridgectl.ctl, "reconcile_daemon_code_identity",
        lambda *a, **k: {"restarted": True, "reason": "restarted", "state": "in_progress"})
    with pytest.raises(SystemExit) as ei:
        bridgectl.cmd_bind(_Args(CHAT, "g"))
    assert ei.value.code == 5
    assert env.conn.execute("SELECT COUNT(*) FROM pending_bind").fetchone()[0] == 0
    assert env.conn.execute("SELECT COUNT(*) FROM bindings").fetchone()[0] == 0


class TestWaitListenerClaim:
    """bind 等 follower 认领:已认领立即 true 不 sleep;后到认领 true;超时 false。"""

    def test_already_claimed_returns_without_sleep(self, env):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s1",
                               listener_epoch=1)
        sleeps = []
        assert ctl.wait_listener_claim(env.conn, bid, env.clock, sleeps.append, 6000) is True
        assert sleeps == []

    def test_claimed_after_second_poll(self, env):
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        calls = []

        def sleep(s):
            calls.append(s)
            env.clock.tick(int(s * 1000))
            if len(calls) == 2:
                env.conn.execute(
                    "UPDATE bindings SET listener_pid=7001, listener_start='ls', "
                    "listener_epoch=1, listener_beat_at=? WHERE binding_id=?",
                    (env.clock.wall_ms(), bid))
            if len(calls) > 10:
                pytest.fail("认领后仍在轮询")

        assert ctl.wait_listener_claim(env.conn, bid, env.clock, sleep, 6000) is True
        assert calls == [0.5, 0.5]  # 0.5s 轮询,认领后立即停

    def test_never_claimed_times_out(self, env):
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        start = env.clock.mono_ms()
        calls = []

        def sleep(s):
            calls.append(s)
            env.clock.tick(int(s * 1000))
            if len(calls) > 100:
                pytest.fail("超时后仍在轮询")

        assert ctl.wait_listener_claim(env.conn, bid, env.clock, sleep, 6000) is False
        assert env.clock.mono_ms() - start == 6000  # 恰在上限放弃,不多等


def _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys, claim_at_s=None):
    """跑 bridgectl bind(等待真实执行,fake sleep 推时钟);claim_at_s 给定时在该秒数由"别人"认领该行。"""
    import sys
    bridgectl = _load_bridgectl()
    monkeypatch.setattr(bridgectl.ctl, "ensure_daemon", lambda *a, **k: "running")
    monkeypatch.setattr(bridgectl.ctl, "reconcile_daemon_code_identity",
                        lambda *a, **k: {"restarted": False, "reason": "match"})
    monkeypatch.setattr(bridgectl.procs, "SystemProber", lambda: ctl_prober)
    monkeypatch.setattr(bridgectl.os, "getppid", lambda: ZSH_PID)
    monkeypatch.setattr(bridgectl, "SystemClock", lambda: env.clock)
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        env.clock.tick(int(s * 1000))
        if claim_at_s is not None and abs(sum(sleeps) - claim_at_s) < 1e-9:
            env.conn.execute(
                "UPDATE bindings SET listener_pid=7001, listener_start='ls', listener_epoch=1, "
                "listener_beat_at=? WHERE status='starting'", (env.clock.wall_ms(),))

    monkeypatch.setattr(bridgectl.time, "sleep", fake_sleep)
    monkeypatch.setattr(sys, "argv", ["bridgectl", "bind", "--chat-id", CHAT, "--chat-name", "g"])
    with pytest.raises(SystemExit) as ei:
        bridgectl.main()
    assert ei.value.code == 0
    return json.loads(capsys.readouterr().out), sleeps


def test_cmd_bind_reports_listener_claimed_false_when_nobody_claims(env, ctl_prober, monkeypatch, capsys):
    res, sleeps = _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys)
    assert res["ok"] is True and res["listener_claimed"] is False
    assert sum(sleeps) == 6.0  # 等满 3 tick(6s)才放弃


def test_cmd_bind_reports_listener_claimed_true_when_claimed_at_4s(env, ctl_prober, monkeypatch, capsys):
    """follower 2s tick 下第 4 秒才认领(慢启动)→ 仍报 true;上限若缩成 1 tick 会错报 false、把模型导向手动 Monitor。"""
    res, sleeps = _run_cmd_bind_with_wait(env, ctl_prober, monkeypatch, capsys, claim_at_s=4.0)
    assert res["ok"] is True and res["listener_claimed"] is True
    assert sum(sleeps) == 4.0  # 认领后立即返回,不等满
