"""InstanceFollower(跟随本 CC 实例的常驻 listener)+ bin/listener.py 入口:
等待→认领 / 关闭→等待→再 bind / 不抢新鲜持有者→其死后接管→持续心跳 / 只跟本实例 /
实例确定死 exit / 入口 main 三态 / follower 先认领、Stop hook 后确认 → 激活。"""
import importlib.util
import json
import os
import pathlib

import pytest

from tests.conftest import CHAT, CC_PID, CC_START
from tests.helpers import FakeProber
from lib import constants, ctl, db as dbmod, hooklib, lifecycle, paths, util
from lib.listener_core import InstanceFollower, ListenerCore


ME = (7001, "Tue Jul 15 11:00:00 2026")
OTHER = (7002, "Tue Jul 15 11:05:00 2026")
ZSH_PID = 8100
OTHER_START = "Wed Jul 15 00:00:00 2026"   # 同 PID、不同启动时间 = 另一个 CC 实例


class _Stop(Exception):
    """fake sleep 抛出以终止 main 主循环。"""


def brow(env, bid):
    return env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()


def farewells(lines):
    return [json.loads(x) for x in lines if json.loads(x).get("type") == "farewell"]


def make_follower(env, me=ME, cc_pid=CC_PID, cc_start=CC_START):
    lines, created = [], []

    def factory(binding_id):
        created.append(binding_id)
        return ListenerCore(env.conn, binding_id, env.clock, env.prober,
                            me_pid=me[0], me_start=me[1],
                            printer=lambda s: lines.append(s),
                            daemon_alive_probe=lambda: True,
                            ensure_daemon=lambda: None)

    return InstanceFollower(env.conn, cc_pid, cc_start, env.prober, factory), lines, created


def bare_active(env, **kw):
    return env.make_binding(status="active", listener_pid=None, listener_start=None,
                            listener_epoch=0, listener_beat_at=None, **kw)


def _load_listener():
    root = pathlib.Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("listener_mod", root / "bin" / "listener.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestFollow:
    def test_idle_until_starting_row_then_claims(self, env):
        follower, lines, created = make_follower(env)
        changes = env.conn.total_changes
        for _ in range(3):
            env.clock.tick(2000)
            assert follower.step() == "idle"
        assert lines == [] and created == []
        assert env.conn.total_changes == changes  # 无绑定:零写
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        env.clock.tick(2000)
        assert follower.step() == "ok"
        b = brow(env, bid)
        assert (b["listener_pid"], b["listener_start"]) == ME
        assert b["listener_epoch"] == 1
        assert b["listener_beat_at"] == env.clock.wall_ms()
        assert lines == []

    def test_close_then_idle_then_rebind_claimed_by_fresh_core(self, env):
        follower, lines, created = make_follower(env)
        bid = bare_active(env)
        assert follower.step() == "ok"
        assert brow(env, bid)["listener_epoch"] == 1
        lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        env.clock.tick(2000)
        assert follower.step() == "idle"
        assert farewells(lines) == [{"type": "farewell", "code": "user_unbind"}]
        changes = env.conn.total_changes
        for _ in range(5):
            env.clock.tick(2000)
            assert follower.step() == "idle"
        assert len(farewells(lines)) == 1
        assert env.conn.total_changes == changes  # 等待期零写
        bid2 = env.make_binding(status="starting", bind_phase="unconfirmed")
        env.clock.tick(2000)
        assert follower.step() == "ok"
        b2 = brow(env, bid2)
        assert (b2["listener_pid"], b2["listener_start"]) == ME
        assert b2["listener_epoch"] == 1
        assert created == [bid, bid2]  # 每个绑定一个新 core

    def test_respects_fresh_holder_then_takes_over_and_keeps_heartbeat(self, env):
        follower, lines, created = make_follower(env)
        env.prober.set(OTHER[0], 1, OTHER[1], "python3")
        bid = env.make_binding(status="active", listener_pid=OTHER[0], listener_start=OTHER[1],
                               listener_epoch=1, listener_beat_at=env.clock.wall_ms())
        before = tuple(brow(env, bid))
        for _ in range(3):
            env.clock.tick(2000)
            # 持有者心跳保持新鲜(它自己在跳)
            env.conn.execute("UPDATE bindings SET listener_beat_at=? WHERE binding_id=?",
                             (env.clock.wall_ms(), bid))
            before = tuple(brow(env, bid))
            assert follower.step() == "idle"
            assert tuple(brow(env, bid)) == before
        assert lines == []
        # 持有者死、心跳陈旧 → 接管 epoch+1
        env.prober.remove(OTHER[0])
        env.clock.tick(constants.HEARTBEAT_FRESH_MS + 1000)
        assert follower.step() == "ok"
        b = brow(env, bid)
        assert (b["listener_pid"], b["listener_start"]) == ME
        assert b["listener_epoch"] == 2
        # 之后与 daemon 判死同跑 ≥40s:心跳随 tick 前进,始终 active,无 suspect
        last_beat = b["listener_beat_at"]
        for _ in range(25):
            env.clock.tick(2000)
            assert follower.step() == "ok"
            lifecycle.death_scan(env.conn, env.prober, env.clock)
            b = brow(env, bid)
            assert b["listener_beat_at"] > last_beat
            last_beat = b["listener_beat_at"]
            assert b["status"] == "active"
        assert b["suspect_since"] is None
        assert lines == []

    def test_only_claims_rows_of_this_instance(self, env):
        follower, lines, created = make_follower(env)
        other = env.make_binding(status="starting", bind_phase="unconfirmed",
                                 chat_id="oc_other", cc_pid=CC_PID, cc_start=OTHER_START)
        for _ in range(3):
            env.clock.tick(2000)
            assert follower.step() == "idle"
        assert created == []
        o = brow(env, other)
        assert o["listener_pid"] is None and o["listener_epoch"] == 0
        mine = env.make_binding(status="starting", bind_phase="unconfirmed")
        env.clock.tick(2000)
        assert follower.step() == "ok"
        assert created == [mine]
        assert brow(env, mine)["listener_epoch"] == 1
        o = brow(env, other)
        assert o["listener_pid"] is None and o["listener_epoch"] == 0

    def test_instance_dead_exits(self, env):
        follower, lines, created = make_follower(env)
        env.prober.remove(CC_PID)
        assert follower.step() == "exit"
        assert lines == [] and created == []


class TestMain:
    def test_arg_mode_claims_given_binding(self, env, monkeypatch):
        mod = _load_listener()
        monkeypatch.setattr(mod, "ensure_daemon", lambda: None)
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")

        def stopper(_s):
            raise _Stop

        with pytest.raises(_Stop):
            mod.main([bid], prober=env.prober, start_pid=ZSH_PID, sleep=stopper)
        b = brow(env, bid)
        assert b["listener_pid"] == os.getpid() and b["listener_epoch"] == 1

    def test_noarg_waits_for_db_then_claims(self, data_dir, prober, monkeypatch):
        mod = _load_listener()
        monkeypatch.setattr(mod, "ensure_daemon", lambda: None)
        prober.set(ZSH_PID, CC_PID, "Tue Jul 15 12:00:00 2026", "zsh")
        assert not paths.db_path().exists()
        state = {"calls": 0, "bid": None}

        def sleep(_s):
            state["calls"] += 1
            if state["calls"] == 2:
                paths.ensure_data_dir()
                c = dbmod.connect(paths.db_path())
                dbmod.init_schema(c, paths.schema_path())
                state["bid"] = util.new_id()
                c.execute(
                    "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,"
                    "cc_start,cwd,status,bind_phase) VALUES(?,?,?,NULL,?,?,?,'starting',"
                    "'unconfirmed')", (state["bid"], CHAT, "g", CC_PID, CC_START, "/tmp/x"))
                c.close()
            if state["calls"] >= 6:
                raise _Stop

        with pytest.raises(_Stop):
            mod.main([], prober=prober, start_pid=ZSH_PID, sleep=sleep)
        c = dbmod.connect(paths.db_path())
        b = c.execute("SELECT * FROM bindings WHERE binding_id=?", (state["bid"],)).fetchone()
        c.close()
        assert b["listener_pid"] == os.getpid() and b["listener_epoch"] == 1

    def test_noarg_no_claude_ancestor_farewell_no_instance(self, data_dir):
        mod = _load_listener()
        p = FakeProber()
        p.set(ZSH_PID, 1, "Tue Jul 15 12:00:00 2026", "zsh")  # 上溯到 init,无 claude
        lines, sleeps = [], []
        rc = mod.main([], prober=p, start_pid=ZSH_PID, sleep=sleeps.append,
                      printer=lines.append)
        assert rc == 0
        assert [json.loads(x) for x in lines] == [{"type": "farewell", "code": "no-instance"}]
        assert len(sleeps) == 2  # 启动瞬时失败:再试 2 tick


    def test_noarg_waiting_for_db_exits_when_instance_dies(self, data_dir, prober):
        """等 DB 期间 CC 实例确定死(如崩溃)→ 退出,别留孤儿进程;静默、不建库。"""
        mod = _load_listener()
        prober.set(ZSH_PID, CC_PID, "Tue Jul 15 12:00:00 2026", "zsh")
        lines, state = [], {"calls": 0}

        def sleep(_s):
            state["calls"] += 1
            if state["calls"] == 2:
                prober.remove(CC_PID)
            if state["calls"] >= 6:
                raise _Stop

        rc = mod.main([], prober=prober, start_pid=ZSH_PID, sleep=sleep, printer=lines.append)
        assert rc == 0 and lines == []
        assert not paths.db_path().exists()
        assert state["calls"] == 2  # 死后下一 tick 即退,不再等


    def test_noarg_transient_lookup_failure_then_claims(self, env, monkeypatch):
        """启动时首次 ps 探测抛错(瞬时失败)→ 下一 tick 再查成功 → 正常认领;只查一次会永久丢掉本 session 的 listener。"""
        mod = _load_listener()
        monkeypatch.setattr(mod, "ensure_daemon", lambda: None)
        env.prober.set(ZSH_PID, CC_PID, "Tue Jul 15 12:00:00 2026", "zsh")
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        real_get, state = env.prober.get, {"failed_once": False, "sleeps": 0}

        def flaky_get(pid):
            # 只让"祖先上溯"的首次探测失败(自身身份探测不在此列)
            if pid == ZSH_PID and not state["failed_once"]:
                state["failed_once"] = True
                raise RuntimeError("ps flaky")
            return real_get(pid)

        env.prober.get = flaky_get

        def sleep(_s):
            state["sleeps"] += 1
            if state["sleeps"] >= 4:
                raise _Stop

        lines = []
        with pytest.raises(_Stop):
            mod.main([], prober=env.prober, start_pid=ZSH_PID, sleep=sleep, printer=lines.append)
        assert farewells(lines) == []
        b = brow(env, bid)
        assert b["listener_pid"] == os.getpid() and b["listener_epoch"] == 1


class TestHandshakeOrdering:
    def test_follower_claims_before_stop_hook_confirms(self, env):
        env.prober.set(ZSH_PID, CC_PID, "Tue Jul 15 12:00:00 2026", "zsh")
        res = ctl.bind_prepare(env.conn, env.cfg, env.clock, env.prober,
                               chat_id=CHAT, chat_name="g", cwd="/tmp/x", start_pid=ZSH_PID)
        bid = res["binding_id"]
        follower, lines, created = make_follower(env)
        assert follower.step() == "ok"
        b = brow(env, bid)
        assert b["status"] == "starting" and b["listener_epoch"] == 1
        env.clock.tick(1000)
        r = hooklib.stop_hook_entry(
            {"session_id": "sess-1", "last_assistant_message": f"绑定确认 {res['marker']} 完成",
             "stop_hook_active": False, "cwd": "/tmp/x"},
            conn=env.conn, prober=env.prober, clock=env.clock, start_pid=ZSH_PID)
        assert r["suppressed"] and r["reason"] == "bind-handshake"
        env.recovery.fast_tick()  # 恢复工人:激活 / 判死
        b = brow(env, bid)
        assert b["status"] == "active" and b["session_id"] == "sess-1"
        keys = [j["idempotency_key"] for j in env.jobs("lifecycle_notice")]
        assert f"lc:{bid}:bound" in keys
        env.clock.tick(2000)
        assert follower.step() == "ok"  # 同一 core 继续持有
        assert brow(env, bid)["listener_epoch"] == 1 and created == [bid]
