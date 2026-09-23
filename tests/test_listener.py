"""listener(plan 4.4):父自检 / 身份+epoch 单条 CAS / 心跳 / 领取(提交后才 print)/
farewell / 双 listener 排他 / daemon 自愈 singleflight。"""
import json

import pytest

from tests.conftest import CHAT, CC_PID
from lib import constants, util
from lib.listener_core import ListenerCore


ME = (7001, "Tue Jul 15 11:00:00 2026")
OTHER = (7002, "Tue Jul 15 11:05:00 2026")


def make_core(env, binding_id, me=ME, daemon_alive=True, ensure=None):
    lines = []
    probe = daemon_alive if callable(daemon_alive) else (lambda: daemon_alive)
    core = ListenerCore(
        env.conn, binding_id, env.clock, env.prober,
        me_pid=me[0], me_start=me[1],
        printer=lambda s: lines.append(s),
        daemon_alive_probe=probe,
        ensure_daemon=ensure or (lambda: None))
    return core, lines


def bare_binding(env, status="active", **kw):
    return env.make_binding(status=status, listener_pid=None, listener_start=None,
                            listener_epoch=0, listener_beat_at=None, **kw)


def brow(env, bid):
    return env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()


def add_delivery(env, bid, mid, payload=None):
    env.conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
        "VALUES(?,?,?,?,'enqueued',0)", ("ev_" + mid, mid, CHAT, bid))
    env.conn.execute(
        "INSERT INTO deliveries(binding_id,message_id,payload_json,state,enq_at) "
        "VALUES(?,?,?,'enqueued',0)",
        (bid, mid, util.jdumps(payload or {"message_id": mid, "text": "hi"})))


class TestTakeoverAndHeartbeat:
    def test_takeover_from_null_bumps_epoch(self, env):
        bid = bare_binding(env)
        core, _ = make_core(env, bid)
        assert core.step() == "ok"
        b = brow(env, bid)
        assert (b["listener_pid"], b["listener_start"]) == ME
        assert b["listener_epoch"] == 1 and core.my_epoch == 1
        assert b["listener_beat_at"] == env.clock.wall_ms()

    def test_redundant_copy_exits_when_holder_fresh(self, env):
        bid = bare_binding(env)
        core_a, _ = make_core(env, bid)
        core_a.step()
        core_b, lines_b = make_core(env, bid, me=OTHER)
        assert core_b.step() == "exit"  # 他人新鲜心跳 → 多余副本退出
        assert brow(env, bid)["listener_epoch"] == 1
        assert lines_b == []  # 静默

    def test_stale_holder_probe_unknown_waits_not_takes(self, env):
        bid = bare_binding(env)
        core_a, _ = make_core(env, bid)
        core_a.step()
        env.clock.tick(constants.HEARTBEAT_FRESH_MS + 1000)
        # holder(7001)不在 prober 表 → probe DEAD;先测 UNKNOWN:注册一个 lstart 异常?
        # UNKNOWN 场景:prober 表中放一个同 pid 但探测抛错做不到局部……改用包装 prober。
        real_get = env.prober.get

        def get(pid):
            if pid == ME[0]:
                raise RuntimeError("ps flaky")
            return real_get(pid)

        env.prober.get = get
        core_b, _ = make_core(env, bid, me=OTHER)
        assert core_b.step() == "ok"  # UNKNOWN=存活 → 等待,不抢
        assert brow(env, bid)["listener_epoch"] == 1
        env.prober.get = real_get

    def test_takeover_when_holder_definitively_dead(self, env):
        bid = bare_binding(env)
        core_a, _ = make_core(env, bid)
        core_a.step()
        env.clock.tick(constants.HEARTBEAT_FRESH_MS + 1000)
        # holder ME 不在 prober 表 → DEAD(确定死)
        core_b, _ = make_core(env, bid, me=OTHER)
        assert core_b.step() == "ok"
        b = brow(env, bid)
        assert (b["listener_pid"], b["listener_start"]) == OTHER
        assert b["listener_epoch"] == 2
        # 旧 listener 心跳 CAS(epoch 1)失败 → 静默退出
        assert core_a.step() == "exit"

    def test_heartbeat_clears_suspect(self, env):
        bid = bare_binding(env)
        core, _ = make_core(env, bid)
        core.step()
        env.conn.execute("UPDATE bindings SET suspect_since=123 WHERE binding_id=?", (bid,))
        env.clock.tick(2000)
        core.step()
        b = brow(env, bid)
        assert b["suspect_since"] is None
        assert b["listener_beat_at"] == env.clock.wall_ms()


class TestClaim:
    def test_claim_prints_after_commit_and_marks_emitted(self, env):
        bid = bare_binding(env)
        add_delivery(env, bid, "om_1")
        add_delivery(env, bid, "om_2")
        core, lines = make_core(env, bid)
        core.step()
        assert len(lines) == 2
        objs = [json.loads(x) for x in lines]
        assert [o["message_id"] for o in objs] == ["om_1", "om_2"]  # 按 delivery_seq 序
        assert all(o["type"] == "feishu_message" and "delivery_seq" in o for o in objs)
        rows = env.deliveries(bid)
        assert all(r["state"] == "emitted" for r in rows)
        assert all(r["lease_epoch"] == 1 for r in rows)

    def test_no_claim_when_starting(self, env):
        bid = bare_binding(env, status="starting", bind_phase="confirmed", session_id="s1")
        add_delivery(env, bid, "om_1")  # 不该存在,防御性
        core, lines = make_core(env, bid)
        core.step()
        assert lines == []
        assert env.deliveries(bid)[0]["state"] == "enqueued"

    def test_printer_crash_leaves_leased_at_least_once(self, env):
        bid = bare_binding(env)
        add_delivery(env, bid, "om_1")

        def boom(s):
            raise BrokenPipeError("monitor gone")

        core = ListenerCore(env.conn, bid, env.clock, env.prober,
                            me_pid=ME[0], me_start=ME[1], printer=boom,
                            daemon_alive_probe=lambda: True,
                            ensure_daemon=lambda: None)
        with pytest.raises(BrokenPipeError):
            core.step()
        d = env.deliveries(bid)[0]
        assert d["state"] == "leased" and d["lease_token"] is not None  # 崩溃窗=至多重复,不丢

    def test_claim_blocked_after_epoch_lost(self, env):
        bid = bare_binding(env)
        add_delivery(env, bid, "om_1")
        core_a, lines_a = make_core(env, bid)
        core_a.step()  # 领取+emit
        add_delivery(env, bid, "om_2")
        env.clock.tick(constants.HEARTBEAT_FRESH_MS + 1000)
        core_b, lines_b = make_core(env, bid, me=OTHER)
        core_b.step()  # 接管 epoch 2 并领取 om_2
        assert [json.loads(x)["message_id"] for x in lines_b] == ["om_2"]
        assert core_a.step() == "exit"  # 心跳 CAS 失败退出,不再领取


class TestFarewell:
    def test_terminated_farewell_once_then_exit(self, env):
        bid = env.make_binding(status="closed", close_reason="user_unbind")
        core, lines = make_core(env, bid)
        assert core.step() == "exit"
        assert core.step() == "exit"
        farewells = [json.loads(x) for x in lines]
        assert len(farewells) == 1
        assert farewells[0] == {"type": "farewell", "code": "user_unbind"}  # 固定文案(I1)

    def test_missing_row_farewell_gone(self, env):
        core, lines = make_core(env, "no-such-binding")
        assert core.step() == "exit"
        assert json.loads(lines[0])["code"] == "gone"

    def test_cc_dead_silent_exit(self, env):
        bid = bare_binding(env)
        env.prober.remove(CC_PID)
        core, lines = make_core(env, bid)
        assert core.step() == "exit"
        assert lines == []


class TestDaemonSelfHeal:
    def test_ensure_singleflight_backoff_and_alert_once(self, env):
        bid = bare_binding(env)
        calls = {"n": 0}

        def ensure():
            calls["n"] += 1

        core, lines = make_core(env, bid, daemon_alive=False, ensure=ensure)
        core.step()
        assert calls["n"] == 1
        core.step()  # 同刻重复 step:退避期内不再 spawn
        assert calls["n"] == 1
        for _ in range(6):
            env.clock.tick(120_000)
            core.step()
        assert calls["n"] >= 3
        alerts = [x for x in lines if json.loads(x).get("type") == "daemon_alert"]
        assert len(alerts) == 1  # 连续失败 N 次才一条固定 daemon_alert

    def test_alert_resets_when_daemon_back(self, env):
        bid = bare_binding(env)
        state = {"alive": False}
        core, lines = make_core(env, bid, daemon_alive=lambda: state["alive"],
                                ensure=lambda: None)
        for _ in range(6):
            env.clock.tick(120_000)
            core.step()
        assert any(json.loads(x).get("type") == "daemon_alert" for x in lines)
        state["alive"] = True
        env.clock.tick(120_000)
        core.step()
        n = len(lines)
        state["alive"] = False
        for _ in range(6):
            env.clock.tick(120_000)
            core.step()
        assert len([x for x in lines[n:] if json.loads(x).get("type") == "daemon_alert"]) == 1
