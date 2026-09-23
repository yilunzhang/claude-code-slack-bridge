"""bind 建行原子性 / 终止事务级联 / 映射函数 / pending_bind 超时 / 激活。plan 4.1/4.6/4.8。"""
import pytest

from tests.conftest import CHAT, CC_PID, CC_START
from lib import lifecycle, jobs, constants
from lib.lifecycle import BindConflict


def make_pending_message(env, bid, mid="om_m1", state="awaiting_approval", chat=CHAT):
    env.conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts,sender_open_id) "
        "VALUES(?,?,?,?,?,?,?)",
        ("ev_" + mid, mid, chat, bid, state, env.clock.wall_ms(), "ou_member"))
    if state == "awaiting_approval":
        env.conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,created_at) "
            "VALUES(?,?,?,?,'pending',?)",
            ("p_" + mid, mid, bid, "n", env.clock.wall_ms()))


class TestCreateBinding:
    def test_creates_pending_and_starting_atomically(self, env):
        res = lifecycle.create_binding(
            env.conn, chat_id=CHAT, chat_name="测试群", cwd="/tmp/x",
            cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                             (res["binding_id"],)).fetchone()
        p = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?",
                             (res["binding_id"],)).fetchone()
        assert b["status"] == "starting" and b["session_id"] is None
        assert b["bind_phase"] == "unconfirmed"
        assert p["state"] == "pending" and p["latch_open"] == 0
        assert p["expires_at"] == env.clock.wall_ms() + constants.PENDING_BIND_TTL_MS
        assert res["marker"].startswith("[feishu-bridge-bind:") and res["marker"].endswith("]")
        assert len(p["nonce"]) == 32

    def test_chat_busy_conflict(self, env):
        env.make_binding(status="active", chat_id=CHAT)
        with pytest.raises(BindConflict) as ei:
            lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                     cc_pid=999, cc_start="x", clock=env.clock)
        assert ei.value.code == "chat_busy"
        # 失败后无残留 pending_bind
        assert env.conn.execute("SELECT COUNT(*) FROM pending_bind").fetchone()[0] == 0

    def test_instance_busy_conflict(self, env):
        env.make_binding(status="active", chat_id="oc_other", cc_pid=CC_PID, cc_start=CC_START)
        with pytest.raises(BindConflict) as ei:
            lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                     cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        assert ei.value.code == "instance_busy"

    def test_terminal_binding_frees_chat(self, env):
        env.make_binding(status="closed", chat_id=CHAT, close_reason="user_unbind")
        res = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                       cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        assert res["binding_id"]


class TestMapTerminated:
    def test_no_history_is_unbound(self):
        assert lifecycle.map_terminated_to_inbox_state(None) == "unbound"

    @pytest.mark.parametrize("reason", ["user_unbind", "bind_failed", "bind_timeout"])
    def test_unbound_reasons(self, env, reason):
        bid = env.make_binding(status="closed", close_reason=reason)
        row = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert lifecycle.map_terminated_to_inbox_state(row) == "unbound"

    @pytest.mark.parametrize("status,reason", [
        ("closed", "cc_gone"), ("closed", "session_end"),
        ("dead", "listener_gone"), ("closed", "listener_never_ready")])
    def test_session_closed_reasons(self, env, status, reason):
        bid = env.make_binding(status=status, close_reason=reason)
        row = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert lifecycle.map_terminated_to_inbox_state(row) == "session_closed"


class TestTerminate:
    def _rich_binding(self, env):
        bid = env.make_binding(status="active")
        # 未决审批 + 其 inbox
        make_pending_message(env, bid, "om_appr", "awaiting_approval")
        # waiting_binding 行(激活前积压)
        make_pending_message(env, bid, "om_wait", "waiting_binding")
        # enqueued delivery
        make_pending_message(env, bid, "om_enq", "enqueued")
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES(?, 'om_enq','{}','enqueued')", (bid,))
        # leased delivery
        make_pending_message(env, bid, "om_leased", "enqueued")
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state,lease_token,lease_until) "
            "VALUES(?, 'om_leased','{}','leased','tok', ?)", (bid, env.clock.wall_ms() + 99999))
        # outbound jobs: pending turn / unknown notice / sent turn
        jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                        idempotency_key="turn:g1:0", turn_group="g1", chunk_index=0,
                        body="x", now=env.clock.wall_ms())
        jobs.create_job(env.conn, kind="decision_notice", chat_id=CHAT, binding_id=bid,
                        idempotency_key="dec:p_om_appr:approved", ref_pending_id="p_om_appr",
                        expected_state="approved", body="y", now=env.clock.wall_ms())
        env.conn.execute("UPDATE outbound_jobs SET state='unknown' WHERE idempotency_key='dec:p_om_appr:approved'")
        jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                        idempotency_key="turn:g0:0", turn_group="g0", chunk_index=0,
                        body="z", now=env.clock.wall_ms())
        env.conn.execute("UPDATE outbound_jobs SET state='sent', sent_message_id='om_sent' "
                         "WHERE idempotency_key='turn:g0:0'")
        # 进行中的 pending_bind(异常路径:active 却还有 pending 行——覆盖级联)
        env.conn.execute(
            "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state,latch_open,expires_at) "
            "VALUES(?,?,?,?,'n','pending',1,?)", (bid, CHAT, CC_PID, CC_START,
                                                  env.clock.wall_ms() + 10000))
        return bid

    def test_cascades(self, env):
        bid = self._rich_binding(env)
        ok = lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        assert ok
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "user_unbind"
        assert env.conn.execute("SELECT state FROM pendings WHERE pending_id='p_om_appr'").fetchone()[0] == "expired"
        assert env.inbox_row("om_appr")["state"] == "expired"
        # 未领 delivery dropped;leased 保持(恢复工人收口)
        rows = {r["message_id"]: r["state"] for r in env.deliveries(bid)}
        assert rows["om_enq"] == "dropped" and rows["om_leased"] == "leased"
        # jobs:pending/unknown → cancelled;sent 不动;唯一豁免=本次 lifecycle_notice
        st = {r["idempotency_key"]: r["state"] for r in env.jobs()}
        assert st["turn:g1:0"] == "cancelled"
        assert st["dec:p_om_appr:approved"] == "cancelled"
        assert st["turn:g0:0"] == "sent"
        assert st[f"lc:{bid}:user_unbind"] == "pending"
        lc = env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?",
                              (f"lc:{bid}:user_unbind",)).fetchone()
        assert lc["expected_state"] == "closed:user_unbind"
        # waiting 行按 4.2.4 映射 + 回执(user_unbind→unbound)
        assert env.inbox_row("om_wait")["state"] == "unbound"
        assert st.get("notice:om_wait:unbound") == "pending"
        # pending_bind 关闩终态化
        pb = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?", (bid,)).fetchone()
        assert pb["state"] == "expired" and pb["latch_open"] == 0

    def test_terminate_cas_single_winner(self, env):
        bid = env.make_binding(status="active")
        assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        assert not lifecycle.terminate_binding(env.conn, bid, "cc_gone", env.clock)
        b = env.conn.execute("SELECT close_reason FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["close_reason"] == "user_unbind"

    def test_terminate_dead_status_maps_session_closed_for_waiting(self, env):
        bid = env.make_binding(status="active")
        make_pending_message(env, bid, "om_w2", "waiting_binding")
        lifecycle.terminate_binding(env.conn, bid, "listener_gone", env.clock, new_status="dead")
        assert env.inbox_row("om_w2")["state"] == "session_closed"
        keys = {r["idempotency_key"] for r in env.jobs("inbound_notice")}
        assert "notice:om_w2:session_closed" in keys

    def test_lifecycle_notice_can_be_suppressed(self, env):
        bid = env.make_binding(status="active")
        lifecycle.terminate_binding(env.conn, bid, "bind_failed", env.clock, notify=False)
        assert env.jobs("lifecycle_notice") == []


class TestExpirePendingBind:
    def test_timeout_closes_orphan_starting_single_tx(self, env):
        """r6/M2 + r7-①:超时=同一终止事务;bind_timeout 映射 unbound。"""
        res = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                       cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        bid = res["binding_id"]
        # starting 期积压一条 waiting_binding
        make_pending_message(env, bid, "om_w", "waiting_binding")
        env.clock.tick(constants.PENDING_BIND_TTL_MS + 1)
        n = lifecycle.expire_stale_pending_binds(env.conn, env.clock)
        assert n == 1
        pb = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?", (bid,)).fetchone()
        assert pb["state"] == "expired" and pb["latch_open"] == 0
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "bind_timeout"
        assert env.inbox_row("om_w")["state"] == "unbound"  # r7-①
        keys = {r["idempotency_key"] for r in env.jobs("inbound_notice")}
        assert "notice:om_w:unbound" in keys

    def test_not_expired_untouched(self, env):
        lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                 cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        assert lifecycle.expire_stale_pending_binds(env.conn, env.clock) == 0


class TestActivation:
    def _confirmed_starting(self, env, beat_age_ms=0, confirmed_ago_ms=0):
        bid = env.make_binding(status="starting", bind_phase="confirmed",
                               session_id="sess-1", listener_epoch=3,
                               listener_beat_at=env.clock.wall_ms() - beat_age_ms,
                               confirmed_at=env.clock.wall_ms() - confirmed_ago_ms)
        return bid

    def test_activates_with_fresh_heartbeat(self, env):
        bid = self._confirmed_starting(env, beat_age_ms=1000)
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "activated"
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "active"
        keys = {r["idempotency_key"] for r in env.jobs("lifecycle_notice")}
        assert f"lc:{bid}:bound" in keys

    def test_waits_when_stale_but_young(self, env):
        bid = self._confirmed_starting(env, beat_age_ms=constants.HEARTBEAT_FRESH_MS + 1000,
                                       confirmed_ago_ms=1000)
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "waiting"
        b = env.conn.execute("SELECT status FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "starting"

    def test_timeout_closes_listener_never_ready(self, env):
        bid = self._confirmed_starting(env, beat_age_ms=99999,
                                       confirmed_ago_ms=constants.ACTIVATION_TIMEOUT_MS + 1)
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "timed_out"
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "listener_never_ready"

    def test_no_listener_yet_never_activates(self, env):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s2",
                               listener_epoch=0, listener_pid=None,
                               listener_beat_at=None,
                               confirmed_at=env.clock.wall_ms())
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "waiting"


class TestBindSupersede:
    """修复项2:同实例残留 starting/pending(如握手前 /clear)→ 同一终止事务 close(bind_superseded)再插新行。"""

    def test_stale_starting_superseded_same_chat(self, env):
        r1 = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        make_pending_message(env, r1["binding_id"], "om_w", "waiting_binding")
        # 同实例同群直接 rebind:不再被 10 分钟占位挡住
        r2 = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        old = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                               (r1["binding_id"],)).fetchone()
        assert old["status"] == "closed" and old["close_reason"] == "bind_superseded"
        pb_old = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?",
                                  (r1["binding_id"],)).fetchone()
        assert pb_old["state"] == "expired" and pb_old["latch_open"] == 0
        # 旧 waiting 行按 4.2.4 映射为 unbound(bind_superseded ∈ unbound 类)
        assert env.inbox_row("om_w")["state"] == "unbound"
        new = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                               (r2["binding_id"],)).fetchone()
        assert new["status"] == "starting"
        pb_new = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?",
                                  (r2["binding_id"],)).fetchone()
        assert pb_new["state"] == "pending"

    def test_supersede_across_chats(self, env):
        r1 = lifecycle.create_binding(env.conn, chat_id="oc_A", chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        r2 = lifecycle.create_binding(env.conn, chat_id="oc_B", chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        old = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                               (r1["binding_id"],)).fetchone()
        assert old["close_reason"] == "bind_superseded"
        assert env.conn.execute(
            "SELECT chat_id FROM bindings WHERE binding_id=?",
            (r2["binding_id"],)).fetchone()[0] == "oc_B"

    def test_active_binding_not_superseded(self, env):
        env.make_binding(status="active", chat_id="oc_other",
                         cc_pid=CC_PID, cc_start=CC_START)
        with pytest.raises(BindConflict) as ei:
            lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                     cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        assert ei.value.code == "instance_busy"

    def test_superseded_maps_unbound(self, env):
        bid = env.make_binding(status="closed", close_reason="bind_superseded")
        row = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert lifecycle.map_terminated_to_inbox_state(row) == "unbound"
