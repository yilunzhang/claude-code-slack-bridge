"""bind 建行原子性 / 终止事务级联(contracts §5.6 绑定范围 + 顺序)/ 映射函数 / pending_bind 超时 / 激活。"""
import pytest

from lib import lifecycle, jobs, constants
from lib.lifecycle import BindConflict
from tests.conftest import CHAT, CC_PID, CC_START, OWNER


def make_pending_message(env, bid, mid="m1", state="awaiting_approval", chat=CHAT,
                         pending_state=None, materialize_reason=None):
    mid = "%s:%s" % (chat, mid) if ":" not in mid else mid
    env.conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts,sender_user_id,"
        "reply_thread_ts,materialize_reason) VALUES(?,?,?,?,?,?,?,?,?)",
        ("ev_" + mid, mid, chat, bid, state, env.clock.wall_ms(), "U0MEMBER",
         mid.split(":")[1], materialize_reason))
    if pending_state is None and state == "awaiting_approval":
        pending_state = "pending"
    if pending_state is not None:
        env.conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,created_at,decided_by) "
            "VALUES(?,?,?,?,?,?,?)",
            ("p_" + mid.split(":")[1], mid, bid, "n", pending_state, env.clock.wall_ms(),
             OWNER if pending_state != "pending" else None))
    return mid


class TestCreateBinding:
    def test_creates_pending_and_starting_atomically(self, env):
        res = lifecycle.create_binding(
            env.conn, chat_id=CHAT, chat_name="测试频道", cwd="/tmp/x",
            cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                             (res["binding_id"],)).fetchone()
        p = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?",
                             (res["binding_id"],)).fetchone()
        assert b["status"] == "starting" and b["session_id"] is None
        assert b["bind_phase"] == "unconfirmed"
        assert p["state"] == "pending" and p["latch_open"] == 0
        assert p["expires_at"] == env.clock.wall_ms() + constants.PENDING_BIND_TTL_MS
        assert res["marker"].startswith(constants.MARKER_PREFIX) and res["marker"].endswith("]")
        assert res["marker"].startswith("[slack-bridge-bind:")
        assert len(p["nonce"]) == 32

    def test_chat_busy_conflict(self, env):
        env.make_binding(status="active", chat_id=CHAT)
        with pytest.raises(BindConflict) as ei:
            lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                     cc_pid=999, cc_start="x", clock=env.clock)
        assert ei.value.code == "chat_busy"
        assert env.conn.execute("SELECT COUNT(*) FROM pending_bind").fetchone()[0] == 0

    def test_instance_busy_conflict(self, env):
        env.make_binding(status="active", chat_id="C_OTHER", cc_pid=CC_PID, cc_start=CC_START)
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

    @pytest.mark.parametrize("reason", ["user_unbind", "bind_failed", "bind_timeout", "bind_superseded"])
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


class TestDecisionNoticeHelper:
    def test_creates_job_with_key_guard_and_text_body(self, env):
        bid = env.make_binding(status="active")
        mid = make_pending_message(env, bid, "1.1")
        assert lifecycle.create_decision_notice(
            env.conn, pending_id="p_1.1", binding_id=bid, chat_id=CHAT, message_id=mid,
            reply_to="1.1", outcome="rejected", now=env.clock.wall_ms())
        j = env.jobs("decision_notice")[0]
        assert j["idempotency_key"] == "dec:p_1.1:rejected" and j["expected_state"] == "rejected"
        assert j["reply_to"] == "1.1" and j["ref_message_id"] == mid and j["ref_pending_id"] == "p_1.1"
        assert j["body"] == "🚫 已忽略。"
        with pytest.raises(ValueError):
            lifecycle.create_decision_notice(
                env.conn, pending_id="p", binding_id=bid, chat_id=CHAT, message_id=mid,
                reply_to=None, outcome="approved", now=0)


class TestTerminate:
    def _rich_binding(self, env):
        bid = env.make_binding(status="active")
        now = env.clock.wall_ms()
        # 未决审批 + 其 inbox(card job pending)
        m_appr = make_pending_message(env, bid, "appr.1", "awaiting_approval")
        jobs.create_job(env.conn, kind="approval_card", chat_id=CHAT, binding_id=bid,
                        idempotency_key="card:p_appr.1", ref_pending_id="p_appr.1", now=now)
        # 已批准、附件物化中(approved_pending_files 未发)
        m_mat = make_pending_message(env, bid, "mat.1", "materializing", pending_state="approved",
                                     materialize_reason="approved")
        jobs.create_job(env.conn, kind="decision_notice", chat_id=CHAT, binding_id=bid,
                        idempotency_key="dec:p_mat.1:approved_pending_files", ref_pending_id="p_mat.1",
                        expected_state="approved_pending_files", now=now)
        # 已决审批的更新(delivered / rejected / attachment_failed)—— 必须保留
        m_del = make_pending_message(env, bid, "del.1", "enqueued", pending_state="approved")
        for pid, outcome in (("p_del.1", "delivered"), ("p_rej.1", "rejected"), ("p_af.1", "attachment_failed")):
            jobs.create_job(env.conn, kind="decision_notice", chat_id=CHAT, binding_id=bid,
                            idempotency_key="dec:%s:%s" % (pid, outcome), ref_pending_id=pid,
                            expected_state=outcome, now=now)
        env.conn.execute("UPDATE outbound_jobs SET state='unknown' WHERE idempotency_key='dec:p_rej.1:rejected'")
        # owner 物化中(无 pending)
        make_pending_message(env, bid, "own.1", "materializing", materialize_reason="owner")
        # waiting_binding 行
        make_pending_message(env, bid, "wait.1", "waiting_binding")
        # enqueued / leased deliveries
        m_enq = make_pending_message(env, bid, "enq.1", "enqueued")
        env.conn.execute("INSERT INTO deliveries(binding_id,message_id,payload_json,state) VALUES(?,?,'{}','enqueued')",
                         (bid, m_enq))
        m_lea = make_pending_message(env, bid, "lea.1", "enqueued")
        env.conn.execute("INSERT INTO deliveries(binding_id,message_id,payload_json,state,lease_token,lease_until) "
                         "VALUES(?,?,'{}','leased','tok',?)", (bid, m_lea, now + 99999))
        # session_turn pending / unknown / sent;receipt pending;inbound_notice pending
        jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                        idempotency_key="turn:g1:0", turn_group="g1", chunk_index=0, body="x", now=now)
        jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                        idempotency_key="turn:g2:0", turn_group="g2", chunk_index=0, body="y", now=now)
        env.conn.execute("UPDATE outbound_jobs SET state='unknown', had_unknown=1 WHERE idempotency_key='turn:g2:0'")
        jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                        idempotency_key="turn:g0:0", turn_group="g0", chunk_index=0, body="z", now=now)
        env.conn.execute("UPDATE outbound_jobs SET state='sent', sent_message_id='C0CHAT:1.1' WHERE idempotency_key='turn:g0:0'")
        jobs.create_job(env.conn, kind="receipt_reaction", chat_id=CHAT, binding_id=bid,
                        idempotency_key="rc:1", ref_delivery_seq=1, body="eyes", now=now)
        jobs.create_job(env.conn, kind="unsupported_notice", chat_id=CHAT, binding_id=bid,
                        idempotency_key="un:x", ref_message_id="x", expected_state="unsupported", now=now)
        # 进行中的 pending_bind(异常路径:active 却还有 pending 行——覆盖级联)
        env.conn.execute(
            "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state,latch_open,expires_at) "
            "VALUES(?,?,?,?,'n','pending',1,?)", (bid, CHAT, CC_PID, CC_START, now + 10000))
        return bid, {"appr": m_appr, "mat": m_mat, "del": m_del, "enq": m_enq, "lea": m_lea}

    def test_cascades_in_contract_order(self, env):
        bid, m = self._rich_binding(env)
        seq_before = env.conn.execute("SELECT MAX(job_seq) FROM outbound_jobs").fetchone()[0]
        assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "user_unbind"
        st = {r["idempotency_key"]: r["state"] for r in env.jobs()}
        # ① 业务发送取消(pending/unknown);已决审批的更新保留;终态不动
        assert st["turn:g1:0"] == "cancelled" and st["turn:g2:0"] == "cancelled" and st["turn:g0:0"] == "sent"
        assert st["card:p_appr.1"] == "cancelled" and st["rc:1"] == "cancelled"
        assert st["dec:p_mat.1:approved_pending_files"] == "cancelled"
        errs = {r["idempotency_key"]: r["error"] for r in env.jobs()}
        assert errs["turn:g2:0"] == "unbind-while-unknown" and errs["turn:g1:0"] == "binding-terminated"
        assert st["dec:p_del.1:delivered"] == "pending" and st["dec:p_rej.1:rejected"] == "unknown"
        assert st["dec:p_af.1:attachment_failed"] == "pending" and st["un:x"] == "pending"
        # ② 仍 pending 的审批 → expired + decision_notice(expired)
        assert env.conn.execute("SELECT state FROM pendings WHERE pending_id='p_appr.1'").fetchone()[0] == "expired"
        assert env.inbox_row(m["appr"])["state"] == "expired" and st["dec:p_appr.1:expired"] == "pending"
        # ③ approved ∧ materializing → undeliverable + closed_undelivered;已 approved 的 pending 不变
        assert env.inbox_row(m["mat"])["state"] == "undeliverable"
        assert st["dec:p_mat.1:closed_undelivered"] == "pending"
        assert env.conn.execute("SELECT state FROM pendings WHERE pending_id='p_mat.1'").fetchone()[0] == "approved"
        # owner 物化中:不在终止范围,留给物化驱动按 4.2.4 映射
        assert env.inbox_row("%s:own.1" % CHAT)["state"] == "materializing"
        # ④ deliveries / waiting / lifecycle_notice / pending_bind
        rows = {r["message_id"]: r["state"] for r in env.deliveries(bid)}
        assert rows[m["enq"]] == "dropped" and rows[m["lea"]] == "leased"
        assert env.inbox_row("%s:wait.1" % CHAT)["state"] == "unbound"
        assert st["notice:%s:wait.1:unbound" % CHAT] == "pending"
        assert st["lc:%s:user_unbind" % bid] == "pending"
        lc = env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?",
                              ("lc:%s:user_unbind" % bid,)).fetchone()
        assert lc["expected_state"] == "closed:user_unbind"
        pb = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?", (bid,)).fetchone()
        assert pb["state"] == "expired" and pb["latch_open"] == 0
        # 全部新建 job 都在取消步骤之后(job_seq 更大且都 pending)
        new = env.conn.execute("SELECT state FROM outbound_jobs WHERE job_seq>?", (seq_before,)).fetchall()
        assert new and all(r["state"] == "pending" for r in new)

    def test_terminate_cas_single_winner(self, env):
        bid = env.make_binding(status="active")
        assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        assert not lifecycle.terminate_binding(env.conn, bid, "cc_gone", env.clock)
        b = env.conn.execute("SELECT close_reason FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["close_reason"] == "user_unbind"
        assert len(env.jobs("lifecycle_notice")) == 1

    def test_terminate_dead_status_maps_session_closed_for_waiting(self, env):
        bid = env.make_binding(status="active")
        make_pending_message(env, bid, "w2.1", "waiting_binding")
        lifecycle.terminate_binding(env.conn, bid, "listener_gone", env.clock, new_status="dead")
        assert env.inbox_row("%s:w2.1" % CHAT)["state"] == "session_closed"
        keys = {r["idempotency_key"] for r in env.jobs("inbound_notice")}
        assert "notice:%s:w2.1:session_closed" % CHAT in keys

    def test_lifecycle_notice_can_be_suppressed(self, env):
        bid = env.make_binding(status="active")
        lifecycle.terminate_binding(env.conn, bid, "bind_failed", env.clock, notify=False)
        assert env.jobs("lifecycle_notice") == []

    def test_other_bindings_untouched(self, env):
        bid = env.make_binding(status="active")
        other = env.make_binding(status="active", chat_id="C_OTHER", session_id="s2", cc_pid=5555,
                                 cc_start="x")
        make_pending_message(env, other, "o.1", "awaiting_approval", chat="C_OTHER")
        jobs.create_job(env.conn, kind="session_turn", chat_id="C_OTHER", binding_id=other,
                        idempotency_key="turn:o:0", turn_group="o", chunk_index=0, body="x", now=0)
        lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        assert env.conn.execute("SELECT state FROM pendings WHERE pending_id='p_o.1'").fetchone()[0] == "pending"
        assert env.conn.execute("SELECT state FROM outbound_jobs WHERE idempotency_key='turn:o:0'").fetchone()[0] == "pending"


class TestExpirePendingBind:
    def test_timeout_closes_orphan_starting_single_tx(self, env):
        res = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                       cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        bid = res["binding_id"]
        make_pending_message(env, bid, "w.1", "waiting_binding")
        env.clock.tick(constants.PENDING_BIND_TTL_MS + 1)
        n = lifecycle.expire_stale_pending_binds(env.conn, env.clock)
        assert n == 1
        pb = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?", (bid,)).fetchone()
        assert pb["state"] == "expired" and pb["latch_open"] == 0
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "bind_timeout"
        assert env.inbox_row("%s:w.1" % CHAT)["state"] == "unbound"
        keys = {r["idempotency_key"] for r in env.jobs("inbound_notice")}
        assert "notice:%s:w.1:unbound" % CHAT in keys

    def test_not_expired_untouched(self, env):
        lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                 cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        assert lifecycle.expire_stale_pending_binds(env.conn, env.clock) == 0


class TestActivation:
    def _confirmed_starting(self, env, beat_age_ms=0, confirmed_ago_ms=0):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s1",
                               listener_epoch=1, listener_pid=7777,
                               listener_beat_at=env.clock.wall_ms() - beat_age_ms,
                               confirmed_at=env.clock.wall_ms() - confirmed_ago_ms)
        return bid

    def test_activates_with_fresh_heartbeat(self, env):
        bid = self._confirmed_starting(env, beat_age_ms=1000)
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "activated"
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "active"
        keys = {r["idempotency_key"] for r in env.jobs("lifecycle_notice")}
        assert "lc:%s:bound" % bid in keys

    def test_waits_when_stale_but_young(self, env):
        bid = self._confirmed_starting(env, beat_age_ms=constants.HEARTBEAT_FRESH_MS + 1000,
                                       confirmed_ago_ms=1000)
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "waiting"
        assert env.conn.execute("SELECT status FROM bindings WHERE binding_id=?", (bid,)).fetchone()[0] == "starting"

    def test_timeout_closes_listener_never_ready(self, env):
        bid = self._confirmed_starting(env, beat_age_ms=99999,
                                       confirmed_ago_ms=constants.ACTIVATION_TIMEOUT_MS + 1)
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "timed_out"
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "listener_never_ready"

    def test_no_listener_yet_never_activates(self, env):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s2",
                               listener_epoch=0, listener_pid=None, listener_beat_at=None,
                               confirmed_at=env.clock.wall_ms())
        assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "waiting"


class TestBindSupersede:
    def test_stale_starting_superseded_same_chat(self, env):
        r1 = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        make_pending_message(env, r1["binding_id"], "w.1", "waiting_binding")
        r2 = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        old = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (r1["binding_id"],)).fetchone()
        assert old["status"] == "closed" and old["close_reason"] == "bind_superseded"
        pb_old = env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?", (r1["binding_id"],)).fetchone()
        assert pb_old["state"] == "expired" and pb_old["latch_open"] == 0
        assert env.inbox_row("%s:w.1" % CHAT)["state"] == "unbound"
        new = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (r2["binding_id"],)).fetchone()
        assert new["status"] == "starting"

    def test_supersede_across_chats(self, env):
        r1 = lifecycle.create_binding(env.conn, chat_id="C_A", chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        r2 = lifecycle.create_binding(env.conn, chat_id="C_B", chat_name=None, cwd=None,
                                      cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        old = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (r1["binding_id"],)).fetchone()
        assert old["close_reason"] == "bind_superseded"
        assert env.conn.execute("SELECT chat_id FROM bindings WHERE binding_id=?",
                                (r2["binding_id"],)).fetchone()[0] == "C_B"

    def test_active_binding_not_superseded(self, env):
        env.make_binding(status="active", chat_id="C_OTHER", cc_pid=CC_PID, cc_start=CC_START)
        with pytest.raises(BindConflict) as ei:
            lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name=None, cwd=None,
                                     cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
        assert ei.value.code == "instance_busy"
