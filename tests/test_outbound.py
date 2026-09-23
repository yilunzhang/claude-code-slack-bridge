"""出站(plan 4.5):per-kind 守卫线性化 / chunk 组内外序 / 契约解析 / unknown 同 key 重试一次。"""
import json

import pytest

from tests.conftest import CHAT
from tests.helpers import FakeRunResult, ok_envelope, err_envelope, network_err_envelope
from lib import constants, jobs


def mk_job(env, kind="session_turn", key=None, binding_id=None, chat_id=CHAT,
           body="hello", turn_group=None, chunk_index=None, **kw):
    key = key or f"k:{kind}:{env.conn.execute('SELECT COUNT(*) FROM outbound_jobs').fetchone()[0]}"
    jobs.create_job(env.conn, kind=kind, chat_id=chat_id, idempotency_key=key,
                    binding_id=binding_id, body=body, turn_group=turn_group,
                    chunk_index=chunk_index, now=env.clock.wall_ms(), **kw)
    return env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()


def job_state(env, key):
    return env.conn.execute(
        "SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()


def arm_send_ok(env, prefix=("im", "+messages-send")):
    counter = {"n": 0}

    def fn(args, cwd):
        counter["n"] += 1
        return ok_envelope({"message_id": f"om_sent_{counter['n']}"}, notice="rate hint")

    env.runner.on_prefix(list(prefix), fn)
    return counter


class TestSendContract:
    def test_session_turn_sends_text_with_key(self, env):
        bid = env.make_binding(status="active")
        arm_send_ok(env)
        j = mk_job(env, binding_id=bid, key="turn:g1:0", turn_group="g1", chunk_index=0)
        assert env.outbound.tick() == 1
        row = job_state(env, "turn:g1:0")
        assert row["state"] == "sent" and row["sent_message_id"] == "om_sent_1"
        args, _ = env.runner.calls_matching("im", "+messages-send")[0]
        assert args[args.index("--chat-id") + 1] == CHAT
        # session_turn 走 --markdown(可信群前提,2026-07-17),不走 --text
        assert args[args.index("--markdown") + 1] == "hello"
        assert "--text" not in args
        from lib import util
        wire_key = args[args.index("--idempotency-key") + 1]
        assert wire_key == util.short_key("turn:g1:0") and len(wire_key) <= 40  # E4b

    def test_missing_message_id_is_unknown_then_retry_same_key(self, env):
        bid = env.make_binding(status="active")
        calls = []

        def fn(args, cwd):
            calls.append(args)
            if len(calls) == 1:
                return ok_envelope({})  # 缺 message_id → UNKNOWN 非成功(F10)
            return ok_envelope({"message_id": "om_ok"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.outbound.tick()
        row = job_state(env, "turn:g:0")
        assert row["state"] == "unknown" and row["attempt_count"] == 1
        assert row["next_attempt_at"] is not None
        env.clock.tick(constants.UNKNOWN_RETRY_DELAY_MS + 1)
        env.outbound.tick()
        row = job_state(env, "turn:g:0")
        assert row["state"] == "sent" and row["sent_message_id"] == "om_ok"
        # 同 key 重试(S4 服务端幂等)
        assert calls[0][calls[0].index("--idempotency-key") + 1] == \
               calls[1][calls[1].index("--idempotency-key") + 1]

    def test_retryable_timeout_persists_beyond_two_attempts(self, env):
        """旧「2 次后终局」已废(2026-07-18 事故修复):timeout=retryable session_turn →
        指数退避持久重试(远超 2 次),靠 idempotency-key 安全。见 TestSessionTurnRetryableHardening 全景。"""
        bid = env.make_binding(status="active")
        n = {"c": 0}

        def fn(a, c):
            n["c"] += 1
            return FakeRunResult(rc=1, stdout="", timed_out=True)

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.outbound.tick()                          # attempt1 → +8s
        env.clock.tick(constants.TURN_RETRY_BACKOFF_MS + 1)
        env.outbound.tick()                          # attempt2 → +16s
        env.clock.tick(2 * constants.TURN_RETRY_BACKOFF_MS + 1)
        env.outbound.tick()                          # attempt3 —— 旧实现到此绝不会有第 3 次
        row = job_state(env, "turn:g:0")
        assert n["c"] == 3 and row["state"] == "unknown" and row["attempt_count"] == 3

    def test_explicit_error_code_is_failed(self, env):
        bid = env.make_binding(status="active")
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: err_envelope(230002, "perm"))
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.outbound.tick()
        row = job_state(env, "turn:g:0")
        assert row["state"] == "failed" and "230002" in row["error"]

    def test_startup_scan_sending_to_unknown(self, env):
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.conn.execute("UPDATE outbound_jobs SET state='sending', attempt_count=1 "
                         "WHERE idempotency_key='turn:g:0'")
        env.outbound.startup_scan()
        row = job_state(env, "turn:g:0")
        assert row["state"] == "unknown" and row["next_attempt_at"] is not None


class TestGuards:
    def test_session_turn_binding_not_active_cancelled(self, env):
        bid = env.make_binding(status="closed", close_reason="user_unbind")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "cancelled"
        assert env.runner.calls == []

    def test_approval_card_guard_and_backfill(self, env):
        from tests.test_approval import member_pending
        p = member_pending(env)
        env.runner.on_prefix(["im", "+messages-reply"],
                             lambda a, c: ok_envelope({"message_id": "om_card_1"}))
        env.outbound.tick()
        card = job_state(env, f"card:{p['pending_id']}")
        assert card["state"] == "sent"
        args, _ = env.runner.calls_matching("im", "+messages-reply")[0]
        assert args[args.index("--message-id") + 1] == "om_1"
        assert args[args.index("--msg-type") + 1] == "interactive"
        # 发出后回填 card_message_id
        pr = env.conn.execute("SELECT card_message_id FROM pendings WHERE pending_id=?",
                              (p["pending_id"],)).fetchone()
        assert pr[0] == "om_card_1"

    def test_approval_card_cancelled_if_decided(self, env):
        from tests.test_approval import member_pending
        p = member_pending(env)
        env.conn.execute("UPDATE pendings SET state='rejected' WHERE pending_id=?",
                         (p["pending_id"],))
        env.outbound.tick()
        assert job_state(env, f"card:{p['pending_id']}")["state"] == "cancelled"

    def test_approval_card_cancelled_if_already_backfilled(self, env):
        from tests.test_approval import member_pending
        p = member_pending(env)
        env.conn.execute("UPDATE pendings SET card_message_id='om_prev' WHERE pending_id=?",
                         (p["pending_id"],))
        env.outbound.tick()
        assert job_state(env, f"card:{p['pending_id']}")["state"] == "cancelled"

    def test_decision_notice_guard_by_pending_state(self, env):
        bid = env.make_binding(status="active")
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_1','om_1',?,?,'rejected',0)", (CHAT, bid))
        env.conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
            "VALUES('p1','om_1',?,'n','rejected')", (bid,))
        arm_send_ok(env)
        mk_job(env, kind="decision_notice", key="dec:p1:rejected", binding_id=bid,
               ref_pending_id="p1", expected_state="rejected")
        mk_job(env, kind="decision_notice", key="dec:p1:approved", binding_id=bid,
               ref_pending_id="p1", expected_state="approved")
        env.outbound.tick()
        assert job_state(env, "dec:p1:rejected")["state"] == "sent"
        assert job_state(env, "dec:p1:approved")["state"] == "cancelled"

    def test_lifecycle_notice_guard(self, env):
        bid = env.make_binding(status="active")
        arm_send_ok(env)
        mk_job(env, kind="lifecycle_notice", key=f"lc:{bid}:bound", binding_id=bid,
               expected_state="active")
        mk_job(env, kind="lifecycle_notice", key=f"lc:{bid}:user_unbind", binding_id=bid,
               expected_state="closed:user_unbind")
        env.outbound.tick()
        assert job_state(env, f"lc:{bid}:bound")["state"] == "sent"
        assert job_state(env, f"lc:{bid}:user_unbind")["state"] == "cancelled"

    def test_inbound_and_unsupported_notice_guards(self, env):
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) "
            "VALUES('ev_1','om_1',?,'unbound',0)", (CHAT,))
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) "
            "VALUES('ev_2','om_2',?,'enqueued',0)", (CHAT,))
        arm_send_ok(env)
        mk_job(env, kind="inbound_notice", key="notice:om_1:unbound",
               ref_message_id="om_1", expected_state="unbound")
        mk_job(env, kind="unsupported_notice", key="un:om_2",
               ref_message_id="om_2", expected_state="unsupported")
        env.outbound.tick()
        assert job_state(env, "notice:om_1:unbound")["state"] == "sent"
        assert job_state(env, "un:om_2")["state"] == "cancelled"  # inbox 状态不符

    def test_receipt_reaction_guard_and_no_retry(self, env):
        bid = env.make_binding(status="active")
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_1','om_1',?,?,'enqueued',0)", (CHAT, bid))
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES(?,'om_1','{}','enqueued')", (bid,))
        seq = env.conn.execute("SELECT delivery_seq FROM deliveries").fetchone()[0]
        env.runner.on_prefix(["im", "reactions", "create"],
                             lambda a, c: FakeRunResult(rc=1, stdout="", timed_out=True))
        mk_job(env, kind="receipt_reaction", key=f"rc:{seq}", binding_id=bid,
               ref_delivery_seq=seq, ref_message_id="om_1", body="GLANCE")
        env.outbound.tick()
        assert job_state(env, f"rc:{seq}")["state"] == "failed"  # 失败即 failed 不重试

    def test_receipt_reaction_cancelled_on_dropped_delivery(self, env):
        bid = env.make_binding(status="active")
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_1','om_1',?,?,'enqueued',0)", (CHAT, bid))
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES(?,'om_1','{}','dropped')", (bid,))
        seq = env.conn.execute("SELECT delivery_seq FROM deliveries").fetchone()[0]
        mk_job(env, kind="receipt_reaction", key=f"rc:{seq}", binding_id=bid,
               ref_delivery_seq=seq, ref_message_id="om_1", body="GLANCE")
        env.outbound.tick()
        assert job_state(env, f"rc:{seq}")["state"] == "cancelled"

    def test_reaction_success_argv_shape(self, env):
        bid = env.make_binding(status="active")
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_1','om_1',?,?,'enqueued',0)", (CHAT, bid))
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES(?,'om_1','{}','enqueued')", (bid,))
        seq = env.conn.execute("SELECT delivery_seq FROM deliveries").fetchone()[0]
        env.runner.on_prefix(["im", "reactions", "create"],
                             lambda a, c: ok_envelope({"reaction_id": "r1"}))
        mk_job(env, kind="receipt_reaction", key=f"rc:{seq}", binding_id=bid,
               ref_delivery_seq=seq, ref_message_id="om_1", body="GLANCE")
        env.outbound.tick()
        assert job_state(env, f"rc:{seq}")["state"] == "sent"
        args, _ = env.runner.calls_matching("im", "reactions", "create")[0]
        params = json.loads(args[args.index("--params") + 1])
        data = json.loads(args[args.index("--data") + 1])
        assert params == {"message_id": "om_1"}
        assert data == {"reaction_type": {"emoji_type": "GLANCE"}}


class TestOrdering:
    def test_chunks_in_group_strict_order(self, env):
        bid = env.make_binding(status="active")
        sent = []

        def fn(args, cwd):
            body = args[args.index("--markdown") + 1]  # session_turn 走 --markdown
            sent.append(body)
            return ok_envelope({"message_id": f"om_{len(sent)}"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        for i in range(3):
            mk_job(env, binding_id=bid, key=f"turn:g:{i}", body=f"chunk{i}",
                   turn_group="g", chunk_index=i)
        env.outbound.tick()
        assert sent == ["chunk0", "chunk1", "chunk2"]

    def test_chunk_blocked_while_prev_unknown(self, env):
        bid = env.make_binding(status="active")
        calls = {"n": 0}

        def fn(args, cwd):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeRunResult(rc=1, stdout="", timed_out=True)  # chunk0 unknown
            return ok_envelope({"message_id": f"om_{calls['n']}"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, binding_id=bid, key="turn:g:0", body="c0", turn_group="g", chunk_index=0)
        mk_job(env, binding_id=bid, key="turn:g:1", body="c1", turn_group="g", chunk_index=1)
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "unknown"
        assert job_state(env, "turn:g:1")["state"] == "pending"  # 前块非 sent 后块不发
        env.clock.tick(constants.UNKNOWN_RETRY_DELAY_MS + 1)
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "sent"
        assert job_state(env, "turn:g:1")["state"] == "sent"

    def test_chunk_after_failed_prev_cancelled(self, env):
        bid = env.make_binding(status="active")
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: err_envelope(230002))
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        mk_job(env, binding_id=bid, key="turn:g:1", turn_group="g", chunk_index=1)
        env.outbound.tick()
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "failed"
        assert job_state(env, "turn:g:1")["state"] == "cancelled"

    def test_turn_groups_ordered_unknown_blocks_next_group(self, env):
        bid = env.make_binding(status="active")
        env.runner.on_prefix(["im", "+messages-send"],
                             lambda a, c: FakeRunResult(rc=1, stdout="", timed_out=True))
        mk_job(env, binding_id=bid, key="turn:gA:0", turn_group="gA", chunk_index=0)
        mk_job(env, binding_id=bid, key="turn:gB:0", turn_group="gB", chunk_index=0)
        env.outbound.tick()                          # gA attempt1 unknown(+8s);gB 被挡
        env.clock.tick(constants.TURN_RETRY_BACKOFF_MS + 1)
        env.outbound.tick()                          # gA attempt2 unknown(+16s,retryable 重试中,非终局)
        # gA 仍 unknown(退避未到)→ 前组 unknown 期间后组不发(HOL 排序保持)
        assert job_state(env, "turn:gA:0")["state"] == "unknown"
        assert job_state(env, "turn:gB:0")["state"] == "pending"  # 前组 unknown 后组不发

    def test_other_binding_not_blocked(self, env):
        bid_a = env.make_binding(status="active", chat_id="oc_A", session_id="sA",
                                 cc_pid=1111, cc_start="t1")
        bid_b = env.make_binding(status="active", chat_id="oc_B", session_id="sB",
                                 cc_pid=2222, cc_start="t2")
        env.conn.execute("UPDATE outbound_jobs SET state='cancelled'")  # 清 lifecycle 噪声
        calls = {"n": 0}

        def fn(args, cwd):
            calls["n"] += 1
            chat = args[args.index("--chat-id") + 1]
            if chat == "oc_A":
                return FakeRunResult(rc=1, stdout="", timed_out=True)
            return ok_envelope({"message_id": "om_b"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, binding_id=bid_a, chat_id="oc_A", key="turn:ga:0", turn_group="ga", chunk_index=0)
        mk_job(env, binding_id=bid_b, chat_id="oc_B", key="turn:gb:0", turn_group="gb", chunk_index=0)
        env.outbound.tick()
        assert job_state(env, "turn:ga:0")["state"] == "unknown"
        assert job_state(env, "turn:gb:0")["state"] == "sent"

    def test_notices_same_chat_ordered_by_job_seq(self, env):
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) VALUES('e1','om_1',?, 'unbound',0)",
            (CHAT,))
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) VALUES('e2','om_2',?, 'unbound',0)",
            (CHAT,))
        calls = {"n": 0}

        def fn(args, cwd):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeRunResult(rc=1, stdout="", timed_out=True)
            return ok_envelope({"message_id": "om_x"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, kind="inbound_notice", key="notice:om_1:unbound",
               ref_message_id="om_1", expected_state="unbound")
        mk_job(env, kind="inbound_notice", key="notice:om_2:unbound",
               ref_message_id="om_2", expected_state="unbound")
        env.outbound.tick()
        assert job_state(env, "notice:om_1:unbound")["state"] == "unknown"
        assert job_state(env, "notice:om_2:unbound")["state"] == "pending"  # 同 chat 按序


class TestErrorClassification:
    """修复项4:表驱动错误分类:永久集→failed;瞬态集→unknown(仍≤1 次同 key 重试);未知→unknown。"""

    def _job(self, env):
        bid = env.make_binding(status="active")
        return mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)

    def test_permanent_code_failed(self, env):
        self._job(env)
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: err_envelope(230002))
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "failed"

    def test_transient_code_persists_with_backoff(self, env):
        """230020(频控)=瞬态 retryable session_turn → 指数退避持久重试(不再 ≤1 次)。"""
        self._job(env)
        calls = {"n": 0}

        def fn(args, cwd):
            calls["n"] += 1
            return err_envelope(230020, "req too frequent")  # 频控=瞬态

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()                          # a1 +8s
        row = job_state(env, "turn:g:0")
        assert row["state"] == "unknown" and row["next_attempt_at"] is not None
        env.clock.tick(constants.TURN_RETRY_BACKOFF_MS + 1)
        env.outbound.tick()                          # a2 +16s
        env.clock.tick(2 * constants.TURN_RETRY_BACKOFF_MS + 1)
        env.outbound.tick()                          # a3 —— 旧实现停在 2
        assert calls["n"] == 3
        assert job_state(env, "turn:g:0")["state"] == "unknown"

    def test_unknown_code_nonretryable_failed_after_two(self, env):
        """未知 code = 非 retryable → MAX_SEND_ATTEMPTS 次后转 failed(放行后续)+ 告警,
        不再永久 unknown 队头阻塞(2026-07-18 HOL 修复)。"""
        self._job(env)
        calls = {"n": 0}

        def fn(args, cwd):
            calls["n"] += 1
            return err_envelope(123456789)

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()                          # a1 → unknown,非 retryable 平退避 15s
        row = job_state(env, "turn:g:0")
        assert row["state"] == "unknown"
        assert row["next_attempt_at"] - env.clock.wall_ms() == constants.UNKNOWN_RETRY_DELAY_MS
        env.clock.tick(constants.UNKNOWN_RETRY_DELAY_MS + 1)
        env.outbound.tick()                          # a2 → ac=2>=MAX → failed + 告警
        assert calls["n"] == 2
        assert job_state(env, "turn:g:0")["state"] == "failed"
        assert env.conn.execute(
            "SELECT 1 FROM outbound_jobs WHERE turn_group LIKE '__sendfail__:%'").fetchone() is not None


class TestPendingBackoffGate:
    def test_pending_with_future_next_attempt_not_sent(self, env):
        """修复项3配套:重臂后的 pending 带 next_attempt_at,未到期不发。"""
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET next_attempt_at=? WHERE idempotency_key='turn:g:0'",
            (env.clock.wall_ms() + 60_000,))
        arm_send_ok(env)
        assert env.outbound.tick() == 0
        env.clock.tick(60_001)
        assert env.outbound.tick() == 1


class TestReactionContract:
    def test_reaction_ok_without_reaction_id_is_failed(self, env):
        """修复项9:reaction 成功必须解析出 .data.reaction_id,缺=failed(仍不重试)。"""
        bid = env.make_binding(status="active")
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_1','om_1',?,?,'enqueued',0)", (CHAT, bid))
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES(?,'om_1','{}','enqueued')", (bid,))
        seq = env.conn.execute("SELECT delivery_seq FROM deliveries").fetchone()[0]
        env.runner.on_prefix(["im", "reactions", "create"], lambda a, c: ok_envelope({}))
        mk_job(env, kind="receipt_reaction", key=f"rc:{seq}", binding_id=bid,
               ref_delivery_seq=seq, ref_message_id="om_1", body="GLANCE")
        env.outbound.tick()
        assert job_state(env, f"rc:{seq}")["state"] == "failed"


class TestE4StderrEnvelope:
    """E4a:错误信封在 stderr(stdout 空),code 嵌套 .error.code;分类照走永久/瞬态表。"""

    def _job(self, env, key="turn:g:0"):
        bid = env.make_binding(status="active")
        return mk_job(env, binding_id=bid, key=key, turn_group="g", chunk_index=0)

    def test_stderr_permanent_code_failed(self, env):
        from tests.helpers import stderr_err_envelope
        self._job(env)
        env.runner.on_prefix(["im", "+messages-send"],
                             lambda a, c: stderr_err_envelope(99992402))
        env.outbound.tick()
        row = job_state(env, "turn:g:0")
        assert row["state"] == "failed" and "99992402" in row["error"]

    def test_stderr_transient_code_unknown_with_retry(self, env):
        from tests.helpers import stderr_err_envelope
        self._job(env)
        env.runner.on_prefix(["im", "+messages-send"],
                             lambda a, c: stderr_err_envelope(230020, subtype="rate_limited",
                                                              msg="req too frequent"))
        env.outbound.tick()
        row = job_state(env, "turn:g:0")
        assert row["state"] == "unknown" and row["next_attempt_at"] is not None

    def test_failure_logs_raw_streams(self, env):
        """E4a 可观测性:unknown/failed 把 rc/stdout/stderr 截断记入 daemon.log。"""
        from tests.helpers import stderr_err_envelope
        logs = []
        env.outbound.log = logs.append
        self._job(env)
        env.runner.on_prefix(["im", "+messages-send"],
                             lambda a, c: stderr_err_envelope(99992402))
        env.outbound.tick()
        joined = "\n".join(logs)
        assert "rc=" in joined and "99992402" in joined and "turn:g:0" in joined


class TestE4ShortKey:
    """E4b:飞书 uuid 参数上限 ~50 字符 → wire 短键 ≤40;DB 逻辑键与 UNIQUE 语义不变。"""

    def test_short_key_properties(self):
        from lib import util
        k1 = util.short_key("notice:om_" + "a" * 40 + ":session_closed")
        k2 = util.short_key("notice:om_" + "a" * 40 + ":session_closed")
        k3 = util.short_key("notice:om_" + "b" * 40 + ":session_closed")
        assert k1 == k2 and k1 != k3
        assert len(k1) <= 40 and k1.startswith("fb:")

    def test_long_logical_key_transmitted_as_short_key(self, env):
        """真形状:fake 对 >50 字符 key 返回 99992402 stderr 信封,逼实现走短键。"""
        from tests.helpers import stderr_err_envelope
        from lib import util
        long_mid = "om_" + "a" * 40
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) VALUES('e1',?,?,?,0)",
            (long_mid, CHAT, "session_closed"))
        logical = f"notice:{long_mid}:session_closed"
        assert len(logical) > 50  # 逻辑键必超限
        mk_job(env, kind="inbound_notice", key=logical,
               ref_message_id=long_mid, expected_state="session_closed", body="提示")
        wire_keys = []

        def fn(args, cwd):
            key = args[args.index("--idempotency-key") + 1]
            wire_keys.append(key)
            if len(key) > 50:
                return stderr_err_envelope(99992402)  # 真机行为:超长键被拒
            return ok_envelope({"message_id": "om_ok"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()
        row = job_state(env, logical)
        assert row["state"] == "sent"  # 走短键才可能成功
        assert wire_keys == [util.short_key(logical)]
        assert len(wire_keys[0]) <= 40
        # DB 存逻辑键,UNIQUE 语义不变
        assert row["idempotency_key"] == logical

    def test_retry_uses_same_short_key(self, env):
        from lib import util
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        seen = []

        def fn(args, cwd):
            seen.append(args[args.index("--idempotency-key") + 1])
            if len(seen) == 1:
                return FakeRunResult(rc=1, stdout="", timed_out=True)
            return ok_envelope({"message_id": "om_ok"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()
        env.clock.tick(constants.UNKNOWN_RETRY_DELAY_MS + 1)
        env.outbound.tick()
        assert seen[0] == seen[1] == util.short_key("turn:g:0")  # S4 同键重试语义保持


class TestAllowlistOutboundGate:
    def test_out_of_list_job_cancelled(self, env):
        """r3-1③:allowlist 生效且 job.chat_id 不在列 → cancelled,零外发。"""
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.cfg["chat_allowlist"] = ["oc_other"]
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "cancelled"
        assert env.runner.calls == []

    def test_in_list_job_sends(self, env):
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.cfg["chat_allowlist"] = [CHAT]
        arm_send_ok(env)
        assert env.outbound.tick() == 1


class TestAllowlistBeforeGate:
    def test_out_of_list_job_cancelled_even_when_gate_degraded(self, env):
        """r4-3:allowlist 列外 job 无论 fingerprint 门状态都确定性 cancelled。"""
        from lib import db as dbmod
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.cfg["chat_allowlist"] = ["oc_other"]
        dbmod.set_state(env.conn, "outbound_gate", "degraded:identity_unverified")
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "cancelled"
        assert env.runner.calls == []

    def test_in_list_job_still_blocked_by_degraded_gate(self, env):
        """对照:列内 job 在 degraded 门下仍停摆(不 cancel,保持 pending)。"""
        from lib import db as dbmod
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.cfg["chat_allowlist"] = [CHAT]
        dbmod.set_state(env.conn, "outbound_gate", "degraded:version_mismatch")
        assert env.outbound.tick() == 0
        assert job_state(env, "turn:g:0")["state"] == "pending"


class TestWireFlagsByKind:
    """出站 wire flag 按 kind(2026-07-17):session_turn=--markdown(可信群前提),
    通知=--text,审批卡=转义 interactive。"""

    def test_session_turn_uses_markdown_not_text(self, env):
        bid = env.make_binding(status="active")
        arm_send_ok(env)
        mk_job(env, kind="session_turn", key="turn:g:0", binding_id=bid,
               turn_group="g", chunk_index=0, body="# 标题\n- a\n```py\nx=1\n```")
        env.outbound.tick()
        args, _ = env.runner.calls_matching("im", "+messages-send")[0]
        assert "--markdown" in args and "--text" not in args
        assert args[args.index("--markdown") + 1].startswith("# 标题")

    def test_lifecycle_notice_uses_text_not_markdown(self, env):
        bid = env.make_binding(status="active")
        arm_send_ok(env)
        mk_job(env, kind="lifecycle_notice", key=f"lc:{bid}:bound", binding_id=bid,
               expected_state="active", body="✅ 已绑定")
        env.outbound.tick()
        args, _ = env.runner.calls_matching("im", "+messages-send")[0]
        assert "--text" in args and "--markdown" not in args

    def test_inbound_notice_uses_text_not_markdown(self, env):
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) "
            "VALUES('e1','om_1',?,'unbound',0)", (CHAT,))
        arm_send_ok(env)
        mk_job(env, kind="inbound_notice", key="notice:om_1:unbound",
               ref_message_id="om_1", expected_state="unbound", body="⚠️ 未绑定")
        env.outbound.tick()
        args, _ = env.runner.calls_matching("im", "+messages-send")[0]
        assert "--text" in args and "--markdown" not in args

    def test_approval_card_stays_escaped_interactive(self, env):
        from tests.test_approval import member_pending
        p = member_pending(env)
        env.runner.on_prefix(["im", "+messages-reply"],
                             lambda a, c: ok_envelope({"message_id": "om_card"}))
        env.outbound.tick()
        args, _ = env.runner.calls_matching("im", "+messages-reply")[0]
        assert args[args.index("--msg-type") + 1] == "interactive"
        assert "--markdown" not in args and "--text" not in args  # 成员预览绝不 markdown 渲染


# retryable session_turn 持久退避的完整退避时间线(基数 8s、×2、封顶 45s、上限 6 次)
_RETRY_DELAYS = [8_000, 16_000, 32_000, 45_000, 45_000]  # 尝试 1..5 后的 next_attempt 退避


def _alert_row(env):
    return env.conn.execute(
        "SELECT * FROM outbound_jobs WHERE turn_group LIKE '__sendfail__:%'").fetchone()


def _alert_count(env):
    return env.conn.execute(
        "SELECT COUNT(*) FROM outbound_jobs WHERE turn_group LIKE '__sendfail__:%'").fetchone()[0]


class TestSessionTurnRetryableHardening:
    """2026-07-18 事故根因修复:retryable(503/网络/频控/超时)session_turn 持久指数退避重试;
    耗尽 → failed(放行后续 turn,不再终态 unknown 队头阻塞)+ 群内可见告警。靠 idempotency-key
    保证重发去重、转 failed 安全。仅 session_turn 生效。"""

    def _turn(self, env, key="turn:g:0", group="g", body="hello", **kw):
        bid = kw.pop("bid", None) or env.make_binding(status="active")
        mk_job(env, binding_id=bid, key=key, turn_group=group, chunk_index=0, body=body, **kw)
        return bid

    def test_503_backoff_schedule_then_exhaust_to_failed_and_alert(self, env):
        self._turn(env)
        calls = {"n": 0}

        def fn(a, c):
            calls["n"] += 1
            return network_err_envelope(503)

        env.runner.on_prefix(["im", "+messages-send"], fn)
        for i, d in enumerate(_RETRY_DELAYS, start=1):
            env.outbound.tick()
            row = job_state(env, "turn:g:0")
            assert row["state"] == "unknown", f"attempt {i} state"
            assert row["attempt_count"] == i, f"attempt {i} count"
            assert row["next_attempt_at"] - env.clock.wall_ms() == d, f"attempt {i} backoff"
            env.clock.tick(d + 1)
        env.outbound.tick()                          # 第 6 次 → 耗尽
        row = job_state(env, "turn:g:0")
        assert row["state"] == "failed" and row["attempt_count"] == 6
        assert calls["n"] == 6
        assert "exhausted after 6" in (row["error"] or "")
        alert = _alert_row(env)
        assert alert is not None and alert["kind"] == "session_turn"
        assert alert["chunk_index"] == 0 and "未能确认送达" in alert["body"]

    def test_retryable_send_uses_same_idempotency_key_each_attempt(self, env):
        from lib import util
        self._turn(env)
        seen = []

        def fn(a, c):
            seen.append(a[a.index("--idempotency-key") + 1])
            return network_err_envelope(503)

        env.runner.on_prefix(["im", "+messages-send"], fn)
        for d in _RETRY_DELAYS:
            env.outbound.tick()
            env.clock.tick(d + 1)
        env.outbound.tick()
        assert len(seen) == 6
        assert set(seen) == {util.short_key("turn:g:0")}   # 全程同键 → 服务端去重

    def test_exhausted_turn_unblocks_later_group(self, env):
        """HOL 修复核心:gA 503 耗尽 → failed 后,gB 放行(旧实现会被终态 unknown 永久阻塞)。"""
        bid = env.make_binding(status="active")

        def fn(a, c):
            body = a[a.index("--markdown") + 1]
            return network_err_envelope(503) if body == "AAA" else ok_envelope({"message_id": "om_b"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, binding_id=bid, key="turn:gA:0", turn_group="gA", chunk_index=0, body="AAA")
        mk_job(env, binding_id=bid, key="turn:gB:0", turn_group="gB", chunk_index=0, body="BBB")
        for d in _RETRY_DELAYS:
            env.outbound.tick()
            assert job_state(env, "turn:gB:0")["state"] == "pending"  # gA unknown 期间 gB 被挡
            env.clock.tick(d + 1)
        env.outbound.tick()                          # gA 耗尽→failed;同 tick gB 放行
        assert job_state(env, "turn:gA:0")["state"] == "failed"
        if job_state(env, "turn:gB:0")["state"] != "sent":
            env.outbound.tick()
        assert job_state(env, "turn:gB:0")["state"] == "sent"

    def test_alert_turn_exhaustion_does_not_cascade(self, env):
        """防级联:告警 turn(哨兵 turn_group)自身持续 503 耗尽 → failed,但绝不再生第二条告警。"""
        bid = env.make_binding(status="active")
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: network_err_envelope(503))
        tg = "__sendfail__:jobX"
        mk_job(env, binding_id=bid, key=jobs.key_turn(tg, 0), turn_group=tg, chunk_index=0,
               body="alert")
        for d in _RETRY_DELAYS:
            env.outbound.tick()
            env.clock.tick(d + 1)
        env.outbound.tick()
        row = env.conn.execute("SELECT * FROM outbound_jobs WHERE turn_group=?", (tg,)).fetchone()
        assert row["state"] == "failed"
        assert _alert_count(env) == 1                # 仍只有原告警一条,无级联

    def test_alert_sent_as_markdown_with_key(self, env):
        bid = env.make_binding(status="active")
        mode = {"v": "fail"}

        def fn(a, c):
            if mode["v"] == "fail":
                return network_err_envelope(503)
            return ok_envelope({"message_id": "om_ok"})

        env.runner.on_prefix(["im", "+messages-send"], fn)
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0, body="orig")
        for d in _RETRY_DELAYS:
            env.outbound.tick()
            env.clock.tick(d + 1)
        env.outbound.tick()                          # 耗尽→failed+告警入队
        assert job_state(env, "turn:g:0")["state"] == "failed"
        mode["v"] = "ok"                             # 后端恢复 → 告警应发出
        env.outbound.tick()
        alert_calls = [a for a, _ in env.runner.calls_matching("im", "+messages-send")
                       if "--markdown" in a and "未能确认送达" in a[a.index("--markdown") + 1]]
        assert alert_calls, "告警未以 --markdown 发出"
        a = alert_calls[0]
        assert "--idempotency-key" in a and a[a.index("--chat-id") + 1] == CHAT
        assert "--text" not in a
        assert _alert_row(env)["state"] == "sent"

    def test_permanent_code_failed_without_alert(self, env):
        """对照:永久 code(230002 不在群)→ failed,**不**发告警(渠道本身可能已坏,告警无意义)。"""
        self._turn(env)
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: err_envelope(230002))
        env.outbound.tick()
        assert job_state(env, "turn:g:0")["state"] == "failed"
        assert _alert_count(env) == 0

    def test_crash_loop_bounded_by_startup_scan(self, env):
        """删 tick/_prepare 的 attempt_count 卡后的崩溃循环兜底:sending 崩溃且 attempt_count 达
        硬上限 → startup_scan 失败出局(不无限重臂);上限以下 → 正常重臂 unknown。"""
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='sending', attempt_count=? "
            "WHERE idempotency_key='turn:g:0'", (constants.TURN_RETRYABLE_MAX_ATTEMPTS,))
        env.outbound.startup_scan()
        assert job_state(env, "turn:g:0")["state"] == "failed"
        mk_job(env, binding_id=bid, key="turn:h:0", turn_group="h", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='sending', attempt_count=1 "
            "WHERE idempotency_key='turn:h:0'")
        env.outbound.startup_scan()
        r = job_state(env, "turn:h:0")
        assert r["state"] == "unknown" and r["next_attempt_at"] is not None

    def test_explicit_retryable_false_not_persisted(self, env):
        """codex MAJOR-2:官方 error.retryable=false(如 99991661 token 失效)绝不因本地码表被翻成
        retryable → 走非 retryable 上限(2)后 failed+告警,不刷 6 次。"""
        self._turn(env)
        calls = {"n": 0}

        def fn(a, c):
            calls["n"] += 1
            return FakeRunResult(4, "", json.dumps({"ok": False, "error": {
                "type": "authentication", "code": 99991661, "retryable": False}}))

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()                          # a1 → unknown,非 retryable 平退避 15s
        row = job_state(env, "turn:g:0")
        assert row["state"] == "unknown"
        assert row["next_attempt_at"] - env.clock.wall_ms() == constants.UNKNOWN_RETRY_DELAY_MS
        env.clock.tick(constants.UNKNOWN_RETRY_DELAY_MS + 1)
        env.outbound.tick()                          # a2 → 达上限 2 → failed+告警
        assert calls["n"] == 2
        assert job_state(env, "turn:g:0")["state"] == "failed"
        assert _alert_row(env) is not None

    def test_network_error_without_code_is_retryable(self, env):
        """codex MINOR-2:无 numeric code 的 network 错误信封也应持久重试(判定不只看 code 分支)。"""
        self._turn(env)
        calls = {"n": 0}

        def fn(a, c):
            calls["n"] += 1
            return FakeRunResult(4, "", json.dumps(
                {"ok": False, "error": {"type": "network", "retryable": True}}))   # 无 code

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()
        env.clock.tick(_RETRY_DELAYS[0] + 1)
        env.outbound.tick()
        env.clock.tick(_RETRY_DELAYS[1] + 1)
        env.outbound.tick()                          # 第 3 次 —— 证明持久(非 2 次即停)
        row = job_state(env, "turn:g:0")
        assert calls["n"] == 3 and row["state"] == "unknown" and row["attempt_count"] == 3

    def test_network_type_without_retryable_field_is_retryable(self, env):
        """type=network 但无 retryable 字段、无 code → 靠 network-type 回退仍持久重试。"""
        self._turn(env)
        calls = {"n": 0}

        def fn(a, c):
            calls["n"] += 1
            return FakeRunResult(4, "", json.dumps({"ok": False, "error": {"type": "network"}}))

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()
        env.clock.tick(_RETRY_DELAYS[0] + 1)
        env.outbound.tick()
        env.clock.tick(_RETRY_DELAYS[1] + 1)
        env.outbound.tick()
        assert calls["n"] == 3 and job_state(env, "turn:g:0")["attempt_count"] == 3

    def test_legacy_terminal_unknown_session_turn_rearmed_by_startup(self, env):
        """codex BLOCKER-2:升级前遗留的终态 session_turn unknown(next=NULL)被 startup_scan 重臂,
        交新策略收口 → 不再永久 HOL(建立库级 invariant,不靠手工修复)。"""
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:old:0", turn_group="old", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', attempt_count=2, next_attempt_at=NULL "
            "WHERE idempotency_key='turn:old:0'")
        arm_send_ok(env)
        assert env.outbound.tick() == 0                       # 重臂前:tick 选不中(next IS NULL)
        assert job_state(env, "turn:old:0")["state"] == "unknown"
        env.outbound.startup_scan()
        r = job_state(env, "turn:old:0")
        assert r["state"] == "unknown" and r["next_attempt_at"] is not None
        env.outbound.tick()
        assert job_state(env, "turn:old:0")["state"] == "sent"

    def test_nonsession_crash_at_cap_stays_terminal_unknown(self, env):
        """codex BLOCKER-1:非 session_turn 崩溃于 sending 且达旧上限(2)→ startup_scan 收为终态
        unknown(next=NULL),**行为不变**(不转 failed、不刷到 6)。"""
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) "
            "VALUES('e1','om_1',?,'unbound',0)", (CHAT,))
        mk_job(env, kind="inbound_notice", key="notice:om_1:unbound",
               ref_message_id="om_1", expected_state="unbound", body="x")
        env.conn.execute(
            "UPDATE outbound_jobs SET state='sending', attempt_count=? "
            "WHERE idempotency_key='notice:om_1:unbound'", (constants.MAX_SEND_ATTEMPTS,))
        env.outbound.startup_scan()
        r = job_state(env, "notice:om_1:unbound")
        assert r["state"] == "unknown" and r["next_attempt_at"] is None   # 终态,旧语义不变

    def test_crash_at_cap_session_turn_failed_with_alert(self, env):
        """codex R2 MAJOR-2:session_turn 崩溃于第 6 次发送(sending,ac=6)→ startup_scan 转 failed
        **且发告警**(与耗尽分支一致,不静默丢失)。"""
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='sending', attempt_count=? "
            "WHERE idempotency_key='turn:g:0'", (constants.TURN_RETRYABLE_MAX_ATTEMPTS,))
        env.outbound.startup_scan()
        assert job_state(env, "turn:g:0")["state"] == "failed"
        assert _alert_row(env) is not None and _alert_count(env) == 1

    def test_legacy_nonsession_unknown_at_cap_normalized_to_terminal(self, env):
        """codex R2 MAJOR-1:升级前遗留的非 session_turn unknown(ac=2,next!=NULL,旧 tick cap 令其
        逻辑终态)→ startup_scan 收为真终态 next=NULL,新 tick 不再发第 3 次(保旧「≤2 次」语义)。"""
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,ts) "
            "VALUES('e1','om_1',?,'unbound',0)", (CHAT,))
        mk_job(env, kind="inbound_notice", key="notice:om_1:unbound",
               ref_message_id="om_1", expected_state="unbound", body="x")
        env.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', attempt_count=?, next_attempt_at=? "
            "WHERE idempotency_key='notice:om_1:unbound'",
            (constants.MAX_SEND_ATTEMPTS, env.clock.wall_ms()))
        env.outbound.startup_scan()
        r = job_state(env, "notice:om_1:unbound")
        assert r["state"] == "unknown" and r["next_attempt_at"] is None   # 收为真终态
        assert env.outbound.tick() == 0 and env.runner.calls == []        # 新 tick 不再发

    def test_over_cap_unknown_session_turn_failed_not_rearmed(self, env):
        """codex R3 MINOR:schema 合法的 over-cap unknown session_turn(ac>=6,next=NULL 或 未来)→
        startup_scan 转 failed+告警,**不**被重臂发第 7 次、也不永久 HOL。"""
        bid = env.make_binding(status="active")
        # next=NULL 形态
        mk_job(env, binding_id=bid, key="turn:a:0", turn_group="a", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', attempt_count=?, next_attempt_at=NULL "
            "WHERE idempotency_key='turn:a:0'", (constants.TURN_RETRYABLE_MAX_ATTEMPTS,))
        # next=遥远未来形态(旧 tick cap 曾令其逻辑终态,新 tick 无 cap → 会 HOL / 越限)
        mk_job(env, binding_id=bid, key="turn:b:0", turn_group="b", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', attempt_count=?, next_attempt_at=? "
            "WHERE idempotency_key='turn:b:0'",
            (constants.TURN_RETRYABLE_MAX_ATTEMPTS + 1, env.clock.wall_ms() + 10 ** 9))
        env.outbound.startup_scan()
        assert job_state(env, "turn:a:0")["state"] == "failed"
        assert job_state(env, "turn:b:0")["state"] == "failed"
        assert _alert_count(env) == 2                     # 两条都发告警
        arm_send_ok(env)
        env.outbound.tick()                               # 不会再发这两条(已 failed)
        assert job_state(env, "turn:a:0")["attempt_count"] == constants.TURN_RETRYABLE_MAX_ATTEMPTS
        assert job_state(env, "turn:b:0")["attempt_count"] == constants.TURN_RETRYABLE_MAX_ATTEMPTS + 1

    def test_negative_attempt_count_session_turn_failed_not_sent(self, env):
        """codex R4 M1:schema 合法但代码不可达的负 attempt_count(需外部篡改)→ startup_scan
        fail-closed 转 failed(不发第 7 次、不 HOL),守「六次硬上限对所有 schema 合法态成立」。"""
        bid = env.make_binding(status="active")
        mk_job(env, binding_id=bid, key="turn:neg:0", turn_group="neg", chunk_index=0)
        env.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', attempt_count=-1, next_attempt_at=NULL "
            "WHERE idempotency_key='turn:neg:0'")
        env.outbound.startup_scan()
        assert job_state(env, "turn:neg:0")["state"] == "failed"
        arm_send_ok(env)
        env.outbound.tick()
        assert job_state(env, "turn:neg:0")["attempt_count"] == -1   # 从未发送(未经 _prepare +1)

    def test_prepare_hard_cap_gate_blocks_corrupt_pending(self, env):
        """codex R5:pending(startup_scan 不收口)携异常 attempt_count(篡改/损坏)→ _prepare 硬门
        在发送前 fail-closed,绝不发第 7 次。ac>=6 与 ac<0 都拦,attempt_count 不 +1(证明未发送)。"""
        bid = env.make_binding(status="active")
        arm_send_ok(env)
        mk_job(env, binding_id=bid, key="turn:p6:0", turn_group="p6", chunk_index=0)
        mk_job(env, binding_id=bid, key="turn:pn:0", turn_group="pn", chunk_index=0)
        env.conn.execute("UPDATE outbound_jobs SET attempt_count=? WHERE idempotency_key='turn:p6:0'",
                         (constants.TURN_RETRYABLE_MAX_ATTEMPTS,))
        env.conn.execute("UPDATE outbound_jobs SET attempt_count=-1 WHERE idempotency_key='turn:pn:0'")
        env.outbound.tick()
        p6, pn = job_state(env, "turn:p6:0"), job_state(env, "turn:pn:0")
        assert p6["state"] == "failed" and p6["attempt_count"] == constants.TURN_RETRYABLE_MAX_ATTEMPTS
        assert pn["state"] == "failed" and pn["attempt_count"] == -1   # 未经 _prepare +1 → 从未发送

    def test_prepare_hard_cap_gate_allows_normal_final_attempt(self, env):
        """对照:正常 unknown ac=5 未触发硬门(prepare 入口 ac<6)→ 照常发第 6 次,再由 finalize 耗尽。"""
        bid = env.make_binding(status="active")
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: network_err_envelope(503))
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0)
        env.conn.execute("UPDATE outbound_jobs SET state='unknown', attempt_count=5, next_attempt_at=? "
                         "WHERE idempotency_key='turn:g:0'", (env.clock.wall_ms(),))
        env.outbound.tick()                          # 第 6 次发送(ac 5→6),finalize 耗尽
        row = job_state(env, "turn:g:0")
        assert row["state"] == "failed" and row["attempt_count"] == 6
        assert len(env.runner.calls_matching("im", "+messages-send")) == 1   # 硬门未拦第 6 次

    def test_timeout_with_ok_without_id_is_retryable(self, env):
        """codex R2 MINOR-1:超时且信封 ok 但缺 message_id → 超时信号占先,应 retryable 持久重试
        (曾在 timeout 判断前 return 硬编码非 retryable)。"""
        self._turn(env)
        calls = {"n": 0}

        def fn(a, c):
            calls["n"] += 1
            return FakeRunResult(rc=0, stdout='{"ok":true,"data":{}}', timed_out=True)

        env.runner.on_prefix(["im", "+messages-send"], fn)
        env.outbound.tick()
        env.clock.tick(_RETRY_DELAYS[0] + 1)
        env.outbound.tick()
        env.clock.tick(_RETRY_DELAYS[1] + 1)
        env.outbound.tick()                          # 第 3 次 → 证明持久(非超时误判为 2 次即停)
        assert calls["n"] == 3 and job_state(env, "turn:g:0")["attempt_count"] == 3

    def test_alert_suppressed_when_binding_closed(self, env):
        """告警走 session_turn 的 _binding_active 守卫:群已关 → 告警 cancelled(不外发)。"""
        bid = env.make_binding(status="active")
        env.runner.on_prefix(["im", "+messages-send"], lambda a, c: network_err_envelope(503))
        mk_job(env, binding_id=bid, key="turn:g:0", turn_group="g", chunk_index=0, body="orig")
        for d in _RETRY_DELAYS:
            env.outbound.tick()
            env.clock.tick(d + 1)
        env.outbound.tick()                          # 耗尽→failed+告警入队
        assert _alert_row(env) is not None
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' "
                         "WHERE binding_id=?", (bid,))
        env.outbound.tick()
        assert _alert_row(env)["state"] == "cancelled"   # 守卫拦下,零外发告警
