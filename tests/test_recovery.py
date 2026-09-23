"""恢复工人(contracts §2.9 / §2.10 / §5.2 / §5.6):本地重驱无死线、materializing 按预算重驱、
审批过期单条范围、补卡只在缺 job 时、legacy sending 收口、retention(含 slack_events / unconfirmed /
孤儿 .tmp-*)、lease 回收、孤儿 starting、终态绝不复活。"""
import json
import os
import time

import pytest

from lib import constants, db as dbmod, jobs, lifecycle, texts
from lib.recovery import Recovery
from tests.conftest import CHAT, OWNER
from tests.helpers import envelope, message_event, slack_file
from tests.test_approval import click, member_pending
from tests.test_inbound import ingest, mention, mid_of, none_materializer, ok_materializer


def counter(env, key):
    return int(dbmod.get_state(env.conn, key, "0") or 0)


class TestLocalRedrive:
    def test_received_rows_redriven_regardless_of_age(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("old"), user=OWNER)
        ingest(env, envelope(ev))
        env.conn.execute("UPDATE inbox SET ts=? WHERE message_id=?", (env.clock.wall_ms() - 48 * 3600 * 1000, mid_of(ev)))
        env.clock.tick(24 * 3600 * 1000)
        env.recovery.slow_tick()
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued" and len(env.deliveries(bid)) == 1
        assert env.conn.execute("SELECT COUNT(*) FROM inbox WHERE state='failed'").fetchone()[0] == 0

    def test_resolving_row_from_crash_redriven(self, env):
        env.make_binding(status="active")
        ev = message_event(text=mention("mid-crash"), user=OWNER)
        ingest(env, envelope(ev))
        env.conn.execute("UPDATE inbox SET state='resolving' WHERE message_id=?", (mid_of(ev),))
        env.recovery.slow_tick()
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"

    def test_fast_tick_drives_waiting_after_activation(self, env):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s1",
                               listener_epoch=1, listener_pid=7777, listener_beat_at=env.clock.wall_ms(),
                               confirmed_at=env.clock.wall_ms())
        ev = message_event(text=mention("early"), user=OWNER)
        ingest(env, envelope(ev))
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "waiting_binding"
        env.recovery.fast_tick()
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued" and len(env.deliveries(bid)) == 1


class TestCardReplenish:
    def test_missing_card_job_recreated_with_reply_thread(self, env):
        p, ev = member_pending(env, thread_ts="1699999990.000001")
        env.conn.execute("DELETE FROM outbound_jobs WHERE idempotency_key=?", (jobs.key_card(p["pending_id"]),))
        env.recovery.slow_tick()
        j = env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?",
                             (jobs.key_card(p["pending_id"]),)).fetchone()
        assert j is not None and j["state"] == "pending" and j["reply_to"] == "1699999990.000001"
        body = json.loads(j["body"])
        assert "run task" in body["blocks"][1]["text"]["text"]
        vals = json.loads(body["blocks"][2]["elements"][0]["value"])
        assert vals["nonce"] == p["nonce"] and vals["pending_id"] == p["pending_id"]

    @pytest.mark.parametrize("state", ["failed", "cancelled", "unconfirmed", "sent"])
    def test_existing_terminal_card_job_never_revived(self, env, state):
        p, ev = member_pending(env)
        key = jobs.key_card(p["pending_id"])
        env.conn.execute("UPDATE outbound_jobs SET state=?, attempt_count=3 WHERE idempotency_key=?", (state, key))
        env.recovery.slow_tick()
        env.recovery.slow_tick()
        assert env.conn.execute("SELECT state FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()[0] == state
        assert env.conn.execute("SELECT COUNT(*) FROM outbound_jobs WHERE kind='approval_card'").fetchone()[0] == 1

    def test_no_rearm_helper_exists(self):
        assert not hasattr(Recovery, "_rearm_failed_cards")

    def test_card_message_id_backfilled_from_sent_job(self, env):
        p, ev = member_pending(env)
        env.conn.execute("UPDATE outbound_jobs SET state='sent', sent_message_id='C0CHAT:9.9' "
                         "WHERE idempotency_key=?", (jobs.key_card(p["pending_id"]),))
        env.recovery.slow_tick()
        assert env.conn.execute("SELECT card_message_id FROM pendings WHERE pending_id=?",
                                (p["pending_id"],)).fetchone()[0] == "C0CHAT:9.9"


class TestMaterializingRedrive:
    def _approved_files(self, env):
        p, ev = member_pending(env, files=[slack_file()])
        res, _ = click(env, p)
        assert res[0] == "handed"
        return p, ev

    def test_redrive_succeeds_later_within_budget(self, env):
        """materializing 的重驱走唯一入口 drive_pending_rows(主循环 run_followups);
        slow_tick 只做本地重驱等,**不下载**(R1-m1)。"""
        p, ev = self._approved_files(env)
        calls = []
        env.inbound.materializer = none_materializer(calls)
        env.recovery.slow_tick()
        assert env.inbox_row(mid_of(ev))["state"] == "materializing" and calls == []   # slow_tick 不下载
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "materializing" and len(calls) == 1
        env.inbound.drive_pending_rows()                          # 未到 next_at → 不重试
        env.recovery.slow_tick()
        assert len(calls) == 1
        env.clock.tick(constants.MEDIA_RETRY_BACKOFF_MS)
        env.inbound.materializer = ok_materializer(calls)
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued" and len(env.deliveries()) == 1
        keys = [j["idempotency_key"] for j in env.jobs("decision_notice")]
        assert keys[-1] == "dec:%s:delivered" % p["pending_id"]

    def test_budget_exhausted_with_notice_persists_over_restart(self, env, conn, cfg, clock, client, prober, data_dir):
        from tests.conftest import Env
        p, ev = self._approved_files(env)
        env.inbound.materializer = none_materializer([])
        env.inbound.drive_pending_rows()
        started = env.inbox_row(mid_of(ev))["materialize_started_at"]
        assert started == env.clock.wall_ms()
        env2 = Env(conn, cfg, clock, client, prober, data_dir)       # 重启:预算列不重置
        env2.inbound.materializer = none_materializer([])
        clock.tick(constants.MEDIA_RETRY_DEADLINE_MS + 1)
        env2.recovery.slow_tick()                                    # 不碰 materializing 行
        assert env2.inbox_row(mid_of(ev))["state"] == "materializing"
        env2.inbound.drive_pending_rows()
        row = env2.inbox_row(mid_of(ev))
        assert row["state"] == "failed" and row["materialize_started_at"] == started
        assert [j["idempotency_key"] for j in env2.jobs("decision_notice")][-1] == \
            "dec:%s:attachment_failed" % p["pending_id"]
        assert counter(env2, "media_budget_exhausted") == 1

    def test_slow_tick_never_downloads(self, env):
        """R1-m1:slow_tick 不得领取 followup 预算 —— 到期的 materializing 行由主循环唯一驱动。"""
        p, ev = self._approved_files(env)
        calls = []
        env.inbound.materializer = none_materializer(calls)
        for _ in range(3):
            env.recovery.slow_tick()
            env.clock.tick(constants.RECOVERY_INTERVAL_MS)
        assert calls == [] and env.inbox_row(mid_of(ev))["state"] == "materializing"
        assert not hasattr(env.recovery, "_redrive_materializing")

    def test_terminated_binding_goes_undeliverable(self, env):
        p, ev = self._approved_files(env)
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='cc_gone' WHERE binding_id=?",
                         (p["binding_id"],))
        env.recovery.slow_tick()                                  # 不碰 materializing 行(R1-m1)
        assert env.inbox_row(mid_of(ev))["state"] == "materializing"
        env.inbound.drive_pending_rows()                          # 唯一入口:绑定非 active → 不下载直接收口
        assert env.inbox_row(mid_of(ev))["state"] == "undeliverable"
        assert [j["idempotency_key"] for j in env.jobs("decision_notice")][-1] == \
            "dec:%s:closed_undelivered" % p["pending_id"]


class TestExpirePendings:
    def test_expired_pending_notice_inbox_and_card_cancel(self, env):
        p, ev = member_pending(env)
        key = jobs.key_card(p["pending_id"])
        env.conn.execute("UPDATE outbound_jobs SET state='unknown' WHERE idempotency_key=?", (key,))
        env.clock.tick(constants.PENDING_TTL_MS + 1)
        env.recovery.slow_tick()
        assert env.conn.execute("SELECT state FROM pendings WHERE pending_id=?", (p["pending_id"],)).fetchone()[0] == "expired"
        assert env.inbox_row(mid_of(ev))["state"] == "expired"
        assert env.conn.execute("SELECT state FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()[0] == "cancelled"
        dec = env.jobs("decision_notice")
        assert [j["idempotency_key"] for j in dec] == ["dec:%s:expired" % p["pending_id"]]
        assert dec[0]["reply_to"] == ev["ts"] and dec[0]["body"] == texts.decision_notice_body("expired")
        # 过期后的点击 → late
        assert click(env, p)[0] == ("dropped", "late")

    def test_sent_card_job_not_cancelled_on_expiry(self, env):
        p, ev = member_pending(env)
        key = jobs.key_card(p["pending_id"])
        env.conn.execute("UPDATE outbound_jobs SET state='sent', sent_message_id='C0CHAT:1.1' WHERE idempotency_key=?", (key,))
        env.clock.tick(constants.PENDING_TTL_MS + 1)
        env.recovery._expire_pendings(env.clock.wall_ms())
        assert env.conn.execute("SELECT state FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()[0] == "sent"

    def test_single_scope_isolation(self, env):
        """审批 A 过期不影响已批准附件 B、其它 pending C、绑定的待发输出与 receipt。"""
        bid = env.make_binding(status="active")
        now = env.clock.wall_ms()
        pa, eva = member_pending(env, text="A")
        env.clock.tick(constants.SENDER_COOLDOWN_MS + 1)
        pb, evb = member_pending(env, text="B", files=[slack_file()])
        assert click(env, pb)[0][0] == "handed"                  # B approved → materializing
        env.clock.tick(constants.SENDER_COOLDOWN_MS + 1)
        pc, evc = member_pending(env, text="C", user="U0MEMBER2")
        jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                        idempotency_key="turn:g:0", turn_group="g", chunk_index=0, body="x", now=now)
        jobs.create_job(env.conn, kind="receipt_reaction", chat_id=CHAT, binding_id=bid,
                        idempotency_key="rc:99", ref_delivery_seq=99, body="eyes", now=now)
        env.conn.execute("UPDATE pendings SET created_at=? WHERE pending_id=?",
                         (now - constants.PENDING_TTL_MS - 1, pa["pending_id"]))
        env.recovery._expire_pendings(env.clock.wall_ms())
        states = {p["pending_id"]: p["state"] for p in env.pendings()}
        assert states == {pa["pending_id"]: "expired", pb["pending_id"]: "approved", pc["pending_id"]: "pending"}
        assert env.inbox_row(mid_of(eva))["state"] == "expired"
        assert env.inbox_row(mid_of(evb))["state"] == "materializing"
        assert env.inbox_row(mid_of(evc))["state"] == "awaiting_approval"
        st = {j["idempotency_key"]: j["state"] for j in env.jobs()}
        assert st["turn:g:0"] == "pending" and st["rc:99"] == "pending"
        assert st[jobs.key_card(pa["pending_id"])] == "cancelled"
        assert st[jobs.key_card(pc["pending_id"])] == "pending"
        assert st["dec:%s:approved_pending_files" % pb["pending_id"]] == "pending"
        assert "dec:%s:expired" % pa["pending_id"] in st and "dec:%s:expired" % pc["pending_id"] not in st


class TestLeaseReclaim:
    def _leased(self, env, status="active"):
        bid = env.make_binding(status=status)
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_1','C0CHAT:1.1',?,?,'enqueued',0)", (CHAT, bid))
        env.conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state,lease_token,"
            "lease_epoch,lease_until,attempts) VALUES(?,'C0CHAT:1.1','{}','leased','tok',1,?,1)",
            (bid, env.clock.wall_ms() - 1))
        return bid

    def test_active_binding_reclaims_to_enqueued(self, env):
        self._leased(env, "active")
        env.recovery.slow_tick()
        d = env.deliveries()[0]
        assert d["state"] == "enqueued" and d["lease_token"] is None and d["attempts"] == 1

    def test_terminated_binding_drops(self, env):
        self._leased(env, "closed")
        env.recovery.slow_tick()
        assert env.deliveries()[0]["state"] == "dropped"

    def test_concurrent_unbind_between_read_and_write_drops(self, env):
        from lib import db as dblib, paths
        bid = self._leased(env, "active")
        conn2 = dblib.connect(paths.db_path())
        assert lifecycle.terminate_binding(conn2, bid, "user_unbind", env.clock)
        conn2.close()
        env.recovery.slow_tick()
        assert env.deliveries(bid)[0]["state"] == "dropped"

    def test_stranded_enqueued_on_terminal_binding_swept(self, env):
        bid = env.make_binding(status="closed", close_reason="cc_gone")
        env.conn.execute("INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
                         "VALUES('ev_1','C0CHAT:1.1',?,?,'enqueued',0)", (CHAT, bid))
        env.conn.execute("INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
                         "VALUES(?,'C0CHAT:1.1','{}','enqueued')", (bid,))
        env.recovery.slow_tick()
        assert env.deliveries(bid)[0]["state"] == "dropped"


class TestOrphanStarting:
    def test_unconfirmed_starting_without_pending_row_closed(self, env):
        bid = env.make_binding(status="starting", bind_phase="unconfirmed", session_id=None)
        env.recovery.slow_tick()
        b = env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()
        assert b["status"] == "closed" and b["close_reason"] == "bind_timeout"


class TestLegacySending:
    def _sending(self, env, kind, key, op_method, attempts, age_factor=3):
        b = env.conn.execute("SELECT binding_id FROM bindings WHERE status='active' AND chat_id=?", (CHAT,)).fetchone()
        bid = b[0] if b else env.make_binding(status="active")
        jobs.create_job(env.conn, kind=kind, chat_id=CHAT, binding_id=bid, idempotency_key=key,
                        turn_group=key if kind == "session_turn" else None,
                        chunk_index=0 if kind == "session_turn" else None, body="x",
                        now=env.clock.wall_ms())
        env.conn.execute(
            "UPDATE outbound_jobs SET state='sending', attempt_count=?, sending_at=?, op_method=?, "
            "op_target=?, op_payload_kind='text' WHERE idempotency_key=?",
            (attempts, env.clock.wall_ms() - age_factor * constants.SEND_TIMEOUT_S * 1000, op_method, CHAT, key))

    def _job(self, env, key):
        return env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()

    def test_stale_postmessage_to_unknown_verify_only(self, env):
        self._sending(env, "session_turn", "turn:g:0", "chat.postMessage", 1)
        env.recovery.slow_tick()
        j = self._job(env, "turn:g:0")
        assert j["state"] == "unknown" and j["had_unknown"] == 1
        assert j["verify_after"] == env.clock.wall_ms() and j["next_attempt_at"] is None
        assert j["error"] == "stale-sending"

    def test_unfrozen_op_method_classified_by_kind(self, env):
        self._sending(env, "session_turn", "turn:g:0", None, 1)
        env.conn.execute("UPDATE outbound_jobs SET next_attempt_at=123 WHERE idempotency_key='turn:g:0'")
        env.recovery.slow_tick()
        j = self._job(env, "turn:g:0")
        assert j["state"] == "unknown" and j["had_unknown"] == 1
        assert j["verify_after"] == env.clock.wall_ms() and j["next_attempt_at"] is None
        self._sending(env, "receipt_reaction", "rc:7", None, 1)
        env.recovery.slow_tick()
        j = self._job(env, "rc:7")
        assert j["state"] == "unknown" and j["had_unknown"] == 0 and j["next_attempt_at"] == env.clock.wall_ms()

    def test_stale_idempotent_below_cap_rearmed(self, env):
        self._sending(env, "receipt_reaction", "rc:1", "reactions.add", constants.IDEMPOTENT_CAP - 1)
        env.recovery.slow_tick()
        j = self._job(env, "rc:1")
        assert j["state"] == "unknown" and j["had_unknown"] == 0 and j["next_attempt_at"] == env.clock.wall_ms()

    def test_stale_idempotent_at_cap_failed(self, env):
        self._sending(env, "receipt_reaction", "rc:1", "reactions.add", constants.IDEMPOTENT_CAP)
        env.recovery.slow_tick()
        assert self._job(env, "rc:1")["state"] == "failed"

    def test_fresh_sending_untouched(self, env):
        self._sending(env, "session_turn", "turn:g:0", "chat.postMessage", 1, age_factor=1)
        env.recovery.slow_tick()
        assert self._job(env, "turn:g:0")["state"] == "sending"


class TestRetention:
    def test_trims_terminal_and_keeps_nonterminal(self, env):
        bid = env.make_binding(status="active")
        old = env.clock.wall_ms() - constants.RETENTION_MS - 1000
        for mid, state in (("C0CHAT:old.1", "enqueued"), ("C0CHAT:live.1", "awaiting_approval")):
            env.conn.execute(
                "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts,snapshot_json) "
                "VALUES(?,?,?,?,?,?, '{\"big\":1}')", ("e_" + mid, mid, CHAT, bid, state, old))
            d = env.media_root / bid / mid
            d.mkdir(parents=True)
            (d / "f.bin").write_bytes(b"x")
        for key, state in (("turn:a:0", "sent"), ("turn:b:0", "unconfirmed"), ("turn:c:0", "unknown")):
            jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid, idempotency_key=key,
                            turn_group=key, chunk_index=0, body="secret", now=old)
            env.conn.execute("UPDATE outbound_jobs SET state=? WHERE idempotency_key=?", (state, key))
        env.recovery.slow_tick()
        assert env.inbox_row("C0CHAT:old.1")["snapshot_json"] is None
        assert env.inbox_row("C0CHAT:live.1")["snapshot_json"] is not None
        assert not (env.media_root / bid / "C0CHAT:old.1").exists()
        assert (env.media_root / bid / "C0CHAT:live.1").exists()
        bodies = {r["idempotency_key"]: r["body"] for r in env.jobs("session_turn")}
        assert bodies["turn:a:0"] is None and bodies["turn:b:0"] is None and bodies["turn:c:0"] == "secret"

    def test_slack_events_retention(self, env):
        now = env.clock.wall_ms()
        rows = [
            ("ev:c_old", "consumed", now - constants.SLACK_EVENTS_RETENTION_MS - 1, now - constants.SLACK_EVENTS_RETENTION_MS - 1),
            ("ev:c_new", "consumed", now, now),
            ("ev:q_old", "quarantined", now - constants.SLACK_EVENTS_QUARANTINE_RETENTION_MS - 1, None),
            ("ev:q_new", "quarantined", now - constants.SLACK_EVENTS_RETENTION_MS - 1, None),
            ("ev:s_old", "staged", 0, None),
        ]
        for key, state, received, consumed in rows:
            env.conn.execute(
                "INSERT INTO slack_events(envelope_type,event_key,payload_json,received_at,state,consumed_at) "
                "VALUES('events_api',?,'{}',?,?,?)", (key, received, state, consumed))
        env.recovery.slow_tick()
        left = {r["event_key"] for r in env.slack_events()}
        assert left == {"ev:c_new", "ev:q_new", "ev:s_old"}

    def test_orphan_tmp_dirs_old_removed_fresh_kept(self, env):
        bid = env.make_binding(status="active")
        d = env.media_root / bid
        d.mkdir(parents=True)
        old = d / ".tmp-C0CHAT:1.1-deadbeef"
        old.mkdir()
        (old / "part").write_bytes(b"x")
        past = time.time() - 2 * constants.DOWNLOAD_DEADLINE_S - 60
        os.utime(old, (past, past))
        fresh = d / ".tmp-C0CHAT:2.2-cafebabe"
        fresh.mkdir()
        env.recovery.slow_tick()
        assert not old.exists() and fresh.exists()

    def test_symlinked_media_dir_not_followed(self, env, tmp_path):
        bid = env.make_binding(status="active")
        old = env.clock.wall_ms() - constants.RETENTION_MS - 1000
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('e_s','C0CHAT:sym.1',?,?,'enqueued',?)", (CHAT, bid, old))
        victim = tmp_path / "victim"
        victim.mkdir()
        (victim / "precious.txt").write_bytes(b"keep me")
        (env.media_root / bid).mkdir(parents=True, exist_ok=True)
        os.symlink(victim, env.media_root / bid / "C0CHAT:sym.1")
        env.recovery.slow_tick()
        assert (victim / "precious.txt").exists()
