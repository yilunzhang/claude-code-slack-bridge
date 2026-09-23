"""审批回调(contracts §5.4 / §5.5):process_in_tx 在测试自开的 db.tx 内(不依赖 WP1 drain);
block_actions 矩阵:approve / reject / dup action_ts / non-owner / nonce 不符 / 回填后 card_ts 不符 /
late / card_ts 非法不回填 / 回填把 card job 置 sent(confirmed-by-click)/ team 不符 skipped /
approve 带 files → materializing(approved) + approved_pending_files → 物化后 delivered。"""
import json

import pytest

from lib import approval, db as dbmod, jobs, lifecycle, texts, util
from lib.approval import Approval
from tests.conftest import CHAT, MEMBER, OWNER, TEAM
from tests.helpers import block_action, envelope, message_event, slack_file
from tests.test_inbound import ingest, mention, mid_of, ok_materializer, none_materializer, raising_materializer

CARD_TS = "1700000100.000001"


def member_pending(env, text="run task", files=None, thread_ts=None, chat=CHAT, user=MEMBER):
    if not env.conn.execute("SELECT 1 FROM bindings WHERE status='active' AND chat_id=?", (chat,)).fetchone():
        env.make_binding(status="active", chat_id=chat)
    ev = message_event(text=mention(text), channel=chat, user=user, files=files, thread_ts=thread_ts)
    assert ingest(env, envelope(ev))[0] == "handed"
    env.inbound.drive_pending_rows()
    p = env.pendings()[-1]
    assert p["state"] == "pending" and p["message_id"] == mid_of(ev)
    return p, ev


def click(env, p, act="approve", user=OWNER, card_ts=CARD_TS, action_ts=None, channel=CHAT,
          nonce=None, team=TEAM, **kw):
    payload = block_action(p["pending_id"], nonce or p["nonce"], act=act, user=user, channel=channel,
                           card_ts=card_ts, action_ts=action_ts, team=team, **kw)
    with dbmod.tx(env.conn):
        res = approval.process_in_tx(env.conn, payload)
    assert not env.conn.in_transaction
    return res, payload


def pending_row(env, pid):
    return env.conn.execute("SELECT * FROM pendings WHERE pending_id=?", (pid,)).fetchone()


def cb_count(env):
    return env.conn.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0]


def dec_keys(env):
    return [j["idempotency_key"] for j in env.jobs("decision_notice")]


class TestApproveReject:
    def test_approve_text_single_tx_effects(self, env):
        p, ev = member_pending(env)
        res, payload = click(env, p)
        assert res == ("handed", None)
        pr = pending_row(env, p["pending_id"])
        assert pr["state"] == "approved" and pr["decided_by"] == OWNER
        assert pr["decided_event_id"] == "act:%s:%s:%s:%s:sb_approve:%s" % (
            TEAM, CHAT, CARD_TS, OWNER, payload["actions"][0]["action_ts"])
        assert pr["card_message_id"] == util.message_id_of(CHAT, CARD_TS)   # 点击回填卡片身份
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"
        d = env.deliveries()
        assert len(d) == 1
        pl = json.loads(d[0]["payload_json"])
        assert pl["sender_is_owner"] is False and pl["approved_by"] == OWNER
        assert pl["sender_user_id"] == MEMBER and pl["text"] == "run task"
        dec = env.jobs("decision_notice")
        assert len(dec) == 1
        assert dec[0]["idempotency_key"] == "dec:%s:delivered" % p["pending_id"]
        assert dec[0]["expected_state"] == "delivered" and dec[0]["ref_pending_id"] == p["pending_id"]
        assert dec[0]["reply_to"] == ev["ts"] and dec[0]["ref_message_id"] == mid_of(ev)
        assert dec[0]["body"] == texts.decision_notice_body("delivered")
        assert env.jobs("receipt_reaction") == []                            # 审批路径无 👀
        assert cb_count(env) == 1

    def test_reject(self, env):
        p, ev = member_pending(env)
        assert click(env, p, act="reject")[0] == ("handed", None)
        assert pending_row(env, p["pending_id"])["state"] == "rejected"
        assert env.inbox_row(mid_of(ev))["state"] == "rejected"
        assert env.deliveries() == [] and dec_keys(env) == ["dec:%s:rejected" % p["pending_id"]]

    def test_threaded_message_decision_reply_to_thread(self, env):
        p, ev = member_pending(env, thread_ts="1699999990.000001")
        click(env, p)
        assert env.jobs("decision_notice")[0]["reply_to"] == "1699999990.000001"


class TestMatrix:
    def test_dup_same_action_ts(self, env):
        p, ev = member_pending(env)
        assert click(env, p, action_ts="1700000200.000001")[0] == ("handed", None)
        assert click(env, p, action_ts="1700000200.000001")[0] == ("dropped", "dup")
        assert len(env.deliveries()) == 1 and len(env.jobs("decision_notice")) == 1 and cb_count(env) == 1

    def test_second_click_different_action_ts_is_late(self, env):
        p, ev = member_pending(env)
        assert click(env, p, act="reject")[0] == ("handed", None)
        assert click(env, p, act="approve")[0] == ("dropped", "late")
        assert pending_row(env, p["pending_id"])["state"] == "rejected" and env.deliveries() == []
        assert cb_count(env) == 2

    @pytest.mark.parametrize("mutate,desc", [
        (dict(user=MEMBER), "非 owner 自批"),
        (dict(nonce="deadbeef" * 4), "nonce 不符"),
        (dict(channel="C_OTHER"), "channel 与 inbox.chat_id 不符"),
        (dict(card_ts="not-a-ts"), "card_ts 非法"),
        (dict(card_ts="1.2.3"), "card_ts 非法(多点)"),
    ])
    def test_invalid_no_state_change_no_backfill(self, env, mutate, desc):
        p, ev = member_pending(env)
        res, _ = click(env, p, **mutate)
        assert res == ("dropped", "invalid"), desc
        pr = pending_row(env, p["pending_id"])
        assert pr["state"] == "pending" and pr["card_message_id"] is None
        assert env.deliveries() == [] and env.jobs("decision_notice") == []
        assert env.jobs("approval_card")[0]["state"] == "pending"          # card job 未被动
        assert cb_count(env) == 1                                          # 裸去重记录
        assert env.inbox_row(mid_of(ev))["state"] == "awaiting_approval"

    def test_non_ascii_nonce_invalid_not_crash(self, env):
        p, ev = member_pending(env)
        assert click(env, p, nonce="坏心思 nonce")[0] == ("dropped", "invalid")

    def test_unknown_pending_invalid(self, env):
        env.make_binding(status="active")
        fake = {"pending_id": "no-such", "nonce": "n"}
        assert click(env, fake)[0] == ("dropped", "invalid") and cb_count(env) == 1

    def test_card_ts_mismatch_after_backfill(self, env):
        p, ev = member_pending(env)
        env.conn.execute("UPDATE pendings SET card_message_id=? WHERE pending_id=?",
                         (util.message_id_of(CHAT, "1700000100.000009"), p["pending_id"]))
        assert click(env, p, card_ts="1700000100.000001")[0] == ("dropped", "invalid")
        assert pending_row(env, p["pending_id"])["state"] == "pending"
        assert click(env, p, card_ts="1700000100.000009")[0] == ("handed", None)

    def test_team_mismatch_skipped_with_bare_record(self, env):
        p, ev = member_pending(env)
        assert click(env, p, team="T_OTHER")[0] == ("dropped", "skipped")
        assert pending_row(env, p["pending_id"])["state"] == "pending" and cb_count(env) == 1

    def test_malformed_value_skipped_with_bare_record(self, env):
        p, ev = member_pending(env)
        assert click(env, p, value="garbage")[0] == ("dropped", "skipped")
        assert cb_count(env) == 1
        with dbmod.tx(env.conn):
            assert approval.process_in_tx(env.conn, {"type": "block_actions"}) == ("dropped", "skipped")
        assert cb_count(env) == 1                                          # 无 key 可记

    def test_allowlist_gate(self, env):
        p, ev = member_pending(env)
        env.cfg["chat_allowlist"] = ["C_OTHER"]
        assert click(env, p)[0] == ("dropped", "invalid")
        assert pending_row(env, p["pending_id"])["state"] == "pending" and env.deliveries() == []
        env.cfg["chat_allowlist"] = [CHAT]
        assert click(env, p)[0] == ("handed", None)

    def test_binding_terminated_then_click_is_late(self, env):
        p, ev = member_pending(env)
        lifecycle.terminate_binding(env.conn, p["binding_id"], "user_unbind", env.clock)
        assert click(env, p)[0] == ("dropped", "late")
        assert env.deliveries() == []

    def test_binding_closed_without_cascade_approve_is_undeliverable(self, env):
        """崩溃缝:绑定已 closed 但 pending 仍 pending → approve 通过 CAS,但不可投递 → closed_undelivered。"""
        p, ev = member_pending(env)
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='cc_gone' WHERE binding_id=?",
                         (p["binding_id"],))
        assert click(env, p)[0] == ("handed", None)
        assert pending_row(env, p["pending_id"])["state"] == "approved"
        assert env.inbox_row(mid_of(ev))["state"] == "undeliverable" and env.deliveries() == []
        assert dec_keys(env) == ["dec:%s:closed_undelivered" % p["pending_id"]]


class TestBackfillCardJob:
    @pytest.mark.parametrize("state", ["unknown", "pending", "sending"])
    def test_click_confirms_card_job(self, env, state):
        p, ev = member_pending(env)
        key = jobs.key_card(p["pending_id"])
        env.conn.execute("UPDATE outbound_jobs SET state=?, attempt_count=1 WHERE idempotency_key=?", (state, key))
        assert click(env, p)[0] == ("handed", None)
        j = env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()
        assert j["state"] == "sent" and j["error"] == "confirmed-by-click"
        assert j["sent_message_id"] == util.message_id_of(CHAT, CARD_TS)
        assert pending_row(env, p["pending_id"])["card_message_id"] == util.message_id_of(CHAT, CARD_TS)

    def test_already_sent_card_job_untouched(self, env):
        p, ev = member_pending(env)
        key = jobs.key_card(p["pending_id"])
        env.conn.execute("UPDATE outbound_jobs SET state='sent', sent_message_id='C0CHAT:9.9' "
                         "WHERE idempotency_key=?", (key,))
        env.conn.execute("UPDATE pendings SET card_message_id='C0CHAT:9.9' WHERE pending_id=?", (p["pending_id"],))
        assert click(env, p, card_ts="9.9")[0] == ("handed", None)
        j = env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()
        assert j["state"] == "sent" and j["error"] is None and j["sent_message_id"] == "C0CHAT:9.9"

    def test_invalid_card_ts_leaves_job_and_pending_alone(self, env):
        p, ev = member_pending(env)
        key = jobs.key_card(p["pending_id"])
        env.conn.execute("UPDATE outbound_jobs SET state='unknown' WHERE idempotency_key=?", (key,))
        assert click(env, p, card_ts="bogus")[0] == ("dropped", "invalid")
        j = env.conn.execute("SELECT state FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()
        assert j["state"] == "unknown" and pending_row(env, p["pending_id"])["card_message_id"] is None

    def test_late_click_still_backfills_card_identity(self, env):
        """迟到点击:CAS 失败 → late,但卡片身份已由点击证实(回填先于 CAS)。"""
        p, ev = member_pending(env)
        env.conn.execute("UPDATE pendings SET state='expired' WHERE pending_id=?", (p["pending_id"],))
        assert click(env, p)[0] == ("dropped", "late")
        assert pending_row(env, p["pending_id"])["card_message_id"] == util.message_id_of(CHAT, CARD_TS)


class TestApproveWithFiles:
    def _approve_files(self, env):
        p, ev = member_pending(env, files=[slack_file(id="F1", name="a.pdf", size=3)])
        res, _ = click(env, p)
        assert res[0] == "handed" and res[1] is not None
        row = env.inbox_row(mid_of(ev))
        assert row["state"] == "materializing" and row["materialize_reason"] == "approved"
        assert row["materialize_started_at"] is None and row["materialize_attempts"] == 0
        assert res[1]["message_id"] == row["message_id"] and res[1]["state"] == "materializing"
        assert dec_keys(env) == ["dec:%s:approved_pending_files" % p["pending_id"]]
        assert env.jobs("decision_notice")[0]["expected_state"] == "approved_pending_files"
        assert env.deliveries() == []
        return p, ev, res[1]

    def test_materialize_then_delivered(self, env):
        p, ev, row = self._approve_files(env)
        env.inbound.materializer = ok_materializer()
        assert env.approval.run_followup(row) is True
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"
        d = env.deliveries()
        assert len(d) == 1
        pl = json.loads(d[0]["payload_json"])
        assert pl["approved_by"] == OWNER and pl["media_paths"] and pl["files"][0]["local_path"]
        assert dec_keys(env) == ["dec:%s:approved_pending_files" % p["pending_id"],
                                 "dec:%s:delivered" % p["pending_id"]]
        assert env.jobs("receipt_reaction") == []

    def test_budget_driver_picks_it_up(self, env):
        p, ev, row = self._approve_files(env)
        env.inbound.materializer = ok_materializer()
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"

    def test_media_error_attachment_failed(self, env):
        p, ev, row = self._approve_files(env)
        env.inbound.materializer = raising_materializer
        env.approval.run_followup(row)
        assert env.inbox_row(mid_of(ev))["state"] == "failed" and env.deliveries() == []
        assert dec_keys(env)[-1] == "dec:%s:attachment_failed" % p["pending_id"]

    def test_transient_then_budget_exhausted_attachment_failed(self, env):
        p, ev, row = self._approve_files(env)
        calls = []
        env.inbound.materializer = none_materializer(calls)
        env.approval.run_followup(row)
        assert env.inbox_row(mid_of(ev))["state"] == "materializing"
        while env.inbox_row(mid_of(ev))["state"] == "materializing":
            env.clock.tick(env.inbox_row(mid_of(ev))["materialize_next_at"] - env.clock.wall_ms())
            env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "failed"
        assert dec_keys(env)[-1] == "dec:%s:attachment_failed" % p["pending_id"]

    def test_unbind_while_materializing_closed_undelivered(self, env):
        p, ev, row = self._approve_files(env)
        env.inbound.materializer = none_materializer([])
        env.approval.run_followup(row)
        lifecycle.terminate_binding(env.conn, p["binding_id"], "user_unbind", env.clock)
        assert env.inbox_row(mid_of(ev))["state"] == "undeliverable"
        assert dec_keys(env)[-1] == "dec:%s:closed_undelivered" % p["pending_id"]
        st = {j["idempotency_key"]: j["state"] for j in env.jobs("decision_notice")}
        assert st["dec:%s:approved_pending_files" % p["pending_id"]] == "cancelled"
        env.inbound.drive_pending_rows()                                   # 不再物化、不再改状态
        assert env.inbox_row(mid_of(ev))["state"] == "undeliverable"

    def test_binding_closed_before_download_closed_undelivered(self, env):
        p, ev, row = self._approve_files(env)
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='cc_gone' WHERE binding_id=?",
                         (p["binding_id"],))
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "undeliverable" and calls == []
        assert dec_keys(env)[-1] == "dec:%s:closed_undelivered" % p["pending_id"]


class TestClassShape:
    def test_module_function_without_registered_inbound_fails_closed(self, env, monkeypatch):
        p, ev = member_pending(env)
        monkeypatch.setitem(approval._DEFAULTS, "inbound", None)
        payload = block_action(p["pending_id"], p["nonce"], user=OWNER, channel=CHAT, card_ts=CARD_TS)
        with pytest.raises(RuntimeError, match="no Inbound registered"):
            with dbmod.tx(env.conn):
                approval.process_in_tx(env.conn, payload)
        assert pending_row(env, p["pending_id"])["state"] == "pending"      # 随事务回滚,零副作用
        assert cb_count(env) == 0

    def test_frozen_constructor_and_delegation(self, env):
        a = Approval(env.conn, env.cfg, env.clock, env.inbound)
        p, ev = member_pending(env)
        payload = block_action(p["pending_id"], p["nonce"], user=OWNER, channel=CHAT, card_ts=CARD_TS)
        with dbmod.tx(env.conn):
            assert a.process_in_tx(env.conn, payload) == ("handed", None)
        assert a.run_followup(None) is False
