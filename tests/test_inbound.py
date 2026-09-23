"""入站管线(contracts §1 / §5.1 / §5.2 / §4.3):ingest_in_tx 在真实 SQLite 事务内(测试自开 db.tx,
不依赖 WP1 drain_staging)→ drive_pending_rows 本地分流 / 预算物化。全程离线。"""
import json
import pathlib

import pytest

from lib import constants, db as dbmod, inbound, lifecycle, media, util
from tests.conftest import APP_ID, BOT_ID, BOT_USER, CHAT, DM, MEMBER, OWNER, TEAM, Env
from tests.helpers import (app_mention_event, envelope, message_event, rich_text_mention_blocks,
                           slack_file)


def ingest(env, payload, received_at=None):
    """像 drain 那样:stage(钉 binding)→ 单事务内 ingest_in_tx。"""
    row = env.stage("events_api", payload, received_at=received_at)
    with dbmod.tx(env.conn):
        res = inbound.ingest_in_tx(env.conn, row)
    assert not env.conn.in_transaction
    return res


def receive(env, event, event_id=None, drive=True, **envkw):
    res = ingest(env, envelope(event, event_id=event_id, **envkw))
    if drive:
        env.inbound.drive_pending_rows()
    return res


def mid_of(ev):
    return util.message_id_of(ev["channel"], ev["ts"])


def mention(text=""):
    return ("<@%s> %s" % (BOT_USER, text)).rstrip()


def counter(env, key):
    return int(dbmod.get_state(env.conn, key, "0") or 0)


def ok_materializer(calls=None):
    def fn(client_tokens, media_root, binding_id, message_id, files, **kw):
        if calls is not None:
            calls.append(message_id)
        plan = media.file_plan(files)
        d = pathlib.Path(media_root) / binding_id / message_id
        paths = []
        for e in plan:
            if e["skip"] is None:
                d.mkdir(parents=True, exist_ok=True)
                p = d / e["dest_name"]
                p.write_bytes(b"x")
                paths.append(str(p))
        return sorted(paths), media.skipped_of(plan)
    return fn


def none_materializer(calls):
    def fn(**kw):
        calls.append(kw)
        return None
    return fn


def raising_materializer(**kw):
    raise media.MediaError("deterministic")


# ====================================================================== drop 序
class TestDropOrder:
    def test_foreign_team_and_app(self, env):
        env.make_binding(status="active")
        assert receive(env, message_event(user=OWNER), team_id="T_X") == ("dropped", "foreign_team")
        assert receive(env, message_event(user=OWNER), api_app_id="A_X") == ("dropped", "foreign_app")
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0
        assert counter(env, "event_dropped_foreign_team") == 1
        assert counter(env, "event_dropped_foreign_app") == 1

    def test_event_type_not_message(self, env):
        env.make_binding(status="active")
        ev = message_event(user=OWNER)
        ev["type"] = "reaction_added"
        assert receive(env, ev) == ("dropped", "event_type")

    def test_self_event_three_identities(self, env):
        env.make_binding(status="active")
        assert receive(env, message_event(user=OWNER, bot_id=BOT_ID)) == ("dropped", "self")
        assert receive(env, message_event(user=BOT_USER)) == ("dropped", "self")
        assert receive(env, message_event(user=OWNER, app_id=APP_ID)) == ("dropped", "self")
        assert counter(env, "event_dropped_self") == 3

    def test_self_event_none_safety(self, env):
        """cfg 有 app_id 但事件无 bot_profile/app_id/bot_id → 不是自身;cfg 缺值 → 绝不把所有消息判自身。"""
        env.make_binding(status="active")
        ev = message_event(text=mention("hi"), user=OWNER)
        assert ev.get("app_id") is None and ev.get("bot_id") is None
        assert receive(env, ev)[0] == "handed"
        env.cfg["bot_id"] = None                   # cfg 缺 bot_id:绝不把所有消息判自身
        ev2 = message_event(text=mention("hi"), user=OWNER)
        assert receive(env, ev2)[0] == "handed"
        from lib import slackwire
        assert slackwire.is_self_event({"user": None, "bot_id": None, "app_id": None},
                                       {"bot_user_id": None, "bot_id": None, "app_id": None}) is False
        # 另一 bot 的消息(bot_id 不同)不是自身
        assert receive(env, message_event(text=mention("x"), user=OWNER, bot_id="B_OTHER"))[0] == "handed"

    @pytest.mark.parametrize("subtype,expect", [
        (None, "handed"), ("file_share", "handed"), ("thread_broadcast", "handed"),
        ("message_changed", "dropped"), ("message_deleted", "dropped"), ("bot_message", "dropped"),
        ("channel_join", "dropped"), ("message_replied", "dropped"),
    ])
    def test_subtype_matrix(self, env, subtype, expect):
        env.make_binding(status="active")
        ev = message_event(text=mention("hi"), user=OWNER, subtype=subtype)
        res = receive(env, ev, drive=False)
        assert res[0] == expect
        if expect == "dropped":
            assert res[1] == "subtype"

    def test_chat_allowlist_zero_trace(self, env):
        env.cfg["chat_allowlist"] = ["C_ALLOWED_ONLY"]
        env.make_binding(status="active")
        assert receive(env, message_event(text=mention("hi"), user=OWNER)) == ("dropped", "chat_not_allowed")
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0
        assert env.jobs() == []
        env.cfg["chat_allowlist"] = []
        assert receive(env, message_event(text=mention("hi"), user=OWNER))[0] == "handed"

    def test_prefilter_unbound_not_mentioned_not_dm(self, env):
        assert receive(env, message_event(text="hello", user=OWNER)) == ("dropped", "not_mentioned_unbound")
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0
        # DM 无需提及;提及也放行(之后由本地决策给 unbound 提示)
        assert receive(env, message_event(text="hello", channel=DM, user=OWNER))[0] == "handed"
        assert receive(env, message_event(text=mention("hello"), user=OWNER))[0] == "handed"

    def test_cap_blocks_member_not_owner(self, env, monkeypatch):
        monkeypatch.setattr(constants, "INBOX_NONTERMINAL_CAP", 2)
        bid = env.make_binding(status="active")
        for i in range(2):
            env.conn.execute(
                "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
                "VALUES(?,?,?,?,'awaiting_approval',0)", ("Evfill%d" % i, "%s:9.%d" % (CHAT, i), CHAT, bid))
        assert receive(env, message_event(text=mention("x"), user=MEMBER), drive=False) == ("dropped", "cap")
        assert receive(env, message_event(text=mention("x"), user=OWNER), drive=False)[0] == "handed"

    def test_invalid_payload_shapes(self, env):
        env.make_binding(status="active")
        ev = message_event(text=mention("x"), user=OWNER)
        del ev["ts"]
        row = env.stage("events_api", envelope(ev, event_id="EvNoTs"))
        with dbmod.tx(env.conn):
            assert inbound.ingest_in_tx(env.conn, row) == ("dropped", "invalid")
        row2 = env.stage("events_api", {"event_id": "EvNoEvent", "team_id": TEAM, "api_app_id": APP_ID})
        with dbmod.tx(env.conn):
            assert inbound.ingest_in_tx(env.conn, row2) == ("dropped", "invalid")


# ====================================================================== 插入 / 去重 / 双投
class TestInsertAndDedupe:
    def test_inserted_row_shape(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("hi"), user=OWNER, thread_ts="1699999999.000001")
        res = receive(env, ev, event_id="EvShape", drive=False)
        assert res[0] == "handed"
        row = env.inbox_row(mid_of(ev))
        assert dict(row)["event_id"] == "EvShape" and row["binding_id"] == bid
        assert row["chat_id"] == CHAT and row["sender_user_id"] == OWNER and row["sender_type"] == "user"
        assert row["message_type"] == "message" and row["state"] == "received"
        assert row["thread_ts"] == "1699999999.000001" and row["reply_thread_ts"] == "1699999999.000001"
        assert json.loads(row["snapshot_json"]) == ev
        assert res[1]["message_id"] == mid_of(ev)

    def test_reply_thread_ts_defaults_to_ts(self, env):
        env.make_binding(status="active")
        ev = message_event(text=mention("hi"), user=OWNER)
        receive(env, ev, drive=False)
        row = env.inbox_row(mid_of(ev))
        assert row["thread_ts"] is None and row["reply_thread_ts"] == ev["ts"]

    def test_same_event_id_redelivery_resumes_single_row(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("hi"), user=OWNER)
        assert receive(env, ev, event_id="EvSame", drive=False)[0] == "handed"
        # Slack 重投同一 event_id(consumer 会 ON CONFLICT 去重;这里直接对同一行再 ingest)
        row = env.conn.execute("SELECT * FROM slack_events WHERE event_key='ev:EvSame'").fetchone()
        with dbmod.tx(env.conn):
            res = inbound.ingest_in_tx(env.conn, row)
        assert res[0] == "handed" and res[1]["message_id"] == mid_of(ev)
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 1
        env.inbound.drive_pending_rows()
        env.inbound.drive_pending_rows()
        assert len(env.deliveries(bid)) == 1
        assert counter(env, "inbox_dup_message") == 0

    def test_double_delivery_both_orders_and_upgrade(self, env):
        env.make_binding(status="active")
        # mention 先到 → message(带 files/blocks)升级快照
        ts1 = "1700000001.000100"
        a = app_mention_event(text=mention("look"), user=OWNER, ts=ts1)
        m = message_event(text=mention("look"), user=OWNER, ts=ts1, files=[slack_file(id="F1")])
        assert ingest(env, envelope(a, event_id="EvA1"))[0] == "handed"
        res = ingest(env, envelope(m, event_id="EvM1"))
        assert res[0] == "handed" and res[1]["message_type"] == "message"
        row = env.inbox_row("%s:%s" % (CHAT, ts1))
        assert json.loads(row["snapshot_json"]) == m and row["state"] == "received"
        assert counter(env, "inbox_snapshot_upgraded") == 1
        # message 先到 → 之后的 app_mention 是 dup
        ts2 = "1700000002.000100"
        m2 = message_event(text=mention("look"), user=OWNER, ts=ts2, files=[slack_file(id="F2")])
        a2 = app_mention_event(text=mention("look"), user=OWNER, ts=ts2)
        assert ingest(env, envelope(m2, event_id="EvM2"))[0] == "handed"
        assert ingest(env, envelope(a2, event_id="EvA2")) == ("dropped", "dup_message")
        assert json.loads(env.inbox_row("%s:%s" % (CHAT, ts2))["snapshot_json"]) == m2
        assert counter(env, "inbox_dup_message") == 1 and counter(env, "event_dropped_dup_message") == 1
        # 两个 event_id 都不再新建行
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 2

    def test_upgrade_requires_files_or_blocks(self, env):
        env.make_binding(status="active")
        ts = "1700000003.000100"
        a = app_mention_event(text=mention("plain"), user=OWNER, ts=ts)
        m = message_event(text=mention("plain"), user=OWNER, ts=ts)
        ingest(env, envelope(a, event_id="EvA3"))
        assert ingest(env, envelope(m, event_id="EvM3")) == ("dropped", "dup_message")
        assert env.inbox_row("%s:%s" % (CHAT, ts))["message_type"] == "app_mention"

    def test_no_replacement_after_approval_or_enqueue(self, env):
        env.make_binding(status="active")
        ts = "1700000004.000100"
        a = app_mention_event(text=mention("pls"), user=MEMBER, ts=ts)
        receive(env, a, event_id="EvA4")                      # → awaiting_approval
        mid = "%s:%s" % (CHAT, ts)
        assert env.inbox_row(mid)["state"] == "awaiting_approval"
        m = message_event(text=mention("pls"), user=MEMBER, ts=ts, files=[slack_file(id="F9")])
        assert ingest(env, envelope(m, event_id="EvM4")) == ("dropped", "dup_message")
        row = env.inbox_row(mid)
        assert row["state"] == "awaiting_approval" and '"files"' not in row["snapshot_json"]
        assert counter(env, "inbox_snapshot_upgraded") == 0
        # enqueued 行同样不替换
        ts2 = "1700000005.000100"
        receive(env, app_mention_event(text=mention("go"), user=OWNER, ts=ts2), event_id="EvA5")
        assert env.inbox_row("%s:%s" % (CHAT, ts2))["state"] == "enqueued"
        m2 = message_event(text=mention("go"), user=OWNER, ts=ts2, blocks=rich_text_mention_blocks(BOT_USER))
        assert ingest(env, envelope(m2, event_id="EvM5")) == ("dropped", "dup_message")

    def test_binding_pinned_at_receive_not_at_drive(self, env):
        ev = message_event(text=mention("hi"), user=OWNER)
        payload = envelope(ev, event_id="EvPin")
        staged = env.stage("events_api", payload)                # 到达时无绑定
        assert staged["binding_id"] is None
        bid = env.make_binding(status="active")                  # 之后才 bind
        with dbmod.tx(env.conn):
            inbound.ingest_in_tx(env.conn, staged)
        env.inbound.drive_pending_rows()
        row = env.inbox_row(mid_of(ev))
        assert row["binding_id"] is None and row["state"] == "unbound"
        assert env.deliveries(bid) == [] and len(env.jobs("inbound_notice")) == 1

    def test_ingest_never_opens_transaction_and_is_atomic_with_caller(self, env):
        env.make_binding(status="active")
        row = env.stage("events_api", envelope(message_event(text=mention("x"), user=OWNER), event_id="EvTx"))
        try:
            with dbmod.tx(env.conn):
                res = inbound.ingest_in_tx(env.conn, row)
                assert res[0] == "handed" and env.conn.in_transaction
                raise RuntimeError("simulated drain crash")
        except RuntimeError:
            pass
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0   # 随调用方回滚


# ====================================================================== 提及 / DM
class TestMentionGate:
    def test_dm_without_mention_delivered(self, env):
        bid = env.make_binding(status="active", chat_id=DM)
        ev = message_event(text="just text", channel=DM, user=OWNER)
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"
        assert json.loads(env.deliveries(bid)[0]["payload_json"])["text"] == "just text"

    def test_channel_without_mention_ignored_and_trimmed(self, env):
        env.make_binding(status="active")
        ev = message_event(text="no mention here", user=OWNER)
        receive(env, ev)
        row = env.inbox_row(mid_of(ev))
        assert row["state"] == "ignored_not_mentioned"
        assert "no mention" not in (row["snapshot_json"] or "") and env.deliveries() == []

    def test_mention_via_rich_text_blocks(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text="hi there", user=OWNER, blocks=rich_text_mention_blocks(BOT_USER, " hi there"))
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"
        assert json.loads(env.deliveries(bid)[0]["payload_json"])["text"] == "hi there"

    def test_mention_via_text_with_name_suffix(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text="<@%s|bridgebot> fix &lt;it&gt;" % BOT_USER, user=OWNER)
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued"
        assert json.loads(env.deliveries(bid)[0]["payload_json"])["text"] == "fix <it>"

    def test_other_user_mention_is_not_ours(self, env):
        env.make_binding(status="active")
        ev = message_event(text="<@U0OTHER> hi", user=OWNER, blocks=rich_text_mention_blocks("U0OTHER"))
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "ignored_not_mentioned"

    def test_app_mention_event_delivered(self, env):
        bid = env.make_binding(status="active")
        ev = app_mention_event(text=mention("do it"), user=OWNER)
        receive(env, ev)
        p = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert p["text"] == "do it" and p["message_type"] == "app_mention"


# ====================================================================== 未绑定 / 已关闭
class TestUnboundNotices:
    def test_unbound_notice_with_cooldown(self, env):
        evs = [message_event(text=mention("a"), channel="C_U", user=OWNER) for _ in range(3)]
        receive(env, evs[0])
        receive(env, evs[1])
        jobs = env.jobs("inbound_notice")
        assert len(jobs) == 1
        assert jobs[0]["idempotency_key"] == "notice:%s:unbound" % mid_of(evs[0])
        assert jobs[0]["expected_state"] == "unbound" and jobs[0]["ref_message_id"] == mid_of(evs[0])
        assert env.inbox_row(mid_of(evs[1]))["state"] == "unbound"
        env.clock.tick(constants.NOTICE_COOLDOWN_MS + 1)
        receive(env, evs[2])
        assert len(env.jobs("inbound_notice")) == 2

    def test_session_closed_notice(self, env):
        """到达时钉住 active 绑定,决策前绑定判死 → session_closed(dead 绑定不会被 consumer 钉住)。"""
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("a"), user=OWNER)
        receive(env, ev, drive=False)
        env.conn.execute("UPDATE bindings SET status='dead', close_reason='listener_gone' WHERE binding_id=?", (bid,))
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "session_closed"
        j = env.jobs("inbound_notice")[0]
        assert j["idempotency_key"] == "notice:%s:session_closed" % mid_of(ev)

    def test_dm_notice_suppressed_for_non_owner(self, env):
        ev = message_event(text="hello bot", channel="D_STRANGER", user=MEMBER)
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "unbound"
        assert env.jobs("inbound_notice") == [] and counter(env, "dm_notice_suppressed") == 1
        ev2 = message_event(text="hello bot", channel=DM, user=OWNER)
        receive(env, ev2)
        assert len(env.jobs("inbound_notice")) == 1

    def test_old_binding_message_never_drifts_to_new_binding(self, env):
        old = env.make_binding(status="active")
        ev = message_event(text=mention("old"), user=OWNER)
        receive(env, ev, drive=False)
        lifecycle.terminate_binding(env.conn, old, "user_unbind", env.clock)
        new = env.make_binding(status="active", session_id="sess-2", cc_pid=5555,
                               cc_start="Wed Jul 15 08:00:00 2026")
        env.inbound.drive_pending_rows()
        row = env.inbox_row(mid_of(ev))
        assert row["binding_id"] == old and row["state"] == "unbound"
        assert env.deliveries(new) == []

    def test_redrive_never_fails_on_age(self, env):
        """received 行搁置很久后重驱仍正常投递(无 10 分钟死线)。"""
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("late"), user=OWNER)
        receive(env, ev, drive=False)
        env.clock.tick(3 * 3600 * 1000)
        env.recovery.slow_tick()
        assert env.inbox_row(mid_of(ev))["state"] == "enqueued" and len(env.deliveries(bid)) == 1


# ====================================================================== active 门禁
class TestActiveGate:
    def test_owner_text_direct_delivery_payload_and_receipt(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("修一下 bug"), user=OWNER)
        receive(env, ev)
        mid = mid_of(ev)
        assert env.inbox_row(mid)["state"] == "enqueued"
        d = env.deliveries(bid)[0]
        p = json.loads(d["payload_json"])
        assert p == {"message_id": mid, "chat_id": CHAT, "ts": ev["ts"], "thread_ts": None,
                     "sender_user_id": OWNER, "sender_is_owner": True, "approved_by": None,
                     "message_type": "message", "text": "修一下 bug", "media_paths": [], "files": []}
        rc = env.jobs("receipt_reaction")
        assert len(rc) == 1 and rc[0]["ref_delivery_seq"] == d["delivery_seq"]
        assert rc[0]["idempotency_key"] == "rc:%d" % d["delivery_seq"]
        assert rc[0]["body"] == constants.RECEIPT_REACTION == "eyes"
        assert rc[0]["ref_message_id"] == mid and rc[0]["chat_id"] == CHAT

    def test_member_text_goes_to_approval_card(self, env):
        bid = env.make_binding(status="active")
        ev = message_event(text=mention("帮我跑个脚本"), user=MEMBER)
        receive(env, ev)
        mid = mid_of(ev)
        assert env.inbox_row(mid)["state"] == "awaiting_approval"
        p = env.pendings()[0]
        assert p["state"] == "pending" and p["message_id"] == mid and p["binding_id"] == bid
        assert p["card_message_id"] is None
        cards = env.jobs("approval_card")
        assert len(cards) == 1
        card = cards[0]
        assert card["reply_to"] == ev["ts"] and card["idempotency_key"] == "card:%s" % p["pending_id"]
        assert card["expected_state"] == "pending" and card["ref_pending_id"] == p["pending_id"]
        body = json.loads(card["body"])
        vals = json.loads(body["blocks"][2]["elements"][0]["value"])
        assert vals == {"pending_id": p["pending_id"], "nonce": p["nonce"], "act": "approve"}
        assert "帮我跑个脚本" in body["blocks"][1]["text"]["text"]
        assert env.deliveries() == [] and env.jobs("receipt_reaction") == []

    def test_threaded_member_message_card_reply_to_thread(self, env):
        env.make_binding(status="active")
        ev = message_event(text=mention("in thread"), user=MEMBER, thread_ts="1699999990.000001")
        receive(env, ev)
        row = env.inbox_row(mid_of(ev))
        assert row["reply_thread_ts"] == "1699999990.000001"
        assert env.jobs("approval_card")[0]["reply_to"] == "1699999990.000001"

    def test_unsupported_only_when_blocks_without_text_or_files(self, env):
        env.make_binding(status="active", chat_id=DM)
        ev = message_event(text="", channel=DM, user=OWNER, blocks=[{"type": "section", "block_id": "x"}])
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "unsupported"
        j = env.jobs("unsupported_notice")
        assert len(j) == 1 and j[0]["idempotency_key"] == "un:%s" % mid_of(ev)
        assert j[0]["reply_to"] == ev["ts"] and j[0]["ref_message_id"] == mid_of(ev)
        ev2 = message_event(text="", channel=DM, user=OWNER)          # 无 blocks:不算 unsupported
        receive(env, ev2)
        assert env.inbox_row(mid_of(ev2))["state"] == "enqueued"

    def test_owner_files_materialize_then_deliver(self, env):
        bid = env.make_binding(status="active")
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        files = [slack_file(id="F1", name="a.pdf", size=10),
                 slack_file(id="F2", name="big.zip", mimetype="application/zip", size=constants.MEDIA_FILE_MAX_BYTES + 1)]
        ev = message_event(text=mention("look"), user=OWNER, files=files)
        receive(env, ev, drive=False)
        env.inbound.drive_local_rows()
        row = env.inbox_row(mid_of(ev))
        assert row["state"] == "materializing" and row["materialize_reason"] == "owner"
        assert row["materialize_started_at"] is None            # 首次实际尝试才写
        assert env.deliveries() == []
        stats = env.inbound.drive_pending_rows()
        assert stats["downloads"] == 1 and calls == [mid_of(ev)]
        row = env.inbox_row(mid_of(ev))
        assert row["state"] == "enqueued"
        p = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert len(p["media_paths"]) == 1 and p["media_paths"][0].endswith("/a.pdf")
        assert p["files"][0]["local_path"] == p["media_paths"][0] and p["files"][0]["id"] == "F1"
        assert p["files"][1] == {"id": "F2", "name": "big.zip", "mimetype": "application/zip",
                                 "size": constants.MEDIA_FILE_MAX_BYTES + 1, "skipped_reason": "too_large"}
        assert p["approved_by"] is None and p["sender_is_owner"] is True
        assert len(env.jobs("receipt_reaction")) == 1

    def test_text_only_materializing_uses_text_budget(self, env):
        bid = env.make_binding(status="active")
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        ev = message_event(text=mention("hidden"), user=OWNER, files=[slack_file(id="H", hidden_by_limit=True)])
        receive(env, ev, drive=False)
        stats = env.inbound.drive_pending_rows(budget=(0, 5))
        assert stats["text_only"] == 1 and stats["downloads"] == 0
        p = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert p["media_paths"] == [] and p["files"][0]["skipped_reason"] == "hidden_by_limit"

    def test_member_files_not_materialized_before_approval(self, env):
        env.make_binding(status="active")
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        ev = message_event(text=mention("pic"), user=MEMBER, files=[slack_file()])
        receive(env, ev)
        assert env.inbox_row(mid_of(ev))["state"] == "awaiting_approval" and calls == []

    def test_allowlist_member_files_marked_allowlist(self, env):
        from lib import senderallow
        bid = env.make_binding(status="active")
        senderallow.add_entry(CHAT, MEMBER)
        env.inbound.materializer = ok_materializer()
        ev = message_event(text=mention("img"), user=MEMBER, files=[slack_file()])
        receive(env, ev, drive=False)
        env.inbound.drive_local_rows()
        assert env.inbox_row(mid_of(ev))["materialize_reason"] == "allowlist"
        env.inbound.drive_pending_rows()
        p = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert p["approved_by"] == "allowlist" and p["sender_is_owner"] is False and p["media_paths"]
        assert env.pendings() == [] and len(env.jobs("receipt_reaction")) == 1

    def test_heartbeat_after_each_materialize(self, env):
        env.make_binding(status="active")
        beats = []
        env.inbound.heartbeat = lambda: beats.append(1)
        env.inbound.materializer = ok_materializer()
        for _ in range(2):
            receive(env, message_event(text=mention("f"), user=OWNER, files=[slack_file()]), drive=False)
        env.inbound.drive_pending_rows(budget=(5, 5))
        assert len(beats) == 2


# ====================================================================== 附件预算(§5.2)
class TestMaterializeBudget:
    def _owner_files(self, env, n=1):
        evs = []
        for _ in range(n):
            ev = message_event(text=mention("f"), user=OWNER, files=[slack_file()])
            receive(env, ev, drive=False)
            evs.append(ev)
        env.inbound.drive_local_rows()
        return evs

    def test_transient_failures_backoff_then_budget_exhausted_persisted(self, env, conn, cfg, clock, client,
                                                                      prober, data_dir):
        env.make_binding(status="active")
        calls = []
        env.inbound.materializer = none_materializer(calls)
        ev = self._owner_files(env)[0]
        mid = mid_of(ev)
        t0 = env.clock.wall_ms()
        env.inbound.drive_pending_rows()
        row = env.inbox_row(mid)
        assert row["state"] == "materializing" and row["materialize_started_at"] == t0
        assert row["materialize_attempts"] == 1
        assert row["materialize_next_at"] == t0 + constants.MEDIA_RETRY_BACKOFF_MS
        env.inbound.drive_pending_rows()                        # 未到期 → 不尝试
        assert len(calls) == 1
        delays = []
        cur = env
        while True:
            row = cur.inbox_row(mid)
            if row["state"] != "materializing":
                break
            cur.clock.tick(row["materialize_next_at"] - cur.clock.wall_ms())
            if row["materialize_attempts"] == 3:
                cur = Env(conn, cfg, clock, client, prober, data_dir)   # "重启":预算在列上
                cur.inbound.materializer = none_materializer(calls)
            before = row["materialize_next_at"]
            cur.inbound.drive_pending_rows()
            after = cur.inbox_row(mid)
            if after["state"] == "materializing":
                delays.append(after["materialize_next_at"] - before)
        assert cur.inbox_row(mid)["state"] == "failed"
        assert cur.clock.wall_ms() - t0 > constants.MEDIA_RETRY_DEADLINE_MS
        assert delays[:5] == [20_000, 40_000, 80_000, 160_000, 300_000]   # min(10s·2^n, 5min)
        assert max(delays) == constants.MEDIA_RETRY_BACKOFF_MAX_MS
        assert counter(cur, "media_budget_exhausted") == 1
        assert cur.jobs() == [] and cur.deliveries() == []                 # 静默

    def test_media_error_is_terminal_immediately(self, env):
        env.make_binding(status="active")
        env.inbound.materializer = raising_materializer
        ev = self._owner_files(env)[0]
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "failed"
        assert counter(env, "media_failed") == 1 and env.jobs() == []

    def test_budget_per_tick_downloads_and_text(self, env):
        env.make_binding(status="active")
        env.inbound.materializer = ok_materializer()
        for _ in range(3):
            receive(env, message_event(text=mention("dl"), user=OWNER, files=[slack_file()]), drive=False)
        for _ in range(7):
            receive(env, message_event(text=mention("t"), user=OWNER, files=[slack_file(mode="tombstone")]),
                    drive=False)
        s1 = env.inbound.drive_pending_rows()
        assert (s1["downloads"], s1["text_only"], s1["skipped_budget"]) == (1, 5, 4)
        assert env.conn.execute("SELECT COUNT(*) FROM inbox WHERE state='enqueued'").fetchone()[0] == 6
        s2 = env.inbound.drive_pending_rows()
        assert (s2["downloads"], s2["text_only"]) == (1, 2)
        s3 = env.inbound.drive_pending_rows()
        assert (s3["downloads"], s3["text_only"]) == (1, 0)
        assert env.conn.execute("SELECT COUNT(*) FROM inbox WHERE state='materializing'").fetchone()[0] == 0

    def test_worker_unexpected_exit_counter_bubbles(self, env):
        env.make_binding(status="active")

        def fn(**kw):
            kw["stats"]["worker_unexpected_exit"] = 1
            return None
        env.inbound.materializer = fn
        self._owner_files(env)
        env.inbound.drive_pending_rows()
        assert counter(env, "worker_unexpected_exit") == 1

    def test_binding_terminated_before_attempt_maps_without_download(self, env):
        bid = env.make_binding(status="active")
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        ev = self._owner_files(env)[0]
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='session_end' WHERE binding_id=?", (bid,))
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "session_closed" and calls == []
        assert [j["idempotency_key"] for j in env.jobs("inbound_notice")] == ["notice:%s:session_closed" % mid_of(ev)]

    def test_binding_terminated_after_download_maps(self, env):
        bid = env.make_binding(status="active")

        def fn(**kw):
            env.conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' WHERE binding_id=?",
                             (bid,))
            return ok_materializer()(**kw)
        env.inbound.materializer = fn
        ev = self._owner_files(env)[0]
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "unbound" and env.deliveries() == []

    def test_tokens_unavailable_is_transient(self, env):
        """FakeSlackClient 无 tokens 文件 → 拿不到 bot_token → 瞬态(走预算),不崩。"""
        env.make_binding(status="active")
        ev = self._owner_files(env)[0]
        env.inbound.drive_pending_rows()
        row = env.inbox_row(mid_of(ev))
        assert row["state"] == "materializing" and row["materialize_attempts"] == 1

    def test_real_media_with_fake_worker_via_inbound(self, env, tokens):
        """走真实 media.materialize + 假 worker 子进程(tokens 来自 tokens.json)。"""
        from tests.test_media import FAKE_WORKER
        bid = env.make_binding(status="active")
        env.inbound.worker_path = FAKE_WORKER
        f = slack_file(id="F1", name="a.pdf", size=4, url_private_download="https://files.slack.com/ok/4")
        ev = message_event(text=mention("real"), user=OWNER, files=[f])
        receive(env, ev)
        p = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert p["media_paths"] and pathlib.Path(p["media_paths"][0]).read_bytes() == b"xxxx"
