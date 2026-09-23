"""入站管线(plan 4.2):预滤/去重/钉死/结构化@判定/分流门/通知冷却。全程离线(FakeRunner)。"""
import json

from tests.conftest import APP_ID, BOT_OPEN_ID, CHAT, MEMBER, OWNER
from tests.helpers import bot_mention, mget_snapshot, ok_envelope, user_mention
from lib import constants


class TestPrefilterAndFilters:
    def test_p2p_ignored(self, env):
        env.recv_event(chat_type="p2p")
        assert env.inbox_row("om_1") is None
        assert env.runner.calls == []

    def test_bot_sender_ignored(self, env):
        env.make_binding(status="active")
        env.recv_event(sender_id=BOT_OPEN_ID)
        env.recv_event(message_id="om_2", sender_id=APP_ID)
        assert env.inbox_row("om_1") is None and env.inbox_row("om_2") is None

    def test_unbound_chat_without_at_skipped_cheaply(self, env):
        env.recv_event(chat_id="oc_unbound", content="hello no mention")
        assert env.inbox_row("om_1") is None
        assert env.runner.calls == []  # 零 API 调用

    def test_bound_chat_all_messages_recorded(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, text="no mention here")])
        env.recv_event(content="no at sign")  # S1:bot 收绑定群全部消息
        row = env.inbox_row("om_1")
        assert row is not None and row["state"] == "ignored_not_mentioned"


class TestDedupe:
    def test_same_event_twice_single_row(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER,
                                    mentions=[bot_mention(APP_ID)])])
        ev = env.recv_event()
        env.inbound.process_event(ev)  # 完整重放
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 1
        assert len(env.deliveries()) == 1

    def test_cross_row_conflict_fail_closed(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_a", CHAT, OWNER, mentions=[bot_mention(APP_ID)]),
                      mget_snapshot("om_b", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_id="om_a", event_id="ev_1")
        env.recv_event(message_id="om_b", event_id="ev_2")
        # 伪造:ev_1 携带 om_b(两键命中不同行)
        before = env.conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        env.recv_event(message_id="om_b", event_id="ev_1")
        assert env.conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == before
        from lib import db as dbmod
        assert int(dbmod.get_state(env.conn, "inbox_conflict_alerts", "0")) >= 1


class TestPinning:
    def test_pins_latest_binding_including_terminal(self, env):
        old = env.make_binding(status="closed", close_reason="user_unbind", chat_id=CHAT)
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        assert env.inbox_row("om_1")["binding_id"] == old

    def test_no_history_pins_null(self, env):
        env.arm_mget([mget_snapshot("om_1", "oc_nohist", OWNER,
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(chat_id="oc_nohist", content="@TestBot hi")
        row = env.inbox_row("om_1")
        assert row["binding_id"] is None and row["state"] == "unbound"

    def test_old_message_never_drifts_to_new_binding(self, env):
        """inbox 钉死:旧绑定的消息重驱绝不投给新绑定(plan 4.8/B4)。"""
        old = env.make_binding(status="active", chat_id=CHAT)
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_old','om_old',?,?,'received',?)", (CHAT, old, env.clock.wall_ms()))
        from lib import lifecycle
        lifecycle.terminate_binding(env.conn, old, "user_unbind", env.clock)
        new = env.make_binding(status="active", chat_id=CHAT, session_id="sess-2",
                               cc_pid=5555, cc_start="Wed Jul 15 08:00:00 2026")
        env.arm_mget([mget_snapshot("om_old", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.inbound.drive_row(env.inbox_row("om_old"))
        row = env.inbox_row("om_old")
        assert row["state"] == "unbound"  # user_unbind → unbound,不漂移
        assert env.deliveries(new) == []


class TestMentionGate:
    def test_not_mentioned_trimmed(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, text="hi",
                                    mentions=[user_mention("ou_other")])])
        env.recv_event(content="@某人 hi")
        row = env.inbox_row("om_1")
        assert row["state"] == "ignored_not_mentioned"
        assert "hi" not in (row["snapshot_json"] or "")  # 正文即时裁剪

    def test_unbound_chat_also_uses_structural_check(self, env):
        """启发式只当预滤:含 @ 但结构化未@bot → ignored,不发未绑定提示。"""
        env.arm_mget([mget_snapshot("om_1", "oc_u", OWNER,
                                    mentions=[user_mention("ou_other")])])
        env.recv_event(chat_id="oc_u", content="@某人 hello")
        assert env.inbox_row("om_1")["state"] == "ignored_not_mentioned"
        assert env.jobs("inbound_notice") == []


class TestUnboundNotices:
    def test_unbound_notice_with_cooldown(self, env):
        env.arm_mget([mget_snapshot("om_1", "oc_u", OWNER, mentions=[bot_mention(APP_ID)]),
                      mget_snapshot("om_2", "oc_u", OWNER, mentions=[bot_mention(APP_ID)]),
                      mget_snapshot("om_3", "oc_u", OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_id="om_1", chat_id="oc_u")
        env.recv_event(message_id="om_2", chat_id="oc_u")
        jobs = env.jobs("inbound_notice")
        assert len(jobs) == 1  # 冷却抑制第二条
        assert jobs[0]["idempotency_key"] == "notice:om_1:unbound"
        assert env.inbox_row("om_2")["state"] == "unbound"  # 终态照落
        env.clock.tick(constants.NOTICE_COOLDOWN_MS + 1)
        env.recv_event(message_id="om_3", chat_id="oc_u")
        assert len(env.jobs("inbound_notice")) == 2

    def test_session_closed_notice(self, env):
        env.make_binding(status="dead", close_reason="listener_gone", chat_id=CHAT)
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        row = env.inbox_row("om_1")
        assert row["state"] == "session_closed"
        j = env.jobs("inbound_notice")[0]
        assert j["idempotency_key"] == "notice:om_1:session_closed"
        assert j["expected_state"] == "session_closed"


class TestActiveGate:
    def test_owner_text_direct_delivery(self, env):
        bid = env.make_binding(status="active")
        env.arm_mget([mget_snapshot(
            "om_1", CHAT, OWNER, text="修一下 bug",
            mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        row = env.inbox_row("om_1")
        assert row["state"] == "enqueued"
        d = env.deliveries(bid)[0]
        assert d["state"] == "enqueued"
        payload = json.loads(d["payload_json"])
        assert payload["sender_is_owner"] is True
        # E3 回归:payload.text == 原始群消息文本去掉 @bot 前缀
        assert payload["text"] == "修一下 bug"
        assert payload["message_id"] == "om_1"
        rc = env.jobs("receipt_reaction")
        assert len(rc) == 1 and rc[0]["ref_delivery_seq"] == d["delivery_seq"]
        assert rc[0]["idempotency_key"] == f"rc:{d['delivery_seq']}"

    def test_member_text_goes_to_approval(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, MEMBER, text="帮我跑个脚本",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(sender_id=MEMBER)
        row = env.inbox_row("om_1")
        assert row["state"] == "awaiting_approval"
        p = env.pendings()[0]
        assert p["state"] == "pending" and p["message_id"] == "om_1"
        cards = env.jobs("approval_card")
        assert len(cards) == 1
        card = cards[0]
        assert card["reply_to"] == "om_1"
        assert card["idempotency_key"] == f"card:{p['pending_id']}"
        assert card["expected_state"] == "pending"
        body = json.loads(card["body"])
        vals = body["elements"][-1]["actions"][0]["value"]
        assert vals == {"pending_id": p["pending_id"], "nonce": p["nonce"], "act": "approve"}
        # E3 回归:审批卡预览非空(真实 mget 形状下也取到正文)
        preview = body["elements"][1]["text"]["content"]
        assert "帮我跑个脚本" in preview
        assert env.deliveries() == []  # 绝不直投

    def test_member_media_not_downloaded_before_approval(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, MEMBER, msg_type="image",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(sender_id=MEMBER, message_type="image")
        assert env.inbox_row("om_1")["state"] == "awaiting_approval"
        dl = [c for c in env.runner.calls if "--download-resources" in c[0]]
        assert dl == []

    def test_unsupported_type(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, msg_type="sticker",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_type="sticker")
        assert env.inbox_row("om_1")["state"] == "unsupported"
        j = env.jobs("unsupported_notice")
        assert len(j) == 1 and j[0]["idempotency_key"] == "un:om_1"

    def test_owner_post_text_extracted(self, env):
        bid = env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, msg_type="post", text="富文本正文",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_type="post")
        payload = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert "富文本正文" in payload["text"]

    def test_mget_failure_stays_resolving(self, env):
        env.make_binding(status="active")
        from tests.helpers import err_envelope
        env.runner.on_prefix(["im", "+messages-mget"], lambda a, c: err_envelope(99991400))
        env.recv_event()
        assert env.inbox_row("om_1")["state"] == "resolving"

    def test_redrive_after_terminated_binding_maps(self, env):
        bid = env.make_binding(status="active")
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_x','om_x',?,?,'received',?)", (CHAT, bid, env.clock.wall_ms()))
        from lib import lifecycle
        lifecycle.terminate_binding(env.conn, bid, "session_end", env.clock)
        env.arm_mget([mget_snapshot("om_x", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.inbound.drive_row(env.inbox_row("om_x"))
        assert env.inbox_row("om_x")["state"] == "session_closed"
        assert env.deliveries(bid) == []


class TestChatAllowlist:
    """E2:config.chat_allowlist 灰度门 —— 非列内 chat 在任何 inbox/notice 之前直接丢弃。"""

    def test_out_of_list_zero_trace(self, env):
        env.cfg["chat_allowlist"] = ["oc_allowed_only"]
        env.make_binding(status="active")  # CHAT 有活跃绑定也不行
        env.recv_event(content="@TestBot hi")
        assert env.inbox_row("om_1") is None  # 零 inbox 行
        assert env.jobs() == []               # 零 job
        assert env.runner.calls == []         # 零 API 调用(零副作用零回复)

    def test_in_list_processed_normally(self, env):
        env.cfg["chat_allowlist"] = [CHAT]
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        assert env.inbox_row("om_1")["state"] == "enqueued"

    def test_empty_or_absent_means_all(self, env):
        env.cfg["chat_allowlist"] = []
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        assert env.inbox_row("om_1") is not None


class TestExtractTextE3:
    """E3:真实 mget 正文在顶层 content(渲染文本);双形状容忍;剥本 bot mention。"""

    def test_real_shape_strips_bot_mention_only(self):
        from lib.inbound import extract_text
        from tests.helpers import mget_snapshot, bot_mention, user_mention
        snap = mget_snapshot(
            "om_1", CHAT, OWNER, text="",
            mentions=[bot_mention(APP_ID, name="Yilun's agent")],
            content="@Yilun's agent e2e-2 owner 直投探针:请列出当前目录文件")
        assert extract_text(snap, app_id=APP_ID) == "e2e-2 owner 直投探针:请列出当前目录文件"

    def test_real_shape_preserves_other_mentions(self):
        from lib.inbound import extract_text
        from tests.helpers import mget_snapshot, bot_mention, user_mention
        snap = mget_snapshot(
            "om_1", CHAT, OWNER,
            mentions=[bot_mention(APP_ID), user_mention("ou_x", key="@_user_2", name="Some One")],
            content="@TestBot 转告 @Some One 开会")
        assert extract_text(snap, app_id=APP_ID) == "转告 @Some One 开会"

    def test_fallback_raw_body_shape_still_works(self):
        from lib.inbound import extract_text
        from tests.helpers import raw_body_snapshot, bot_mention
        snap = raw_body_snapshot("om_1", CHAT, OWNER, text="@_user_1 老形状消息",
                                 mentions=[bot_mention(APP_ID)])
        assert extract_text(snap, app_id=APP_ID) == "老形状消息"

    def test_media_rendered_content_passthrough(self):
        from lib.inbound import extract_text
        from tests.helpers import mget_snapshot, bot_mention
        img = mget_snapshot("om_i", CHAT, OWNER, msg_type="image",
                            mentions=[bot_mention(APP_ID)])
        f = mget_snapshot("om_f", CHAT, OWNER, msg_type="file",
                          mentions=[bot_mention(APP_ID)])
        assert extract_text(img, app_id=APP_ID) == "[图片]"
        assert extract_text(f, app_id=APP_ID) == "(文件) a.pdf"

    def test_no_app_id_keeps_bot_mention(self):
        from lib.inbound import extract_text
        from tests.helpers import mget_snapshot, bot_mention
        snap = mget_snapshot("om_1", CHAT, OWNER, text="hi",
                             mentions=[bot_mention(APP_ID)])
        assert extract_text(snap) == "@TestBot hi"


class TestIoForensics:
    def test_mget_failure_logged(self, env):
        """r3-6:mget 失败留原始现场(rc/stdout/stderr 截断)。"""
        from tests.helpers import FakeRunResult
        logs = []
        env.inbound.log = logs.append
        env.make_binding(status="active")
        env.runner.on(lambda a: a[:2] == ["im", "+messages-mget"],
                      lambda a, c: FakeRunResult(1, "", '{"ok":false,"error":{"code":99991400}}'))
        env.recv_event()
        joined = "\n".join(logs)
        assert "mget" in joined and "rc=1" in joined and "99991400" in joined


class TestForensicsMissingMessage:
    def test_mget_ok_but_no_target_message_logged(self, env):
        """r4-4:mget ok:true 但结果里没有目标 message → 记原因(非静默 None)。"""
        from tests.helpers import ok_envelope
        logs = []
        env.inbound.log = logs.append
        env.make_binding(status="active")
        # 返回 ok 但 messages 里是别的 id
        env.runner.on(lambda a: a[:2] == ["im", "+messages-mget"],
                      lambda a, c: ok_envelope({"messages": [
                          {"message_id": "om_OTHER", "content": "x"}]}))
        env.recv_event(message_id="om_1")
        joined = "\n".join(logs)
        assert "om_1" in joined and ("no target" in joined or "not found" in joined)
