"""starting 期语义(plan 4.2.5 定案)+ 激活重过分流门(r6-B)+ r7-③ 单一路径竞态断言。"""
import json

from tests.conftest import APP_ID, CHAT, MEMBER, OWNER
from tests.helpers import bot_mention, mget_snapshot
from lib import lifecycle


def make_starting(env, **kw):
    return env.make_binding(status="starting", bind_phase="confirmed", session_id="sess-1",
                            listener_epoch=1, listener_beat_at=env.clock.wall_ms(), **kw)


def activate(env, bid):
    assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "activated"


class TestStartingSemantics:
    def test_messages_wait_no_jobs_no_deliveries(self, env):
        make_starting(env)
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        assert env.inbox_row("om_1")["state"] == "waiting_binding"
        assert env.deliveries() == []
        # 不建任何外发 job(receipt/card/notice 都不建)
        assert env.jobs() == []


class TestActivationRegate:
    def test_owner_text_delivered_after_activation(self, env):
        bid = make_starting(env)
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, text="早排队的指令",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        activate(env, bid)
        n = env.inbound.drive_waiting_rows()
        assert n == 1
        row = env.inbox_row("om_1")
        assert row["state"] == "enqueued"
        d = env.deliveries(bid)
        assert len(d) == 1
        assert json.loads(d[0]["payload_json"])["text"] == "早排队的指令"

    def test_member_must_go_through_approval(self, env):
        """r6-B:激活重过分流门,member 绝不绕审批。"""
        bid = make_starting(env)
        env.arm_mget([mget_snapshot("om_1", CHAT, MEMBER, text="member 消息",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(sender_id=MEMBER)
        activate(env, bid)
        env.inbound.drive_waiting_rows()
        row = env.inbox_row("om_1")
        assert row["state"] == "awaiting_approval"
        assert len(env.jobs("approval_card")) == 1
        assert env.deliveries(bid) == []  # 未批准不投

    def test_owner_media_materializes_on_activation(self, env, tmp_path):
        bid = make_starting(env)
        snap = mget_snapshot("om_1", CHAT, OWNER, msg_type="image",
                             mentions=[bot_mention(APP_ID)])
        env.arm_mget([snap])

        def dl(args, cwd):
            import pathlib
            d = pathlib.Path(cwd) / "lark-im-resources"
            d.mkdir(parents=True, exist_ok=True)
            (d / "pic.png").write_bytes(b"PNGDATA")
            from tests.helpers import ok_envelope
            return ok_envelope({"messages": [snap]})

        env.runner.on(lambda a: a[:2] == ["im", "+messages-mget"] and "--download-resources" in a, dl)
        env.recv_event(message_type="image")
        activate(env, bid)
        env.inbound.drive_waiting_rows()
        d = env.deliveries(bid)
        assert len(d) == 1
        payload = json.loads(d[0]["payload_json"])
        paths = payload["media_paths"]
        assert len(paths) == 1 and paths[0].endswith("pic.png")
        import os
        assert os.path.isabs(paths[0]) and os.path.exists(paths[0])
        # v1.4.2:非文本一律带 hint 字段(**真投递路径**上钉住"所有非文本",
        # 否则把闸门改成 `msg_type == "post"` 全套仍绿)。image 已下好 → keys 空、
        # 但 hint 在,且措辞必须先指向 media_paths(不能谎称桥不下载)。
        assert payload["media_keys"] == []
        assert "media_paths" in payload["fetch_hint"]


class TestExactlyOnePath:
    """r7-③:激活与终止对同一消息只产生一条路径,绝不并存。"""

    def _waiting_msg(self, env, bid):
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        env.recv_event()
        assert env.inbox_row("om_1")["state"] == "waiting_binding"

    def test_activate_then_drive_delivery_only(self, env):
        bid = make_starting(env)
        self._waiting_msg(env, bid)
        activate(env, bid)
        env.inbound.drive_waiting_rows()
        env.inbound.drive_waiting_rows()  # 幂等重驱
        assert len(env.deliveries(bid)) == 1
        assert env.jobs("inbound_notice") == []

    def test_terminate_then_drive_notice_only(self, env):
        bid = make_starting(env)
        self._waiting_msg(env, bid)
        lifecycle.terminate_binding(env.conn, bid, "session_end", env.clock)
        env.inbound.drive_waiting_rows()  # 终止事务已映射;重驱=幂等
        row = env.inbox_row("om_1")
        assert row["state"] == "session_closed"
        assert env.deliveries(bid) == []
        keys = [j["idempotency_key"] for j in env.jobs("inbound_notice")]
        assert keys == ["notice:om_1:session_closed"]

    def test_activate_then_terminate_before_drive_notice_only(self, env):
        """激活后、驱动前终止:重驱看到的是终态绑定 → 只出 notice,不出 delivery。"""
        bid = make_starting(env)
        self._waiting_msg(env, bid)
        activate(env, bid)
        lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        env.inbound.drive_waiting_rows()
        assert env.deliveries(bid) == []
        assert env.inbox_row("om_1")["state"] == "unbound"
        keys = [j["idempotency_key"] for j in env.jobs("inbound_notice")]
        assert keys == ["notice:om_1:unbound"]

    def test_recovery_generic_branch_never_intercepts_waiting(self, env):
        """r7-②:waiting 专用分支先于通用 undeliverable/dropped 分支。
        构造"终止级联漏掉的 waiting 行"(崩溃缝模拟),由恢复工人收口。"""
        bid = env.make_binding(status="closed", close_reason="cc_gone")
        import json as _json
        from tests.helpers import mget_snapshot as _snap, bot_mention as _bm
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts,snapshot_json) "
            "VALUES('ev_1','om_1',?,?,'waiting_binding',?,?)",
            (CHAT, bid, env.clock.wall_ms(),
             _json.dumps(_snap("om_1", CHAT, OWNER, mentions=[_bm(APP_ID)]))))
        env.recovery.slow_tick()
        row = env.inbox_row("om_1")
        assert row["state"] == "session_closed"  # 专用分支按 4.2.4 映射,不是 undeliverable
        keys = [j["idempotency_key"] for j in env.jobs("inbound_notice")]
        assert keys == ["notice:om_1:session_closed"]
        assert env.deliveries(bid) == []
