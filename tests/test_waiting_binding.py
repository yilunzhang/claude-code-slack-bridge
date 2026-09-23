"""starting 期语义 + 激活重过分流门 + 单一路径竞态断言(Slack 事件形状;ingest 走真实 db.tx)。"""
import json

from lib import lifecycle
from tests.conftest import CHAT, MEMBER, OWNER
from tests.helpers import envelope, message_event, slack_file
from tests.test_inbound import ingest, mention, mid_of, ok_materializer


def make_starting(env, **kw):
    return env.make_binding(status="starting", bind_phase="confirmed", session_id="sess-1",
                            listener_epoch=1, listener_beat_at=env.clock.wall_ms(), **kw)


def activate(env, bid):
    assert lifecycle.activate_if_ready(env.conn, bid, env.clock) == "activated"


def recv(env, ev):
    assert ingest(env, envelope(ev))[0] == "handed"
    env.inbound.drive_pending_rows()
    return mid_of(ev)


class TestStartingSemantics:
    def test_messages_wait_no_jobs_no_deliveries(self, env):
        make_starting(env)
        mid = recv(env, message_event(text=mention("hi"), user=OWNER))
        assert env.inbox_row(mid)["state"] == "waiting_binding"
        assert env.deliveries() == [] and env.jobs() == []
        env.inbound.drive_pending_rows()                        # 仍 starting → 继续等
        assert env.inbox_row(mid)["state"] == "waiting_binding"


class TestActivationRegate:
    def test_owner_text_delivered_after_activation(self, env):
        bid = make_starting(env)
        mid = recv(env, message_event(text=mention("早排队的指令"), user=OWNER))
        activate(env, bid)
        assert env.inbound.drive_waiting_rows() == 1
        assert env.inbox_row(mid)["state"] == "enqueued"
        d = env.deliveries(bid)
        assert len(d) == 1 and json.loads(d[0]["payload_json"])["text"] == "早排队的指令"

    def test_member_must_go_through_approval(self, env):
        bid = make_starting(env)
        mid = recv(env, message_event(text=mention("member 消息"), user=MEMBER))
        activate(env, bid)
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid)["state"] == "awaiting_approval"
        assert len(env.jobs("approval_card")) == 1 and env.deliveries(bid) == []

    def test_owner_files_materialize_on_activation(self, env):
        bid = make_starting(env)
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        mid = recv(env, message_event(text=mention("pic"), user=OWNER, files=[slack_file(name="pic.png")]))
        assert calls == []                                       # starting 期不下载
        activate(env, bid)
        env.inbound.drive_pending_rows()
        row = env.inbox_row(mid)
        assert row["state"] == "enqueued" and calls == [mid]
        p = json.loads(env.deliveries(bid)[0]["payload_json"])
        assert len(p["media_paths"]) == 1 and p["media_paths"][0].endswith("pic.png")
        assert p["files"][0]["local_path"] == p["media_paths"][0]

    def test_unlisted_member_files_still_gated_after_activation(self, env):
        bid = make_starting(env)
        calls = []
        env.inbound.materializer = ok_materializer(calls)
        mid = recv(env, message_event(text=mention("pic"), user=MEMBER, files=[slack_file()]))
        activate(env, bid)
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid)["state"] == "awaiting_approval" and calls == []
        assert env.deliveries() == [] and len(env.pendings()) == 1


class TestExactlyOnePath:
    def _waiting_msg(self, env):
        mid = recv(env, message_event(text=mention("w"), user=OWNER))
        assert env.inbox_row(mid)["state"] == "waiting_binding"
        return mid

    def test_activate_then_drive_delivery_only(self, env):
        bid = make_starting(env)
        self._waiting_msg(env)
        activate(env, bid)
        env.inbound.drive_pending_rows()
        env.inbound.drive_pending_rows()
        assert len(env.deliveries(bid)) == 1 and env.jobs("inbound_notice") == []

    def test_terminate_then_drive_notice_only(self, env):
        bid = make_starting(env)
        mid = self._waiting_msg(env)
        lifecycle.terminate_binding(env.conn, bid, "session_end", env.clock)
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid)["state"] == "session_closed" and env.deliveries(bid) == []
        assert [j["idempotency_key"] for j in env.jobs("inbound_notice")] == ["notice:%s:session_closed" % mid]

    def test_activate_then_terminate_before_drive_notice_only(self, env):
        bid = make_starting(env)
        mid = self._waiting_msg(env)
        activate(env, bid)
        lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
        env.inbound.drive_pending_rows()
        assert env.deliveries(bid) == [] and env.inbox_row(mid)["state"] == "unbound"
        assert [j["idempotency_key"] for j in env.jobs("inbound_notice")] == ["notice:%s:unbound" % mid]

    def test_recovery_generic_branch_never_intercepts_waiting(self, env):
        """终止级联漏掉的 waiting 行(崩溃缝)→ 恢复工人按 4.2.4 映射,不是 undeliverable。"""
        bid = env.make_binding(status="closed", close_reason="cc_gone")
        ev = message_event(text=mention("w"), user=OWNER)
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts,snapshot_json,sender_user_id) "
            "VALUES('ev_1',?,?,?,'waiting_binding',?,?,?)",
            (mid_of(ev), CHAT, bid, env.clock.wall_ms(), json.dumps(ev), OWNER))
        env.recovery.slow_tick()
        assert env.inbox_row(mid_of(ev))["state"] == "session_closed"
        assert [j["idempotency_key"] for j in env.jobs("inbound_notice")] == ["notice:%s:session_closed" % mid_of(ev)]
        assert env.deliveries(bid) == []
