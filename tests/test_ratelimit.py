"""member 全链限速配额(plan 4.2.7/§5):未决 pending/chat≤5、sender 冷却、inbox 总量配额。"""
from tests.conftest import APP_ID, CHAT, MEMBER, OWNER
from tests.helpers import bot_mention, mget_snapshot
from lib import constants


def arm_member(env, mids, sender=MEMBER):
    env.arm_mget([mget_snapshot(m, CHAT, sender, text=f"msg {m}",
                                mentions=[bot_mention(APP_ID)]) for m in mids])


def test_undecided_pending_per_chat_cap(env):
    env.make_binding(status="active")
    mids = [f"om_{i}" for i in range(constants.MAX_UNDECIDED_PER_CHAT + 1)]
    arm_member(env, mids)
    for i, m in enumerate(mids):
        env.clock.tick(constants.SENDER_COOLDOWN_MS + 1)  # 排除 sender 冷却干扰
        env.recv_event(message_id=m, sender_id=MEMBER)
    states = [env.inbox_row(m)["state"] for m in mids]
    assert states[:-1] == ["awaiting_approval"] * constants.MAX_UNDECIDED_PER_CHAT
    assert states[-1] == "failed"  # 第 6 条被限速(静默)
    assert len(env.pendings()) == constants.MAX_UNDECIDED_PER_CHAT


def test_sender_cooldown(env):
    env.make_binding(status="active")
    arm_member(env, ["om_1", "om_2", "om_3"])
    env.recv_event(message_id="om_1", sender_id=MEMBER)
    env.recv_event(message_id="om_2", sender_id=MEMBER)  # 冷却内
    assert env.inbox_row("om_1")["state"] == "awaiting_approval"
    assert env.inbox_row("om_2")["state"] == "failed"
    env.clock.tick(constants.SENDER_COOLDOWN_MS + 1)
    env.recv_event(message_id="om_3", sender_id=MEMBER)
    assert env.inbox_row("om_3")["state"] == "awaiting_approval"


def test_owner_not_rate_limited(env):
    env.make_binding(status="active")
    arm_member(env, ["om_1", "om_2"], sender=OWNER)
    env.recv_event(message_id="om_1", sender_id=OWNER)
    env.recv_event(message_id="om_2", sender_id=OWNER)
    assert env.inbox_row("om_1")["state"] == "enqueued"
    assert env.inbox_row("om_2")["state"] == "enqueued"


def test_inbox_nonterminal_cap_blocks_member_not_owner(env, monkeypatch):
    monkeypatch.setattr(constants, "INBOX_NONTERMINAL_CAP", 3)
    env.make_binding(status="active")
    # 占满非终态:3 条 resolving(mget 失败保持非终态)
    from tests.helpers import err_envelope
    env.runner.on_prefix(["im", "+messages-mget"], lambda a, c: err_envelope(500))
    for i in range(3):
        env.recv_event(message_id=f"om_{i}", sender_id=MEMBER)
    assert env.conn.execute(
        "SELECT COUNT(*) FROM inbox WHERE state='resolving'").fetchone()[0] == 3
    # member 第 4 条:连 inbox 行都不建
    env.recv_event(message_id="om_blocked", sender_id=MEMBER)
    assert env.inbox_row("om_blocked") is None
    # owner 不受 cap
    env.recv_event(message_id="om_owner", sender_id=OWNER)
    assert env.inbox_row("om_owner") is not None
