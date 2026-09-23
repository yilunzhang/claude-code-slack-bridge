"""member 全链限速配额:未决 pending/chat ≤ MAX_UNDECIDED_PER_CHAT、sender 冷却、inbox 非终态总量配额。"""
from lib import constants
from tests.conftest import CHAT, MEMBER, OWNER
from tests.helpers import envelope, message_event
from tests.test_inbound import ingest, mention, mid_of


def recv(env, user, text="msg"):
    ev = message_event(text=mention(text), user=user)
    res = ingest(env, envelope(ev))
    env.inbound.drive_pending_rows()
    return res, mid_of(ev)


def test_undecided_pending_per_chat_cap(env):
    env.make_binding(status="active")
    mids = []
    for i in range(constants.MAX_UNDECIDED_PER_CHAT + 1):
        env.clock.tick(constants.SENDER_COOLDOWN_MS + 1)          # 排除 sender 冷却干扰
        mids.append(recv(env, MEMBER, "m%d" % i)[1])
    states = [env.inbox_row(m)["state"] for m in mids]
    assert states[:-1] == ["awaiting_approval"] * constants.MAX_UNDECIDED_PER_CHAT
    assert states[-1] == "failed"                                # 第 6 条被限速(静默)
    assert len(env.pendings()) == constants.MAX_UNDECIDED_PER_CHAT
    assert len(env.jobs("approval_card")) == constants.MAX_UNDECIDED_PER_CHAT


def test_sender_cooldown(env):
    env.make_binding(status="active")
    _, m1 = recv(env, MEMBER)
    _, m2 = recv(env, MEMBER)
    assert env.inbox_row(m1)["state"] == "awaiting_approval"
    assert env.inbox_row(m2)["state"] == "failed"
    env.clock.tick(constants.SENDER_COOLDOWN_MS + 1)
    _, m3 = recv(env, MEMBER)
    assert env.inbox_row(m3)["state"] == "awaiting_approval"


def test_owner_not_rate_limited(env):
    env.make_binding(status="active")
    _, m1 = recv(env, OWNER)
    _, m2 = recv(env, OWNER)
    assert env.inbox_row(m1)["state"] == "enqueued" and env.inbox_row(m2)["state"] == "enqueued"


def test_inbox_nonterminal_cap_blocks_member_not_owner(env, monkeypatch):
    monkeypatch.setattr(constants, "INBOX_NONTERMINAL_CAP", 3)
    bid = env.make_binding(status="active")
    for i in range(3):                                            # 占满非终态(awaiting_approval)
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES(?,?,?,?,'awaiting_approval',0)", ("Evfill%d" % i, "%s:9.%d" % (CHAT, i), CHAT, bid))
    res, m_blocked = recv(env, MEMBER)
    assert res == ("dropped", "cap") and env.inbox_row(m_blocked) is None
    res, m_owner = recv(env, OWNER)
    assert res[0] == "handed" and env.inbox_row(m_owner) is not None
