"""跨模块事务测试骨架(R2-M16):真实 SQLite 上验证 drain 的事务提交/回滚/退避/隔离/崩溃恢复。
WP1(drain_staging)/ WP2(ingest_in_tx / process_in_tx / drive_pending_rows)合入前必须全绿并摘掉 xfail。
不用 fake 代替事务语义:所有断言直接查 bridge.db。"""
import pytest

from lib import constants, db as dbmod
from tests.conftest import CHAT, DM, MEMBER, OWNER, TEAM
from tests.helpers import app_mention_event, block_action, envelope, message_event, slack_file

# WP1 的 drain_staging 已落地(事务/退避/隔离机制在 tests/test_daemon_plumbing.py 用契约形状的假 ingest 验证);
# 下面标 WP1 的用例还依赖 WP2 的 lib/inbound.ingest_in_tx(真实 handed/dropped 语义、monkeypatch 目标属性),
# WP2 合入即转绿(strict → 报错提醒)→ WP5 摘标记。
WP1 = pytest.mark.xfail(strict=True, reason="WP1 done; needs WP2 inbound.ingest_in_tx")
WP2 = pytest.mark.xfail(strict=True, reason="WP2 inbound/approval 事务接口未落地")


def _staged_row(env, key):
    return env.conn.execute("SELECT * FROM slack_events WHERE event_key=?", (key,)).fetchone()


# ---------------------------------------------------------------- 提交
@WP1
def test_handed_row_is_consumed_in_same_transaction_as_inbox_insert(env):
    env.make_binding(status="active", chat_id=CHAT)
    ev = message_event(text="hi", channel=CHAT, user=OWNER)
    env.stage("events_api", envelope(ev, event_id="EvA"))
    env.drain()
    r = _staged_row(env, "ev:EvA")
    assert r["state"] == "consumed" and r["consumed_at"] == env.clock.wall_ms() and r["error"] is None
    row = env.inbox_row("%s:%s" % (CHAT, ev["ts"]))
    assert row is not None and row["binding_id"] == r["binding_id"]
    assert row["reply_thread_ts"] == ev["ts"] and row["sender_user_id"] == OWNER
    assert not env.conn.in_transaction


@WP1
def test_dropped_row_is_consumed_with_reason(env):
    env.stage("events_api", envelope(message_event(channel=CHAT), event_id="EvF", team_id="T_FOREIGN"))
    env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER, subtype="message_changed"),
                                     event_id="EvS"))
    env.drain()
    assert _staged_row(env, "ev:EvF")["state"] == "consumed"
    assert _staged_row(env, "ev:EvF")["error"] == "foreign_team"
    assert _staged_row(env, "ev:EvS")["error"] == "subtype"
    assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0


@WP1
def test_drain_respects_next_drain_at_batch_and_seq_order(env):
    env.make_binding(status="active", chat_id=CHAT)
    now = env.clock.wall_ms()
    for i in range(constants.DRAIN_BATCH + 3):
        env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER), event_id="Ev%03d" % i))
    env.conn.execute("UPDATE slack_events SET next_drain_at=? WHERE event_key='ev:Ev000'", (now + 10_000,))
    env.drain()
    states = {r["event_key"]: r["state"] for r in env.slack_events()}
    assert states["ev:Ev000"] == "staged"                    # 未到期不动
    assert sum(1 for s in states.values() if s == "consumed") == constants.DRAIN_BATCH
    assert states["ev:Ev%03d" % (constants.DRAIN_BATCH + 2)] == "staged"   # 超批次留下
    env.drain()
    assert all(r["state"] == "consumed" for r in env.slack_events() if r["event_key"] != "ev:Ev000")


# ---------------------------------------------------------------- 回滚 / 退避 / 隔离
@WP1
def test_business_exception_rolls_back_everything_and_backs_off(env, monkeypatch):
    from lib import inbound
    env.make_binding(status="active", chat_id=CHAT)
    env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER), event_id="EvX"))
    seen = {}

    def boom(conn, row):
        seen["in_tx"] = conn.in_transaction
        conn.execute("INSERT INTO callback_events(event_id,seen_at) VALUES('poison',0)")
        raise RuntimeError("boom")
    monkeypatch.setattr(inbound, "ingest_in_tx", boom)
    now = env.clock.wall_ms()
    env.drain()
    assert seen["in_tx"] is True                              # 业务在 drain 的事务内运行
    r = _staged_row(env, "ev:EvX")
    assert r["state"] == "staged" and r["drain_attempts"] == 1 and "boom" in (r["error"] or "")
    assert now + constants.DRAIN_BACKOFF_MS <= r["next_drain_at"] <= now + constants.DRAIN_BACKOFF_MAX_MS
    assert env.conn.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0] == 0
    assert not env.conn.in_transaction
    # 未到期 → 不重试
    env.drain()
    assert _staged_row(env, "ev:EvX")["drain_attempts"] == 1


@WP1
def test_quarantine_after_max_attempts_keeps_payload_and_error(env, monkeypatch):
    from lib import inbound
    env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER), event_id="EvQ"))
    monkeypatch.setattr(inbound, "ingest_in_tx", lambda conn, row: (_ for _ in ()).throw(RuntimeError("q")))
    for _ in range(constants.DRAIN_MAX_ATTEMPTS + 2):
        env.drain()
        env.clock.tick(constants.DRAIN_BACKOFF_MAX_MS + 1)
    r = _staged_row(env, "ev:EvQ")
    assert r["state"] == "quarantined" and r["drain_attempts"] == constants.DRAIN_MAX_ATTEMPTS
    assert r["payload_json"] and "q" in r["error"]
    assert int(dbmod.get_state(env.conn, "drain_quarantined", "0")) == 1
    # 隔离行不再被取
    env.drain()
    assert _staged_row(env, "ev:EvQ")["drain_attempts"] == constants.DRAIN_MAX_ATTEMPTS


@WP1
def test_business_must_not_open_nested_transaction(env, monkeypatch):
    """ingest_in_tx 若自己 BEGIN(db.tx 嵌套)→ RuntimeError → 按业务异常处理(staged + 退避),不崩 drain。"""
    from lib import inbound

    def nested(conn, row):
        with dbmod.tx(conn):
            return ("handed", None)
    monkeypatch.setattr(inbound, "ingest_in_tx", nested)
    env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER), event_id="EvN"))
    env.drain()
    r = _staged_row(env, "ev:EvN")
    assert r["state"] == "staged" and "nested" in (r["error"] or "")


# ---------------------------------------------------------------- 交接与 followup 分离 / 崩溃恢复
@WP2
def test_consumed_then_crash_before_followup_still_delivers(env, conn, cfg, clock, client, prober, data_dir):
    """drain 把行交接(inbox received)后 daemon 崩溃;10 分钟后新进程只靠 drive_pending_rows 仍投递。"""
    from tests.conftest import Env
    env.make_binding(status="active", chat_id=CHAT)
    ev = message_event(text="hi", channel=CHAT, user=OWNER)
    env.stage("events_api", envelope(ev, event_id="EvC"))
    env.drain()
    mid = "%s:%s" % (CHAT, ev["ts"])
    assert env.inbox_row(mid)["state"] in ("received", "resolving")   # drain 不驱动
    assert env.deliveries() == []
    clock.tick(10 * 60 * 1000)
    env2 = Env(conn, cfg, clock, client, prober, data_dir)             # "重启"
    env2.inbound.drive_pending_rows()
    assert env2.inbox_row(mid)["state"] == "enqueued"
    d = env2.deliveries()
    assert len(d) == 1 and d[0]["message_id"] == mid


@WP2
def test_binding_pinned_at_receive_not_at_drain(env):
    """事件到达时无绑定 → binding_id NULL;之后 bind 再 drain,仍按 unbound 处理(不查 latest)。"""
    ev = message_event(text="<@U0BOT> hi", channel=CHAT, user=OWNER)
    env.stage("events_api", envelope(ev, event_id="EvP"))
    env.make_binding(status="active", chat_id=CHAT)
    env.drain()
    env.inbound.drive_pending_rows()
    row = env.inbox_row("%s:%s" % (CHAT, ev["ts"]))
    assert row["binding_id"] is None and row["state"] == "unbound"
    assert env.deliveries() == []
    assert env.jobs("inbound_notice")


@WP2
def test_duplicate_delivery_message_then_app_mention_both_orders(env):
    env.make_binding(status="active", chat_id=CHAT)
    for order in ("message_first", "mention_first"):
        ts = "1700000%03d.000100" % (1 if order == "message_first" else 2)
        m = message_event(text="<@U0BOT> look", channel=CHAT, user=OWNER, ts=ts,
                          files=[slack_file(id="F" + ts[-3:])])
        a = app_mention_event(text="<@U0BOT> look", channel=CHAT, user=OWNER, ts=ts)
        first, second = (m, a) if order == "message_first" else (a, m)
        env.stage("events_api", envelope(first, event_id="Ev1" + ts[-3:]))
        env.stage("events_api", envelope(second, event_id="Ev2" + ts[-3:]))
        env.drain()
        row = env.inbox_row("%s:%s" % (CHAT, ts))
        assert row is not None
        snap = row["snapshot_json"]
        assert '"files"' in snap                                   # 两种顺序最终都拿到 files
    assert int(dbmod.get_state(env.conn, "inbox_dup_message", "0")) == 1
    assert int(dbmod.get_state(env.conn, "inbox_snapshot_upgraded", "0")) == 1


@WP2
def test_interactive_drain_dropped_reasons(env):
    env.make_binding(status="active", chat_id=CHAT)
    env.click(pending_id="nope", nonce="n", card_ts="1.1", action_ts="2.2")          # invalid(pending 不存在)
    env.stage("interactive", block_action("p", "n", value="garbage", card_ts="1.2", action_ts="2.3"))  # skipped
    env.drain()
    errs = {r["event_key"].split(":")[3]: r["error"] for r in env.slack_events("consumed")}
    assert errs == {"1.1": "invalid", "1.2": "skipped"}
    assert env.conn.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0] == 2  # 裸去重也落库


@WP2
def test_approve_click_is_single_transaction_with_decision_and_delivery(env):
    from lib import util
    bid = env.make_binding(status="active", chat_id=CHAT)
    ev = message_event(text="<@U0BOT> pls", channel=CHAT, user=MEMBER)
    env.stage("events_api", envelope(ev, event_id="EvM"))
    env.drain()
    env.inbound.drive_pending_rows()
    p = env.pendings()[0]
    mid = "%s:%s" % (CHAT, ev["ts"])
    assert env.inbox_row(mid)["state"] == "awaiting_approval" and p["state"] == "pending"
    env.click(pending_id=p["pending_id"], nonce=p["nonce"], card_ts="5.5")
    env.drain()
    p = env.pendings()[0]
    assert p["state"] == "approved" and p["decided_by"] == OWNER
    assert p["card_message_id"] == util.message_id_of(CHAT, "5.5")        # 点击回填卡片身份
    assert env.inbox_row(mid)["state"] == "enqueued"
    assert len(env.deliveries(bid)) == 1
    keys = {j["idempotency_key"] for j in env.jobs("decision_notice")}
    assert "dec:%s:delivered" % p["pending_id"] in keys
