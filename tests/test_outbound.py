"""出站状态机(docs/contracts.md §2):op_for 纯函数 / 传输形态 / §2.5 转移表逐行 / §2.6 核验逐行 /
_prepare 顺序与冻结断言 / 冷却预检与节流 / 排序(组内、跨组、同 chat 通知)/ 守卫表 / startup_scan / 告警。
全部离线:FakeSlackClient 未注册方法即 AssertionError(绝不静默放过外呼)。"""
import json

import pytest

from lib import constants, db as dbmod, jobs, outbound, texts
from lib.outbound import Outbound
from tests.conftest import BOT_ID, CHAT, MEMBER, OWNER
from tests.helpers import err, http5xx, next_ts, not_sent, ok, posted, ratelimited, timeout

C = constants
PM = "chat.postMessage"
UPD = "chat.update"
REACT = "reactions.add"
HIST = "conversations.history"
REPL = "conversations.replies"


# ======================================================================
# helpers
# ======================================================================
def row(env, key):
    return env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()


def by_id(env, job_id):
    return env.conn.execute("SELECT * FROM outbound_jobs WHERE job_id=?", (job_id,)).fetchone()


def mk_job(env, kind="session_turn", key=None, binding_id=None, chat_id=CHAT, body="hello",
           turn_group=None, chunk_index=None, **kw):
    if key is None:
        n = env.conn.execute("SELECT COUNT(*) FROM outbound_jobs").fetchone()[0]
        key = "k:%s:%d" % (kind, n)
    jobs.create_job(env.conn, kind=kind, chat_id=chat_id, idempotency_key=key, binding_id=binding_id,
                    body=body, turn_group=turn_group, chunk_index=chunk_index,
                    now=env.clock.wall_ms(), **kw)
    return row(env, key)


def mk_turn(env, bid, group="g", idx=0, body="hello", chat_id=CHAT):
    return mk_job(env, key=jobs.key_turn(group, idx), binding_id=bid, chat_id=chat_id, body=body,
                  turn_group=group, chunk_index=idx)


def set_cols(env, key, **cols):
    sets = ", ".join("%s=?" % k for k in cols)
    env.conn.execute("UPDATE outbound_jobs SET %s WHERE idempotency_key=?" % sets, (*cols.values(), key))


def freeze_turn(env, key, payload_kind="markdown_text"):
    """把一条 turn 的 op_* 冻结成 postMessage 顶层(模拟已发过一次)。"""
    set_cols(env, key, op_method=PM, op_target=CHAT, op_thread_ts=None, op_payload_kind=payload_kind)


def counter(env, key):
    return int(dbmod.get_state(env.conn, key, "0") or 0)


def run_ticks(env, n, gap=C.POST_MIN_INTERVAL_MS):
    total = 0
    for _ in range(n):
        total += env.outbound.tick()
        env.clock.tick(gap)
    return total


def seq(*results):
    """依次返回 results(耗尽后重复最后一个);元素可为 CallResult 或 fn(params)->CallResult。"""
    it = list(results)

    def fn(method, params):
        r = it.pop(0) if len(it) > 1 else it[0]
        return r(params) if callable(r) else r
    return fn


def post_ok(params):
    return posted(channel=params["channel"])


def arm_post(env, *results):
    env.client.on(PM, seq(*(results or (post_ok,))))


def set_verify_ok(env, version=None):
    dbmod.set_state(env.conn, C.VERIFY_CAPABILITY_KEY, C.VERIFY_CAP_OK)
    dbmod.set_state(env.conn, C.VERIFY_CAPABILITY_VERSION_KEY,
                    version if version is not None else env.client.tokens_version)


def hit_msg(job_id, ts=None, bot_id=BOT_ID):
    return {"type": "message", "bot_id": bot_id, "ts": ts or next_ts(), "text": "x",
            "metadata": {"event_type": C.METADATA_EVENT_TYPE, "event_payload": {"job_id": job_id}}}


def history(messages=(), has_more=False, cursor=None):
    d = {"messages": list(messages), "has_more": has_more}
    if cursor:
        d["response_metadata"] = {"next_cursor": cursor}
    return ok(d)


def alerts(env):
    return [j for j in env.jobs("session_turn") if (j["turn_group"] or "").startswith("__sendfail__:")]


def inbox(env, mid, state, bid=None, thread_ts=None, reply_thread_ts=None):
    env.conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,sender_user_id,message_type,thread_ts,"
        "reply_thread_ts,state,ts) VALUES(?,?,?,?,?,?,?,?,?,0)",
        ("Ev-" + mid, mid, CHAT, bid, MEMBER, "app_mention", thread_ts, reply_thread_ts, state))


def pending(env, pid, mid, bid, state="pending", card=None, decided_by=None):
    env.conn.execute(
        "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,card_message_id,state,decided_by,created_at) "
        "VALUES(?,?,?,?,?,?,?,?)", (pid, mid, bid, "n" * 32, card, state, decided_by, env.clock.wall_ms()))
    return env.conn.execute("SELECT * FROM pendings WHERE pending_id=?", (pid,)).fetchone()


def member_pending(env, bid, pid="p1", thread_ts=None):
    """成员消息 inbox + pendings(pending)+ approval_card job(线程 = reply_thread_ts)。不依赖 WP2。"""
    ts = next_ts()
    mid = "%s:%s" % (CHAT, ts)
    rtt = thread_ts or ts
    inbox(env, mid, "awaiting_approval", bid, thread_ts=thread_ts, reply_thread_ts=rtt)
    p = pending(env, pid, mid, bid)
    body = texts.build_approval_card(pid, "n" * 32, MEMBER, "run <task>")
    jobs.create_job(env.conn, kind="approval_card", chat_id=CHAT, binding_id=bid,
                    idempotency_key=jobs.key_card(pid), reply_to=rtt, ref_pending_id=pid,
                    ref_message_id=mid, body=body, now=env.clock.wall_ms())
    return p


def decision_job(env, bid, pid, outcome, card=None, pstate="rejected", istate="rejected", decided_by=OWNER):
    mid = "%s:%s" % (CHAT, next_ts())
    inbox(env, mid, istate, bid, reply_thread_ts=mid.split(":")[1])
    pending(env, pid, mid, bid, state=pstate, card=card, decided_by=decided_by)
    key = jobs.key_dec(pid, outcome)
    jobs.create_job(env.conn, kind="decision_notice", chat_id=CHAT, binding_id=bid, idempotency_key=key,
                    reply_to=mid.split(":")[1], ref_pending_id=pid, ref_message_id=mid,
                    expected_state=outcome, body=texts.decision_notice_body(outcome), now=env.clock.wall_ms())
    return key


def reaction_job(env, bid, ref_ts=None, delivery_state="enqueued"):
    ts = ref_ts or next_ts()
    mid = "%s:%s" % (CHAT, ts)
    inbox(env, mid, "enqueued", bid)
    env.conn.execute("INSERT INTO deliveries(binding_id,message_id,payload_json,state) VALUES(?,?,'{}',?)",
                     (bid, mid, delivery_state))
    dseq = env.conn.execute("SELECT MAX(delivery_seq) FROM deliveries").fetchone()[0]
    key = jobs.key_rc(dseq)
    jobs.create_job(env.conn, kind="receipt_reaction", chat_id=CHAT, binding_id=bid, idempotency_key=key,
                    ref_delivery_seq=dseq, ref_message_id=ts, now=env.clock.wall_ms())
    return key, ts


def send_unknown(env, bid, group="g", then=()):
    """一条 turn:首次 postMessage 超时 → unknown(had_unknown=1, verify_after=now+5s)。返回 job 行。"""
    arm_post(env, timeout(), *(then or (post_ok,)))
    key = jobs.key_turn(group, 0)
    mk_turn(env, bid, group=group)
    env.outbound.tick()
    r = row(env, key)
    assert r["state"] == "unknown" and r["had_unknown"] == 1
    assert r["verify_after"] == env.clock.wall_ms() + C.VERIFY_SCHEDULE_MS[0]
    assert len(env.client.calls_for(PM)) == 1
    return r


def verify_tick(env, ms=None):
    """推进到下一次核验到期并 tick。"""
    r = env.conn.execute("SELECT MIN(verify_after) FROM outbound_jobs WHERE state='unknown'").fetchone()[0]
    if ms is None:
        ms = max(0, (r or env.clock.wall_ms()) - env.clock.wall_ms())
    env.clock.tick(ms)
    env.outbound.tick()


# ======================================================================
# op_for / 类别 / cap(§2.2 / §2.3)
# ======================================================================
class TestOpFor:
    def test_table_by_kind(self, cfg):
        base = {"job_id": "j", "chat_id": CHAT, "reply_to": "1.1", "ref_pending_id": None, "ref_message_id": None,
                "body": None, "op_method": None, "op_target": None, "op_thread_ts": None, "op_payload_kind": None,
                "had_unknown": 0, "attempt_count": 0, "state": "pending", "card_message_id": None}
        f = lambda **kw: outbound.op_for(dict(base, **kw), cfg)  # noqa: E731
        assert f(kind="session_turn") == (PM, CHAT, None, "markdown_text")
        assert f(kind="lifecycle_notice") == (PM, CHAT, None, "text")            # 顶层,忽略 reply_to
        assert f(kind="inbound_notice") == (PM, CHAT, "1.1", "text")
        assert f(kind="unsupported_notice") == (PM, CHAT, "1.1", "text")
        assert f(kind="approval_card") == (PM, CHAT, "1.1", "blocks")
        assert f(kind="decision_notice") == (PM, CHAT, "1.1", "text")
        assert f(kind="decision_notice", card_message_id=CHAT + ":9.9") == (UPD, CHAT + ":9.9", None, "blocks")
        assert f(kind="receipt_reaction") == (REACT, CHAT, None, "reaction")
        with pytest.raises(ValueError):
            f(kind="nope")

    def test_markdown_mode_from_cfg_and_frozen_wins(self, cfg):
        view = {"kind": "session_turn", "chat_id": CHAT, "reply_to": None, "op_method": None,
                "op_target": None, "op_thread_ts": None, "op_payload_kind": None, "card_message_id": None}
        cfg["markdown_mode"] = "text"
        assert outbound.op_for(view, cfg)[3] == "text"
        cfg["markdown_mode"] = "garbage"
        assert outbound.op_for(view, cfg)[3] == C.MARKDOWN_MODE_DEFAULT
        frozen = dict(view, op_method=PM, op_target="C_OLD", op_thread_ts="7.7", op_payload_kind="markdown_text")
        cfg["markdown_mode"] = "text"
        assert outbound.op_for(frozen, cfg) == (PM, "C_OLD", "7.7", "markdown_text")  # 冻结后不因 cfg 变化重选

    def test_category_and_cap(self):
        assert outbound.category_of(PM) == "postMessage"
        assert outbound.category_of(UPD) == outbound.category_of(REACT) == "idempotent"
        assert outbound.category_of("x") is None
        assert outbound.cap_for("session_turn", "postMessage") == C.TURN_CAP
        assert outbound.cap_for("approval_card", "postMessage") == C.CARD_CAP
        assert outbound.cap_for("decision_notice", "postMessage") == C.NOTICE_CAP
        assert outbound.cap_for("decision_notice", "idempotent") == C.IDEMPOTENT_CAP


# ======================================================================
# 传输形态(§2.2 / §4.4)
# ======================================================================
class TestTransmitShapes:
    def test_session_turn_markdown_text_shape(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, lambda p: posted(channel=p["channel"], ts="1700000000.000999"))
        mk_turn(env, bid, body="**hi**")
        assert env.outbound.tick() == 1
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["sent_message_id"] == CHAT + ":1700000000.000999"
        assert r["sent_at"] == env.clock.wall_ms() and r["attempt_count"] == 1 and r["error"] is None
        assert (r["op_method"], r["op_target"], r["op_thread_ts"], r["op_payload_kind"]) == (PM, CHAT, None, "markdown_text")
        p = env.client.calls_for(PM)[0]
        assert p["channel"] == CHAT and p["markdown_text"] == "**hi**" and "text" not in p
        assert p["unfurl_links"] is False and p["unfurl_media"] is False and "thread_ts" not in p
        assert p["metadata"] == {"event_type": "slack_bridge", "event_payload": {"job_id": r["job_id"]}}

    def test_session_turn_text_when_cfg_text(self, env):
        bid = env.make_binding(status="active")
        env.cfg["markdown_mode"] = "text"
        arm_post(env)
        mk_turn(env, bid, body="plain")
        env.outbound.tick()
        p = env.client.calls_for(PM)[0]
        assert p["text"] == "plain" and "markdown_text" not in p
        assert row(env, "turn:g:0")["op_payload_kind"] == "text"

    def test_lifecycle_notice_top_level_text(self, env):
        bid = env.make_binding(status="active")
        arm_post(env)
        mk_job(env, kind="lifecycle_notice", key="lc:%s:bound" % bid, binding_id=bid, expected_state="active",
               body=texts.LC_BOUND, reply_to="9.9")
        env.outbound.tick()
        p = env.client.calls_for(PM)[0]
        assert p["text"] == texts.LC_BOUND and "thread_ts" not in p and "markdown_text" not in p

    def test_inbound_notice_threads_to_reply_to(self, env):
        mid = "%s:1.1" % CHAT
        inbox(env, mid, "unbound")
        arm_post(env)
        mk_job(env, kind="inbound_notice", key=jobs.key_notice(mid, "unbound"), ref_message_id=mid,
               expected_state="unbound", body=texts.inbound_notice_body("unbound"), reply_to="1.1")
        env.outbound.tick()
        p = env.client.calls_for(PM)[0]
        assert p["thread_ts"] == "1.1" and p["text"] == texts.inbound_notice_body("unbound")

    def test_approval_card_blocks_text_thread_and_backfill(self, env):
        bid = env.make_binding(status="active")
        p = member_pending(env, bid, thread_ts="1700000000.000001")
        arm_post(env, lambda q: posted(channel=q["channel"], ts="1700000000.000777"))
        env.outbound.tick()
        card = row(env, "card:p1")
        assert card["state"] == "sent" and card["op_payload_kind"] == "blocks"
        q = env.client.calls_for(PM)[0]
        expected = json.loads(texts.build_approval_card("p1", "n" * 32, MEMBER, "run <task>"))
        assert q["blocks"] == expected["blocks"] and q["text"] == expected["text"]
        assert q["thread_ts"] == "1700000000.000001" and "markdown_text" not in q
        assert q["blocks"][1]["text"]["text"] == "run &lt;task&gt;"  # 成员预览保持转义
        pr = env.conn.execute("SELECT card_message_id FROM pendings WHERE pending_id='p1'").fetchone()
        assert pr[0] == CHAT + ":1700000000.000777"
        assert p["card_message_id"] is None  # 发前为空

    def test_card_backfill_only_when_null(self, env):
        bid = env.make_binding(status="active")
        member_pending(env, bid)
        arm_post(env, lambda q: posted(channel=q["channel"], ts="2.2"))
        j = row(env, "card:p1")
        assert env.outbound._prepare(j) == "send"
        # 发送期间 owner 点击已回填(confirmed-by-click)→ 不覆盖
        env.conn.execute("UPDATE pendings SET card_message_id=? WHERE pending_id='p1'", (CHAT + ":1.1",))
        env.outbound._send_and_finalize(j["job_id"])
        assert env.conn.execute("SELECT card_message_id FROM pendings").fetchone()[0] == CHAT + ":1.1"

    def test_decision_update_shape_when_card_known(self, env):
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", "rejected", card=CHAT + ":5.5")
        env.client.on(UPD, lambda m, q: ok({"channel": q["channel"], "ts": q["ts"]}))
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "sent" and r["sent_message_id"] == CHAT + ":5.5"
        assert (r["op_method"], r["op_target"], r["op_thread_ts"], r["op_payload_kind"]) == (UPD, CHAT + ":5.5", None, "blocks")
        q = env.client.calls_for(UPD)[0]
        assert q["channel"] == CHAT and q["ts"] == "5.5"
        assert q["blocks"] == texts.decision_update_blocks("rejected", OWNER)
        assert q["text"] == texts.decision_update_text("rejected")
        assert all(b["type"] != "actions" for b in q["blocks"])
        assert env.client.calls_for(PM) == []

    def test_decision_postmessage_fallback_when_card_unknown(self, env):
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", "rejected", card=None)
        arm_post(env)
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "sent" and r["op_method"] == PM and r["op_payload_kind"] == "text"
        q = env.client.calls_for(PM)[0]
        assert q["text"] == texts.decision_notice_body("rejected") and q["thread_ts"] == r["reply_to"]
        assert q["metadata"]["event_payload"]["job_id"] == r["job_id"]

    def test_reaction_shape_and_already_reacted(self, env):
        bid = env.make_binding(status="active")
        key, ts = reaction_job(env, bid)
        env.client.on(REACT, lambda m, q: err("already_reacted"))
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "sent" and r["op_method"] == REACT and r["op_payload_kind"] == "reaction"
        q = env.client.calls_for(REACT)[0]
        assert q == {"channel": CHAT, "timestamp": ts, "name": "eyes"}
        assert alerts(env) == []

    def test_reaction_ref_message_id_with_channel_prefix(self, env):
        bid = env.make_binding(status="active")
        key, ts = reaction_job(env, bid)
        set_cols(env, key, ref_message_id="%s:%s" % (CHAT, ts))
        env.client.on(REACT, lambda m, q: ok())
        env.outbound.tick()
        assert env.client.calls_for(REACT)[0]["timestamp"] == ts and row(env, key)["state"] == "sent"

    def test_transmit_uses_frozen_op_not_cfg(self, env):
        """§2.7:had_unknown=1 后 cfg.markdown_mode 变化不得重选形态。"""
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0", payload_kind="markdown_text")
        set_cols(env, "turn:g:0", had_unknown=1)
        env.cfg["markdown_mode"] = "text"
        arm_post(env)
        env.outbound.tick()
        q = env.client.calls_for(PM)[0]
        assert "markdown_text" in q and "text" not in q
        assert row(env, "turn:g:0")["op_payload_kind"] == "markdown_text"


# ======================================================================
# §2.5 转移表(逐行)
# ======================================================================
class TestTransitionTable:
    def test_wait_send_branch_restores_pending_and_ac(self, env):
        """任意 | wait(发送分支):真实 _prepare → call(wait) → _finalize:sending 不残留、ac 不变、不动 count。"""
        bid = env.make_binding(status="active")
        j = mk_turn(env, bid)
        assert env.outbound._prepare(j) == "send"
        r = row(env, "turn:g:0")
        assert r["state"] == "sending" and r["attempt_count"] == 1 and r["op_method"] == PM
        until = env.clock.wall_ms() + 5000
        env.client.cooldown_store.publish(PM, until)  # 竞态:检查与调用之间其它进程发布冷却
        env.outbound._send_and_finalize(r["job_id"])
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["attempt_count"] == 0 and r["next_attempt_at"] == until
        assert r["transient_count"] == 0 and r["ratelimit_count"] == 0 and r["had_unknown"] == 0
        assert env.client.calls == [] and env.client.waits == [(PM, until)]
        assert counter(env, "cooldown_waits") == 1
        env.clock.tick(5001)
        arm_post(env)
        assert env.outbound.tick() == 1
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["attempt_count"] == 1

    def test_postmessage_sent(self, env):
        bid = env.make_binding(status="active")
        arm_post(env)
        mk_turn(env, bid)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["sent_message_id"].startswith(CHAT + ":") and r["verify_after"] is None

    def test_permanent_failed_had_unknown_0_turn_alerts(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, err("channel_not_found"), post_ok)
        mk_turn(env, bid)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "failed" and r["error"] == "channel_not_found" and r["attempt_count"] == 1
        a = alerts(env)
        assert len(a) == 1 and a[0]["body"] == texts.send_failure_alert_body()
        assert a[0]["idempotency_key"] == "turn:__sendfail__:%s:0" % r["job_id"]
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        assert by_id(env, a[0]["job_id"])["state"] == "sent"  # 告警本身照常发出

    def test_permanent_failed_card_no_alert(self, env):
        bid = env.make_binding(status="active")
        member_pending(env, bid)
        arm_post(env, err("msg_too_long"))
        env.outbound.tick()
        assert row(env, "card:p1")["state"] == "failed" and alerts(env) == []

    def test_permanent_failed_had_unknown_1_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0")
        set_cols(env, "turn:g:0", had_unknown=1)
        arm_post(env, err("channel_not_found"))
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["had_unknown"] == 1
        a = alerts(env)
        assert len(a) == 1 and a[0]["body"] == texts.unconfirmed_alert_body()

    def test_markdown_rejected_had_unknown_0_retries_as_text(self, env):
        bid = env.make_binding(status="active")

        def fn(q):
            return err("invalid_arguments") if "markdown_text" in q else posted(channel=q["channel"])
        arm_post(env, fn)
        mk_turn(env, bid, body="**b**")
        env.outbound.tick()
        r = row(env, "turn:g:0")
        # 同一 attempt 内不发第二个请求;ac-1 不计 transient;op_* 归零待下一次 _prepare 记 text
        assert len(env.client.calls_for(PM)) == 1
        assert r["state"] == "pending" and r["attempt_count"] == 0 and r["transient_count"] == 0
        assert r["next_attempt_at"] == env.clock.wall_ms() and r["error"] == "markdown_rejected"
        assert r["op_payload_kind"] is None and r["op_method"] is None and r["had_unknown"] == 0
        assert env.cfg["markdown_mode"] == "text"
        assert json.loads(env.cfg.path.read_text())["markdown_mode"] == "text"  # 已持久化
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["op_payload_kind"] == "text" and r["attempt_count"] == 1
        q = env.client.calls_for(PM)[1]
        assert q["text"] == "**b**" and "markdown_text" not in q

    def test_markdown_rejected_had_unknown_1_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0", payload_kind="markdown_text")
        set_cols(env, "turn:g:0", had_unknown=1)
        arm_post(env, err("invalid_arguments"))
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["error"] == "markdown_rejected"
        assert r["op_payload_kind"] == "markdown_text"       # 不得改形态(R4-m1)
        assert env.cfg["markdown_mode"] == "markdown_text"   # 不动配置
        assert len(alerts(env)) == 1 and len(env.client.calls_for(PM)) == 1

    def test_invalid_arguments_on_text_is_plain_failed(self, env):
        bid = env.make_binding(status="active")
        env.cfg["markdown_mode"] = "text"
        arm_post(env, err("invalid_arguments"))
        mk_turn(env, bid)
        env.outbound.tick()
        assert row(env, "turn:g:0")["state"] == "failed"   # markdown_rejected 仅限 markdown_text 形态

    def test_ratelimited_restores_ac_and_waits(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, ratelimited(retry_after=3), post_ok)
        mk_turn(env, bid)
        t0 = env.clock.wall_ms()
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["attempt_count"] == 0 and r["ratelimit_count"] == 1
        assert r["next_attempt_at"] == t0 + 3000 and r["transient_count"] == 0
        assert counter(env, "ratelimit_hits") == 1
        env.clock.tick(1000)
        env.outbound.tick()
        assert len(env.client.calls_for(PM)) == 1 and row(env, "turn:g:0")["state"] == "pending"
        env.clock.tick(2001)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["attempt_count"] == 1 and len(env.client.calls_for(PM)) == 2

    def test_ratelimited_next_is_max_with_cooldown(self, env):
        bid = env.make_binding(status="active")
        env.client.cooldown_store.publish(PM, env.clock.wall_ms() + 30_000)  # 先有更长冷却(其它方法调用发布)
        env.clock.tick(31_000)
        arm_post(env, ratelimited(retry_after=1))
        mk_turn(env, bid)
        # 发布一条更长的冷却再 finalize:next = max(now+RA, cooldown)
        j = row(env, "turn:g:0")
        assert env.outbound._prepare(j) == "send"
        far = env.clock.wall_ms() + 20_000
        res = env.client.call(PM, {"channel": CHAT})  # 触发 429 → 冷却 now+1000
        env.client.cooldown_store.publish(PM, far)
        env.outbound._finalize(env.outbound._job_view(j["job_id"]), res, "ratelimited", env.clock.wall_ms())
        assert row(env, "turn:g:0")["next_attempt_at"] == far

    def test_ratelimit_cap_failed_when_had_unknown_0(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, ratelimited(retry_after=1))
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", ratelimit_count=C.RATELIMIT_CAP - 1)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "failed" and r["ratelimit_count"] == C.RATELIMIT_CAP and r["attempt_count"] == 0
        assert len(alerts(env)) == 1 and alerts(env)[0]["body"] == texts.send_failure_alert_body()

    def test_ratelimit_cap_unconfirmed_when_had_unknown_1(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        arm_post(env, ratelimited(retry_after=1))
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0")
        set_cols(env, "turn:g:0", ratelimit_count=C.RATELIMIT_CAP - 1, had_unknown=1)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed"
        assert alerts(env)[0]["body"] == texts.unconfirmed_alert_body()

    def test_not_sent_transient_backoff_schedule(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, not_sent("dns"), not_sent("connection_refused"), post_ok)
        mk_turn(env, bid)
        t0 = env.clock.wall_ms()
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["attempt_count"] == 0 and r["transient_count"] == 1
        assert r["next_attempt_at"] == t0 + C.TRANSIENT_BACKOFF_MS and r["error"] == "dns"
        env.clock.tick(C.TRANSIENT_BACKOFF_MS + 1)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["transient_count"] == 2 and r["next_attempt_at"] == env.clock.wall_ms() + 2 * C.TRANSIENT_BACKOFF_MS
        env.clock.tick(2 * C.TRANSIENT_BACKOFF_MS + 1)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["attempt_count"] == 1 and r["ratelimit_count"] == 0
        assert len(env.client.calls_for(PM)) == 3

    def test_not_sent_auth_error_code_is_transient(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, err("invalid_auth"))
        mk_turn(env, bid)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["transient_count"] == 1 and r["had_unknown"] == 0

    def test_transient_backoff_capped_at_max(self, env, monkeypatch):
        bid = env.make_binding(status="active")
        arm_post(env, not_sent())
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", transient_count=3)   # 5s·2^3 = 40s(cap=5 时正常上限恰好不触 45s 顶)
        monkeypatch.setattr(C, "TRANSIENT_BACKOFF_MAX_MS", 30_000)
        env.outbound.tick()
        assert row(env, "turn:g:0")["next_attempt_at"] == env.clock.wall_ms() + 30_000

    def test_transient_cap_terminal_failed(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, not_sent())
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", transient_count=C.TRANSIENT_CAP - 1)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "failed" and r["transient_count"] == C.TRANSIENT_CAP
        assert len(alerts(env)) == 1 and alerts(env)[0]["body"] == texts.send_failure_alert_body()

    def test_transient_cap_terminal_unconfirmed_when_had_unknown(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        arm_post(env, not_sent())
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0")
        set_cols(env, "turn:g:0", transient_count=C.TRANSIENT_CAP - 1, had_unknown=1)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["transient_count"] == C.TRANSIENT_CAP
        assert alerts(env)[0]["body"] == texts.unconfirmed_alert_body()

    @pytest.mark.parametrize("res", [timeout(), http5xx(503), err("internal_error"), err("some_new_code"),
                                     err("http_redirect", http_status=302)])
    def test_unknown_schedules_verify_never_blind_resends(self, env, res):
        bid = env.make_binding(status="active")
        arm_post(env, res, post_ok)
        env.client.on(HIST, lambda m, q: history([]))
        mk_turn(env, bid)
        t0 = env.clock.wall_ms()
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["had_unknown"] == 1 and r["attempt_count"] == 1
        assert r["verify_after"] == t0 + C.VERIFY_SCHEDULE_MS[0] and r["next_attempt_at"] is None
        # 无论 tick 多少次、多久,该 job 绝不出现第二个 postMessage(告警是另一 job_id 的消息,不算)
        for _ in range(200):
            env.clock.tick(5_000)
            env.outbound.tick()
        mine = [q for q in env.client.calls_for(PM) if q["metadata"]["event_payload"]["job_id"] == r["job_id"]]
        assert len(mine) == 1
        assert row(env, "turn:g:0")["state"] == "unconfirmed"  # 能力 unverified → 三次未见即 unconfirmed

    def test_idempotent_unknown_retries_then_cap_failed(self, env):
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", "rejected", card=CHAT + ":5.5")
        env.client.on(UPD, lambda m, q: http5xx())
        t0 = env.clock.wall_ms()
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "unknown" and r["attempt_count"] == 1 and r["had_unknown"] == 0
        assert r["next_attempt_at"] == t0 + C.IDEMPOTENT_RETRY_DELAY_MS and r["verify_after"] is None
        for _ in range(3):
            env.clock.tick(C.IDEMPOTENT_RETRY_DELAY_MS + 1)
            env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "failed" and r["attempt_count"] == C.IDEMPOTENT_CAP
        assert len(env.client.calls_for(UPD)) == C.IDEMPOTENT_CAP  # _prepare 在第 4 次前拦住
        assert alerts(env) == []

    def test_idempotent_failed_no_alert(self, env):
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", "rejected", card=CHAT + ":5.5")
        env.client.on(UPD, lambda m, q: err("message_not_found"))
        env.outbound.tick()
        assert row(env, key)["state"] == "failed" and alerts(env) == []

    def test_idempotent_ratelimited_and_not_sent_backoff(self, env):
        bid = env.make_binding(status="active")
        key, _ = reaction_job(env, bid)
        env.client.on(REACT, seq(ratelimited(retry_after=2), not_sent(), ok()))
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "pending" and r["ratelimit_count"] == 1 and r["attempt_count"] == 0
        env.clock.tick(2001)
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "pending" and r["transient_count"] == 1 and r["attempt_count"] == 0
        env.clock.tick(C.TRANSIENT_BACKOFF_MS + 1)
        env.outbound.tick()
        assert row(env, key)["state"] == "sent"

    def test_cooldown_consumed_five_times_not_failed(self, env):
        """冷却 wait 不消耗任何预算(R3-P2):5 次冷却后既不 failed,ac 也不变。"""
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        for _ in range(5):
            until = env.clock.wall_ms() + 1000
            env.client.cooldown_store.publish(PM, until)
            env.outbound.tick()
            r = row(env, "turn:g:0")
            assert r["state"] == "pending" and r["next_attempt_at"] == until and r["attempt_count"] == 0
            env.clock.tick(1001)
        # 竞态版本:_prepare 之后才发布冷却 → call 返回 wait
        for _ in range(5):
            j = row(env, "turn:g:0")
            assert env.outbound._prepare(j) == "send"
            until = env.clock.wall_ms() + 1000
            env.client.cooldown_store.publish(PM, until)
            env.outbound._send_and_finalize(j["job_id"])
            r = row(env, "turn:g:0")
            assert r["state"] == "pending" and r["attempt_count"] == 0
            env.clock.tick(1001)
        r = row(env, "turn:g:0")
        assert r["transient_count"] == 0 and r["ratelimit_count"] == 0 and env.client.calls == []
        assert counter(env, "cooldown_waits") == 5
        arm_post(env)
        env.outbound.tick()
        assert row(env, "turn:g:0")["state"] == "sent"


# ======================================================================
# §2.6 核验(逐行)
# ======================================================================
class TestVerify:
    def test_hit_via_history_metadata(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        sending_at = r["sending_at"]
        ts = next_ts()
        env.client.on(HIST, lambda m, q: history([
            {"type": "message", "bot_id": "B_OTHER", "ts": "1.1"},
            hit_msg(r["job_id"], ts)]))
        verify_tick(env)
        r2 = row(env, "turn:g:0")
        assert r2["state"] == "sent" and r2["sent_message_id"] == CHAT + ":" + ts and r2["verify_after"] is None
        assert counter(env, "verify_hit") == 1 and len(env.client.calls_for(PM)) == 1
        q = env.client.calls_for(HIST)[0]
        assert q["channel"] == CHAT and q["inclusive"] is True and q["include_all_metadata"] is True
        assert q["limit"] == C.VERIFY_PAGE_LIMIT and "ts" not in q
        assert q["oldest"] == "%.6f" % ((sending_at - C.VERIFY_LOOKBACK_MS) / 1000.0)

    def test_hit_via_replies_for_threaded_card(self, env):
        bid = env.make_binding(status="active")
        member_pending(env, bid, thread_ts="1700000000.000001")
        arm_post(env, timeout())
        env.outbound.tick()
        card = row(env, "card:p1")
        assert card["state"] == "unknown" and card["op_thread_ts"] == "1700000000.000001"
        ts = next_ts()
        env.client.on(REPL, lambda m, q: history([hit_msg(card["job_id"], ts)]))
        verify_tick(env)
        card = row(env, "card:p1")
        assert card["state"] == "sent" and card["sent_message_id"] == CHAT + ":" + ts
        q = env.client.calls_for(REPL)[0]
        assert q["channel"] == CHAT and q["ts"] == "1700000000.000001" and q["include_all_metadata"] is True
        assert env.client.calls_for(HIST) == []
        assert env.conn.execute("SELECT card_message_id FROM pendings").fetchone()[0] == CHAT + ":" + ts

    def test_absent_schedule_n1_n2(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([]))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_absent_count"] == 1 and r["verify_error_count"] == 0
        assert r["verify_after"] == env.clock.wall_ms() + C.VERIFY_SCHEDULE_MS[1]
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_absent_count"] == 2
        assert r["verify_after"] == env.clock.wall_ms() + C.VERIFY_SCHEDULE_MS[2]
        assert counter(env, "verify_absent") == 2 and r["verify_round"] == 0 and r["resend_count"] == 0

    def test_two_errors_one_absent_do_not_resend(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        send_unknown(env, bid)
        env.client.on(HIST, seq(http5xx(), timeout(), history([])))
        verify_tick(env)   # error 1
        r = row(env, "turn:g:0")
        assert r["verify_error_count"] == 1 and r["verify_absent_count"] == 0
        assert r["verify_after"] == env.clock.wall_ms() + C.VERIFY_ERROR_BACKOFF_MS
        verify_tick(env)   # error 2
        r = row(env, "turn:g:0")
        assert r["verify_error_count"] == 2 and r["verify_after"] == env.clock.wall_ms() + 2 * C.VERIFY_ERROR_BACKOFF_MS
        verify_tick(env)   # absent 1(错误不凑数)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_absent_count"] == 1 and r["verify_error_count"] == 2
        assert r["resend_count"] == 0 and len(env.client.calls_for(PM)) == 1
        # 还要两次成功未见才获准重发
        verify_tick(env)
        assert row(env, "turn:g:0")["verify_absent_count"] == 2 and len(env.client.calls_for(PM)) == 1
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["resend_count"] == 1 and len(env.client.calls_for(PM)) == 1

    def test_two_absents_restart_third_absent_resends(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        r0 = send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([]))
        verify_tick(env)
        verify_tick(env)
        assert row(env, "turn:g:0")["verify_absent_count"] == 2
        # 模拟重启:新 Outbound 实例 + startup_scan,不得扰动已排程的核验
        env.outbound = Outbound(env.conn, env.cfg, env.client, env.clock)
        env.outbound.startup_scan()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_absent_count"] == 2 and r["verify_after"] > env.clock.wall_ms()
        verify_tick(env)   # 第三次成功未见 → 获准重发(同一 CAS 内切轮、归零)
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["next_attempt_at"] == env.clock.wall_ms()
        assert r["resend_count"] == 1 and r["verify_round"] == 1
        assert r["verify_absent_count"] == 0 and r["verify_error_count"] == 0 and r["verify_after"] is None
        assert r["had_unknown"] == 1 and counter(env, "verify_resent") == 1
        assert len(env.client.calls_for(PM)) == 1  # 获准 ≠ 已发
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["attempt_count"] == 2 and r["job_id"] == r0["job_id"]
        calls = env.client.calls_for(PM)
        assert len(calls) == 2 and calls[0]["metadata"] == calls[1]["metadata"]  # job_id 重发后不变

    def test_unverified_capability_three_absents_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        assert dbmod.get_state(env.conn, C.VERIFY_CAPABILITY_KEY) == "unverified"
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["resend_count"] == 0 and r["verify_round"] == 0
        assert counter(env, "verify_unconfirmed") == 1 and len(env.client.calls_for(PM)) == 1
        assert alerts(env)[0]["body"] == texts.unconfirmed_alert_body()

    def test_capability_version_mismatch_three_absents_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env, version="some-other-version")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "unconfirmed" and len(env.client.calls_for(PM)) == 1

    def test_capability_degraded_three_absents_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        dbmod.set_state(env.conn, C.VERIFY_CAPABILITY_KEY, "degraded:missing_scope")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "unconfirmed" and len(env.client.calls_for(PM)) == 1

    def test_resend_only_once_second_round_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        send_unknown(env, bid, then=(timeout(),))  # 重发也超时
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "pending"
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_round"] == 1 and r["attempt_count"] == 2
        assert len(env.client.calls_for(PM)) == 2
        for _ in range(3):
            verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["resend_count"] == 1 and len(env.client.calls_for(PM)) == 2

    def test_resend_refused_when_tokens_version_changed(self, env):
        """R5-S1:获准重发后凭据换了 → _prepare 用本次实际凭据复验 → 拒发、unconfirmed。"""
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "pending"
        env.client.set_tokens_version("fake-v2")   # FingerprintGate 重载凭据(探测版本仍是 v1)
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["error"].startswith("resend-refused")
        assert len(env.client.calls_for(PM)) == 1   # 没有第二次 POST
        assert alerts(env)[0]["body"] == texts.unconfirmed_alert_body()

    def test_error_cap_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: http5xx())
        for i in range(C.VERIFY_ERROR_CAP - 1):
            verify_tick(env)
            r = row(env, "turn:g:0")
            assert r["state"] == "unknown" and r["verify_error_count"] == i + 1 and r["verify_absent_count"] == 0
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["verify_error_count"] == C.VERIFY_ERROR_CAP
        assert counter(env, "verify_unconfirmed") == 1 and len(alerts(env)) == 1

    def test_error_backoff_capped(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        set_cols(env, "turn:g:0", verify_error_count=5)
        env.client.on(HIST, lambda m, q: timeout())
        verify_tick(env)
        assert row(env, "turn:g:0")["verify_after"] == env.clock.wall_ms() + C.VERIFY_ERROR_BACKOFF_MAX_MS

    def test_deadline_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        set_cols(env, "turn:g:0", sending_at=env.clock.wall_ms() - C.VERIFY_DEADLINE_MS - 1)
        env.client.on(HIST, lambda m, q: http5xx())
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["verify_error_count"] == 1

    def test_ratelimited_stays_unknown_counts_untouched(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        set_cols(env, "turn:g:0", verify_absent_count=2, verify_error_count=1)
        env.client.on(HIST, lambda m, q: ratelimited(retry_after=7))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_after"] == env.clock.wall_ms() + 7000
        assert r["verify_absent_count"] == 2 and r["verify_error_count"] == 1 and r["resend_count"] == 0
        assert counter(env, "ratelimit_hits") == 1

    def test_wait_stays_unknown_precheck_and_race(self, env):
        """核验分支 wait:tick 预检只写 verify_after;竞态 call(wait) 也只写 verify_after;绝不回 pending。"""
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        r = send_unknown(env, bid)
        set_cols(env, "turn:g:0", verify_absent_count=2)   # 下一次成功未见就会获准重发
        env.clock.tick(C.VERIFY_SCHEDULE_MS[0])
        until = env.clock.wall_ms() + 9000
        env.client.cooldown_store.publish(HIST, until)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_after"] == until and r["verify_absent_count"] == 2
        assert env.client.waits == [] and env.client.calls_for(HIST) == []   # 预检未到 client
        # 竞态:预检之后发布冷却 → call 返回 wait
        set_cols(env, "turn:g:0", verify_after=env.clock.wall_ms())
        env.client.on(HIST, lambda m, q: history([]))
        env.client.cooldown_store.publish(HIST, until)
        assert env.outbound._verify_unknown(env.outbound._job_view(r["job_id"])) == "wait"
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_after"] == until
        assert r["verify_absent_count"] == 2 and r["verify_error_count"] == 0 and r["resend_count"] == 0
        assert env.client.waits == [(HIST, until)] and counter(env, "cooldown_waits") == 1
        assert len(env.client.calls_for(PM)) == 1

    @pytest.mark.parametrize("code", ["not_in_channel", "channel_not_found"])
    def test_channel_error_unconfirmed_only_this_job_capability_unchanged(self, env, code):
        """频道级永久错(VERIFY_CHANNEL_ERRORS):只本 job unconfirmed(+告警),verify_capability 不动,
        其它频道的 job 仍可获准重发。"""
        bid_a = env.make_binding(status="active", chat_id=CHAT, session_id="sA", cc_pid=1111, cc_start="t1")
        bid_b = env.make_binding(status="active", chat_id="C_B", session_id="sB", cc_pid=2222, cc_start="t2")
        set_verify_ok(env)
        arm_post(env, timeout(), timeout(), post_ok)   # A、B 首发超时;之后(A 的告警)正常发出
        mk_turn(env, bid_a, group="ga", chat_id=CHAT)
        mk_turn(env, bid_b, group="gb", chat_id="C_B")
        env.outbound.tick()
        ra, rb = row(env, "turn:ga:0"), row(env, "turn:gb:0")
        assert ra["state"] == "unknown" and rb["state"] == "unknown"
        env.client.on(HIST, lambda m, q: err(code) if q["channel"] == CHAT else history([]))
        verify_tick(env)   # 两者同时到期:A 频道错 → unconfirmed;B 成功未见 n=1
        ra = row(env, "turn:ga:0")
        assert ra["state"] == "unconfirmed" and code in ra["error"]
        assert dbmod.get_state(env.conn, C.VERIFY_CAPABILITY_KEY) == C.VERIFY_CAP_OK   # 能力不动
        assert [a["chat_id"] for a in alerts(env)] == [CHAT] and alerts(env)[0]["body"] == texts.unconfirmed_alert_body()
        assert counter(env, "verify_unconfirmed") == 1
        verify_tick(env)
        verify_tick(env)   # B 第三次成功未见 → 仍获准重发(能力未被 A 的频道错拖累)
        rb = row(env, "turn:gb:0")
        assert rb["state"] == "pending" and rb["resend_count"] == 1 and rb["verify_round"] == 1
        ids = [q["metadata"]["event_payload"]["job_id"] for q in env.client.calls_for(PM)]
        assert ids.count(ra["job_id"]) == 1 and ids.count(rb["job_id"]) == 1   # 各一次首发;B 尚未重发
        assert alerts(env)[0]["state"] == "sent"

    @pytest.mark.parametrize("code", ["missing_scope", "invalid_auth"])
    def test_global_error_unconfirmed_and_degrades_capability(self, env, code):
        """能力级永久错(VERIFY_GLOBAL_DEGRADE_ERRORS,含 NOT_SENT_ERRORS 族的 invalid_auth):
        本 job unconfirmed + verify_capability=degraded:<err>。"""
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: err(code))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and code in r["error"]
        assert dbmod.get_state(env.conn, C.VERIFY_CAPABILITY_KEY) == "degraded:%s" % code
        assert len(env.client.calls_for(PM)) == 1 and len(alerts(env)) == 1

    @pytest.mark.parametrize("code", ["msg_too_long", "some_new_code", "internal_error"])
    def test_other_error_codes_stay_in_error_branch(self, env, code):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: err(code))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_error_count"] == 1 and r["verify_absent_count"] == 0
        assert dbmod.get_state(env.conn, C.VERIFY_CAPABILITY_KEY) == C.VERIFY_CAP_OK

    def test_pagination_cursor_then_hit(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        ts = next_ts()
        env.client.on(HIST, seq(history([], has_more=True, cursor="c1"), history([hit_msg(r["job_id"], ts)])))
        verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "sent"
        calls = env.client.calls_for(HIST)
        assert len(calls) == 2 and "cursor" not in calls[0] and calls[1]["cursor"] == "c1"

    def test_pagination_exhausted_is_error_not_absent(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([], has_more=True, cursor="more"))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_error_count"] == 1 and r["verify_absent_count"] == 0
        assert len(env.client.calls_for(HIST)) == C.VERIFY_MAX_PAGES

    def test_has_more_without_cursor_is_error_not_absent(self, env):
        """Codex 实现 review R1:声称 has_more 却无 cursor = 不完整查询,不能算 absent。"""
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([], has_more=True, cursor=None))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_error_count"] == 1 and r["verify_absent_count"] == 0

    def test_cursor_without_has_more_still_paginates(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        ts = next_ts()
        env.client.on(HIST, seq(history([], has_more=False, cursor="c9"), history([hit_msg(r["job_id"], ts)])))
        verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "sent"
        assert env.client.calls_for(HIST)[1]["cursor"] == "c9"

    def test_response_without_messages_list_is_error(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: ok({"ok": True}))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_error_count"] == 1 and r["verify_absent_count"] == 0

    @pytest.mark.parametrize("data", [
        {"messages": [None], "has_more": False},                                  # 元素非 dict
        {"messages": [], "response_metadata": {"next_cursor": 42}},               # cursor 非 str
        {"messages": [], "has_more": "yes"},                                      # has_more 非 bool
        {"messages": [], "has_more": None},                                       # has_more 存在但 null
        {"messages": [], "has_more": False, "response_metadata": "junk"},         # response_metadata 非 dict
        {"messages": [{"ts": "1.1"}, "str"], "has_more": False},                  # 混入非 dict 元素
    ], ids=["null-element", "int-cursor", "str-has_more", "null-has_more", "junk-metadata", "mixed-element"])
    def test_malformed_page_is_error_never_absent(self, env, data):
        """R2-M1:畸形响应 = 不可解析 → error 分支(verify_error_count+1),绝不把非法 cursor / 非法元素
        折叠成"完整且没有下一页"而记 absent(三次 absent 就会触发重发)。字段**缺省**才取默认值,
        字段**存在但类型错**一律 error。"""
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: ok(data))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_error_count"] == 1 and r["verify_absent_count"] == 0
        assert len(env.client.calls_for(HIST)) == 1 and len(env.client.calls_for(PM)) == 1   # 不翻页、不重发

    def test_empty_cursor_with_has_more_false_is_complete_page(self, env):
        """R2-M1:`next_cursor: ""` 是 Slack 表示"没有下一页"的正规写法(与字段缺省等价)
        → 完整查询未命中 → absent(不是 error)。"""
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: ok({"messages": [], "has_more": False,
                                             "response_metadata": {"next_cursor": ""}}))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_absent_count"] == 1 and r["verify_error_count"] == 0

    def test_page_validator_pure_function(self):
        """`outbound.verify_page(data)` → (messages, next_cursor|None) 或 None(畸形)。"""
        from lib import outbound as ob
        assert ob.verify_page({"messages": []}) == ([], None)
        assert ob.verify_page({"messages": [{"ts": "1"}], "has_more": False, "response_metadata": {}}) == ([{"ts": "1"}], None)
        assert ob.verify_page({"messages": [], "has_more": True, "response_metadata": {"next_cursor": "c"}}) == ([], "c")
        assert ob.verify_page({"messages": [], "has_more": False, "response_metadata": {"next_cursor": "c"}}) == ([], "c")
        assert ob.verify_page({"messages": [], "response_metadata": {"next_cursor": ""}}) == ([], None)
        for bad in ({"messages": [None]}, {"messages": [], "response_metadata": {"next_cursor": 42}},
                    {"messages": [], "has_more": "yes"}, {"messages": [], "has_more": True},
                    {"messages": [], "has_more": True, "response_metadata": {"next_cursor": ""}},
                    {"messages": "x"}, {}, None, {"messages": [], "response_metadata": []}):
            assert ob.verify_page(bad) is None, bad

    def test_hit_requires_bot_id_event_type_and_job_id(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        env.client.on(HIST, lambda m, q: history([
            hit_msg(r["job_id"], bot_id="B_OTHER"),
            hit_msg("other-job"),
            dict(hit_msg(r["job_id"]), metadata={"event_type": "other", "event_payload": {"job_id": r["job_id"]}}),
            {"type": "message", "bot_id": BOT_ID, "ts": "1.1"},
        ]))
        verify_tick(env)
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_absent_count"] == 1

    def test_stale_round_cas_is_noop(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        view = env.outbound._job_view(r["job_id"])
        env.client.on(HIST, lambda m, q: history([hit_msg(r["job_id"])]))
        set_cols(env, "turn:g:0", verify_round=1)   # 期间已切轮
        assert env.outbound._verify_unknown(view) == "stale"
        assert row(env, "turn:g:0")["state"] == "unknown"


# ======================================================================
# _prepare 顺序 / cap / 冻结断言(§2.4)
# ======================================================================
class TestPrepare:
    def test_guard_before_cap_and_cap_before_capability(self, env):
        bid = env.make_binding(status="closed", close_reason="user_unbind")
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", attempt_count=C.TURN_CAP)
        assert env.outbound._prepare(row(env, "turn:g:0")) == "cancelled"   # 守卫先于 cap
        assert row(env, "turn:g:0")["state"] == "cancelled" and alerts(env) == []

    def test_cap_postmessage_terminal_by_had_unknown(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid, group="a")
        set_cols(env, "turn:a:0", attempt_count=C.TURN_CAP)
        assert env.outbound._prepare(row(env, "turn:a:0")) == "terminal"
        assert row(env, "turn:a:0")["state"] == "failed" and len(alerts(env)) == 1
        env.conn.execute("UPDATE outbound_jobs SET state='cancelled' WHERE turn_group LIKE '__sendfail__:%'")  # 告警不挡 b 组
        mk_turn(env, bid, group="b")
        freeze_turn(env, "turn:b:0")
        set_cols(env, "turn:b:0", attempt_count=C.TURN_CAP, had_unknown=1)   # cap 先于能力复验:能力 unverified 也无关
        assert env.outbound._prepare(row(env, "turn:b:0")) == "terminal"
        assert row(env, "turn:b:0")["state"] == "unconfirmed"
        member_pending(env, bid)
        set_cols(env, "card:p1", attempt_count=C.CARD_CAP)
        assert env.outbound._prepare(row(env, "card:p1")) == "terminal"
        assert row(env, "card:p1")["state"] == "failed"
        assert env.client.calls == []

    def test_corrupt_negative_attempt_count_terminal(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", attempt_count=-1)
        assert env.outbound._prepare(row(env, "turn:g:0")) == "terminal"
        assert row(env, "turn:g:0")["state"] == "failed"

    def test_idempotent_cap_failed_before_send(self, env):
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", "rejected", card=CHAT + ":5.5")
        set_cols(env, key, state="unknown", attempt_count=C.IDEMPOTENT_CAP, next_attempt_at=env.clock.wall_ms(),
                 op_method=UPD, op_target=CHAT + ":5.5", op_payload_kind="blocks")
        env.outbound.tick()
        assert row(env, key)["state"] == "failed" and env.client.calls == []

    def test_freeze_assertion_missing_op_is_unconfirmed(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", had_unknown=1)   # had_unknown=1 却无冻结 op_* → 编程错误,fail-closed
        assert env.outbound._prepare(row(env, "turn:g:0")) == "terminal"
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["error"] == "op-freeze-mismatch" and env.client.calls == []

    def test_freeze_assertion_mismatch_is_unconfirmed(self, env, monkeypatch):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0", payload_kind="markdown_text")
        set_cols(env, "turn:g:0", had_unknown=1)
        real = outbound.op_for
        monkeypatch.setattr(outbound, "op_for", lambda v, cfg: (PM, CHAT, None, "text") if v["had_unknown"] else real(v, cfg))
        arm_post(env)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unconfirmed" and r["error"] == "op-freeze-mismatch"
        assert env.client.calls == [] and r["op_payload_kind"] == "markdown_text"

    def test_had_unknown_never_cleared(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        r = send_unknown(env, bid, then=(ratelimited(retry_after=1), post_ok))
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()                      # 重发 → 429 → pending
        assert row(env, "turn:g:0")["state"] == "pending" and row(env, "turn:g:0")["had_unknown"] == 1
        env.clock.tick(1001)
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "sent" and r["had_unknown"] == 1 and r["verify_round"] == 1

    def test_prepare_returns_gone_for_terminal(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", state="sent")
        assert env.outbound._prepare(row(env, "turn:g:0")) == "gone"
        assert env.outbound._prepare({"job_id": "nope"}) == "gone"

    def test_prepare_skips_unknown_postmessage(self, env):
        bid = env.make_binding(status="active")
        r = send_unknown(env, bid)
        set_cols(env, "turn:g:0", next_attempt_at=env.clock.wall_ms())   # 即便有 next 也不从 unknown 发送
        assert env.outbound._prepare(r) == "skip"
        assert row(env, "turn:g:0")["state"] == "unknown"


# ======================================================================
# 冷却预检 / 节流(§2.4-7 / §2.8)
# ======================================================================
class TestCooldownAndPacing:
    def test_cooled_new_job_never_enters_sending(self, env):
        bid = env.make_binding(status="active")
        until = env.clock.wall_ms() + 8000
        env.client.cooldown_store.publish(PM, until)
        mk_turn(env, bid)
        assert env.outbound.tick() == 0
        r = row(env, "turn:g:0")
        assert r["state"] == "pending" and r["next_attempt_at"] == until and r["attempt_count"] == 0
        assert r["op_method"] is None   # 未进 _prepare,未冻结
        assert env.client.calls == [] and env.client.waits == []
        env.clock.tick(8001)
        arm_post(env)
        assert env.outbound.tick() == 1

    def test_cooled_decision_update_does_not_enter_sending(self, env):
        """卡片身份已知的新 decision_notice 遇 chat.update 冷却 → 不进 sending(投影 + 纯函数解析方法)。"""
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", "rejected", card=CHAT + ":5.5")
        until = env.clock.wall_ms() + 4000
        env.client.cooldown_store.publish(UPD, until)
        arm_post(env)   # postMessage 未冷却:若误判类别会发出去
        env.outbound.tick()
        r = row(env, key)
        assert r["state"] == "pending" and r["next_attempt_at"] == until and env.client.calls == []

    def test_cooldown_on_other_method_does_not_block(self, env):
        bid = env.make_binding(status="active")
        env.client.cooldown_store.publish(UPD, env.clock.wall_ms() + 4000)
        arm_post(env)
        mk_turn(env, bid)
        assert env.outbound.tick() == 1

    def test_post_pacing_one_per_second_per_channel(self, env):
        for i in range(3):
            mid = "%s:%d.1" % (CHAT, i)
            inbox(env, mid, "unbound")
            mk_job(env, kind="inbound_notice", key=jobs.key_notice(mid, "unbound"), ref_message_id=mid,
                   expected_state="unbound", body="n%d" % i)
        arm_post(env)
        assert env.outbound.tick() == 1
        assert env.outbound.tick() == 0          # 同一秒内不再发
        env.clock.tick(C.POST_MIN_INTERVAL_MS - 1)
        assert env.outbound.tick() == 0
        env.clock.tick(1)
        assert env.outbound.tick() == 1
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        assert env.outbound.tick() == 1
        assert [q["text"] for q in env.client.calls_for(PM)] == ["n0", "n1", "n2"]

    def test_pacing_independent_across_channels_and_idempotent(self, env):
        bid_a = env.make_binding(status="active", chat_id="C_A", session_id="sA", cc_pid=1111, cc_start="t1")
        bid_b = env.make_binding(status="active", chat_id="C_B", session_id="sB", cc_pid=2222, cc_start="t2")
        env.conn.execute("UPDATE outbound_jobs SET state='cancelled'")
        arm_post(env)
        mk_turn(env, bid_a, group="ga", chat_id="C_A")
        mk_turn(env, bid_b, group="gb", chat_id="C_B")
        key, _ = reaction_job(env, bid_a)
        env.client.on(REACT, lambda m, q: ok())
        assert env.outbound.tick() == 3


# ======================================================================
# 排序(§2.8)
# ======================================================================
class TestOrdering:
    def test_chunks_strict_order_across_ticks(self, env):
        bid = env.make_binding(status="active")
        arm_post(env)
        for i in range(3):
            mk_turn(env, bid, idx=i, body="c%d" % i)
        assert env.outbound.tick() == 1   # 每频道节流:一 tick 一块
        run_ticks(env, 3)
        assert [q["markdown_text"] for q in env.client.calls_for(PM)] == ["c0", "c1", "c2"]
        assert all(row(env, "turn:g:%d" % i)["state"] == "sent" for i in range(3))

    def test_chunk_blocked_while_prev_unknown_then_flows_after_hit(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, timeout(), post_ok)
        mk_turn(env, bid, idx=0, body="c0")
        mk_turn(env, bid, idx=1, body="c1")
        run_ticks(env, 2)
        assert row(env, "turn:g:0")["state"] == "unknown" and row(env, "turn:g:1")["state"] == "pending"
        assert len(env.client.calls_for(PM)) == 1
        jid = row(env, "turn:g:0")["job_id"]
        env.client.on(HIST, lambda m, q: history([hit_msg(jid)]))
        verify_tick(env)
        assert row(env, "turn:g:0")["state"] == "sent"
        run_ticks(env, 2)
        assert row(env, "turn:g:1")["state"] == "sent" and len(env.client.calls_for(PM)) == 2

    def test_chunk_after_failed_prev_cancelled(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, err("channel_not_found"))
        mk_turn(env, bid, idx=0)
        mk_turn(env, bid, idx=1)
        run_ticks(env, 2)
        assert row(env, "turn:g:0")["state"] == "failed"
        r1 = row(env, "turn:g:1")
        assert r1["state"] == "cancelled" and r1["error"] == "prev-chunk-failed"

    def test_pending_backoff_group_blocks_later_group(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, ratelimited(retry_after=3), post_ok)
        mk_turn(env, bid, group="gA")
        mk_turn(env, bid, group="gB")
        env.outbound.tick()
        assert row(env, "turn:gA:0")["state"] == "pending"   # 退避中的 pending 组
        env.clock.tick(1000)
        env.outbound.tick()
        assert row(env, "turn:gB:0")["state"] == "pending" and len(env.client.calls_for(PM)) == 1
        env.clock.tick(2001)
        env.outbound.tick()
        assert row(env, "turn:gA:0")["state"] == "sent" and row(env, "turn:gB:0")["state"] == "pending"
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        assert row(env, "turn:gB:0")["state"] == "sent"

    def test_unknown_group_blocks_later_group(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, timeout(), post_ok)
        env.client.on(HIST, lambda m, q: history([]))
        mk_turn(env, bid, group="gA")
        mk_turn(env, bid, group="gB")
        run_ticks(env, 3)
        assert row(env, "turn:gA:0")["state"] == "unknown" and row(env, "turn:gB:0")["state"] == "pending"
        assert len(env.client.calls_for(PM)) == 1

    def test_group_cancelled_after_unconfirmed_next_turn_sends(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, timeout(), post_ok)
        for i in range(3):
            mk_turn(env, bid, group="gA", idx=i, body="a%d" % i)
        mk_turn(env, bid, group="gB", body="b0")
        env.outbound.tick()
        assert row(env, "turn:gA:0")["state"] == "unknown"
        env.client.on(HIST, lambda m, q: err("channel_not_found"))   # 核验永久错 → unconfirmed
        verify_tick(env)
        assert row(env, "turn:gA:0")["state"] == "unconfirmed"
        for i in (1, 2):
            r = row(env, "turn:gA:%d" % i)
            assert r["state"] == "cancelled" and r["error"] == "prev-unconfirmed"
        assert counter(env, "group_cancelled_after_unconfirmed") == 1
        assert row(env, "turn:gB:0")["state"] == "sent"   # 同一 tick 里下一轮已放行
        bodies = sorted(a["body"] for a in alerts(env))
        assert bodies == sorted([texts.unconfirmed_alert_body(), texts.group_cancelled_alert_body()])
        assert sum(1 for a in alerts(env) if a["body"] == texts.group_cancelled_alert_body()) == 1
        run_ticks(env, 3)
        assert all(a["state"] == "sent" for a in alerts(env))
        assert len(env.client.calls_for(PM)) == 4   # a0、b0、两条告警;a1/a2 绝不发

    def test_group_cancel_via_order_gate_on_legacy_unconfirmed(self, env):
        """前块被外部置为 unconfirmed(未经 _set_unconfirmed)→ 后块 _prepare 的顺序门在同一事务取消余块。"""
        bid = env.make_binding(status="active")
        for i in range(3):
            mk_turn(env, bid, idx=i)
        set_cols(env, "turn:g:0", state="unconfirmed")
        assert env.outbound._prepare(row(env, "turn:g:1")) == "cancelled"
        assert row(env, "turn:g:1")["state"] == "cancelled" and row(env, "turn:g:2")["state"] == "cancelled"
        assert counter(env, "group_cancelled_after_unconfirmed") == 1
        assert [a["body"] for a in alerts(env)] == [texts.group_cancelled_alert_body()]

    def test_other_binding_not_blocked(self, env):
        bid_a = env.make_binding(status="active", chat_id="C_A", session_id="sA", cc_pid=1111, cc_start="t1")
        bid_b = env.make_binding(status="active", chat_id="C_B", session_id="sB", cc_pid=2222, cc_start="t2")
        env.conn.execute("UPDATE outbound_jobs SET state='cancelled'")
        arm_post(env, lambda q: timeout() if q["channel"] == "C_A" else posted(channel=q["channel"]))
        mk_turn(env, bid_a, group="ga", chat_id="C_A")
        mk_turn(env, bid_b, group="gb", chat_id="C_B")
        env.outbound.tick()
        assert row(env, "turn:ga:0")["state"] == "unknown" and row(env, "turn:gb:0")["state"] == "sent"

    def test_notices_same_chat_ordered_by_job_seq(self, env):
        for i in (1, 2):
            inbox(env, "%s:%d.1" % (CHAT, i), "unbound")
        arm_post(env, timeout(), post_ok)
        for i in (1, 2):
            mid = "%s:%d.1" % (CHAT, i)
            mk_job(env, kind="inbound_notice", key=jobs.key_notice(mid, "unbound"), ref_message_id=mid,
                   expected_state="unbound")
        run_ticks(env, 2)
        k1, k2 = ("notice:%s:%d.1:unbound" % (CHAT, i) for i in (1, 2))
        assert row(env, k1)["state"] == "unknown" and row(env, k2)["state"] == "pending"
        env.client.on(HIST, lambda m, q: history([hit_msg(row(env, k1)["job_id"])]))
        verify_tick(env)
        run_ticks(env, 2)
        assert row(env, k1)["state"] == "sent" and row(env, k2)["state"] == "sent"


# ======================================================================
# 守卫表(与 feishu-bridge 相同的 per-kind 表;decision_notice 按 §5.5 六种 outcome)
# ======================================================================
class TestGuards:
    def test_session_turn_binding_not_active_cancelled(self, env):
        bid = env.make_binding(status="closed", close_reason="user_unbind")
        mk_turn(env, bid)
        env.outbound.tick()
        assert row(env, "turn:g:0")["state"] == "cancelled" and env.client.calls == []

    def test_approval_card_cancelled_if_decided(self, env):
        bid = env.make_binding(status="active")
        member_pending(env, bid)
        env.conn.execute("UPDATE pendings SET state='rejected' WHERE pending_id='p1'")
        env.outbound.tick()
        assert row(env, "card:p1")["state"] == "cancelled" and env.client.calls == []

    def test_approval_card_cancelled_if_already_backfilled(self, env):
        bid = env.make_binding(status="active")
        member_pending(env, bid)
        env.conn.execute("UPDATE pendings SET card_message_id=? WHERE pending_id='p1'", (CHAT + ":1.1",))
        env.outbound.tick()
        assert row(env, "card:p1")["state"] == "cancelled"

    def test_approval_card_cancelled_if_binding_closed(self, env):
        bid = env.make_binding(status="active")
        member_pending(env, bid)
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' WHERE binding_id=?", (bid,))
        env.outbound.tick()
        assert row(env, "card:p1")["state"] == "cancelled"

    @pytest.mark.parametrize("outcome,pstate,istate,expect", [
        ("delivered", "approved", "enqueued", "sent"),
        ("delivered", "approved", "materializing", "cancelled"),
        ("delivered", "pending", "enqueued", "cancelled"),
        ("approved_pending_files", "approved", "materializing", "sent"),
        ("approved_pending_files", "approved", "enqueued", "cancelled"),
        ("rejected", "rejected", "rejected", "sent"),
        ("rejected", "approved", "rejected", "cancelled"),
        ("expired", "expired", "expired", "sent"),
        ("expired", "pending", "awaiting_approval", "cancelled"),
        ("attachment_failed", "approved", "failed", "sent"),
        ("attachment_failed", "approved", "materializing", "cancelled"),
        ("closed_undelivered", "approved", "undeliverable", "sent"),
        ("closed_undelivered", "approved", "failed", "cancelled"),
    ])
    def test_decision_notice_six_outcomes_guard(self, env, outcome, pstate, istate, expect):
        bid = env.make_binding(status="active")
        key = decision_job(env, bid, "p1", outcome, card=CHAT + ":5.5", pstate=pstate, istate=istate)
        env.client.on(UPD, lambda m, q: ok({"channel": q["channel"], "ts": q["ts"]}))
        env.outbound.tick()
        assert row(env, key)["state"] == expect
        if expect == "sent":
            assert env.client.calls_for(UPD)[0]["blocks"] == texts.decision_update_blocks(outcome, OWNER)
        else:
            assert env.client.calls == []

    def test_decision_notice_unknown_outcome_cancelled(self, env):
        bid = env.make_binding(status="active")
        mid = "%s:1.1" % CHAT
        inbox(env, mid, "rejected", bid)
        pending(env, "p1", mid, bid, state="rejected")
        mk_job(env, kind="decision_notice", key="dec:p1:bogus", binding_id=bid, ref_pending_id="p1",
               expected_state="bogus")
        env.outbound.tick()
        assert row(env, "dec:p1:bogus")["state"] == "cancelled"

    def test_lifecycle_notice_guard(self, env):
        bid = env.make_binding(status="active")
        arm_post(env)
        mk_job(env, kind="lifecycle_notice", key="lc:%s:bound" % bid, binding_id=bid, expected_state="active")
        mk_job(env, kind="lifecycle_notice", key="lc:%s:user_unbind" % bid, binding_id=bid,
               expected_state="closed:user_unbind")
        run_ticks(env, 2)
        assert row(env, "lc:%s:bound" % bid)["state"] == "sent"
        assert row(env, "lc:%s:user_unbind" % bid)["state"] == "cancelled"

    def test_inbound_and_unsupported_notice_guards(self, env):
        inbox(env, "%s:1.1" % CHAT, "unbound")
        inbox(env, "%s:2.1" % CHAT, "enqueued")
        arm_post(env)
        mk_job(env, kind="inbound_notice", key="notice:%s:1.1:unbound" % CHAT, ref_message_id="%s:1.1" % CHAT,
               expected_state="unbound")
        mk_job(env, kind="unsupported_notice", key="un:%s:2.1" % CHAT, ref_message_id="%s:2.1" % CHAT,
               expected_state="unsupported")
        run_ticks(env, 2)
        assert row(env, "notice:%s:1.1:unbound" % CHAT)["state"] == "sent"
        assert row(env, "un:%s:2.1" % CHAT)["state"] == "cancelled"

    def test_receipt_reaction_cancelled_on_dropped_delivery(self, env):
        bid = env.make_binding(status="active")
        key, _ = reaction_job(env, bid, delivery_state="dropped")
        env.outbound.tick()
        assert row(env, key)["state"] == "cancelled" and env.client.calls == []


# ======================================================================
# allowlist / gate
# ======================================================================
class TestAllowlistAndGate:
    def test_out_of_list_cancelled_even_when_gate_degraded(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        env.cfg["chat_allowlist"] = ["C_OTHER"]
        dbmod.set_state(env.conn, C.GATE_KEY, "degraded:identity_unverified")
        env.outbound.tick()
        assert row(env, "turn:g:0")["state"] == "cancelled" and env.client.calls == []

    def test_in_list_blocked_by_degraded_gate(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        env.cfg["chat_allowlist"] = [CHAT]
        dbmod.set_state(env.conn, C.GATE_KEY, "mismatch")
        assert env.outbound.tick() == 0 and row(env, "turn:g:0")["state"] == "pending"
        dbmod.set_state(env.conn, C.GATE_KEY, "ok")
        arm_post(env)
        assert env.outbound.tick() == 1

    def test_gate_closed_also_pauses_verification(self, env):
        bid = env.make_binding(status="active")
        send_unknown(env, bid)
        dbmod.set_state(env.conn, C.GATE_KEY, "degraded:auth_error")
        env.clock.tick(C.VERIFY_SCHEDULE_MS[0])
        env.outbound.tick()
        assert env.client.calls_for(HIST) == [] and row(env, "turn:g:0")["state"] == "unknown"


# ======================================================================
# startup_scan(§2.9)
# ======================================================================
class TestStartupScan:
    def test_sending_postmessage_to_unknown_verify_only_even_at_cap(self, env):
        bid = env.make_binding(status="active")
        set_verify_ok(env)
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0")
        set_cols(env, "turn:g:0", state="sending", attempt_count=C.TURN_CAP, sending_at=env.clock.wall_ms())
        env.outbound.startup_scan()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["had_unknown"] == 1 and r["verify_after"] == env.clock.wall_ms()
        assert r["next_attempt_at"] is None
        env.client.on(HIST, lambda m, q: history([]))
        for _ in range(3):
            verify_tick(env)
        env.outbound.tick()
        assert row(env, "turn:g:0")["state"] == "unconfirmed" and env.client.calls_for(PM) == []

    def test_sending_idempotent_below_cap_rearmed(self, env):
        bid = env.make_binding(status="active")
        key, ts = reaction_job(env, bid)
        set_cols(env, key, state="sending", attempt_count=1, op_method=REACT, op_target=CHAT, op_payload_kind="reaction")
        env.outbound.startup_scan()
        r = row(env, key)
        assert r["state"] == "unknown" and r["next_attempt_at"] == env.clock.wall_ms() and r["had_unknown"] == 0
        env.client.on(REACT, lambda m, q: ok())
        env.outbound.tick()
        assert row(env, key)["state"] == "sent" and row(env, key)["attempt_count"] == 2

    def test_idempotent_crash_at_cap_no_fourth_request(self, env):
        bid = env.make_binding(status="active")
        key, ts = reaction_job(env, bid)
        set_cols(env, key, state="sending", attempt_count=C.IDEMPOTENT_CAP, op_method=REACT, op_target=CHAT,
                 op_payload_kind="reaction", sending_at=env.clock.wall_ms())
        env.outbound.startup_scan()
        assert row(env, key)["state"] == "failed"
        env.client.on(REACT, lambda m, q: ok())
        for _ in range(3):
            env.clock.tick(C.IDEMPOTENT_RETRY_DELAY_MS + 1)
            env.outbound.tick()
        assert env.client.calls == []

    def test_unknown_without_timing_rearmed_by_category(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0")
        set_cols(env, "turn:g:0", state="unknown", attempt_count=1)
        k_ok, _ = reaction_job(env, bid)
        set_cols(env, k_ok, state="unknown", attempt_count=1, op_method=REACT, op_target=CHAT, op_payload_kind="reaction")
        k_cap, _ = reaction_job(env, bid)
        set_cols(env, k_cap, state="unknown", attempt_count=C.IDEMPOTENT_CAP, op_method=REACT, op_target=CHAT,
                 op_payload_kind="reaction")
        env.outbound.startup_scan()
        now = env.clock.wall_ms()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["verify_after"] == now and r["had_unknown"] == 1
        assert row(env, k_ok)["state"] == "unknown" and row(env, k_ok)["next_attempt_at"] == now
        assert row(env, k_cap)["state"] == "failed"

    def test_pending_at_cap_terminal_by_had_unknown(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid, group="a")
        set_cols(env, "turn:a:0", attempt_count=C.TURN_CAP)
        member_pending(env, bid)
        freeze_turn(env, "card:p1", payload_kind="blocks")
        set_cols(env, "card:p1", attempt_count=C.CARD_CAP, had_unknown=1)
        mk_turn(env, bid, group="b")
        set_cols(env, "turn:b:0", attempt_count=C.TURN_CAP - 1)
        env.outbound.startup_scan()
        assert row(env, "turn:a:0")["state"] == "failed" and len(alerts(env)) == 1
        assert row(env, "card:p1")["state"] == "unconfirmed"
        assert row(env, "turn:b:0")["state"] == "pending"

    def test_sending_without_frozen_op_is_frozen_then_reconciled(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        set_cols(env, "turn:g:0", state="sending", attempt_count=1)
        key = decision_job(env, bid, "p1", "rejected", card=CHAT + ":5.5")
        set_cols(env, key, state="sending", attempt_count=1)
        env.outbound.startup_scan()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["op_method"] == PM and r["had_unknown"] == 1
        d = row(env, key)
        assert d["state"] == "unknown" and d["op_method"] == UPD and d["next_attempt_at"] == env.clock.wall_ms()

    def test_legacy_unknown_with_next_only_self_heals_in_tick(self, env):
        """recovery 旧写法(unknown + next_attempt_at)的 postMessage 行 → tick 自愈为核验,不发送。"""
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        freeze_turn(env, "turn:g:0")
        set_cols(env, "turn:g:0", state="unknown", attempt_count=1, next_attempt_at=env.clock.wall_ms())
        env.client.on(HIST, lambda m, q: history([]))
        env.outbound.tick()
        r = row(env, "turn:g:0")
        assert r["state"] == "unknown" and r["had_unknown"] == 1 and r["next_attempt_at"] is None
        assert r["verify_absent_count"] == 1 and env.client.calls_for(PM) == []


# ======================================================================
# 告警 / 心跳
# ======================================================================
class TestAlertsAndHeartbeat:
    def test_alert_turn_failure_does_not_cascade(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, err("channel_not_found"))
        mk_turn(env, bid)
        env.outbound.tick()
        assert len(alerts(env)) == 1
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()   # 告警本身永久失败
        a = alerts(env)
        assert len(a) == 1 and a[0]["state"] == "failed"

    def test_unconfirmed_alert_turn_does_not_cascade(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, timeout())
        env.client.on(HIST, lambda m, q: err("missing_scope"))
        mk_turn(env, bid)
        env.outbound.tick()
        verify_tick(env)
        assert len(alerts(env)) == 1
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()   # 告警也超时 → unknown → 核验永久错 → unconfirmed,但不再生告警
        verify_tick(env)
        a = alerts(env)
        assert len(a) == 1 and a[0]["state"] == "unconfirmed"

    def test_alert_suppressed_when_binding_closed(self, env):
        bid = env.make_binding(status="active")
        arm_post(env, err("channel_not_found"))
        mk_turn(env, bid)
        env.outbound.tick()
        env.conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' WHERE binding_id=?", (bid,))
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()
        assert alerts(env)[0]["state"] == "cancelled" and len(env.client.calls_for(PM)) == 1

    def test_alert_dedup_by_idempotency_key(self, env):
        bid = env.make_binding(status="active")
        mk_turn(env, bid)
        j = row(env, "turn:g:0")
        with dbmod.tx(env.conn):
            env.outbound._enqueue_alert(j, texts.send_failure_alert_body(), j["job_id"], 0)
            env.outbound._enqueue_alert(j, texts.send_failure_alert_body(), j["job_id"], 0)
        assert len(alerts(env)) == 1

    def test_heartbeat_per_network_roundtrip(self, env):
        beats = []
        env.outbound.heartbeat = lambda: beats.append(1)
        bid = env.make_binding(status="active")
        arm_post(env, timeout(), post_ok)
        env.client.on(HIST, lambda m, q: history([hit_msg(row(env, "turn:ga:0")["job_id"])]))
        mk_turn(env, bid, group="ga")
        mk_turn(env, bid, group="gb")
        env.outbound.tick()          # ga 发送(1)
        verify_tick(env)             # ga 核验命中(2)
        env.clock.tick(C.POST_MIN_INTERVAL_MS)
        env.outbound.tick()          # gb 发送(3)
        assert len(beats) == 3

    def test_log_never_contains_body(self, env):
        lines = []
        env.outbound.log = lines.append
        bid = env.make_binding(status="active")
        arm_post(env, err("channel_not_found"))
        mk_turn(env, bid, body="SECRET-BODY")
        env.outbound.tick()
        assert lines and all("SECRET-BODY" not in ln for ln in lines)
