"""全链路离线集成(WP5):真实模块 + FakeSlackClient + 真实 SQLite,零网络。
daemon 侧用 `DaemonCore.loop_iteration()`(drain → followups → recovery → outbound,与 bin/daemon.py 同序),
listener 侧用 `ListenerCore.step()`,hook 侧用 `hooklib.run_stop_hook`,控制面用 `ctl.bind_prepare / ctl.unbind`
(后者 = `lifecycle.terminate_binding`)。

场景拆成两条(共享 `bind()`):
1. owner DM:bind → 握手激活 + lifecycle_notice(bound) → owner 不 @ 直投(listener 打出 slack_message 行)
   + 👀 回执 → Stop hook 一轮 → session_turn 经 chat.postMessage(markdown_text + metadata)转发。
2. 频道:bind → 成员在线程 @bot → 审批卡(thread_ts=reply_thread_ts,Block Kit)→ owner 点批准 → 投递 +
   decision_notice(delivered) 以 chat.update 去按钮 → 第二张卡一直 pending → 第三张带附件批准后仍在下载 →
   unbind → farewell + lifecycle_notice(close) + 第二张卡 expired + 第三张卡 closed_undelivered。

离线铁律:FakeSlackClient 只登记本场景需要的三个方法;任何未预期外呼 → AssertionError。"""
import json

from lib import constants, ctl, hooklib, texts, util
from lib.listener_core import ListenerCore
from tests.conftest import CC_PID, CHAT, DM, MEMBER, OWNER
from tests.helpers import envelope, message_event, ok, posted, slack_file
from tests.test_inbound import mention, mid_of, none_materializer

PM = "chat.postMessage"
UPD = "chat.update"
REACT = "reactions.add"

ZSH_PID = 8100          # bridgectl:zsh → claude
HOOK_PID = 9001         # hook:python3 → claude
LISTENER = (7001, "listener-start")
SESSION = "sess-1"
MEMBER2 = "U0MEMBER2"
MEMBER3 = "U0MEMBER3"


# ====================================================================== helpers
def wire_instance(env):
    """ppid 链:bridgectl(zsh)与 hook(python3)都挂在同一个 claude(CC_PID)下。"""
    env.prober.set(ZSH_PID, CC_PID, "t-zsh", "zsh")
    env.prober.set(HOOK_PID, CC_PID, "t-hook", "python3")


def arm_slack(env):
    """只登记场景需要的三种外呼;其余任何方法(auth.test / conversations.* / chat.delete …)→ 抛错。"""
    env.client.on(PM, lambda m, p: posted(channel=p["channel"]))
    env.client.on(UPD, lambda m, p: ok({"channel": p["channel"], "ts": p["ts"]}))
    env.client.on(REACT, lambda m, p: ok())


def stop_hook(env, msg, session_id=SESSION):
    return hooklib.run_stop_hook(
        {"session_id": session_id, "last_assistant_message": msg,
         "stop_hook_active": False, "cwd": "/tmp/p"},
        conn=env.conn, prober=env.prober, clock=env.clock, start_pid=HOOK_PID)


def daemon_loop(env, listener, n=1, gap=1500):
    """一轮 = daemon `loop_iteration()`(drain → followups → gate → recovery → outbound)+ listener `step()`
    (心跳 + 领取打印)+ 墙钟前进(越过每频道 postMessage 节流)。→ 最后一次 listener.step() 的结果。"""
    last = None
    for _ in range(n):
        env.core.loop_iteration()
        last = listener.step()
        env.clock.tick(gap)
    return last


def metadata_of(job_id):
    return {"event_type": constants.METADATA_EVENT_TYPE, "event_payload": {"job_id": job_id}}


def post_params(channel, job_id, thread_ts=None, **payload):
    p = {"channel": channel, "unfurl_links": False, "unfurl_media": False, "metadata": metadata_of(job_id)}
    if thread_ts:
        p["thread_ts"] = thread_ts
    p.update(payload)
    return p


def update_params(card_message_id, outcome, decided_by):
    channel, ts = util.split_message_id(card_message_id)
    return {"channel": channel, "ts": ts, "blocks": texts.decision_update_blocks(outcome, decided_by),
            "text": texts.decision_update_text(outcome)}


def job(env, key):
    r = env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()
    assert r is not None, key
    return r


def binding(env, bid):
    return env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()


def pending(env, pid):
    return env.conn.execute("SELECT * FROM pendings WHERE pending_id=?", (pid,)).fetchone()


def msg_lines(lines):
    return [l for l in lines if l.get("type") == "slack_message"]


def has_buttons(blocks):
    return any(b.get("type") == "actions" for b in blocks)


def bind(env, chat_id, chat_name):
    """bind_prepare → listener 认领 → bind turn 的 Stop hook 带 marker 握手 → active →
    daemon 一轮把 lifecycle_notice(bound) 发出。→ (binding_id, listener, lines)。"""
    res = ctl.bind_prepare(env.conn, env.cfg, env.clock, env.prober,
                           chat_id=chat_id, chat_name=chat_name, cwd="/tmp/p", start_pid=ZSH_PID)
    bid = res["binding_id"]
    assert res["marker"].startswith(constants.MARKER_PREFIX)
    assert binding(env, bid)["status"] == "starting"

    lines = []
    listener = ListenerCore(env.conn, bid, env.clock, env.prober,
                            me_pid=LISTENER[0], me_start=LISTENER[1],
                            printer=lambda s: lines.append(json.loads(s)),
                            daemon_alive_probe=lambda: True, ensure_daemon=lambda: None)
    assert listener.step() == "ok"                      # 认领:epoch 0 → 1,写心跳
    b = binding(env, bid)
    assert (b["listener_pid"], b["listener_start"], b["listener_epoch"]) == (*LISTENER, 1)

    # bind turn:模型回复里带 marker → 握手激活;这一 turn 本身被抑制(未绑定 session 的输出绝不外发)
    r = stop_hook(env, "ok\n%s\n%s" % (res["marker"], res["banner"]))
    assert r == {"suppressed": True, "reason": "bind-handshake"}
    b = binding(env, bid)
    assert b["status"] == "active" and b["session_id"] == SESSION
    assert env.client.calls == []                       # 激活之前零外发

    daemon_loop(env, listener)
    lc = job(env, "lc:%s:bound" % bid)
    assert lc["state"] == "sent" and lc["op_method"] == PM and lc["sent_message_id"].startswith(chat_id + ":")
    assert env.client.calls == [(PM, post_params(chat_id, lc["job_id"], text=texts.LC_BOUND))]
    assert lines == []                                  # 还没有投递
    return bid, listener, lines


# ====================================================================== 1) owner DM
def test_owner_dm_bind_deliver_receipt_and_turn(env):
    wire_instance(env)
    arm_slack(env)
    bid, listener, lines = bind(env, DM, "owner DM")

    # owner 在自己的 DM 里发消息:无需 @bot;consumer 接收时钉死 binding_id
    ev = message_event(text="查个数", channel=DM, user=OWNER)
    staged = env.stage("events_api", envelope(ev))
    assert staged["state"] == "staged" and staged["binding_id"] == bid
    mid = mid_of(ev)

    daemon_loop(env, listener)                          # drain → 直投入队 + 👀 → 发出;listener 打出
    se = env.slack_events()
    assert len(se) == 1 and se[0]["state"] == "consumed" and se[0]["error"] is None
    assert env.inbox_row(mid)["state"] == "enqueued"
    d = env.deliveries(bid)
    assert len(d) == 1 and d[0]["state"] == "emitted"
    assert msg_lines(lines) == [{
        "type": "slack_message", "delivery_seq": d[0]["delivery_seq"], "message_id": mid,
        "chat_id": DM, "ts": ev["ts"], "thread_ts": None,
        "sender_user_id": OWNER, "sender_is_owner": True, "approved_by": None,
        "message_type": "message", "text": "查个数", "media_paths": [], "files": [],
    }]
    rc = job(env, "rc:%d" % d[0]["delivery_seq"])
    assert rc["state"] == "sent" and rc["op_method"] == REACT
    assert env.client.calls[-1] == (REACT, {"channel": DM, "timestamp": ev["ts"],
                                            "name": constants.RECEIPT_REACTION})

    # session 一轮最终输出 → Stop hook 入队 → chat.postMessage(markdown_text + metadata)
    r = stop_hook(env, "查到了:42")
    assert r["reason"] == "enqueued" and r["chunks"] == 1
    daemon_loop(env, listener)
    turn = job(env, "turn:%s:0" % r["turn_group"])
    assert turn["state"] == "sent" and turn["op_payload_kind"] == "markdown_text"
    assert turn["body"] == "查到了:42"                  # 无 transcript → 页脚为空,正文原样
    assert turn["sent_message_id"].startswith(DM + ":")
    assert env.client.calls[-1] == (PM, post_params(DM, turn["job_id"], markdown_text="查到了:42"))

    # 全程外呼恰好三次:bound 通知、👀、turn
    assert [m for m, _ in env.client.calls] == [PM, REACT, PM]
    assert env.client.waits == []
    assert binding(env, bid)["status"] == "active"


# ====================================================================== 2) 频道:审批 → unbind
def test_channel_member_approval_pending_cards_and_unbind(env):
    wire_instance(env)
    arm_slack(env)
    bid, listener, lines = bind(env, CHAT, "测试频道")
    thread_ts = "1699999990.000001"                     # 频道里已有的线程

    # ① 成员在线程里 @bot → 审批卡发在同一线程(thread_ts = reply_thread_ts)
    ev1 = message_event(text=mention("member 求助"), channel=CHAT, user=MEMBER, thread_ts=thread_ts)
    env.stage("events_api", envelope(ev1))
    daemon_loop(env, listener)
    mid1 = mid_of(ev1)
    assert env.inbox_row(mid1)["state"] == "awaiting_approval"
    p1 = env.pendings()[0]
    assert p1["state"] == "pending" and p1["message_id"] == mid1
    card1 = job(env, "card:%s" % p1["pending_id"])
    assert card1["state"] == "sent" and card1["reply_to"] == thread_ts and card1["op_thread_ts"] == thread_ts
    card1_json = json.loads(card1["body"])
    assert env.client.calls[-1] == (PM, post_params(CHAT, card1["job_id"], thread_ts=thread_ts,
                                                    blocks=card1_json["blocks"], text=card1_json["text"]))
    actions = [b for b in card1_json["blocks"] if b["type"] == "actions"]
    assert len(actions) == 1
    assert [e["action_id"] for e in actions[0]["elements"]] == list(constants.ACTION_IDS)
    p1 = pending(env, p1["pending_id"])
    assert p1["card_message_id"] == card1["sent_message_id"]   # 发出后回填卡片身份
    assert env.deliveries(bid) == [] and lines == []           # 未批准前不投递

    # ② owner 点「投递」→ 同一事务:approved + deliveries + decision_notice(delivered);
    #    卡片以 chat.update 覆盖(无按钮);listener 打出成员消息(sender_is_owner=false,approved_by=owner)
    env.click(act="approve")                                   # 唯一 pending → 缺省 pending_id/nonce/card_ts
    daemon_loop(env, listener)
    p1 = pending(env, p1["pending_id"])
    assert p1["state"] == "approved" and p1["decided_by"] == OWNER
    assert env.inbox_row(mid1)["state"] == "enqueued"
    d = env.deliveries(bid)
    assert len(d) == 1 and d[0]["state"] == "emitted"
    assert msg_lines(lines) == [{
        "type": "slack_message", "delivery_seq": d[0]["delivery_seq"], "message_id": mid1,
        "chat_id": CHAT, "ts": ev1["ts"], "thread_ts": thread_ts,
        "sender_user_id": MEMBER, "sender_is_owner": False, "approved_by": OWNER,
        "message_type": "message", "text": "member 求助", "media_paths": [], "files": [],
    }]
    assert env.jobs("receipt_reaction") == []                  # 审批路径无 👀
    dec1 = job(env, "dec:%s:delivered" % p1["pending_id"])
    assert dec1["state"] == "sent" and dec1["op_method"] == UPD and dec1["op_target"] == p1["card_message_id"]
    assert env.client.calls[-1] == (UPD, update_params(p1["card_message_id"], "delivered", OWNER))
    assert not has_buttons(env.client.calls[-1][1]["blocks"])
    interactive = env.slack_events()[-1]
    assert interactive["envelope_type"] == "interactive" and interactive["state"] == "consumed"

    # ③ session 对成员请求的回复 → 转发回频道
    r = stop_hook(env, "已按 member 的请求处理")
    assert r["reason"] == "enqueued"
    daemon_loop(env, listener)
    turn = job(env, "turn:%s:0" % r["turn_group"])
    assert turn["state"] == "sent"
    assert env.client.calls[-1] == (PM, post_params(CHAT, turn["job_id"], markdown_text=turn["body"]))

    # ④ 第二个成员(顶层消息)→ 卡片发出后一直没人点
    ev2 = message_event(text=mention("second"), channel=CHAT, user=MEMBER2)
    env.stage("events_api", envelope(ev2))
    daemon_loop(env, listener)
    p2 = [p for p in env.pendings() if p["message_id"] == mid_of(ev2)][0]
    card2 = job(env, "card:%s" % p2["pending_id"])
    assert p2["state"] == "pending" and card2["state"] == "sent" and card2["op_thread_ts"] == ev2["ts"]
    p2 = pending(env, p2["pending_id"])
    assert p2["card_message_id"] == card2["sent_message_id"]

    # ⑤ 第三个成员带附件 → 批准 → 「已批准,附件处理中」;下载瞬态失败 → 仍 materializing(走预算)
    dl_calls = []
    env.inbound.materializer = none_materializer(dl_calls)
    ev3 = message_event(text=mention("看附件"), channel=CHAT, user=MEMBER3,
                        files=[slack_file(id="F0000003", name="a.pdf")])
    env.stage("events_api", envelope(ev3))
    daemon_loop(env, listener)
    mid3 = mid_of(ev3)
    p3 = [p for p in env.pendings() if p["message_id"] == mid3][0]
    assert p3["state"] == "pending" and job(env, "card:%s" % p3["pending_id"])["state"] == "sent"
    env.click(pending_id=p3["pending_id"], nonce=p3["nonce"], act="approve")
    daemon_loop(env, listener)
    p3 = pending(env, p3["pending_id"])
    assert p3["state"] == "approved" and p3["decided_by"] == OWNER
    ib3 = env.inbox_row(mid3)
    assert ib3["state"] == "materializing" and ib3["materialize_reason"] == "approved"
    assert ib3["materialize_attempts"] == 1 and ib3["materialize_next_at"] > env.clock.wall_ms() - 1500
    assert len(dl_calls) == 1 and dl_calls[0]["message_id"] == mid3
    dec3a = job(env, "dec:%s:approved_pending_files" % p3["pending_id"])
    assert dec3a["state"] == "sent"
    assert env.client.calls[-1] == (UPD, update_params(p3["card_message_id"], "approved_pending_files", OWNER))
    assert len(env.deliveries(bid)) == 1                       # 附件未就绪:不投递

    # ⑥ unbind(bridgectl → lifecycle.terminate_binding):立即生效
    res = ctl.unbind(env.conn, env.clock, env.prober, start_pid=ZSH_PID)
    assert res["ok"] and res["binding_id"] == bid
    b = binding(env, bid)
    assert b["status"] == "closed" and b["close_reason"] == "user_unbind"
    assert listener.step() == "exit"                           # listener 立即 farewell 退出
    assert lines[-1] == {"type": "farewell", "code": "user_unbind"}
    # 解绑后的 turn 不入队(Stop hook 视 session 为未绑定)
    assert stop_hook(env, "解绑后的输出") == {"suppressed": False, "reason": "no-binding"}
    assert env.jobs("session_turn")[-1]["idempotency_key"] == turn["idempotency_key"]
    # 级联结果落库:第二张卡 expired,第三张(approved ∧ materializing)undeliverable
    assert pending(env, p2["pending_id"])["state"] == "expired"
    assert env.inbox_row(mid_of(ev2))["state"] == "expired"
    assert pending(env, p3["pending_id"])["state"] == "approved"
    assert env.inbox_row(mid3)["state"] == "undeliverable"

    # daemon 继续跑:只发本次终止的三条通知,零其它外发(deliveries 无 enqueued 残留)
    n_before = len(env.client.calls)
    daemon_loop(env, listener, n=3)
    lc_close = job(env, "lc:%s:user_unbind" % bid)
    dec2 = job(env, "dec:%s:expired" % p2["pending_id"])
    dec3b = job(env, "dec:%s:closed_undelivered" % p3["pending_id"])
    assert (lc_close["state"], dec2["state"], dec3b["state"]) == ("sent", "sent", "sent")
    assert env.client.calls[n_before:] == [
        (UPD, update_params(p2["card_message_id"], "expired", None)),
        (UPD, update_params(p3["card_message_id"], "closed_undelivered", OWNER)),
        (PM, post_params(CHAT, lc_close["job_id"], text=texts.lifecycle_close_body("user_unbind"))),
    ]
    assert not has_buttons(env.client.calls[n_before][1]["blocks"])
    assert not has_buttons(env.client.calls[n_before + 1][1]["blocks"])
    assert all(dl["state"] != "enqueued" for dl in env.deliveries(bid))
    assert dec3a["state"] == "sent" and job(env, "dec:%s:delivered" % p1["pending_id"])["state"] == "sent"

    # 全程外呼统计:postMessage = bound + 3 张卡 + 1 turn + close;update = delivered / approved_pending_files /
    # expired / closed_undelivered;成员路径无 👀;无任何核验 / auth.test / 删除
    methods = [m for m, _ in env.client.calls]
    assert methods.count(PM) == 6 and methods.count(UPD) == 4 and methods.count(REACT) == 0
    assert set(methods) == {PM, UPD}
    assert env.client.waits == []
    assert all(r["state"] == "consumed" for r in env.slack_events())
    assert all(j["state"] in constants.OUTBOUND_TERMINAL_STATES for j in env.jobs())
