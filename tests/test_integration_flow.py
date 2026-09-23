"""全链路离线集成:bind → 握手激活 → owner 直投+回执 → 转发 turn →
member 审批卡 → approve 投递 → unbind farewell。全 fake,零网络。"""
import json

from tests.conftest import APP_ID, CHAT, CC_PID, MEMBER, OWNER
from tests.helpers import bot_mention, mget_snapshot, ok_envelope
from lib import ctl, hooklib
from lib.listener_core import ListenerCore

ZSH_PID = 8100
HOOK_PID = 9001


def test_full_lifecycle(env):
    # ppid 链:bridgectl(zsh→claude)与 hook(python→claude)
    env.prober.set(ZSH_PID, CC_PID, "t", "zsh")
    env.prober.set(HOOK_PID, CC_PID, "t", "python3")
    env.prober.set(7001, 1, "listener-start", "python3")
    sent_texts = []

    def send_ok(args, cwd):
        # session_turn 走 --markdown;通知走 --text;两者都收集
        if "--markdown" in args:
            sent_texts.append(args[args.index("--markdown") + 1])
        elif "--text" in args:
            sent_texts.append(args[args.index("--text") + 1])
        else:
            sent_texts.append("<card>")
        return ok_envelope({"message_id": f"om_out_{len(sent_texts)}"})

    env.runner.on_prefix(["im", "+messages-send"], send_ok)
    env.runner.on_prefix(["im", "+messages-reply"], send_ok)
    env.runner.on_prefix(["im", "reactions", "create"], lambda a, c: ok_envelope({"reaction_id": "r"}))

    # 1) bind
    res = ctl.bind_prepare(env.conn, env.cfg, env.clock, env.prober,
                           chat_id=CHAT, chat_name="测试群", cwd="/tmp/p", start_pid=ZSH_PID)
    bid = res["binding_id"]

    # 2) listener 接管(Monitor 内)
    lines = []
    listener = ListenerCore(env.conn, bid, env.clock, env.prober, me_pid=7001,
                            me_start="listener-start",
                            printer=lambda s: lines.append(json.loads(s)),
                            daemon_alive_probe=lambda: True, ensure_daemon=lambda: None)
    assert listener.step() == "ok"

    # 3) Stop hook 握手(bind turn 抑制 + 激活)
    r = hooklib.run_stop_hook(
        {"session_id": "sess-1", "last_assistant_message": f"ok\n{res['marker']}\nbanner",
         "stop_hook_active": False},
        conn=env.conn, prober=env.prober, clock=env.clock, start_pid=HOOK_PID)
    assert r["suppressed"] and r["reason"] == "bind-handshake"
    b = env.conn.execute("SELECT status FROM bindings WHERE binding_id=?", (bid,)).fetchone()
    assert b["status"] == "active"
    env.outbound.tick()
    assert any("已绑定" in t for t in sent_texts)  # ✅ 已绑定 通知

    # 4) owner @bot 指令 → 直投 → listener 打出 → 回执
    env.arm_mget([mget_snapshot("om_owner", CHAT, OWNER, text="查个数",
                                mentions=[bot_mention(APP_ID)]),
                  mget_snapshot("om_member", CHAT, MEMBER, text="member 求助",
                                mentions=[bot_mention(APP_ID)])])
    env.recv_event(message_id="om_owner", sender_id=OWNER)
    env.clock.tick(3000)
    listener.step()
    msg_lines = [l for l in lines if l.get("type") == "feishu_message"]
    assert len(msg_lines) == 1 and msg_lines[0]["sender_is_owner"] is True
    assert msg_lines[0]["text"] == "查个数"  # E3:去 @bot 前缀后的原文
    env.outbound.tick()
    assert len(env.runner.calls_matching("im", "reactions", "create")) == 1  # 👀

    # 5) session turn 最终输出 → 自动转发
    r = hooklib.run_stop_hook(
        {"session_id": "sess-1", "last_assistant_message": "查到了:42",
         "stop_hook_active": False},
        conn=env.conn, prober=env.prober, clock=env.clock, start_pid=HOOK_PID)
    assert r["reason"] == "enqueued"
    env.outbound.tick()
    assert "查到了:42" in sent_texts

    # 6) member 消息 → 审批卡 → owner 批准 → 投递(标注不可信)
    env.recv_event(message_id="om_member", sender_id=MEMBER)
    p = env.conn.execute("SELECT * FROM pendings WHERE state='pending'").fetchone()
    assert p is not None
    env.outbound.tick()  # 发卡片(reply)
    assert len(env.runner.calls_matching("im", "+messages-reply")) == 1
    card_mid = env.conn.execute("SELECT card_message_id FROM pendings WHERE pending_id=?",
                                (p["pending_id"],)).fetchone()[0]
    assert card_mid is not None  # 发出后已回填
    cb = {"type": "card.action.trigger", "event_id": "cb_int",
          "operator_id": OWNER, "chat_id": CHAT, "message_id": card_mid,
          "action_value": json.dumps({"pending_id": p["pending_id"],
                                      "nonce": p["nonce"], "act": "approve"})}
    assert env.approval.process_event(cb) == "applied"
    env.clock.tick(3000)
    listener.step()
    msg_lines = [l for l in lines if l.get("type") == "feishu_message"]
    assert len(msg_lines) == 2
    assert msg_lines[1]["sender_is_owner"] is False
    assert msg_lines[1]["approved_by"] == OWNER
    env.outbound.tick()
    assert any("已投递" in t for t in sent_texts)

    # 7) unbind → 立即生效:后续 turn 不入队;listener farewell
    res_u = ctl.unbind(env.conn, env.clock, env.prober, start_pid=ZSH_PID)
    assert res_u["ok"]
    r = hooklib.run_stop_hook(
        {"session_id": "sess-1", "last_assistant_message": "解绑后的输出",
         "stop_hook_active": False},
        conn=env.conn, prober=env.prober, clock=env.clock, start_pid=HOOK_PID)
    assert r["reason"] == "no-binding"
    env.outbound.tick()
    assert all("解绑后的输出" not in t for t in sent_texts)
    assert listener.step() == "exit"
    assert lines[-1] == {"type": "farewell", "code": "user_unbind"}
