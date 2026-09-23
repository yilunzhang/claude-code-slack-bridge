"""unbind 级联 + 线性化语义:
提交前已 pending→sending 的 job 允许其后可见(≤1 在途,如实声明),此后零新增外发。"""
import json

from lib import jobs, lifecycle, texts
from tests.conftest import CHAT
from tests.helpers import posted

PM = "chat.postMessage"


def _row(env, key):
    return env.conn.execute("SELECT * FROM outbound_jobs WHERE idempotency_key=?", (key,)).fetchone()


def test_inflight_sending_survives_unbind_then_completes(env):
    bid = env.make_binding(status="active")
    jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                    idempotency_key="turn:g:0", turn_group="g", chunk_index=0,
                    body="hello", now=env.clock.wall_ms())
    # daemon 已把该 job CAS 到 sending(线性化点已过,op_* 已冻结),网络发送尚未返回
    assert env.outbound._prepare(_row(env, "turn:g:0")) == "send"
    r = _row(env, "turn:g:0")
    assert r["state"] == "sending" and r["op_method"] == PM
    # 并发 unbind(bridgectl 进程):立即生效
    assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
    assert _row(env, "turn:g:0")["state"] == "sending"  # 在途豁免(如实声明,不虚称绝无)
    # 发送完成 → sending→sent 仍然成立(结果按冻结 op_* 收口,不受绑定终止影响)
    env.client.on(PM, lambda m, p: posted(channel=p["channel"], ts="1700000000.000900"))
    env.outbound._send_and_finalize(r["job_id"])
    r = _row(env, "turn:g:0")
    assert r["state"] == "sent" and r["sent_message_id"] == CHAT + ":1700000000.000900"


def test_zero_new_sends_after_unbind(env):
    bid = env.make_binding(status="active")
    env.client.on(PM, lambda m, p: posted(channel=p["channel"]))
    lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
    # unbind 之后才轮到的 pending job → 守卫取消,零外发
    jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                    idempotency_key="turn:g2:0", turn_group="g2", chunk_index=0,
                    body="post-unbind", now=env.clock.wall_ms())
    for _ in range(3):
        env.outbound.tick()
        env.clock.tick(2000)
    st = {r["idempotency_key"]: r["state"] for r in env.jobs("session_turn")}
    assert st["turn:g2:0"] == "cancelled"
    sends = env.client.calls_for(PM)
    assert all("post-unbind" not in json.dumps(p, ensure_ascii=False) for p in sends)
    # 只允许本次终止的 lifecycle_notice 外发(固定文案,text 形态)
    assert [p.get("text") for p in sends] == [texts.lifecycle_close_body("user_unbind")]
    assert _row(env, "lc:%s:user_unbind" % bid)["state"] == "sent"


def test_unbind_race_two_terminators_single_cascade(env):
    bid = env.make_binding(status="active")
    assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock) is True
    assert lifecycle.terminate_binding(env.conn, bid, "listener_gone", env.clock,
                                       new_status="dead") is False
    # 胜者的 lifecycle notice 唯一
    lc = env.jobs("lifecycle_notice")
    assert len(lc) == 1 and lc[0]["idempotency_key"] == f"lc:{bid}:user_unbind"
