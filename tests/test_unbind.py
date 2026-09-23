"""unbind 级联 + 线性化语义(plan 4.5/4.6):
提交前已 pending→sending 的 job 允许其后可见(≤1 在途),此后零新增。"""
from tests.conftest import CHAT
from tests.helpers import ok_envelope
from lib import db as dbmod, jobs, lifecycle


def test_inflight_sending_survives_unbind_then_completes(env):
    bid = env.make_binding(status="active")
    jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                    idempotency_key="turn:g:0", turn_group="g", chunk_index=0,
                    body="hello", now=env.clock.wall_ms())
    # 模拟:daemon 已把该 job CAS 到 sending(线性化点已过),网络发送尚未返回
    assert dbmod.cas(env.conn,
                     "UPDATE outbound_jobs SET state='sending', attempt_count=1, sending_at=? "
                     "WHERE idempotency_key='turn:g:0' AND state='pending'",
                     (env.clock.wall_ms(),))
    # 并发 unbind(bridgectl 进程):立即生效
    assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
    row = env.conn.execute(
        "SELECT state FROM outbound_jobs WHERE idempotency_key='turn:g:0'").fetchone()
    assert row[0] == "sending"  # 在途豁免(如实声明,不虚称绝无)
    # 发送完成 → sending→sent 仍然成立
    env.runner.on_prefix(["im", "+messages-send"],
                         lambda a, c: ok_envelope({"message_id": "om_late"}))
    env.outbound._send_and_finalize(env.conn.execute(
        "SELECT job_id FROM outbound_jobs WHERE idempotency_key='turn:g:0'").fetchone()[0])
    row = env.conn.execute(
        "SELECT state, sent_message_id FROM outbound_jobs "
        "WHERE idempotency_key='turn:g:0'").fetchone()
    assert row[0] == "sent" and row[1] == "om_late"


def test_zero_new_sends_after_unbind(env):
    bid = env.make_binding(status="active")
    env.runner.on_prefix(["im", "+messages-send"],
                         lambda a, c: ok_envelope({"message_id": "om_lc"}))
    lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock)
    # unbind 之后才轮到的 pending job → 守卫取消,零外发
    jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                    idempotency_key="turn:g2:0", turn_group="g2", chunk_index=0,
                    body="post-unbind", now=env.clock.wall_ms())
    env.outbound.tick()
    st = {r["idempotency_key"]: r["state"] for r in env.jobs("session_turn")}
    assert st["turn:g2:0"] == "cancelled"
    sends = env.runner.calls_matching("im", "+messages-send")
    # 只允许本次终止的 lifecycle_notice 外发(通知 --text;若有 session_turn 会是 --markdown)
    def _body(argv):
        flag = "--markdown" if "--markdown" in argv else "--text"
        return argv[argv.index(flag) + 1]
    texts_sent = [_body(c[0]) for c in sends]
    assert all("post-unbind" not in t for t in texts_sent)


def test_unbind_race_two_terminators_single_cascade(env):
    bid = env.make_binding(status="active")
    assert lifecycle.terminate_binding(env.conn, bid, "user_unbind", env.clock) is True
    assert lifecycle.terminate_binding(env.conn, bid, "listener_gone", env.clock,
                                       new_status="dead") is False
    # 胜者的 lifecycle notice 唯一
    lc = env.jobs("lifecycle_notice")
    assert len(lc) == 1 and lc[0]["idempotency_key"] == f"lc:{bid}:user_unbind"
