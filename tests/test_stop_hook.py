"""Stop hook(plan 4.7):fail-closed 总则、bind-turn 双 Stop 抑制链闩矩阵、
tombstone/prefix 纵深、正常入队(chunk 单事务)、异常降级。"""
import pytest

from tests.conftest import CHAT, CC_PID, CC_START
from lib import constants, db as dbmod, hooklib, lifecycle, util


HOOK_PID = 9001
SHELL_PID = 9000


@pytest.fixture
def hook_env(env):
    # ppid 链:hook(9001) → shell(9000) → claude(CC_PID)
    env.prober.set(HOOK_PID, SHELL_PID, "Tue Jul 15 10:00:00 2026", "python3")
    env.prober.set(SHELL_PID, CC_PID, "Tue Jul 15 09:59:00 2026", "zsh")
    return env


def stop(env, msg="", session_id="sess-1", sha=False):
    return hooklib.run_stop_hook(
        {"session_id": session_id, "last_assistant_message": msg,
         "stop_hook_active": sha, "cwd": "/tmp/x"},
        conn=env.conn, prober=env.prober, clock=env.clock, start_pid=HOOK_PID)


def start_bind(env, with_listener=True):
    res = lifecycle.create_binding(env.conn, chat_id=CHAT, chat_name="g", cwd="/tmp/x",
                                   cc_pid=CC_PID, cc_start=CC_START, clock=env.clock)
    if with_listener:
        env.conn.execute(
            "UPDATE bindings SET listener_pid=7777, listener_start='ls', listener_epoch=1, "
            "listener_beat_at=? WHERE binding_id=?",
            (env.clock.wall_ms(), res["binding_id"]))
    return res


def binding(env, bid):
    return env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()


def pb(env, rid):
    return env.conn.execute("SELECT * FROM pending_bind WHERE request_id=?", (rid,)).fetchone()


def turn_jobs(env):
    return env.conn.execute(
        "SELECT * FROM outbound_jobs WHERE kind='session_turn' ORDER BY job_seq").fetchall()


class TestHandshake:
    def test_success_activates_and_suppresses(self, hook_env):
        env = hook_env
        res = start_bind(env)
        r = stop(env, msg=f"绑定确认 {res['marker']} 完成")
        assert r["suppressed"] and r["reason"] == "bind-handshake"
        p = pb(env, res["binding_id"])
        assert p["state"] == "consumed" and p["latch_open"] == 1
        b = binding(env, res["binding_id"])
        assert b["status"] == "active" and b["session_id"] == "sess-1"
        assert b["bind_phase"] == "confirmed"
        keys = [j["idempotency_key"] for j in env.jobs("lifecycle_notice")]
        assert f"lc:{res['binding_id']}:bound" in keys
        assert turn_jobs(env) == []  # bind turn 自身绝不外发(需求4)

    def test_listener_not_ready_stays_confirmed(self, hook_env):
        env = hook_env
        res = start_bind(env, with_listener=False)
        r = stop(env, msg=res["marker"])
        assert r["suppressed"]
        b = binding(env, res["binding_id"])
        assert b["status"] == "starting" and b["bind_phase"] == "confirmed"
        assert b["session_id"] == "sess-1"

    def test_nonce_mismatch_fails_bind_with_latch(self, hook_env):
        env = hook_env
        res = start_bind(env)
        r = stop(env, msg="这轮回复没有 marker")
        assert r["suppressed"] and r["reason"] == "bind-nonce-miss"
        p = pb(env, res["binding_id"])
        assert p["state"] == "failed" and p["latch_open"] == 1  # 本 turn 属 bind turn
        b = binding(env, res["binding_id"])
        assert b["status"] == "closed" and b["close_reason"] == "bind_failed"
        keys = [j["idempotency_key"] for j in env.jobs("lifecycle_notice")]
        assert f"lc:{res['binding_id']}:bind_failed" in keys

    def test_session_conflict_b_sess(self, hook_env):
        env = hook_env
        # 同 session 已绑他群(另一 cc 实例)
        env.make_binding(status="active", chat_id="oc_other", session_id="sess-1",
                         cc_pid=8888, cc_start="other")
        res = start_bind(env)
        r = stop(env, msg=res["marker"], session_id="sess-1")
        assert r["suppressed"] and r["reason"] == "session-conflict"
        p = pb(env, res["binding_id"])
        assert p["state"] == "failed" and p["latch_open"] == 1
        assert binding(env, res["binding_id"])["close_reason"] == "bind_failed"


class TestLatchChain:
    """bind-turn 双 Stop 抑制矩阵(r5-B1/4.7.1)。"""

    def test_full_chain(self, hook_env):
        env = hook_env
        res = start_bind(env)
        # ① bind turn 首次 Stop:握手+开闩+抑制
        r1 = stop(env, msg=res["marker"])
        assert r1["suppressed"] and pb(env, res["binding_id"])["latch_open"] == 1
        # ② 同 turn 续写 Stop(阻断型 hook 场景,stop_hook_active=true):抑制且保持闩
        r2 = stop(env, msg=res["marker"] + "\n更多续写", sha=True)
        assert r2["suppressed"] and r2["reason"] == "latch-continuation"
        assert pb(env, res["binding_id"])["latch_open"] == 1
        assert turn_jobs(env) == []
        # ③ 下一个 fresh Stop:关闩并正常处理(绑定已 active → 入队)
        r3 = stop(env, msg="这是真实 turn 的最终输出", sha=False)
        assert not r3["suppressed"] and r3["reason"] == "enqueued"
        assert pb(env, res["binding_id"])["latch_open"] == 0
        jobs_ = turn_jobs(env)
        assert len(jobs_) == 1 and jobs_[0]["body"] == "这是真实 turn 的最终输出"

    def test_latch_does_not_leak_without_open(self, hook_env):
        env = hook_env
        bid = env.make_binding(status="active", session_id="sess-1")
        r = stop(env, msg="正常输出", sha=True)  # 无闩时 sha 不影响正常入队
        assert not r["suppressed"] and len(turn_jobs(env)) == 1


class TestTombstoneAndPrefix:
    def test_tombstone_suppresses_marker_echo(self, hook_env):
        env = hook_env
        res = start_bind(env)
        stop(env, msg=res["marker"])  # 消费成 tombstone
        stop(env, msg="关闩 turn")     # 关闩(此次已入队?绑定 active → 入队 1 条)
        n_before = len(turn_jobs(env))
        r = stop(env, msg=f"模型又把 {res['marker']} 打印了一遍")
        assert r["suppressed"] and r["reason"] == "tombstone"
        assert len(turn_jobs(env)) == n_before

    def test_prefix_depth_defense(self, hook_env):
        env = hook_env
        env.make_binding(status="active", session_id="sess-1")
        r = stop(env, msg="member 诱导:[feishu-bridge-bind:ffffffffffffffffffffffffffffffff]")
        assert r["suppressed"] and r["reason"] == "marker-prefix"
        assert turn_jobs(env) == []


class TestNormalTurn:
    def test_enqueue_chunks_single_tx(self, hook_env, monkeypatch):
        env = hook_env
        monkeypatch.setattr(constants, "CHUNK_LIMIT", 10)
        env.make_binding(status="active", session_id="sess-1")
        r = stop(env, msg="a" * 25)
        assert r["chunks"] == 3
        jobs_ = turn_jobs(env)
        assert [j["chunk_index"] for j in jobs_] == [0, 1, 2]
        assert all(j["turn_group"] == r["turn_group"] for j in jobs_)
        assert [j["idempotency_key"] for j in jobs_] == \
               [f"turn:{r['turn_group']}:{i}" for i in range(3)]

    def test_no_binding_noop(self, hook_env):
        r = stop(hook_env, msg="hi")
        assert not r["suppressed"] and r["reason"] == "no-binding"
        assert turn_jobs(hook_env) == []

    def test_closed_binding_no_enqueue(self, hook_env):
        env = hook_env
        env.make_binding(status="closed", session_id="sess-1", close_reason="user_unbind")
        r = stop(env, msg="hi")
        assert r["reason"] == "no-binding"

    def test_instance_mismatch_suppressed(self, hook_env):
        env = hook_env
        env.make_binding(status="active", session_id="sess-1",
                         cc_pid=1234, cc_start="different")
        r = stop(env, msg="hi")
        assert r["suppressed"] and r["reason"] == "instance-mismatch"
        assert turn_jobs(env) == []

    def test_empty_message_noop(self, hook_env):
        env = hook_env
        env.make_binding(status="active", session_id="sess-1")
        r = stop(env, msg="   ")
        assert r["reason"] == "empty-message" and turn_jobs(env) == []

    def test_ppid_chain_failure_fail_closed(self, hook_env):
        env = hook_env
        env.make_binding(status="active", session_id="sess-1")
        env.prober.raising = True
        r = stop(env, msg="hi")
        assert r["suppressed"] and r["reason"] == "no-instance"
        assert turn_jobs(env) == []


class TestEntryFailClosed:
    def test_exception_drops_with_log_and_counter(self, hook_env, data_dir):
        env = hook_env

        class BoomConn:
            def execute(self, *a, **k):
                raise RuntimeError("db exploded")

            def close(self):
                pass

        r = hooklib.stop_hook_entry(
            {"session_id": "s", "last_assistant_message": "x", "stop_hook_active": False},
            conn=BoomConn(), prober=env.prober, clock=env.clock, start_pid=HOOK_PID)
        assert r["suppressed"] and r["reason"] == "exception"
        from lib import paths
        log = paths.hook_drops_path()
        assert log.exists() and "stop_hook drop code=RuntimeError" in log.read_text()
        # daemon_state 计数镜像(best-effort,本例 DB 可用)
        assert int(dbmod.get_state(env.conn, "hook_drop_count", "0")) >= 1

    def test_enospc_double_failure_stderr_line(self, hook_env, monkeypatch, capsys):
        env = hook_env

        class BoomConn:
            def execute(self, *a, **k):
                raise OSError(28, "No space left on device")

            def close(self):
                pass

        from lib import util as u

        def no_log(*a, **k):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(u, "append_log_line", no_log)
        monkeypatch.setattr(hooklib, "_bump_drop_counter", lambda: False)
        r = hooklib.stop_hook_entry(
            {"session_id": "s", "last_assistant_message": "x"},
            conn=BoomConn(), prober=env.prober, clock=env.clock, start_pid=HOOK_PID)
        assert r["suppressed"]
        err = capsys.readouterr().err
        assert "feishu-bridge" in err and "fail-closed" in err

    def test_no_db_file_noop(self, hook_env, monkeypatch, tmp_path):
        monkeypatch.setenv("FEISHU_BRIDGE_DATA_DIR", str(tmp_path / "empty-nowhere"))
        r = hooklib.stop_hook_entry(
            {"session_id": "s", "last_assistant_message": "x"},
            prober=hook_env.prober, clock=hook_env.clock, start_pid=HOOK_PID)
        assert not r["suppressed"] and r["reason"] == "no-db"


class TestSessionEnd:
    def test_closes_bindings_and_pending(self, hook_env):
        env = hook_env
        res = start_bind(env)
        stop(env, msg=res["marker"])  # 激活
        assert binding(env, res["binding_id"])["status"] == "active"
        r = hooklib.run_session_end(
            {"session_id": "sess-1", "reason": "clear"},
            conn=env.conn, prober=env.prober, clock=env.clock, start_pid=HOOK_PID)
        assert res["binding_id"] in r["closed"]
        b = binding(env, res["binding_id"])
        assert b["status"] == "closed" and b["close_reason"] == "session_end"

    def test_pending_bind_expired_and_latch_closed(self, hook_env):
        env = hook_env
        res = start_bind(env)  # 未握手,session 直接结束
        hooklib.run_session_end(
            {"session_id": "sess-1"}, conn=env.conn, prober=env.prober,
            clock=env.clock, start_pid=HOOK_PID)
        p = pb(env, res["binding_id"])
        assert p["state"] == "expired" and p["latch_open"] == 0
        b = binding(env, res["binding_id"])
        assert b["status"] == "closed" and b["close_reason"] == "session_end"

    def test_latch_closed_on_session_end_after_bind_turn(self, hook_env):
        env = hook_env
        res = start_bind(env)
        stop(env, msg=res["marker"])  # latch 开
        hooklib.run_session_end(
            {"session_id": "sess-1"}, conn=env.conn, prober=env.prober,
            clock=env.clock, start_pid=HOOK_PID)
        assert pb(env, res["binding_id"])["latch_open"] == 0


class TestSupersedeLatchMatrix:
    """r2-m1:consumed+latch_open 的旧 bind 被 supersede → 一并关闩;
    新 bind 的 Stop 即使是 continuation(stop_hook_active=true)也能正常握手。
    nonce-miss 的留闩语义不变(TestHandshake.test_nonce_mismatch_fails_bind_with_latch 覆盖)。"""

    def test_superseded_consumed_tombstone_latch_closed(self, hook_env):
        env = hook_env
        # 旧 bind:握手成功但 listener 未就绪 → starting confirmed + consumed latch=1
        old = start_bind(env, with_listener=False)
        r = stop(env, msg=old["marker"])
        assert r["suppressed"] and pb(env, old["binding_id"])["latch_open"] == 1
        assert binding(env, old["binding_id"])["status"] == "starting"
        # 同实例重新 bind → supersede 关闭旧 starting 并关旧 tombstone 的闩
        new = start_bind(env)
        old_pb = pb(env, old["binding_id"])
        assert old_pb["latch_open"] == 0  # supersede 场景一并关闩
        assert binding(env, old["binding_id"])["close_reason"] == "bind_superseded"

    def test_new_bind_handshake_works_on_continuation_stop(self, hook_env):
        env = hook_env
        old = start_bind(env, with_listener=False)
        stop(env, msg=old["marker"])  # 旧 latch 开
        new = start_bind(env)  # supersede → 旧 latch 关
        # 新 bind 的回复 Stop 是 continuation(其它阻断型 hook 先跑过)
        r = stop(env, msg=f"确认 {new['marker']}", sha=True)
        assert r["suppressed"] and r["reason"] == "bind-handshake"
        p_new = pb(env, new["binding_id"])
        assert p_new["state"] == "consumed" and p_new["latch_open"] == 1
        b_new = binding(env, new["binding_id"])
        assert b_new["status"] == "active"  # 新 listener 就绪 → 激活

    def test_nonce_miss_failed_latch_survives_real_supersede(self, hook_env):
        """r3-2(修假绿):真执行 supersede —— 只有 consumed tombstone 被关闩,
        nonce-miss 的 failed 行(latch=1)必须原样保留。"""
        env = hook_env
        # bind1 → nonce-miss:failed row1 latch=1,binding1 closed(bind_failed)
        res1 = start_bind(env)
        stop(env, msg="没有 marker 的回复")
        p1 = pb(env, res1["binding_id"])
        assert p1["state"] == "failed" and p1["latch_open"] == 1
        # bind2 → starting2 存在
        res2 = start_bind(env)
        # bind3 → **真执行 supersede**(关闭 starting2,close_reason=bind_superseded)
        res3 = start_bind(env)
        b2 = binding(env, res2["binding_id"])
        assert b2["status"] == "closed" and b2["close_reason"] == "bind_superseded"
        # failed 行的闩不被 supersede 收窄 SQL 触碰
        p1 = pb(env, res1["binding_id"])
        assert p1["state"] == "failed" and p1["latch_open"] == 1
