"""daemon 装配层:事件路由 / 睡眠 gap suspect 窗 / ConsumerManager 真子进程管道。"""
import json
import sys
import time

from tests.conftest import APP_ID, CHAT, OWNER
from tests.helpers import FakeClock, bot_mention, mget_snapshot
from lib import constants, db as dbmod
from lib.daemon_core import ConsumerManager, DaemonCore, RECEIVE_KEY, CARD_KEY


def make_core(env):
    return DaemonCore(env.conn, env.cfg, env.clock, env.inbound, env.approval,
                      env.outbound, env.recovery)


class TestRouting:
    def test_receive_line_routes_to_inbound(self, env):
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])])
        core = make_core(env)
        line = json.dumps({"type": RECEIVE_KEY, "event_id": "ev_1", "message_id": "om_1",
                           "chat_id": CHAT, "chat_type": "group", "sender_id": OWNER,
                           "message_type": "text", "content": "@TestBot hi"})
        core.route_line(RECEIVE_KEY, line)
        assert env.inbox_row("om_1")["state"] == "enqueued"

    def test_card_line_routes_to_approval(self, env):
        from tests.test_approval import member_pending, cb
        p = member_pending(env)
        core = make_core(env)
        core.route_line(CARD_KEY, json.dumps(cb(p)))
        assert env.conn.execute("SELECT state FROM pendings").fetchone()[0] == "approved"

    def test_malformed_line_counted_not_crashing(self, env):
        core = make_core(env)
        core.route_line(RECEIVE_KEY, "not json {{{")
        core.route_line(RECEIVE_KEY, '"a bare string"')
        assert int(dbmod.get_state(env.conn, "malformed_event_lines", "0")) == 2

    def test_event_error_caught_and_counted(self, env, monkeypatch):
        core = make_core(env)

        def boom(ev):
            raise RuntimeError("processing exploded")

        monkeypatch.setattr(env.inbound, "process_event", boom)
        core.route_line(RECEIVE_KEY, json.dumps({"event_id": "e", "message_id": "m",
                                                 "chat_id": CHAT}))
        assert int(dbmod.get_state(env.conn, "event_processing_errors", "0")) == 1


class TestSuspectWindow:
    def test_gap_opens_window_and_expires(self, env):
        core = make_core(env)
        assert core.update_suspect_window(env.clock.wall_ms()) is False
        env.clock.tick(constants.DAEMON_GAP_MS + 5000)  # 模拟睡眠
        assert core.update_suspect_window(env.clock.wall_ms()) is True
        # 以正常节奏(5s/步,< gap 阈值)走完宽限窗 → 窗过期,恢复判死
        last = True
        for _ in range(constants.SUSPECT_WINDOW_MS // 5000 + 2):
            env.clock.tick(5000)
            last = core.update_suspect_window(env.clock.wall_ms())
        assert last is False

    def test_clock_rewind_opens_window(self, env):
        core = make_core(env)
        core.update_suspect_window(env.clock.wall_ms())
        env.clock.rewind_wall(60_000)
        assert core.update_suspect_window(env.clock.wall_ms()) is True

    def test_loop_iteration_smoke(self, env):
        core = make_core(env)
        core.loop_iteration()
        assert dbmod.get_state(env.conn, "last_loop_at") is not None


CHILD_OK = (
    "import sys,time\n"
    "sys.stderr.write('[event] ready event_key=x\\n'); sys.stderr.flush()\n"
    "sys.stdout.write('{\"n\":1}\\n{\"n\":2}\\n'); sys.stdout.flush()\n"
    "time.sleep(30)\n")

CHILD_EXIT = "print('{\"n\":9}')\n"


class TestConsumerManagerSubprocess:
    def _mgr(self, child_src, keys=("k1",)):
        lines, statuses = [], []
        clock = FakeClock()
        mgr = ConsumerManager(
            "main", clock,
            on_line=lambda k, l: lines.append((k, l)),
            on_status=lambda k, s, d: statuses.append((k, s)),
            keys=keys,
            argv_builder=lambda key: [sys.executable, "-u", "-c", child_src])
        return mgr, lines, statuses, clock

    def test_ready_detection_and_line_dispatch(self, env):
        mgr, lines, statuses, _ = self._mgr(CHILD_OK)
        mgr.start_all()
        deadline = time.time() + 5
        while time.time() < deadline and len(lines) < 2:
            mgr.poll(0.2)
        mgr.shutdown()
        assert [json.loads(l)["n"] for _, l in lines] == [1, 2]
        assert ("k1", "ready") in statuses
        assert mgr.consumers["k1"].ready is True

    def test_exit_schedules_backoff_restart(self, env):
        mgr, lines, statuses, clock = self._mgr(CHILD_EXIT)
        mgr.start_all()
        deadline = time.time() + 5
        while time.time() < deadline and not mgr.consumers["k1"].exited:
            mgr.poll(0.2)
            mgr.tick()
        assert mgr.consumers["k1"].exited is True
        spawned_before = len([s for s in statuses if s[1] == "spawned"])
        mgr.tick()  # 退避期内不重启
        assert len([s for s in statuses if s[1] == "spawned"]) == spawned_before
        clock.tick(120_000)  # 过退避
        mgr.tick()
        assert len([s for s in statuses if s[1] == "spawned"]) == spawned_before + 1
        mgr.shutdown()


CHILD_STDOUT_CLOSE = (
    "import sys,os,time\n"
    "sys.stderr.write('[event] ready event_key=x\\n'); sys.stderr.flush()\n"
    "os.close(1)\n"
    "time.sleep(30)\n")

CHILD_PARTIAL = (
    "import sys\n"
    "sys.stdout.write('{\"partial\":'); sys.stdout.flush()\n")


class TestConsumerRespawnHygiene:
    """修复项7:任一流 EOF → 完整 teardown(kill+reap+双流关闭)后才 respawn;buffers 清空。"""

    def test_stdout_eof_full_teardown_kills_and_reaps(self, env):
        import sys as _sys
        lines, statuses = [], []
        clock = FakeClock()
        mgr = ConsumerManager(
            "main", clock, on_line=lambda k, l: lines.append(l),
            on_status=lambda k, s, d: statuses.append(s), keys=("k1",),
            argv_builder=lambda key: [_sys.executable, "-u", "-c", CHILD_STDOUT_CLOSE])
        mgr.start_all()
        c = mgr.consumers["k1"]
        proc = c.proc
        deadline = time.time() + 5
        while time.time() < deadline and not c.exited:
            mgr.poll(0.2)
            mgr.tick()
        assert c.exited is True
        assert proc.poll() is not None  # 进程被 kill 且已 reap(不是只关一条流)
        mgr.shutdown()

    def test_respawn_clears_partial_buffers(self, env):
        import sys as _sys
        lines = []
        clock = FakeClock()
        scripts = [CHILD_PARTIAL, CHILD_OK]

        def build(key):
            return [_sys.executable, "-u", "-c", scripts.pop(0)]

        mgr = ConsumerManager(
            "main", clock, on_line=lambda k, l: lines.append(l),
            on_status=lambda k, s, d: None, keys=("k1",), argv_builder=build)
        mgr.start_all()
        c = mgr.consumers["k1"]
        deadline = time.time() + 5
        while time.time() < deadline and not c.exited:
            mgr.poll(0.2)
            mgr.tick()
        assert lines == []  # 半行不外泄
        clock.tick(120_000)
        mgr.tick()  # respawn(buffers 已清)
        deadline = time.time() + 5
        while time.time() < deadline and len(lines) < 2:
            mgr.poll(0.2)
        mgr.shutdown()
        assert json.loads(lines[0]) == {"n": 1}  # 无旧半行拼接污染


class TestMultiPointHeartbeat:
    """r2-M1②:含网络条目处理完即 touch last_loop_at,长下载不饿死心跳。"""

    def _beat_fn(self, env):
        def beat():
            dbmod.set_state(env.conn, "last_loop_at", env.clock.wall_ms())
        return beat

    def test_inbound_beats_after_long_download(self, env):
        from tests.helpers import bot_mention, mget_snapshot, ok_envelope
        from tests.conftest import OWNER, APP_ID
        env.inbound.heartbeat = self._beat_fn(env)
        env.make_binding(status="active")
        dbmod.set_state(env.conn, "last_loop_at", env.clock.wall_ms())

        def slow_mget(args, cwd):
            env.clock.tick(200_000)  # 模拟 200s 慢网络
            return ok_envelope({"messages": [mget_snapshot(
                "om_1", CHAT, OWNER, mentions=[bot_mention(APP_ID)])]})

        env.runner.on(lambda a: a[:2] == ["im", "+messages-mget"], slow_mget)
        env.recv_event()
        last = int(dbmod.get_state(env.conn, "last_loop_at"))
        assert env.clock.wall_ms() - last < 1000  # 处理完即刷新
        from lib import ctl
        import lib.ctl as ctlmod
        # 多点心跳后 daemon_healthy 不误判挂死
        assert (env.clock.wall_ms() - last) <= ctlmod.HUNG_THRESHOLD_MS

    def test_outbound_beats_between_sends(self, env):
        from tests.helpers import ok_envelope
        from lib import jobs
        env.outbound.heartbeat = self._beat_fn(env)
        bid = env.make_binding(status="active")

        def slow_send(args, cwd):
            env.clock.tick(25_000)
            return ok_envelope({"message_id": "om_x" + str(env.clock.wall_ms())})

        env.runner.on_prefix(["im", "+messages-send"], slow_send)
        for i in range(3):
            jobs.create_job(env.conn, kind="session_turn", chat_id=CHAT, binding_id=bid,
                            idempotency_key=f"turn:g{i}:0", turn_group=f"g{i}",
                            chunk_index=0, body="x", now=env.clock.wall_ms())
        env.outbound.tick()
        last = int(dbmod.get_state(env.conn, "last_loop_at"))
        assert env.clock.wall_ms() - last < 1000


class TestConsumerReadyObservability:
    """r2-m2:退出/teardown 清 ready 并同步 daemon_state。"""

    def test_ready_cleared_on_teardown(self, env):
        import sys as _sys
        statuses = []
        clock = FakeClock()
        mgr = ConsumerManager(
            "main", clock, on_line=lambda k, l: None,
            on_status=lambda k, s, d: statuses.append((s, d)), keys=("k1",),
            argv_builder=lambda key: [_sys.executable, "-u", "-c", CHILD_EXIT])
        mgr.start_all()
        deadline = time.time() + 5
        while time.time() < deadline and not mgr.consumers["k1"].exited:
            mgr.poll(0.2)
            mgr.tick()
        assert mgr.consumers["k1"].ready is False
        mgr.shutdown()

    def test_status_writer_syncs_daemon_state(self, env):
        from lib.daemon_core import make_status_writer
        writer = make_status_writer(env.conn, log=lambda s: None)
        writer("im.message.receive_v1", "spawned", "pid=1 gen=1")
        assert dbmod.get_state(env.conn,
                               "consumer_im.message.receive_v1_ready") == "starting"  # r3-3 跨代
        writer("im.message.receive_v1", "ready", "[event] ready")
        assert dbmod.get_state(env.conn, "consumer_im.message.receive_v1_ready").startswith("ready")
        writer("im.message.receive_v1", "exited", "restarts=1")
        assert dbmod.get_state(env.conn, "consumer_im.message.receive_v1_ready") == "down"
        assert int(dbmod.get_state(env.conn,
                                   "consumer_im.message.receive_v1_restarts", "0")) == 1

    def test_mark_consumers_down_on_shutdown(self, env):
        from lib.daemon_core import make_status_writer, mark_consumers_down
        writer = make_status_writer(env.conn, log=lambda s: None)
        writer("k1", "ready", "x")
        mark_consumers_down(env.conn, ["k1", "k2"])
        assert dbmod.get_state(env.conn, "consumer_k1_ready") == "down"
        assert dbmod.get_state(env.conn, "consumer_k2_ready") == "down"

    def test_record_daemon_identity_writes_first_heartbeat(self, env):
        """r3-5/r4-1:身份+首心跳+startup=probing:<gen>(gate 之前;heartbeat 只表活着)。
        MAJOR 3:给了 code_identity 就同事务发布 daemon_code_identity(daemon→supervisor 的胶水)。"""
        from lib.daemon_core import record_daemon_identity
        gen = record_daemon_identity(env.conn, env.clock, env.prober,
                                     code_identity="rootX|1.2.3")
        assert dbmod.get_state(env.conn, "daemon_pid") is not None
        assert dbmod.get_state(env.conn, "daemon_started_at") is not None
        assert dbmod.get_state(env.conn, "daemon_proc_start") is not None
        assert int(dbmod.get_state(env.conn, "last_loop_at")) == env.clock.wall_ms()
        assert dbmod.get_state(env.conn, "daemon_generation") == gen
        assert dbmod.get_state(env.conn, "startup") == f"probing:{gen}"
        assert dbmod.get_state(env.conn, "daemon_code_identity") == "rootX|1.2.3"

    def test_record_identity_resets_consumers_and_gate_transitions(self, env):
        """r4-1/r4-2:generation 建立即置 consumers=down;set_startup_state 推进 running。"""
        from lib.daemon_core import (record_daemon_identity, set_startup_state,
                                     RECEIVE_KEY, CARD_KEY)
        dbmod.set_state(env.conn, f"consumer_{RECEIVE_KEY}_ready", "ready x")  # 上一代残留
        gen = record_daemon_identity(env.conn, env.clock, env.prober)
        assert dbmod.get_state(env.conn, f"consumer_{RECEIVE_KEY}_ready") == "down"
        assert dbmod.get_state(env.conn, f"consumer_{CARD_KEY}_ready") == "down"
        set_startup_state(env.conn, "running", gen)
        assert dbmod.get_state(env.conn, "startup") == f"running:{gen}"
