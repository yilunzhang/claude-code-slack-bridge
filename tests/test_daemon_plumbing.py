"""daemon 装配层(WP1):drain 事务机制(fake *_in_tx,真实 SQLite)/ followup 预算 / loop 顺序 /
睡眠 gap suspect 窗 / ConsumerManager 真子进程管道(单 key、ready 哨兵、stdout 只计数、rc 退避、主动重拉)/
daemon_state 同步 / 身份与启动阶段 / bin/daemon.py 装配辅助。"""
import importlib.util
import json
import pathlib
import sys
import time

import pytest

from lib import approval as approval_mod
from lib import constants, db as dbmod
from lib import inbound as inbound_mod
from lib.daemon_core import (RESTART_BACKOFF_MAX_MS, RESTART_BACKOFF_START_MS, SOCKET_KEY,
                             STARTUP_PHASES, ConsumerManager, DaemonCore, make_status_writer,
                             mark_consumers_down, record_daemon_identity, set_startup_state)
from tests.conftest import CHAT, OWNER, TEAM
from tests.helpers import FakeClock, block_action, envelope, message_event

ROOT = pathlib.Path(__file__).resolve().parents[1]


def make_core(env):
    return DaemonCore(env.conn, env.cfg, env.clock, env.inbound, env.approval,
                      env.outbound, env.recovery)


def row_of(env, key):
    return env.conn.execute("SELECT * FROM slack_events WHERE event_key=?", (key,)).fetchone()


def handed_ingest(conn, row):
    """契约形状的假 ingest:插 inbox 行(同事务)并 handed。"""
    p = json.loads(row["payload_json"])
    ev = p["event"]
    conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,sender_user_id,message_type,"
        "thread_ts,reply_thread_ts,snapshot_json,state,ts) VALUES(?,?,?,?,?,?,?,?,?,'received',?)",
        (p["event_id"], "%s:%s" % (ev["channel"], ev["ts"]), ev["channel"], row["binding_id"],
         ev.get("user"), ev["type"], ev.get("thread_ts"), ev.get("thread_ts") or ev["ts"],
         json.dumps(ev), row["received_at"]))
    return ("handed", conn.execute("SELECT * FROM inbox WHERE event_id=?", (p["event_id"],)).fetchone())


def stage_msg(env, event_id, **kw):
    return env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER, **kw), event_id=event_id))


# ======================================================================
# drain_staging:drain 是唯一事务拥有者
# ======================================================================
class TestDrain:
    def test_handed_row_consumed_in_same_tx_as_business_write(self, env, monkeypatch):
        bid = env.make_binding(status="active", chat_id=CHAT)
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", handed_ingest, raising=False)
        stage_msg(env, "EvH")
        res = make_core(env).drain_staging()
        assert res == {"handed": 1, "dropped": 0, "retried": 0, "quarantined": 0}
        r = row_of(env, "ev:EvH")
        assert r["state"] == "consumed" and r["error"] is None
        assert r["consumed_at"] == env.clock.wall_ms() and r["next_drain_at"] is None
        ib = env.conn.execute("SELECT * FROM inbox WHERE event_id='EvH'").fetchone()
        assert ib is not None and ib["binding_id"] == bid and ib["state"] == "received"
        assert not env.conn.in_transaction

    def test_dropped_row_consumed_with_reason_in_error(self, env, monkeypatch):
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", lambda conn, row: ("dropped", "foreign_team"),
                            raising=False)
        stage_msg(env, "EvD")
        res = make_core(env).drain_staging()
        assert res["dropped"] == 1 and res["handed"] == 0
        r = row_of(env, "ev:EvD")
        assert r["state"] == "consumed" and r["error"] == "foreign_team"
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0

    def test_business_runs_inside_drain_transaction(self, env, monkeypatch):
        seen = {}

        def probe(conn, row):
            seen["in_tx"] = conn.in_transaction
            seen["row_keys"] = set(row.keys())
            return ("dropped", "invalid")
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", probe, raising=False)
        stage_msg(env, "EvT")
        make_core(env).drain_staging()
        assert seen["in_tx"] is True
        assert {"payload_json", "binding_id", "chat_id", "event_key", "envelope_type", "seq"} <= seen["row_keys"]

    def test_interactive_dispatches_to_approval_with_parsed_payload(self, env, monkeypatch):
        env.make_binding(status="active", chat_id=CHAT)
        got = {}

        def process(conn, payload):
            got["payload"] = payload
            got["in_tx"] = conn.in_transaction
            conn.execute("INSERT INTO callback_events(event_id,seen_at) VALUES('x',0)")
            return ("dropped", "dup")
        monkeypatch.setattr(approval_mod, "process_in_tx", process, raising=False)
        env.click(pending_id="p1", nonce="n1", card_ts="1.1", action_ts="2.2")
        res = make_core(env).drain_staging()
        assert res["dropped"] == 1
        assert isinstance(got["payload"], dict) and got["payload"]["type"] == "block_actions"
        assert got["in_tx"] is True
        r = env.slack_events()[0]
        assert r["state"] == "consumed" and r["error"] == "dup"
        assert env.conn.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0] == 1  # 同事务提交

    def test_exception_rolls_back_business_writes_and_backs_off(self, env, monkeypatch):
        def boom(conn, row):
            conn.execute("INSERT INTO callback_events(event_id,seen_at) VALUES('poison',0)")
            raise RuntimeError("boom")
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", boom, raising=False)
        stage_msg(env, "EvX")
        core = make_core(env)
        now = env.clock.wall_ms()
        res = core.drain_staging()
        assert res == {"handed": 0, "dropped": 0, "retried": 1, "quarantined": 0}
        r = row_of(env, "ev:EvX")
        assert r["state"] == "staged" and r["drain_attempts"] == 1 and "boom" in r["error"]
        assert now + constants.DRAIN_BACKOFF_MS <= r["next_drain_at"] <= now + constants.DRAIN_BACKOFF_MAX_MS
        assert env.conn.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0] == 0   # 回滚
        assert not env.conn.in_transaction
        assert dbmod.get_state(env.conn, "last_error").startswith("drain ")
        # 未到期 → 不再尝试;到期 → 第二次尝试、退避变长
        assert core.drain_staging()["retried"] == 0
        assert row_of(env, "ev:EvX")["drain_attempts"] == 1
        env.clock.tick(constants.DRAIN_BACKOFF_MAX_MS + 1)
        core.drain_staging()
        r2 = row_of(env, "ev:EvX")
        assert r2["drain_attempts"] == 2 and r2["next_drain_at"] - env.clock.wall_ms() > r["next_drain_at"] - now

    def test_quarantine_at_cap_keeps_payload_and_error(self, env, monkeypatch):
        monkeypatch.setattr(inbound_mod, "ingest_in_tx",
                            lambda conn, row: (_ for _ in ()).throw(RuntimeError("q")), raising=False)
        stage_msg(env, "EvQ")
        core = make_core(env)
        counts = []
        for _ in range(constants.DRAIN_MAX_ATTEMPTS + 2):
            counts.append(core.drain_staging())
            env.clock.tick(constants.DRAIN_BACKOFF_MAX_MS + 1)
        r = row_of(env, "ev:EvQ")
        assert r["state"] == "quarantined" and r["drain_attempts"] == constants.DRAIN_MAX_ATTEMPTS
        assert r["payload_json"] and "q" in r["error"] and r["next_drain_at"] is None
        assert sum(c["quarantined"] for c in counts) == 1
        assert sum(c["retried"] for c in counts) == constants.DRAIN_MAX_ATTEMPTS - 1
        assert int(dbmod.get_state(env.conn, "drain_quarantined", "0")) == 1
        assert not env.conn.in_transaction

    def test_bad_return_shape_is_business_error(self, env, monkeypatch):
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", lambda conn, row: "handed", raising=False)
        stage_msg(env, "EvB")
        make_core(env).drain_staging()
        r = row_of(env, "ev:EvB")
        assert r["state"] == "staged" and r["drain_attempts"] == 1 and "bad *_in_tx result" in r["error"]

    def test_dropped_without_reason_is_business_error(self, env, monkeypatch):
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", lambda conn, row: ("dropped", None), raising=False)
        stage_msg(env, "EvR")
        make_core(env).drain_staging()
        assert row_of(env, "ev:EvR")["state"] == "staged"

    def test_nested_transaction_in_business_is_business_error(self, env, monkeypatch):
        def nested(conn, row):
            with dbmod.tx(conn):
                return ("handed", None)
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", nested, raising=False)
        stage_msg(env, "EvN")
        make_core(env).drain_staging()
        r = row_of(env, "ev:EvN")
        assert r["state"] == "staged" and "nested" in r["error"]
        assert not env.conn.in_transaction

    def test_batch_limit_and_seq_order(self, env, monkeypatch):
        seen = []
        monkeypatch.setattr(inbound_mod, "ingest_in_tx",
                            lambda conn, row: seen.append(row["event_key"]) or ("dropped", "invalid"),
                            raising=False)
        n = constants.DRAIN_BATCH + 3
        for i in range(n):
            stage_msg(env, "Ev%03d" % i)
        core = make_core(env)
        res = core.drain_staging()
        assert res["dropped"] == constants.DRAIN_BATCH
        assert seen == ["ev:Ev%03d" % i for i in range(constants.DRAIN_BATCH)]   # 按 seq
        states = {r["event_key"]: r["state"] for r in env.slack_events()}
        assert states["ev:Ev%03d" % (n - 1)] == "staged"
        assert core.drain_staging()["dropped"] == 3
        assert all(r["state"] == "consumed" for r in env.slack_events())

    def test_future_next_drain_at_is_skipped(self, env, monkeypatch):
        monkeypatch.setattr(inbound_mod, "ingest_in_tx", lambda conn, row: ("dropped", "x"), raising=False)
        stage_msg(env, "EvF")
        env.conn.execute("UPDATE slack_events SET next_drain_at=?", (env.clock.wall_ms() + 1,))
        assert make_core(env).drain_staging()["dropped"] == 0
        env.clock.tick(1)
        assert make_core(env).drain_staging()["dropped"] == 1

    def test_quarantined_and_consumed_rows_never_picked(self, env, monkeypatch):
        calls = []
        monkeypatch.setattr(inbound_mod, "ingest_in_tx",
                            lambda conn, row: calls.append(1) or ("dropped", "x"), raising=False)
        stage_msg(env, "EvA")
        stage_msg(env, "EvB")
        env.conn.execute("UPDATE slack_events SET state='quarantined' WHERE event_key='ev:EvA'")
        env.conn.execute("UPDATE slack_events SET state='consumed' WHERE event_key='ev:EvB'")
        assert make_core(env).drain_staging() == {"handed": 0, "dropped": 0, "retried": 0, "quarantined": 0}
        assert calls == []


# ======================================================================
# followups / loop 顺序 / consumer stdout 只计数
# ======================================================================
class _Rec:
    """记录调用顺序的假组件。"""

    def __init__(self, log, name, raise_on=None):
        self.log, self.name, self.raise_on = log, name, raise_on
        self.budgets = []

    def drive_pending_rows(self, budget=constants.FOLLOWUP_BUDGET_PER_TICK):
        self.budgets.append(budget)
        self.log.append("followups")
        if self.raise_on == "followups":
            raise RuntimeError("materialize exploded")
        return {"local": 1, "materialized": 0}

    def drive_waiting_rows(self):
        return 0

    def tick(self):
        self.log.append(self.name)

    def fast_tick(self, in_suspect_window=False):
        self.log.append("fast")

    def slow_tick(self):
        self.log.append("slow")


def fake_core(conn, cfg, clock, log, gate=True, raise_on=None):
    inbound = _Rec(log, "inbound", raise_on)
    core = DaemonCore(conn, cfg, clock, inbound, _Rec(log, "approval"), _Rec(log, "outbound"),
                      _Rec(log, "recovery"), gate=_Rec(log, "gate") if gate else None)
    return core, inbound


class TestFollowupsAndLoop:
    def test_run_followups_passes_budget_and_returns_dict(self, conn, cfg, clock):
        log = []
        core, inbound = fake_core(conn, cfg, clock, log)
        assert core.run_followups() == {"local": 1, "materialized": 0}
        assert core.run_followups((2, 9)) == {"local": 1, "materialized": 0}
        assert inbound.budgets == [constants.FOLLOWUP_BUDGET_PER_TICK, (2, 9)]

    def test_run_followups_exception_contained_and_counted(self, conn, cfg, clock):
        log = []
        core, _ = fake_core(conn, cfg, clock, log, raise_on="followups")
        assert core.run_followups() == {}
        assert int(dbmod.get_state(conn, "event_processing_errors", "0")) == 1
        assert "materialize exploded" in dbmod.get_state(conn, "last_error")

    def test_loop_iteration_order(self, conn, cfg, clock, monkeypatch):
        log = []
        core, _ = fake_core(conn, cfg, clock, log)
        monkeypatch.setattr(inbound_mod, "ingest_in_tx",
                            lambda c, row: log.append("drain") or ("dropped", "x"), raising=False)
        from lib import util
        conn.execute(
            "INSERT INTO slack_events(envelope_type,event_key,payload_json,received_at,state) "
            "VALUES('events_api','ev:E1',?,?,'staged')", (util.jdumps({"event_id": "E1"}), clock.wall_ms()))
        core.loop_iteration()
        assert log == ["drain", "followups", "gate", "fast", "outbound", "slow"]
        assert int(dbmod.get_state(conn, "last_loop_at")) == clock.wall_ms()
        # 第二轮(节奏内):无 fast/slow
        log.clear()
        clock.tick(1000)
        core.loop_iteration()
        assert log == ["followups", "gate", "outbound"]
        clock.tick(constants.DEATH_SCAN_INTERVAL_MS)
        log.clear()
        core.loop_iteration()
        assert log == ["followups", "gate", "fast", "outbound"]

    def test_loop_iteration_without_gate(self, conn, cfg, clock):
        log = []
        core, _ = fake_core(conn, cfg, clock, log, gate=False)
        core.loop_iteration()
        assert "gate" not in log and "outbound" in log

    def test_loop_iteration_smoke_with_env(self, env, monkeypatch):
        monkeypatch.setattr(env.inbound, "drive_pending_rows", lambda budget: {}, raising=False)
        core = make_core(env)
        core.loop_iteration()
        assert dbmod.get_state(env.conn, "last_loop_at") is not None

    def test_consumer_stdout_lines_only_counted(self, env):
        core = make_core(env)
        core.on_consumer_line(SOCKET_KEY, json.dumps({"type": "events_api", "payload": {"x": 1}}))
        assert core.consumer_stdout_lines == 1
        assert int(dbmod.get_state(env.conn, "consumer_stdout_lines", "0")) == 1
        assert env.conn.execute("SELECT COUNT(*) FROM slack_events").fetchone()[0] == 0
        assert env.conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0


class TestSuspectWindow:
    def test_gap_opens_window_and_expires(self, env):
        core = make_core(env)
        assert core.update_suspect_window(env.clock.wall_ms()) is False
        env.clock.tick(constants.DAEMON_GAP_MS + 5000)  # 模拟睡眠
        assert core.update_suspect_window(env.clock.wall_ms()) is True
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


# ======================================================================
# ConsumerManager:真子进程
# ======================================================================
READY = constants.CONSUMER_READY_SENTINEL + " num_connections=1"

CHILD_OK = (
    "import sys,time\n"
    "sys.stderr.write(%r + '\\n'); sys.stderr.flush()\n"
    "sys.stdout.write('{\"n\":1}\\n{\"n\":2}\\n'); sys.stdout.flush()\n"
    "time.sleep(30)\n") % READY

CHILD_EXIT_RC = "import sys; sys.stderr.write('[socket] fatal x\\n'); sys.exit(%d)\n"

CHILD_STDOUT_CLOSE = (
    "import sys,os,time\n"
    "sys.stderr.write(%r + '\\n'); sys.stderr.flush()\n"
    "os.close(1)\n"
    "time.sleep(30)\n") % READY

CHILD_PARTIAL = (
    "import sys\n"
    "sys.stdout.write('{\"partial\":'); sys.stdout.flush()\n")


def make_mgr(child_src_or_builder):
    lines, statuses = [], []
    clock = FakeClock()
    if callable(child_src_or_builder):
        builder = child_src_or_builder
    else:
        builder = lambda key: [sys.executable, "-u", "-c", child_src_or_builder]  # noqa: E731
    mgr = ConsumerManager(clock, on_line=lambda k, l: lines.append((k, l)),
                          on_status=lambda k, s, d: statuses.append((k, s, d)),
                          argv_builder=builder)
    return mgr, lines, statuses, clock


def pump_until(mgr, pred, timeout=8):
    deadline = time.time() + timeout
    while time.time() < deadline and not pred():
        mgr.poll(0.1)
        mgr.tick()
    return pred()


class TestConsumerManagerSubprocess:
    def test_single_socket_key_ready_sentinel_and_stdout_counted(self):
        mgr, lines, statuses, _ = make_mgr(CHILD_OK)
        assert tuple(mgr.consumers) == (SOCKET_KEY,) and mgr.keys == (SOCKET_KEY,)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        assert pump_until(mgr, lambda: len(lines) >= 2 and c.ready)
        mgr.shutdown()
        assert [json.loads(l)["n"] for _, l in lines] == [1, 2]
        assert c.stdout_lines == 2
        assert (SOCKET_KEY, "ready", "num_connections=1") in statuses
        assert (SOCKET_KEY, "spawned", "pid=%d gen=1" % c.proc.pid) in statuses

    def test_exit_rc0_schedules_normal_backoff_restart(self):
        mgr, lines, statuses, clock = make_mgr(CHILD_EXIT_RC % 0)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        assert pump_until(mgr, lambda: c.exited)
        assert c.last_rc == 0 and c.ready is False
        assert (SOCKET_KEY, "exited", "rc=0 restarts=1") in statuses
        assert (SOCKET_KEY, "stderr", "[socket] fatal x") in statuses
        assert c.next_restart_at == clock.mono_ms() + RESTART_BACKOFF_START_MS
        spawned_before = len([s for s in statuses if s[1] == "spawned"])
        mgr.tick()  # 退避期内不重启
        assert len([s for s in statuses if s[1] == "spawned"]) == spawned_before
        clock.tick(RESTART_BACKOFF_START_MS)
        mgr.tick()
        assert len([s for s in statuses if s[1] == "spawned"]) == spawned_before + 1
        mgr.shutdown()

    @pytest.mark.parametrize("rc", sorted(constants.CONSUMER_SKIP_BACKOFF_RCS))
    def test_fatal_rc_jumps_to_max_backoff(self, rc):
        mgr, _, statuses, clock = make_mgr(CHILD_EXIT_RC % rc)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        assert pump_until(mgr, lambda: c.exited)
        assert c.last_rc == rc
        assert (SOCKET_KEY, "exited", "rc=%d restarts=1" % rc) in statuses
        assert c.next_restart_at == clock.mono_ms() + RESTART_BACKOFF_MAX_MS
        clock.tick(RESTART_BACKOFF_MAX_MS - 1)
        mgr.tick()
        assert len([s for s in statuses if s[1] == "spawned"]) == 1
        clock.tick(1)
        mgr.tick()
        assert len([s for s in statuses if s[1] == "spawned"]) == 2
        mgr.shutdown()

    def test_other_rc_uses_exponential_backoff(self):
        mgr, _, statuses, clock = make_mgr(CHILD_EXIT_RC % 5)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        assert pump_until(mgr, lambda: c.exited)
        assert c.next_restart_at == clock.mono_ms() + RESTART_BACKOFF_START_MS
        clock.tick(RESTART_BACKOFF_START_MS)
        mgr.tick()
        assert pump_until(mgr, lambda: c.exited and c.restarts == 2)
        assert c.next_restart_at == clock.mono_ms() + 2 * RESTART_BACKOFF_START_MS
        mgr.shutdown()

    def test_restart_request_respawns_immediately_without_backoff(self):
        mgr, _, statuses, clock = make_mgr(CHILD_OK)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        assert pump_until(mgr, lambda: c.ready)
        old_proc = c.proc
        assert mgr.restart(SOCKET_KEY, "app_token_changed") is True
        assert c.exited and not c.ready and old_proc.poll() is not None
        assert c.restarts == 0 and c.backoff == RESTART_BACKOFF_START_MS
        assert any(s[1] == "restart" and "reason=app_token_changed" in s[2] for s in statuses)
        assert not any(s[1] == "exited" for s in statuses)
        mgr.tick()  # 立即 respawn(不等退避)
        assert c.generation == 2 and not c.exited and c.proc is not old_proc
        assert pump_until(mgr, lambda: c.ready)
        mgr.shutdown()
        assert mgr.restart(SOCKET_KEY) is False  # 进程已不在:无事可做
        mgr2, _, _, _ = make_mgr(CHILD_OK)
        assert mgr2.restart(SOCKET_KEY) is False  # 从未启动

    def test_rapid_exit_alert(self):
        mgr, _, statuses, clock = make_mgr(CHILD_EXIT_RC % 1)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        for _ in range(5):
            assert pump_until(mgr, lambda: c.exited)
            clock.tick(RESTART_BACKOFF_MAX_MS)
            mgr.tick()
        assert any(s[1] == "rapid-exit-alert" for s in statuses)
        mgr.shutdown()


class TestConsumerRespawnHygiene:
    """任一流 EOF → 完整 teardown(kill+reap+双流关闭)后才 respawn;buffers 清空。"""

    def test_stdout_eof_full_teardown_kills_and_reaps(self):
        mgr, _, _, _ = make_mgr(CHILD_STDOUT_CLOSE)
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        proc = c.proc
        assert pump_until(mgr, lambda: c.exited)
        assert proc.poll() is not None  # 进程被 kill 且已 reap(不是只关一条流)
        assert c.last_rc is not None
        mgr.shutdown()

    def test_respawn_clears_partial_buffers(self):
        scripts = [CHILD_PARTIAL, CHILD_OK]
        mgr, lines, _, clock = make_mgr(lambda key: [sys.executable, "-u", "-c", scripts.pop(0)])
        mgr.start_all()
        c = mgr.consumers[SOCKET_KEY]
        assert pump_until(mgr, lambda: c.exited)
        assert lines == []  # 半行不外泄
        clock.tick(120_000)
        mgr.tick()  # respawn(buffers 已清)
        assert pump_until(mgr, lambda: len(lines) >= 2)
        mgr.shutdown()
        assert json.loads(lines[0][1]) == {"n": 1}  # 无旧半行拼接污染


class TestDaemonStateSync:
    def test_status_writer_syncs_daemon_state(self, conn):
        writer = make_status_writer(conn, log=lambda s: None)
        writer(SOCKET_KEY, "spawned", "pid=1 gen=1")
        assert dbmod.get_state(conn, "consumer_socket_ready") == "starting"
        writer(SOCKET_KEY, "ready", "num_connections=1")
        assert dbmod.get_state(conn, "consumer_socket_ready") == "ready num_connections=1"
        writer(SOCKET_KEY, "stderr", "[socket] disconnect reason=x")
        assert dbmod.get_state(conn, "consumer_socket_ready") == "ready num_connections=1"
        assert dbmod.get_state(conn, "consumer_socket_last_status").startswith("stderr [socket] disconnect")
        writer(SOCKET_KEY, "exited", "rc=3 restarts=1")
        assert dbmod.get_state(conn, "consumer_socket_ready") == "down"
        assert dbmod.get_state(conn, "consumer_socket_last_exit_rc") == "3"
        assert int(dbmod.get_state(conn, "consumer_socket_restarts", "0")) == 1
        writer(SOCKET_KEY, "restart", "rc=-15 reason=app_token_changed")
        assert dbmod.get_state(conn, "consumer_socket_last_exit_rc") == "-15"
        assert int(dbmod.get_state(conn, "consumer_socket_restarts", "0")) == 1  # 主动重拉不计
        writer(SOCKET_KEY, "rapid-exit-alert", "rc=None restarts=5")
        assert dbmod.get_state(conn, "consumer_socket_last_exit_rc") == "-15"  # rc 未知不覆盖
        assert int(dbmod.get_state(conn, "consumer_socket_restarts", "0")) == 2

    def test_mark_consumers_down_on_shutdown(self, conn):
        writer = make_status_writer(conn, log=lambda s: None)
        writer(SOCKET_KEY, "ready", "num_connections=1")
        mark_consumers_down(conn, [SOCKET_KEY])
        assert dbmod.get_state(conn, "consumer_socket_ready") == "down"

    def test_record_daemon_identity_writes_first_heartbeat(self, env):
        gen = record_daemon_identity(env.conn, env.clock, env.prober, code_identity="rootX|1.2.3")
        assert dbmod.get_state(env.conn, "daemon_pid") is not None
        assert dbmod.get_state(env.conn, "daemon_started_at") is not None
        assert dbmod.get_state(env.conn, "daemon_proc_start") is not None
        assert int(dbmod.get_state(env.conn, "last_loop_at")) == env.clock.wall_ms()
        assert dbmod.get_state(env.conn, "daemon_generation") == gen
        assert dbmod.get_state(env.conn, "startup") == "probing:%s" % gen
        assert dbmod.get_state(env.conn, "daemon_code_identity") == "rootX|1.2.3"
        assert not env.conn.in_transaction

    def test_record_identity_resets_consumer_and_startup_transitions(self, env):
        dbmod.set_state(env.conn, "consumer_socket_ready", "ready num_connections=1")  # 上一代残留
        gen = record_daemon_identity(env.conn, env.clock, env.prober)
        assert dbmod.get_state(env.conn, "consumer_socket_ready") == "down"
        set_startup_state(env.conn, "running", gen)
        assert dbmod.get_state(env.conn, "startup") == "running:%s" % gen
        assert "stopping" in STARTUP_PHASES
        with pytest.raises(AssertionError):
            set_startup_state(env.conn, "bogus", gen)


# ======================================================================
# bin/daemon.py 装配辅助(argv / xapp 变化监测)
# ======================================================================
def _load_daemon_module():
    spec = importlib.util.spec_from_file_location("slack_bridge_daemon_bin", ROOT / "bin" / "daemon.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestDaemonAssembly:
    def test_consumer_argv(self, cfg):
        mod = _load_daemon_module()
        root = ROOT
        assert mod.build_consumer_argv(cfg, root) == [sys.executable, str(root / "bin" / "slack_consumer.py")]
        cfg["consumer_python"] = "/opt/py/bin/python3"
        assert mod.build_consumer_argv(cfg, root)[0] == "/opt/py/bin/python3"

    def test_app_token_watch_only_fires_on_xapp_change(self, tokens):
        import os
        mod = _load_daemon_module()
        w = mod.AppTokenWatch(tokens.path)
        assert w.app_token_changed() is False
        # 只换 bot token(mtime 变)→ 不重拉 consumer
        os.utime(tokens.path, ns=(1, 1))
        from lib import config as configmod
        configmod.save_tokens({"bot_token": "xoxb-new", "app_token": tokens.tokens["app_token"]}, overwrite=True)
        assert w.app_token_changed() is False
        os.utime(tokens.path, ns=(2, 2))
        configmod.save_tokens({"bot_token": "xoxb-new", "app_token": "xapp-NEW"}, overwrite=True)
        assert w.app_token_changed() is True
        assert w.app_token_changed() is False
        os.chmod(tokens.path, 0o644)   # 文件不可用:不裁决(consumer 自己会以 rc 2 退出)
        os.utime(tokens.path, ns=(3, 3))
        assert w.app_token_changed() is False
