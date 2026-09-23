"""daemon 核心:flock 单例 + 单线程事件循环的编排层(contracts §1 / §9)。

- `ConsumerManager`:监督**单一** Socket Mode consumer 子进程(key = SOCKET_KEY;argv 由
  argv_builder 给出;stdin 持有 = 存活信号;stderr 监控 ready 哨兵 / 状态行;SIGTERM 管理,绝不 -9;
  退避重启;rc ∈ CONSUMER_SKIP_BACKOFF_RCS 直接最大退避;快速退出循环 → 告警)。
  consumer 的 stdout **不承载数据**(它自己落库 + ack),这里只计数、不路由。
- `DaemonCore`:每 tick `drain_staging()`(drain 是**唯一**事务拥有者:每行各开一个 db.tx,
  业务模块暴露 *_in_tx 接口)→ `run_followups(budget)`(网络物化按预算公平调度)→ gate → recovery
  → outbound;心跳 / suspect 窗 / 启动阶段 / 身份发布沿用 feishu-bridge 语义。
"""
import json
import os
import selectors
import signal
import subprocess

from . import approval as approval_mod
from . import constants, db
from . import inbound as inbound_mod
from . import slackwire

SOCKET_KEY = constants.SOCKET_KEY

RESTART_BACKOFF_START_MS = 1_000
RESTART_BACKOFF_MAX_MS = 60_000
STABLE_RUN_MS = 60_000
RAPID_EXIT_ALERT_THRESHOLD = 5
DRAIN_ERROR_MAX_LEN = 500


class _Consumer:
    __slots__ = ("key", "proc", "out_buf", "err_buf", "ready", "restarts",
                 "started_at", "next_restart_at", "backoff", "exited",
                 "generation", "streams", "last_rc", "restart_requested", "stdout_lines")

    def __init__(self, key):
        self.key = key
        self.proc = None
        self.out_buf = b""
        self.err_buf = b""
        self.ready = False
        self.restarts = 0
        self.started_at = None
        self.next_restart_at = 0
        self.backoff = RESTART_BACKOFF_START_MS
        self.exited = True
        self.generation = 0   # 进程代数:stale selector 事件按代数丢弃
        self.streams = ()
        self.last_rc = None
        self.restart_requested = None  # 非空 = 主动重拉(凭据变化等),不计退避/不算异常退出
        self.stdout_lines = 0


class ConsumerManager:
    """`ConsumerManager(clock, on_line, on_status, argv_builder, keys=(SOCKET_KEY,))`(contracts §8)。
    on_line(key, line_str):consumer stdout 行(只计数);on_status(key, status, detail):
    spawned / ready / stderr / exited / rapid-exit-alert / spawn-failed / restart。"""

    def __init__(self, clock, on_line, on_status, argv_builder, keys=(SOCKET_KEY,)):
        self.clock = clock
        self.on_line = on_line
        self.on_status = on_status
        self.argv_builder = argv_builder
        self.keys = tuple(keys)
        self.selector = selectors.DefaultSelector()
        self.consumers = {k: _Consumer(k) for k in self.keys}

    # ------------------------------------------------------------------
    def start_all(self):
        now = self.clock.mono_ms()
        for c in self.consumers.values():
            self._spawn(c, now)

    def _spawn(self, c, now):
        # 单消费者不变量:旧进程必须已 teardown+reap 才允许 respawn
        if c.proc is not None and c.proc.poll() is None:
            return
        argv = list(self.argv_builder(c.key))
        c.out_buf = b""   # respawn 卫生:清空半行缓冲,防跨代拼接污染
        c.err_buf = b""
        try:
            c.proc = subprocess.Popen(
                argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True)
        except OSError as e:
            c.exited = True
            c.next_restart_at = now + c.backoff
            c.backoff = min(c.backoff * 2, RESTART_BACKOFF_MAX_MS)
            self.on_status(c.key, "spawn-failed", str(e))
            return
        c.exited = False
        c.ready = False
        c.started_at = now
        c.generation += 1
        c.restart_requested = None
        c.streams = (c.proc.stdout, c.proc.stderr)
        for stream, tag in ((c.proc.stdout, "stdout"), (c.proc.stderr, "stderr")):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, (c, tag, c.generation))
        self.on_status(c.key, "spawned", "pid=%d gen=%d" % (c.proc.pid, c.generation))

    # ------------------------------------------------------------------
    def poll(self, timeout_s):
        """select 一轮并分发行;返回处理的 stdout 行数。"""
        n = 0
        try:
            events = self.selector.select(timeout_s)
        except OSError:
            return 0
        for key, _mask in events:
            c, tag, gen = key.data
            if gen != c.generation or c.exited:
                continue  # stale 代 / 已 teardown:残留就绪事件丢弃
            stream = key.fileobj
            try:
                fd = stream.fileno()
            except ValueError:
                continue  # 同批次内另一条流触发的 teardown 已关闭本流
            try:
                chunk = os.read(fd, 65536)
            except (BlockingIOError, InterruptedError):
                continue
            except (OSError, ValueError):
                chunk = b""
            if chunk == b"":
                # 任一流 EOF → 完整 teardown(双流关闭+kill+reap),不留半死进程
                self._teardown(c)
                continue
            n += self._feed(c, tag, chunk)
        return n

    def _teardown(self, c):
        """完整拆除一代 consumer:双流注销+关闭、SIGTERM(绝不 -9)、有界 reap。"""
        for stream in c.streams:
            try:
                self.selector.unregister(stream)
            except (KeyError, ValueError):
                pass
            try:
                stream.close()
            except OSError:
                pass
        c.streams = ()
        if c.proc is not None and c.proc.poll() is None:
            try:
                os.killpg(os.getpgid(c.proc.pid), signal.SIGTERM)
            except OSError:
                try:
                    c.proc.terminate()
                except OSError:
                    pass
            try:
                c.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass  # 仍未退:_spawn 的单消费者门挡住 respawn,tick 会再 TERM
        self._mark_exited(c)

    def _feed(self, c, tag, chunk):
        n = 0
        if tag == "stdout":
            c.out_buf += chunk
            while b"\n" in c.out_buf:
                line, c.out_buf = c.out_buf.split(b"\n", 1)
                text = line.decode("utf-8", "replace").strip()
                if text:
                    c.stdout_lines += 1
                    self.on_line(c.key, text)  # 只计数/记日志,不路由(stdout 不承载数据)
                    n += 1
        else:
            c.err_buf += chunk
            while b"\n" in c.err_buf:
                line, c.err_buf = c.err_buf.split(b"\n", 1)
                text = line.decode("utf-8", "replace").strip()
                if not text:
                    continue
                if text.startswith(constants.CONSUMER_READY_SENTINEL):
                    c.ready = True
                    detail = text[len(constants.CONSUMER_READY_SENTINEL):].strip()
                    self.on_status(c.key, "ready", detail)
                else:
                    self.on_status(c.key, "stderr", text)
        return n

    def _mark_exited(self, c):
        if c.exited:
            return
        c.exited = True
        c.ready = False  # 退出即不再 ready(daemon_state 由 on_status 同步)
        rc = None
        if c.proc is not None:
            try:
                rc = c.proc.wait(timeout=0.1)
            except Exception:  # noqa: BLE001
                rc = c.proc.poll()
        c.last_rc = rc
        now = self.clock.mono_ms()
        if c.restart_requested:
            # 主动重拉(凭据变化等):立即 respawn,不计退避、不算异常退出
            c.next_restart_at = now
            self.on_status(c.key, "restart", "rc=%s reason=%s" % (rc, c.restart_requested))
            return
        stable = c.started_at is not None and now - c.started_at >= STABLE_RUN_MS
        if stable:
            c.backoff = RESTART_BACKOFF_START_MS
            c.restarts = 0
        c.restarts += 1
        if rc in constants.CONSUMER_SKIP_BACKOFF_RCS:
            c.backoff = RESTART_BACKOFF_MAX_MS  # 致命鉴权 / 缺依赖:重试无意义,直接最大退避
        c.next_restart_at = now + c.backoff
        c.backoff = min(c.backoff * 2, RESTART_BACKOFF_MAX_MS)
        detail = "rc=%s restarts=%d" % (rc, c.restarts)
        if c.restarts >= RAPID_EXIT_ALERT_THRESHOLD:
            self.on_status(c.key, "rapid-exit-alert", detail)
        else:
            self.on_status(c.key, "exited", detail)

    def restart(self, key, reason="requested"):
        """主动重拉(如 xapp token 版本变化):SIGTERM 当前代,下一 tick 立即 respawn(不计退避)。"""
        c = self.consumers[key]
        if c.exited or c.proc is None or c.proc.poll() is not None:
            return False  # 没在跑(或已自行退出:走正常退避路径),无事可做
        c.restart_requested = reason
        self._teardown(c)
        return True

    def tick(self):
        """重启到期的 dead consumer;探测静默退出的进程;催死赖着不走的旧代。"""
        now = self.clock.mono_ms()
        for c in self.consumers.values():
            if not c.exited and c.proc is not None and c.proc.poll() is not None:
                self._teardown(c)
            if c.exited and c.proc is not None and c.proc.poll() is None:
                # teardown 后仍存活(TERM 被忽略):再 TERM,绝不 -9;respawn 被单消费者门挡住
                try:
                    os.killpg(os.getpgid(c.proc.pid), signal.SIGTERM)
                except OSError:
                    pass
                continue
            if c.exited and now >= c.next_restart_at:
                self._spawn(c, now)

    def shutdown(self):
        """SIGTERM(勿 -9)→ 等 5s → 放弃(绝不 SIGKILL consumer)。"""
        for c in self.consumers.values():
            if c.proc is None or c.proc.poll() is not None:
                continue
            try:
                os.killpg(os.getpgid(c.proc.pid), signal.SIGTERM)
            except OSError:
                try:
                    c.proc.terminate()
                except OSError:
                    pass
        for c in self.consumers.values():
            if c.proc is None:
                continue
            try:
                c.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        try:
            self.selector.close()
        except OSError:
            pass


def _parse_rc(detail):
    for tok in str(detail or "").split():
        if tok.startswith("rc="):
            v = tok[3:]
            return None if v in ("", "None") else v
    return None


def make_status_writer(conn, log):
    """consumer 状态 → daemon_state(contracts §4.5:consumer_<key>_ready / _last_status /
    _restarts / _last_exit_rc)。spawn=starting,ready 置位,退出=down。"""
    def on_status(key, status, detail):
        log("consumer[%s] %s: %s" % (key, status, detail))
        if status == "spawned":
            db.set_state(conn, "consumer_%s_ready" % key, "starting")  # 跨代不误报 ready
            db.set_state(conn, "consumer_%s_last_status" % key, ("%s %s" % (status, detail))[:200])
        elif status == "ready":
            db.set_state(conn, "consumer_%s_ready" % key, ("ready %s" % detail).strip()[:200])
        elif status in ("exited", "rapid-exit-alert", "spawn-failed", "restart"):
            db.set_state(conn, "consumer_%s_ready" % key, "down")
            db.set_state(conn, "consumer_%s_last_status" % key, ("%s %s" % (status, detail))[:200])
            rc = _parse_rc(detail)
            if rc is not None:
                db.set_state(conn, "consumer_%s_last_exit_rc" % key, rc)
            if status != "restart":
                db.bump_counter(conn, "consumer_%s_restarts" % key)
        else:
            db.set_state(conn, "consumer_%s_last_status" % key, ("%s %s" % (status, detail))[:200])
    return on_status


def mark_consumers_down(conn, keys):
    """daemon 正常退出:ready 一律清为 down。"""
    for k in keys:
        db.set_state(conn, "consumer_%s_ready" % k, "down")


# 'stopping' = daemon 决定退出、正在 shutdown(finally 首步写,mgr.shutdown 前)。
STARTUP_PHASES = ("probing", "running", "degraded", "refused", "stopping")
_READY_PHASES = ("running", "degraded")


def parse_startup(value):
    """'phase:gen' → (phase, gen);容错空/畸形。"""
    if not value or ":" not in value:
        return (value or "", "")
    phase, gen = value.split(":", 1)
    return (phase, gen)


def set_startup_state(conn, phase, generation):
    """gate.startup 得结论后置 running/degraded/refused(同 generation)。"""
    assert phase in STARTUP_PHASES
    db.set_state(conn, "startup", "%s:%s" % (phase, generation))


def record_daemon_identity(conn, clock, prober, code_identity=None):
    """拿锁+建 conn 后**原子发布**本代身份、首次心跳(仅表活着)、startup=probing:<gen>、
    consumer down —— 同一事务,避免"拿锁到 record_identity 之间"被 supervisor 读到半态。
    heartbeat 与 startup_state 分义:heartbeat 只说"进程活着",startup_state 才说"是否就绪"。
    generation 用**进程内唯一 token**(pid-uuid4);同事务发布 daemon_code_identity(pkg_root|version),
    供 CLI bind 前比对「更新后复用跑旧代码的旧 daemon」。返回本代 generation token。"""
    from . import procs as _procs, util as _util
    pid = os.getpid()
    now = clock.wall_ms()
    ident = _procs.self_identity(prober, pid)
    gen = "%d-%s" % (pid, _util.new_id())
    with db.tx(conn):  # 原子发布(单事务)
        db.set_state(conn, "daemon_pid", pid)
        db.set_state(conn, "daemon_started_at", now)
        db.set_state(conn, "daemon_proc_start", ident[1] if ident else "")
        db.set_state(conn, "daemon_generation", gen)
        db.set_state(conn, "startup", "probing:%s" % gen)
        db.set_state(conn, "last_loop_at", now)
        if code_identity is not None:
            db.set_state(conn, "daemon_code_identity", code_identity)
        mark_consumers_down(conn, [SOCKET_KEY])  # 跨代即刻清 ready
    return gen


class DaemonCore:
    """节奏编排(可测:drain_staging / run_followups / loop_iteration 均为纯入口)。
    构造签名冻结(contracts §8):DaemonCore(conn, cfg, clock, inbound, approval, outbound, recovery,
    log=None, gate=None)。"""

    def __init__(self, conn, cfg, clock, inbound, approval, outbound, recovery, log=None,
                 gate=None):
        self.conn = conn
        self.cfg = cfg
        self.clock = clock
        self.inbound = inbound
        self.approval = approval
        self.outbound = outbound
        self.recovery = recovery
        self.log = log or (lambda s: None)
        self.gate = gate  # 每循环发送前先 gate.tick(复检/重探)
        self._last_fast = 0
        self._last_slow = 0
        self._last_checkpoint = 0
        self.consumer_stdout_lines = 0

    # ------------------------------------------------------------------ consumer stdout(只计数)
    def on_consumer_line(self, key, line):
        """consumer 的 stdout 不承载数据(它自己落库+ack);这里只计数 + 记日志,绝不解析/路由。"""
        self.consumer_stdout_lines += 1
        db.bump_counter(self.conn, "consumer_stdout_lines")
        self.log("consumer[%s] stdout: %s" % (key, line[:200]))

    # ------------------------------------------------------------------ drain(唯一事务拥有者)
    def _dispatch_in_tx(self, row):
        etype = row["envelope_type"]
        if etype == slackwire.EVENTS_API:
            return inbound_mod.ingest_in_tx(self.conn, row)
        if etype == slackwire.INTERACTIVE:
            payload = json.loads(row["payload_json"])
            return approval_mod.process_in_tx(self.conn, payload)
        raise ValueError("unknown envelope_type %r" % (etype,))

    @staticmethod
    def _outcome_of(res):
        """业务返回值 → ("handed"|"dropped", detail);形状不对 = 编程错误,按业务异常处理(fail-closed)。"""
        if not isinstance(res, tuple) or len(res) != 2 or res[0] not in ("handed", "dropped"):
            raise ValueError("bad *_in_tx result: %r" % (res,))
        outcome, detail = res
        if outcome == "dropped" and (not isinstance(detail, str) or not detail):
            raise ValueError("dropped without reason: %r" % (res,))
        return outcome, detail

    def drain_staging(self, batch=constants.DRAIN_BATCH):
        """每 tick:取 staged ∧ next_drain_at 到期的行(按 seq,限 batch);**每行各开一个 db.tx**:
        events_api → inbound.ingest_in_tx(conn, row);interactive → approval.process_in_tx(conn, payload)。
        ("handed", x) | ("dropped", reason) → 同事务 UPDATE state='consumed'(dropped 的 reason 记入 error)。
        业务抛任何异常 → 回滚(行仍 staged)→ 单独小事务:drain_attempts+1、next_drain_at 指数退避、
        error=repr 截断;attempts ≥ DRAIN_MAX_ATTEMPTS → quarantined(保留 payload+error,计数 drain_quarantined)。
        handed 的 inbox 行不在此驱动(run_followups)。返回四计数。"""
        now = self.clock.wall_ms()
        counts = {"handed": 0, "dropped": 0, "retried": 0, "quarantined": 0}
        rows = self.conn.execute(
            "SELECT * FROM slack_events WHERE state='staged' "
            "AND (next_drain_at IS NULL OR next_drain_at<=?) ORDER BY seq LIMIT ?",
            (now, int(batch))).fetchall()
        for row in rows:
            try:
                with db.tx(self.conn):
                    outcome, detail = self._outcome_of(self._dispatch_in_tx(row))
                    err = detail if outcome == "dropped" else None
                    if not db.cas(self.conn,
                                  "UPDATE slack_events SET state='consumed', consumed_at=?, error=?, "
                                  "next_drain_at=NULL WHERE seq=? AND state='staged'",
                                  (now, err, row["seq"])):
                        raise RuntimeError("slack_events seq=%s no longer staged" % (row["seq"],))
                counts[outcome] += 1
            except Exception as e:  # noqa: BLE001 —— 业务异常:已回滚,行仍 staged
                self._record_drain_failure(row, e, now, counts)
        return counts

    def _record_drain_failure(self, row, exc, now, counts):
        attempts = int(row["drain_attempts"] or 0) + 1
        err = repr(exc)[:DRAIN_ERROR_MAX_LEN]
        try:
            if attempts >= constants.DRAIN_MAX_ATTEMPTS:
                with db.tx(self.conn):
                    if db.cas(self.conn,
                              "UPDATE slack_events SET state='quarantined', drain_attempts=?, error=?, "
                              "next_drain_at=NULL WHERE seq=? AND state='staged'",
                              (attempts, err, row["seq"])):
                        db.bump_counter(self.conn, "drain_quarantined")
                counts["quarantined"] += 1
                self.log("drain quarantined seq=%s key=%s err=%s" % (row["seq"], row["event_key"], err))
            else:
                delay = min(constants.DRAIN_BACKOFF_MS * (2 ** attempts), constants.DRAIN_BACKOFF_MAX_MS)
                with db.tx(self.conn):
                    db.cas(self.conn,
                           "UPDATE slack_events SET drain_attempts=?, next_drain_at=?, error=? "
                           "WHERE seq=? AND state='staged'",
                           (attempts, now + delay, err, row["seq"]))
                counts["retried"] += 1
                self.log("drain error seq=%s key=%s attempt=%d err=%s"
                         % (row["seq"], row["event_key"], attempts, err))
        except Exception as e2:  # noqa: BLE001 —— 连记录失败都失败(锁/IO):下 tick 再来
            self.log("drain bookkeeping failed seq=%s: %r" % (row["seq"], e2))
        db.set_state(self.conn, "last_error", ("drain %s" % err)[:200])

    # ------------------------------------------------------------------ followup(网络,按预算)
    def run_followups(self, budget=constants.FOLLOWUP_BUDGET_PER_TICK):
        """handed 的 inbox 行在这里推进:inbound.drive_pending_rows(budget)(先零网络本地分流,
        再按预算物化;每条之后回主循环)。异常不拖垮循环:计数 + last_error。"""
        try:
            res = self.inbound.drive_pending_rows(budget)
        except Exception as e:  # noqa: BLE001
            db.bump_counter(self.conn, "event_processing_errors")
            db.set_state(self.conn, "last_error", ("followup %s: %s" % (type(e).__name__, e))[:200])
            self.log("followup error: %s: %s" % (type(e).__name__, e))
            return {}
        return res if isinstance(res, dict) else {}

    # ------------------------------------------------------------------ 节奏
    def update_suspect_window(self, now):
        """睡眠/时钟回拨检测:loop 间隔异常 → 开 suspect 窗(判死宽限)。返回是否在窗内。"""
        prev = db.get_state(self.conn, "last_loop_at")
        suspect_until = int(db.get_state(self.conn, "suspect_until", "0") or 0)
        if prev is not None:
            prev = int(prev)
            if now < prev or now - prev > constants.DAEMON_GAP_MS:
                suspect_until = now + constants.SUSPECT_WINDOW_MS
                db.set_state(self.conn, "suspect_until", suspect_until)
        db.set_state(self.conn, "last_loop_at", now)
        return now < suspect_until

    def loop_iteration(self):
        """顺序:suspect 窗/心跳 → drain_staging → followups(预算)→ gate.tick → recovery.fast_tick(节奏)
        → outbound.tick → recovery.slow_tick(节奏)→ WAL checkpoint(节奏)。"""
        now = self.clock.wall_ms()
        in_suspect = self.update_suspect_window(now)
        self.drain_staging()
        self.run_followups()
        if self.gate is not None:
            self.gate.tick()  # 门检查先于出站(漂移 → 本循环零发送)
        if now - self._last_fast >= constants.DEATH_SCAN_INTERVAL_MS or self._last_fast == 0 \
                or now < self._last_fast:
            self._last_fast = now
            self.recovery.fast_tick(in_suspect_window=in_suspect)
        self.outbound.tick()
        if now - self._last_slow >= constants.RECOVERY_INTERVAL_MS or self._last_slow == 0 \
                or now < self._last_slow:
            self._last_slow = now
            self.recovery.slow_tick()
        if now - self._last_checkpoint >= constants.CHECKPOINT_INTERVAL_MS \
                or now < self._last_checkpoint:
            self._last_checkpoint = now
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:  # noqa: BLE001
                pass
