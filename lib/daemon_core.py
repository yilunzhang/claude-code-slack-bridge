"""daemon 核心:flock 单例 + 单线程事件循环(consumer 输出、恢复 tick、出站 tick 全部串行)。
consumer = `lark-cli event consume <key> --as bot --timeout 0`(stdin 持有;stderr 监控
ready/WARN;SIGTERM 管理,绝不 kill -9;退避重启;快速退出循环 → 告警)。"""
import json
import os
import selectors
import signal
import subprocess

from . import constants, db

RECEIVE_KEY = "im.message.receive_v1"
CARD_KEY = "card.action.trigger"

RESTART_BACKOFF_START_MS = 1_000
RESTART_BACKOFF_MAX_MS = 60_000
STABLE_RUN_MS = 60_000
RAPID_EXIT_ALERT_THRESHOLD = 5


class _Consumer:
    __slots__ = ("key", "proc", "out_buf", "err_buf", "ready", "restarts",
                 "started_at", "next_restart_at", "backoff", "exited",
                 "generation", "streams")

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
        self.generation = 0   # 进程代数(修复项7):stale selector 事件按代数丢弃
        self.streams = ()


class ConsumerManager:
    def __init__(self, profile, clock, on_line, on_status,
                 lark_bin="lark-cli", keys=(RECEIVE_KEY, CARD_KEY), argv_builder=None):
        self.profile = profile
        self.clock = clock
        self.on_line = on_line      # (key, line_str) -> None
        self.on_status = on_status  # (key, status, detail) -> None
        self.lark_bin = lark_bin
        self.keys = keys
        self.argv_builder = argv_builder or self._default_argv
        self.selector = selectors.DefaultSelector()
        self.consumers = {k: _Consumer(k) for k in keys}

    def _default_argv(self, key):
        return [self.lark_bin, "event", "consume", key, "--as", "bot",
                "--timeout", "0", "--profile", self.profile]

    # ------------------------------------------------------------------
    def start_all(self):
        now = self.clock.mono_ms()
        for c in self.consumers.values():
            self._spawn(c, now)

    def _spawn(self, c, now):
        # 修复项7:单消费者不变量 —— 旧进程必须已 teardown+reap 才允许 respawn
        if c.proc is not None and c.proc.poll() is None:
            return
        argv = self.argv_builder(c.key)
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
        c.streams = (c.proc.stdout, c.proc.stderr)
        for stream, tag in ((c.proc.stdout, "stdout"), (c.proc.stderr, "stderr")):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, (c, tag, c.generation))
        self.on_status(c.key, "spawned", f"pid={c.proc.pid} gen={c.generation}")

    # ------------------------------------------------------------------
    def poll(self, timeout_s):
        """select 一轮并分发行;返回处理的行数。"""
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
                # 修复项7:任一流 EOF → 完整 teardown(双流关闭+kill+reap),不留半死进程
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
                    self.on_line(c.key, text)
                    n += 1
        else:
            c.err_buf += chunk
            while b"\n" in c.err_buf:
                line, c.err_buf = c.err_buf.split(b"\n", 1)
                text = line.decode("utf-8", "replace").strip()
                if not text:
                    continue
                if "[event] ready" in text:
                    c.ready = True
                    self.on_status(c.key, "ready", text)
                else:
                    self.on_status(c.key, "stderr", text)
        return n

    def _mark_exited(self, c):
        if c.exited:
            return
        c.exited = True
        c.ready = False  # r2-m2:退出即不再 ready(daemon_state 由 on_status 同步)
        now = self.clock.mono_ms()
        stable = c.started_at is not None and now - c.started_at >= STABLE_RUN_MS
        if stable:
            c.backoff = RESTART_BACKOFF_START_MS
            c.restarts = 0
        c.restarts += 1
        c.next_restart_at = now + c.backoff
        c.backoff = min(c.backoff * 2, RESTART_BACKOFF_MAX_MS)
        if c.restarts >= RAPID_EXIT_ALERT_THRESHOLD:
            self.on_status(c.key, "rapid-exit-alert", f"restarts={c.restarts}")
        else:
            self.on_status(c.key, "exited", f"restarts={c.restarts}")
        if c.proc is not None:
            try:
                c.proc.wait(timeout=0.1)
            except Exception:
                pass

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
        """SIGTERM(勿 -9,防服务端订阅泄漏)→ 等 5s → 放弃(绝不 SIGKILL consume)。"""
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


def make_status_writer(conn, log):
    """r2-m2/r3-3:consumer 状态 → daemon_state 同步(spawn=starting,ready 置位,退出=down)。"""
    def on_status(key, status, detail):
        log(f"consumer[{key}] {status}: {detail}")
        if status == "spawned":
            db.set_state(conn, f"consumer_{key}_ready", "starting")  # r3-3:跨代不误报 ready
            db.set_state(conn, f"consumer_{key}_last_status", f"{status} {detail}"[:200])
        elif status == "ready":
            db.set_state(conn, f"consumer_{key}_ready", f"ready {detail}"[:200])
        elif status in ("exited", "rapid-exit-alert", "spawn-failed"):
            db.set_state(conn, f"consumer_{key}_ready", "down")
            db.set_state(conn, f"consumer_{key}_last_status", f"{status} {detail}"[:200])
            db.bump_counter(conn, f"consumer_{key}_restarts")
        else:
            db.set_state(conn, f"consumer_{key}_last_status", f"{status} {detail}"[:200])
    return on_status


def mark_consumers_down(conn, keys):
    """daemon 正常退出:ready 一律清为 down(r3-3)。"""
    for k in keys:
        db.set_state(conn, f"consumer_{k}_ready", "down")


# r7-2:'stopping' = daemon 决定退出、正在 shutdown(finally 首步写,mgr.shutdown 前)。
# 缩窗+可观测,**非根治**冷启动窗口竞态(见 README 已知限制)。
STARTUP_PHASES = ("probing", "running", "degraded", "refused", "stopping")
_READY_PHASES = ("running", "degraded")


def parse_startup(value):
    """'phase:gen' → (phase, gen);容错空/畸形。"""
    if not value or ":" not in value:
        return (value or "", "")
    phase, gen = value.split(":", 1)
    return (phase, gen)


def set_startup_state(conn, phase, generation):
    """r4-1:gate.startup 得结论后置 running/degraded/refused(同 generation)。"""
    assert phase in STARTUP_PHASES
    db.set_state(conn, "startup", f"{phase}:{generation}")


def record_daemon_identity(conn, clock, prober, code_identity=None):
    """r3-5/r4-1/r5-M1:拿锁+建 conn 后**原子发布**本代身份、首次心跳(仅表活着)、
    startup=probing:<gen>、consumer down —— 同一事务,避免"拿锁到 record_identity 之间"
    被 supervisor 读到半态(旧代 tuple 完整对齐)。
    heartbeat 与 startup_state 分义:heartbeat 只说"进程活着",startup_state 才说"是否就绪"。
    r5-M1:generation 用**进程内唯一 token**(uuid4;不用 pid/时间——pid 可复用、同 ms 可碰撞)。
    MAJOR 3:同事务发布 daemon_code_identity(pkg_root|version),供 CLI bind 前比对,
    检测「更新/迁移后复用跑旧代码的旧 daemon」。返回本代 generation token。"""
    import os as _os
    from . import procs as _procs, util as _util
    pid = _os.getpid()
    now = clock.wall_ms()
    ident = _procs.self_identity(prober, pid)
    gen = f"{pid}-{_util.new_id()}"  # uuid4 保证唯一;pid 前缀便于排障
    with db.tx(conn):  # 原子发布(单事务)
        db.set_state(conn, "daemon_pid", pid)
        db.set_state(conn, "daemon_started_at", now)
        db.set_state(conn, "daemon_proc_start", ident[1] if ident else "")
        db.set_state(conn, "daemon_generation", gen)
        db.set_state(conn, "startup", f"probing:{gen}")
        db.set_state(conn, "last_loop_at", now)
        if code_identity is not None:
            db.set_state(conn, "daemon_code_identity", code_identity)
        mark_consumers_down(conn, [RECEIVE_KEY, CARD_KEY])  # r4-2:跨代即刻清 ready
    return gen


class DaemonCore:
    """事件路由 + 节奏编排(可测:route_line / loop_iteration 均纯函数式入口)。"""

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
        self.gate = gate  # r2-M2:每循环发送前先 gate.tick(复检/重探)
        self._last_fast = 0
        self._last_slow = 0
        self._last_checkpoint = 0

    # ------------------------------------------------------------------
    def route_line(self, kind, line):
        try:
            obj = json.loads(line)
        except ValueError:
            db.bump_counter(self.conn, "malformed_event_lines")
            return
        if not isinstance(obj, dict):
            db.bump_counter(self.conn, "malformed_event_lines")
            return
        try:
            if kind == RECEIVE_KEY:
                self.inbound.process_event(obj)
            elif kind == CARD_KEY:
                self.approval.process_event(obj)
        except Exception as e:
            # 单条事件失败不拖垮循环;fail-closed(不产生任何外发)+ 计数
            db.bump_counter(self.conn, "event_processing_errors")
            db.set_state(self.conn, "last_error", f"{type(e).__name__}: {e}")
            self.log(f"event error: {type(e).__name__}: {e}")

    # ------------------------------------------------------------------
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
        now = self.clock.wall_ms()
        in_suspect = self.update_suspect_window(now)
        if now - self._last_fast >= constants.DEATH_SCAN_INTERVAL_MS or self._last_fast == 0 \
                or now < self._last_fast:
            self._last_fast = now
            self.recovery.fast_tick(in_suspect_window=in_suspect)
        else:
            # 每轮仍推进 waiting 行与激活(轻量)
            self.inbound.drive_waiting_rows()
        if self.gate is not None:
            self.gate.tick()  # r2-M2:门检查先于出站(漂移 → 本循环零发送)
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
            except Exception:
                pass
