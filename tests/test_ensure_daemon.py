"""修复项5:ensure_daemon 挂死恢复(锁被持有但心跳陈旧→pid+start 精确匹配 SIGTERM→接管);
listener 探活升级为 锁+心跳新鲜;新拉起要求 last_loop_at 晚于 spawn。"""
import signal

from tests.conftest import CHAT
from tests.helpers import FakeClock, FakeProber
from lib import ctl, db as dbmod
from lib.ctl import DaemonSupervisor, HUNG_THRESHOLD_MS

DPID = 5555
DSTART = "Tue Jul 15 00:00:00 2026"


class World:
    """模拟 锁/daemon_state/进程 的小世界。"""

    def __init__(self, clock, held=False, last_loop=None, pid=DPID, pstart=DSTART,
                 startup="running:g1", generation="g1", linger_kill=False):
        self.clock = clock
        self.held = held
        self.last_loop = last_loop
        self.pid = pid
        self.pstart = pstart
        self.startup = startup
        self.generation = generation
        self.kills = []
        self.spawns = 0
        self.linger_kill = linger_kill  # r6:kill 后旧代 lingering(不立即清 pid/锁)
        self.killed = False
        self.prober = FakeProber()
        if pid is not None:
            self.prober.set(pid, 1, pstart, "python3")

    def lock_held(self):
        return self.held

    def read_state(self):
        return {"last_loop_at": self.last_loop, "daemon_pid": self.pid,
                "daemon_proc_start": self.pstart, "startup": self.startup,
                "daemon_generation": self.generation}

    def kill(self, pid, sig):
        self.kills.append((pid, sig))
        self.killed = True
        if self.linger_kill:
            return  # r6 真机:SIGTERM 不立即清 pid/锁;旧代退出前 loop 还会跑完刷心跳
        self.held = False  # 默认:SIGTERM 后 daemon 退出释放锁(既有行为)
        self.prober.remove(pid)

    def finish_exit(self):
        """r6:模拟旧代真正退出——清 pid/锁(linger_kill 场景由 timeline 显式触发)。"""
        self.held = False
        if self.pid is not None:
            self.prober.remove(self.pid)

    def spawn(self):
        self.spawns += 1
        self.held = True
        self.last_loop = self.clock.wall_ms() + 1  # 新 daemon 心跳晚于 spawn
        self.startup = "running:g2"  # 默认 spawn 出的 daemon 就绪
        self.generation = "g2"

    def supervisor(self, **kw):
        # codex-final:注入分离的两个时钟源 —— now_ms=墙钟(心跳/state_ready),
        # mono_ms=单调钟(等待 deadline)。FakeClock 的 wall/mono 可独立推进。
        return DaemonSupervisor(
            lock_held=self.lock_held, read_state=self.read_state,
            spawn=self.spawn, kill=self.kill, prober=self.prober,
            now_ms=self.clock.wall_ms, mono_ms=self.clock.mono_ms,
            sleep=lambda s: None, **kw)


def test_not_held_spawns_and_requires_fresh_heartbeat():
    clock = FakeClock()
    w = World(clock, held=False, last_loop=None)
    assert w.supervisor().ensure() == "started"
    assert w.spawns == 1 and w.kills == []


def test_held_fresh_is_running():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - 1000)
    assert w.supervisor().ensure() == "running"
    assert w.spawns == 0 and w.kills == []


def test_hung_daemon_precise_kill_then_recover():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1)
    assert w.supervisor().ensure() == "recovered"
    assert w.kills == [(DPID, signal.SIGTERM)]  # pid+start 精确匹配后才 SIGTERM
    assert w.spawns == 1


def test_hung_but_identity_mismatch_no_kill():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1)
    w.prober.set(DPID, 1, "DIFFERENT-START", "python3")  # pid 复用:身份不匹配
    assert w.supervisor().ensure() == "failed"
    assert w.kills == [] and w.spawns == 0


def test_hung_without_recorded_identity_no_kill():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1,
              pid=None, pstart=None)
    assert w.supervisor().ensure() == "failed"
    assert w.kills == []


def test_stale_spawn_heartbeat_not_accepted():
    """新拉起要求 last_loop_at 晚于本次 spawn(旧残留心跳不算 ready)。"""
    clock = FakeClock()
    w = World(clock, held=False, last_loop=None)

    def bad_spawn():
        w.spawns += 1
        w.held = True
        w.last_loop = clock.wall_ms() - 999_999  # 陈旧心跳(spawn 前的残留)

    w.spawn = bad_spawn
    assert w.supervisor(wait_s=1).ensure() == "failed"


def test_daemon_healthy_needs_lock_and_fresh_heartbeat(env, monkeypatch):
    monkeypatch.setattr(ctl, "daemon_lock_held", lambda: True)
    dbmod.set_state(env.conn, "daemon_generation", "g1")
    dbmod.set_state(env.conn, "startup", "running:g1")
    dbmod.set_state(env.conn, "last_loop_at", env.clock.wall_ms() - 1000)
    assert ctl.daemon_healthy(env.conn, now_ms=env.clock.wall_ms()) is True
    # r4-1:probing 不算就绪
    dbmod.set_state(env.conn, "startup", "probing:g1")
    assert ctl.daemon_healthy(env.conn, now_ms=env.clock.wall_ms()) is False
    dbmod.set_state(env.conn, "startup", "running:g1")
    dbmod.set_state(env.conn, "last_loop_at",
                    env.clock.wall_ms() - HUNG_THRESHOLD_MS - 1)
    assert ctl.daemon_healthy(env.conn, now_ms=env.clock.wall_ms()) is False
    monkeypatch.setattr(ctl, "daemon_lock_held", lambda: False)
    dbmod.set_state(env.conn, "last_loop_at", env.clock.wall_ms())
    assert ctl.daemon_healthy(env.conn, now_ms=env.clock.wall_ms()) is False


class FakeSingleflight:
    def __init__(self, available=True):
        self.available = available
        self.acquired = 0
        self.released = 0

    def try_acquire(self):
        if self.available:
            self.acquired += 1
            return True
        return False

    def release(self):
        self.released += 1


def test_long_download_not_taken_over_under_threshold():
    """r7-3:阈值收窄到 DOWNLOAD_TIMEOUT_S+60(=180s);单次 120s 合法下载(daemon 单线程,
    下载期间主循环阻塞不刷心跳)仍 < 阈值 → 不误判挂死。确认阈值 ≥ 单次最长同步网络操作。"""
    from lib import constants
    clock = FakeClock()
    assert HUNG_THRESHOLD_MS >= constants.DOWNLOAD_TIMEOUT_S * 1000  # 覆盖最长同步下载
    assert HUNG_THRESHOLD_MS < 300_000  # 从 r2 的 300s 收窄
    w = World(clock, held=True, last_loop=clock.wall_ms() - constants.DOWNLOAD_TIMEOUT_S * 1000)
    assert w.supervisor().ensure() == "running"
    assert w.kills == [] and w.spawns == 0


def test_singleflight_busy_no_kill_no_spawn():
    """r2-M1④/r6:并发 owner 持锁 → 本次绝不 kill/spawn,等 handoff:
    owner 结束释放 singleflight → waiter 拿锁走权威 _ensure_locked。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1)
    sf = FakeSingleflight(available=False)
    sup = w.supervisor(wait_s=2)
    sup.singleflight = sf
    steps = {"n": 0}

    def owner_finishing_sleep(s):
        steps["n"] += 1
        if steps["n"] >= 2:
            w.last_loop = clock.wall_ms()  # owner 完成后世界健康
            sf.available = True            # owner 释放 → waiter 可接手

    sup.sleep = owner_finishing_sleep
    assert sup.ensure() == "running"
    assert w.kills == [] and w.spawns == 0


def test_singleflight_busy_and_still_unhealthy_fails():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1)
    sf = FakeSingleflight(available=False)
    sup = w.supervisor(wait_s=1)
    sup.singleflight = sf
    assert sup.ensure() == "failed"
    assert w.kills == [] and w.spawns == 0


def test_singleflight_acquired_released_around_takeover():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1)
    sf = FakeSingleflight(available=True)
    sup = w.supervisor()
    sup.singleflight = sf
    assert sup.ensure() == "recovered"
    assert sf.acquired == 1 and sf.released == 1


class TestStartupStateGate:
    """r4-1:probing/refused 不算就绪;running/degraded 才算。"""

    def test_probing_not_ready_running_fast_path_requires_startup(self):
        from lib import ctl
        st = {"last_loop_at": 1000, "daemon_generation": "g1", "startup": "probing:g1"}
        assert ctl.state_ready(st, now_ms=1000) is False
        st["startup"] = "running:g1"
        assert ctl.state_ready(st, now_ms=1000) is True
        st["startup"] = "degraded:g1"
        assert ctl.state_ready(st, now_ms=1000) is True
        st["startup"] = "refused:g1"
        assert ctl.state_ready(st, now_ms=1000) is False

    def test_generation_mismatch_not_ready(self):
        from lib import ctl
        st = {"last_loop_at": 1000, "daemon_generation": "gNEW", "startup": "running:gOLD"}
        assert ctl.state_ready(st, now_ms=1000) is False

    def test_stale_heartbeat_not_ready_even_if_running(self):
        from lib import ctl
        st = {"last_loop_at": 0, "daemon_generation": "g1", "startup": "running:g1"}
        assert ctl.state_ready(st, now_ms=ctl.HUNG_THRESHOLD_MS + 1) is False

    def test_slow_probe_concludes_degraded_bind_can_continue(self):
        """慢探测最终 degraded → ensure 视为 started(就绪),bind 可继续。"""
        clock = FakeClock()
        # 起始:刚 spawn,probing;几步后转 degraded
        w = World(clock, held=False, last_loop=None)
        steps = {"n": 0}
        orig_spawn = w.spawn

        def slow_spawn():
            w.spawns += 1
            w.held = True
            w.startup = "probing:g2"
            w.generation = "g2"
            w.last_loop = clock.wall_ms()  # 首心跳(仅表活着)

        def transitioning_sleep(s):
            steps["n"] += 1
            if steps["n"] >= 3:
                w.startup = "degraded:g2"  # 探测最终结论=degraded
                w.last_loop = clock.wall_ms()

        w.spawn = slow_spawn
        sup = w.supervisor(wait_s=5)
        sup.sleep = transitioning_sleep
        assert sup.ensure() == "started"

    def test_slow_probe_concludes_mismatch_daemon_refused_failed(self):
        """慢探测最终 mismatch → daemon refused 退出 → ensure failed(bind 不会继续)。"""
        clock = FakeClock()
        w = World(clock, held=False, last_loop=None)
        steps = {"n": 0}

        def slow_spawn():
            w.spawns += 1
            w.held = True
            w.startup = "probing:g2"
            w.generation = "g2"
            w.last_loop = clock.wall_ms()

        def refusing_sleep(s):
            steps["n"] += 1
            if steps["n"] >= 3:
                w.startup = "refused:g2"  # 结论=mismatch
                w.held = False            # daemon 退出释放锁

        w.spawn = slow_spawn
        sup = w.supervisor(wait_s=5)
        sup.sleep = refusing_sleep
        assert sup.ensure() == "failed"


# ===== r5-M1: 旧代完整 tuple 假成功窗 =====

def test_spawn_wait_rejects_stale_aligned_old_generation():
    """r5-M1 回归:spawn 后 DB 仍是**旧代完整对齐**(新鲜心跳 + running:gOLD + gen=gOLD),
    模拟'新 daemon 已拿 flock 但尚未 record_identity' → 结果**不是** started。"""
    clock = FakeClock()
    w = World(clock, held=False, last_loop=None, startup="running:gOLD", generation="gOLD")

    def stale_spawn():
        w.spawns += 1
        w.held = True
        w.last_loop = clock.wall_ms()  # 新鲜心跳,但仍是旧代残留
        # 不发布新代:startup/generation 保持 gOLD

    w.spawn = stale_spawn
    sup = w.supervisor(wait_s=1, probe_wait_s=1)
    assert sup.ensure() == "failed"  # 旧代完整对齐 ≠ 新进程就绪
    assert w.spawns == 1 and w.kills == []


def test_spawn_wait_accepts_only_new_generation():
    """对照:发布新代 gNEW 就绪 → started。"""
    clock = FakeClock()
    w = World(clock, held=False, last_loop=None, startup="running:gOLD", generation="gOLD")

    def real_spawn():
        w.spawns += 1
        w.held = True
        w.startup = "running:gNEW"
        w.generation = "gNEW"
        w.last_loop = clock.wall_ms()

    w.spawn = real_spawn
    assert w.supervisor(wait_s=2, probe_wait_s=2).ensure() == "started"


def test_await_handoff_acquires_and_self_ensures_when_owner_releases():
    """r6:owner 结束释放 singleflight → busy waiter 拿到锁,走权威 _ensure_locked(而非观察)。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - 1000,
              startup="running:gCUR", generation="gCUR")
    sf = FakeSingleflight(available=False)  # owner 持锁
    sup = w.supervisor(wait_s=2)
    sup.singleflight = sf
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        if steps["n"] >= 2:
            w.last_loop = clock.wall_ms()
            sf.available = True  # owner 释放

    sup.sleep = sleep
    assert sup.ensure() == "running"
    assert sf.acquired >= 1 and w.kills == [] and w.spawns == 0


def test_await_handoff_never_trusts_baseline_gen_even_fresh_hb_pid_lock():
    """r6-① 核心洞:owner 正接管旧代(SIGTERM 后旧代退出前刷新鲜心跳、pid/锁暂存活)。
    busy waiter 对 baseline 代的任何观察量都不可信 → 绝不判 running;
    只等到 owner 拉起的**新代** ready(或拿锁自己 ensure)。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="running:gOLD", generation="gOLD")  # 旧代:新鲜心跳+pid 活+持锁
    sf = FakeSingleflight(available=False)  # owner E1 持锁接管中
    sup = w.supervisor(wait_s=2, probe_wait_s=3)
    sup.singleflight = sf
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        # 前两轮:旧代垂死持续刷新鲜心跳(baseline 代不可信,绝不能被判 running)
        w.last_loop = clock.wall_ms()
        if steps["n"] == 3:
            # owner 拉起的新代就绪(仍持 singleflight,尚未 release)
            w.startup = "running:gNEW"
            w.generation = "gNEW"

    sup.sleep = sleep
    assert sup.ensure() == "running"        # 等到 gNEW,不是 gOLD
    assert w.kills == [] and w.spawns == 0   # busy waiter 不 kill/spawn


def test_await_handoff_baseline_never_running_owner_stuck_daemon_alive():
    """r6+r7-2:baseline 代无论 pid 死活一律不算 running。owner 卡住但 gOLD 还活着(心跳新鲜)
    → 超时返回结构化 **in_progress**(可重试,transition 仍有效),核心=**绝不 running**。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="running:gOLD", generation="gOLD")
    sf = FakeSingleflight(available=False)  # owner 永不释放(卡住)
    sup = w.supervisor(wait_s=1, probe_wait_s=1)
    sup.singleflight = sf
    r = sup.ensure()
    assert r == "in_progress" and r != "running"  # gOLD==baseline 永不算成功;daemon 活着 → in_progress


def test_await_handoff_owner_gone_daemon_dead_is_failed():
    """r7-2:owner 释放锁但没留下活 daemon(心跳陈旧)→ 无进展 → failed(非 in_progress)。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1,
              startup="running:gOLD", generation="gOLD")
    sf = FakeSingleflight(available=False)  # owner 卡住不释放,且 daemon 心跳陈旧
    sup = w.supervisor(wait_s=1, probe_wait_s=1)
    sup.singleflight = sf
    assert sup.ensure() == "failed"


def test_await_handoff_covers_full_takeover_critical_section():
    """r7-2 核心 bug:busy waiter 上限须覆盖 owner **最坏临界区**(等旧锁释放 wait_s +
    spawn 等新代就绪 wait_s+probe_wait_s = 2*wait_s+probe_wait_s)。owner 在
    (wait_s+probe_wait_s, 2*wait_s+probe_wait_s] 之间结束 → 新上限等到 running,旧上限会假失败。
    codex-final:deadline 由**单调钟**驱动,故用 mono 相对时长控制 owner 结束点。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:gNEW", generation="gNEW")
    sf = FakeSingleflight(available=False)
    wait_s, probe_wait_s = 10, 5  # 旧上限 15s;新上限 25s(mono 时长)
    sup = w.supervisor(wait_s=wait_s, probe_wait_s=probe_wait_s)
    sup.singleflight = sf
    start_mono = clock.mono_ms()

    def sleep(s):
        clock.tick(int(s * 1000))  # 墙钟+单调钟同步推进
        if clock.mono_ms() - start_mono >= 20_000:  # owner 单调 20s 后结束(> 旧 15s,< 新 25s)
            w.startup = "running:gNEW"
            w.last_loop = clock.wall_ms()
            sf.available = True  # owner 释放 → waiter 拿锁自 ensure

    sup.sleep = sleep
    assert sup.ensure() == "running"  # 新上限(单调 25s)覆盖到 20s → 等到;旧 15s 上限会在此前假失败


def test_await_handoff_deadline_uses_monotonic_not_wall():
    """codex-final(唯一真 bug 回归):deadline 用**单调钟**。墙钟疯狂前跳/回拨不得让
    busy 等待提前结束;只有单调钟正常推进才决定超时。若 deadline 误用墙钟(now_ms),
    第一步墙钟前跳 +10min 就会立刻超时返回 in_progress,拿不到 owner 在单调 20s 的成功。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:gNEW", generation="gNEW")
    sf = FakeSingleflight(available=False)
    wait_s, probe_wait_s = 10, 5  # 单调 deadline = 25s
    sup = w.supervisor(wait_s=wait_s, probe_wait_s=probe_wait_s)
    sup.singleflight = sf
    start_mono = clock.mono_ms()

    def sleep(s):
        clock.mono += int(s * 1000)          # 单调钟正常推进(每步 +0.3s)
        clock.wall += 10 * 60 * 1000          # 墙钟每步疯狂前跳 +10min(误用墙钟必提前超时)
        w.last_loop = clock.wall_ms()         # 心跳跟随当前墙钟 → 始终 fresh
        if clock.mono_ms() - start_mono >= 20_000:  # owner 单调 20s(< 25s deadline)结束
            w.startup = "running:gNEW"
            sf.available = True

    sup.sleep = sleep
    assert sup.ensure() == "running"  # 单调钟撑到 20s 等到;若用墙钟则第 1 步就超时 → 拿不到 running


def test_await_handoff_wall_rewind_does_not_shorten_deadline():
    """codex-final:墙钟**回拨**(负跳)也不得影响单调 deadline —— waiter 仍等满单调预算。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:gNEW", generation="gNEW")
    sf = FakeSingleflight(available=False)
    sup = w.supervisor(wait_s=10, probe_wait_s=5)  # 单调 25s
    sup.singleflight = sf
    start_mono = clock.mono_ms()

    def sleep(s):
        clock.mono += int(s * 1000)
        clock.rewind_wall(5 * 60 * 1000)     # 墙钟每步回拨 5min
        w.last_loop = clock.wall_ms()
        if clock.mono_ms() - start_mono >= 18_000:  # 单调 18s owner 结束
            w.startup = "running:gNEW"
            sf.available = True

    sup.sleep = sleep
    assert sup.ensure() == "running"


def test_fast_path_does_not_bypass_singleflight():
    """r5-M1④:健康态 fast-path 也必须经 singleflight(不越过正在进行的 ensure)。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="running:g1", generation="g1")
    sf = FakeSingleflight(available=True)
    sup = w.supervisor()
    sup.singleflight = sf
    assert sup.ensure() == "running"
    assert sf.acquired == 1 and sf.released == 1  # 走了 singleflight,没绕过


# ===== r5-M2: 误杀健康 probing =====

def test_fresh_probing_not_killed_waits_conclusion_running():
    """r5-M2:心跳新鲜的 probing 绝不 kill;等其结论 running → ready。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:g1", generation="g1")
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        w.last_loop = clock.wall_ms()  # 探测中持续心跳
        if steps["n"] >= 2:
            w.startup = "running:g1"

    sup = w.supervisor(probe_wait_s=5)
    sup.sleep = sleep
    assert sup.ensure() == "running"
    assert w.kills == [] and w.spawns == 0


def test_fresh_probing_concludes_degraded_is_ready():
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:g1", generation="g1")
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        w.last_loop = clock.wall_ms()
        if steps["n"] >= 2:
            w.startup = "degraded:g1"

    sup = w.supervisor(probe_wait_s=5)
    sup.sleep = sleep
    assert sup.ensure() == "running"
    assert w.kills == []


def test_lock_held_refused_returns_failed_no_restart():
    """r5-M2:refused 终态 → 等锁释放返回失败,不进接管/重启分支。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="refused:g1", generation="g1")
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        if steps["n"] >= 2:
            w.held = False  # daemon 退出释放锁

    sup = w.supervisor()
    sup.sleep = sleep
    assert sup.ensure() == "failed"
    assert w.kills == [] and w.spawns == 0


def test_slow_probe_timeout_not_mis_killed():
    """r5-M2:合法慢探测(心跳一直新鲜、始终 probing)超时 → 返回失败但**绝不误杀**。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:g1", generation="g1")

    def sleep(s):
        w.last_loop = clock.wall_ms()  # 探测中,心跳始终新鲜

    sup = w.supervisor(probe_wait_s=1)
    sup.sleep = sleep
    assert sup.ensure() == "failed"
    assert w.kills == [] and w.spawns == 0


def test_probing_then_heartbeat_goes_stale_next_ensure_takes_over():
    """probing 中途 daemon 挂死(心跳陈旧)→ 本次不误杀返回失败;下次 ensure 才接管。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:g1", generation="g1")
    # 心跳不再更新(挂死);sleep 推进单调时间使其陈旧
    def sleep(s):
        clock.tick(200_000)
    sup = w.supervisor(probe_wait_s=2)
    sup.sleep = sleep
    r1 = sup.ensure()
    assert r1 == "failed" and w.kills == []  # 本次不 kill(进入时是 fresh probing)


# ===== r6 named regressions: fake 调真 + 并发临界区上限 =====

def test_takeover_tolerates_lingering_old_gen_before_exit():
    """r6(fake 调真):SIGTERM 后旧代 lingering —— pid/锁暂活、退出前再刷一次心跳。
    takeover 只等锁释放,不因旧代 lingering 的观察量误判;新代 ready 才 recovered。"""
    clock = FakeClock()
    w = World(clock, held=True, last_loop=clock.wall_ms() - HUNG_THRESHOLD_MS - 1,
              startup="running:gOLD", generation="gOLD", linger_kill=True)
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        if steps["n"] == 1:
            w.last_loop = clock.wall_ms()  # 旧代垂死刷心跳(pid/锁仍活)
        elif steps["n"] == 2:
            w.finish_exit()                # 旧代真正退出,锁释放

    def spawn():
        w.spawns += 1                      # E1 拉起新代
        w.held = True
        w.pid = 7000
        w.prober.set(7000, 1, "new-start", "python3")
        w.pstart = "new-start"
        w.startup = "running:gNEW"
        w.generation = "gNEW"
        w.last_loop = clock.wall_ms()

    w.spawn = spawn
    sup = w.supervisor(wait_s=5, probe_wait_s=2)
    sup.sleep = sleep
    assert sup.ensure() == "recovered"
    assert w.kills == [(DPID, signal.SIGTERM)]  # 精确身份匹配 kill 一次
    assert w.spawns == 1


def test_await_handoff_waits_full_owner_critical_section_for_slow_probing():
    """r6-②:并发 waiter + owner 慢 probing(~30 步)→ busy waiter 在 owner 临界区上限
    (wait_s+probe_wait_s)内等到成功,而非孤立 wait_s(旧 12s)假失败。"""
    clock = FakeClock()
    # owner 已拉起新代且仍 probing(baseline 此刻即 = gNEW → 走 acquire-handoff 路径)
    w = World(clock, held=True, last_loop=clock.wall_ms(),
              startup="probing:gNEW", generation="gNEW")
    sf = FakeSingleflight(available=False)
    small_wait_s = 2  # 若上限仅为 wait_s(=2s→~7 步),第 30 步的结论必然假失败
    sup = w.supervisor(wait_s=small_wait_s, probe_wait_s=40)
    sup.singleflight = sf
    steps = {"n": 0}

    def sleep(s):
        steps["n"] += 1
        w.last_loop = clock.wall_ms()  # probing 中持续心跳
        if steps["n"] >= 30:           # owner 30 步后探测结束并释放
            w.startup = "running:gNEW"
            sf.available = True

    sup.sleep = sleep
    # 上限 = (2+40)/0.3 ≈ 141 步 ≥ 30 → 等到;若仅 (2)/0.3 ≈ 7 步则会 30>7 假失败
    assert sup.ensure() == "running"
    assert w.kills == [] and w.spawns == 0
    assert steps["n"] >= 30  # 确实等过了 wait_s-only 的上限


# ===== r7-1: 结构化 ready 判定 =====

def test_is_ready_result_semantics():
    from lib import ctl
    assert all(ctl.is_ready_result(r) for r in ("running", "started", "recovered"))
    assert not any(ctl.is_ready_result(r) for r in ("in_progress", "failed", "down", "timeout", ""))


# ===== r7-2: stopping 不算就绪(缩窗+可观测) =====

def test_stopping_phase_not_ready(env):
    from lib import ctl, db as dbmod
    from lib.daemon_core import STARTUP_PHASES, set_startup_state
    assert "stopping" in STARTUP_PHASES
    st = {"last_loop_at": 1000, "daemon_generation": "g1", "startup": "stopping:g1"}
    assert ctl.state_ready(st, now_ms=1000) is False  # 心跳新鲜也不算就绪(正在退出)
    # daemon_healthy 亦排除 stopping
    import lib.ctl as ctlmod
    orig = ctlmod.daemon_lock_held
    ctlmod.daemon_lock_held = lambda: True
    try:
        dbmod.set_state(env.conn, "daemon_generation", "g1")
        dbmod.set_state(env.conn, "last_loop_at", env.clock.wall_ms())
        set_startup_state(env.conn, "stopping", "g1")
        assert ctl.daemon_healthy(env.conn, now_ms=env.clock.wall_ms()) is False
        set_startup_state(env.conn, "running", "g1")
        assert ctl.daemon_healthy(env.conn, now_ms=env.clock.wall_ms()) is True
    finally:
        ctlmod.daemon_lock_held = orig


# 注:MAJOR 3 的 code-identity 检测**已从 supervisor 移出**(换层)→ supervisor 不再有
# is_code_current,恢复到只判「daemon 健康在跑吗」。bind 前置串行检查见 test_bridgectl.py::TestReconcileCodeIdentity。


def test_version_code_identity_str_no_git_stable():
    """coderev:code_identity = pkg_root|plugin_version(**删了 git 段**),两段、稳定。"""
    import json
    from lib import version, paths
    ci = version.code_identity_str()
    parts = ci.split("|")
    assert len(parts) == 2                    # 无 git 段
    assert parts[0] == str(paths.pkg_root())  # resolve 后绝对路径
    # 版本段 = **独立**解析 plugin.json 的 version(不走 version.plugin_version() 免同义反复);
    # 动态读避免每次 bump 手改硬编码而 rot(21c1089 bump 1.0.0→1.0.1 时本断言未更新即漏)。
    expected_ver = json.loads(
        (paths.pkg_root() / ".claude-plugin" / "plugin.json").read_text())["version"]
    assert parts[1] == expected_ver           # code_identity 版本段 == plugin.json 实际 version
    assert version.code_identity_str() == ci  # 稳定(不含易变 git hash)
    assert not hasattr(version, "git_build")  # git 探测已删除
