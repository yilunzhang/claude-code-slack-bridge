#!/usr/bin/env python3
"""bridge daemon:flock 单例;真实依赖装配;单线程事件循环(I2:唯一发送进程)。"""
import fcntl
import os
import pathlib
import signal
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import config as configmod  # noqa: E402
from lib import constants, db, paths, procs, util, version  # noqa: E402
from lib.approval import Approval  # noqa: E402
from lib.clock import SystemClock  # noqa: E402
from lib.daemon_core import (ConsumerManager, DaemonCore, make_status_writer,  # noqa: E402
                             mark_consumers_down, record_daemon_identity,
                             set_startup_state)
from lib.inbound import Inbound  # noqa: E402
from lib.outbound import Outbound  # noqa: E402
from lib.recovery import Recovery  # noqa: E402
from lib.fingerprint import FingerprintGate  # noqa: E402
from lib.runner import LarkRunner  # noqa: E402


def log_line(msg):
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        util.append_log_line(paths.daemon_log_path(), f"{ts} {msg}")
    except OSError:
        pass


def main():
    paths.ensure_data_dir()
    # flock 单例(F7):锁被持有 → 已有 daemon → 静默退出
    lock_fd = os.open(str(paths.lock_path()), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return 0
    try:
        cfg = configmod.require_config()
    except configmod.ConfigError as e:
        log_line(f"refuse start: {e}")
        return 2
    clock = SystemClock()
    conn = db.connect(paths.db_path(), busy_timeout_ms=constants.BUSY_TIMEOUT_DAEMON_MS)
    db.init_schema(conn, paths.schema_path())
    prober = procs.SystemProber()
    # r3-5/r4-1:拿锁+身份落库后**立即**写首次心跳 + startup_state=probing:<gen>(gate 前)。
    # heartbeat 只表"活着";startup_state 才表"就绪"——ensure/daemon_healthy 以后者判就绪,
    # 消除"首心跳被当启动完成"的 bind 假成功窗(daemon 可能因 mismatch 随后退出)。
    # MAJOR 3:发布本 daemon 跑的**代码身份**(pkg_root|version),供 CLI bind 前比对,
    # 检测「plugin 更新/迁移后复用跑旧代码的旧 daemon」。
    generation = record_daemon_identity(
        conn, clock, prober, code_identity=version.code_identity_str())
    runner = LarkRunner(cfg["profile"])
    # 修复项1:指纹/版本门 fail-closed(缺字段≠ok;unknown/版本不符 → 出站停摆+退避重探)
    gate = FingerprintGate(conn, cfg, runner, clock)
    state = gate.startup()
    if state == "mismatch":
        set_startup_state(conn, "refused", generation)  # r4-1:refused 不算就绪
        log_line("refuse start: identity fingerprint mismatch (profile/app_id/owner)")
        db.set_state(conn, "last_error", "fingerprint mismatch — daemon refused to start")
        return 3
    if state == "degraded":
        set_startup_state(conn, "degraded", generation)  # 就绪(出站停摆但 daemon 正常运行)
        log_line(f"degraded start: outbound gated "
                 f"({db.get_state(conn, 'outbound_gate')}); 入站照常入库,带退避重探")
    else:
        set_startup_state(conn, "running", generation)  # r4-1:就绪

    def heartbeat():
        # r2-M1②:多点心跳 —— 含网络条目处理完即 touch,长下载不会被误判挂死
        db.set_state(conn, "last_loop_at", clock.wall_ms())

    inbound = Inbound(conn, cfg, runner, clock, paths.media_root(),
                      heartbeat=heartbeat, log=log_line)
    outbound = Outbound(conn, cfg, runner, clock, heartbeat=heartbeat, log=log_line)
    approval = Approval(conn, cfg, clock, inbound=inbound)
    recovery = Recovery(conn, cfg, runner, clock, inbound, prober)
    core = DaemonCore(conn, cfg, clock, inbound, approval, outbound, recovery,
                      log=log_line, gate=gate)  # r2-M2:gate.tick 在 loop 内先于出站

    on_status = make_status_writer(conn, log_line)  # r2-m2:ready 置位/清除同步 daemon_state
    mgr = ConsumerManager(cfg["profile"], clock,
                          on_line=core.route_line, on_status=on_status)

    stop = {"flag": False}

    def on_signal(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    heartbeat()  # 进入主循环前再刷一次(身份+首心跳已在 gate.startup 之前落库)
    outbound.startup_scan()
    recovery.slow_tick()
    mgr.start_all()
    log_line(f"daemon started pid={os.getpid()} profile={cfg['profile']}")
    try:
        while True:
            mgr.poll(1.0)
            # r7-2:退出检查提到 loop_iteration/刷心跳**之前** —— 收到 SIGTERM 后不再刷新鲜心跳,
            # 让 supervisor 更快看到该 daemon 停摆(缩窗;非根治冷启动竞态)。
            if stop["flag"]:
                break
            core.loop_iteration()  # 内含刷心跳 + gate.tick(先于出站)
            mgr.tick()
    finally:
        # r7-2:安全点标记 stopping(finally 首步,mgr.shutdown 前)—— supervisor 由此可观测到
        # "daemon 正在退出",且 state_ready 天然排除 stopping(缩窗+可观测,**非根治**冷启动竞态)。
        try:
            set_startup_state(conn, "stopping", generation)
        except Exception:
            pass
        log_line("daemon shutting down")
        mgr.shutdown()
        mark_consumers_down(conn, list(mgr.consumers.keys()))  # r3-3:正常退出清 ready
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        conn.close()
        os.close(lock_fd)
    return 0


if __name__ == "__main__":
    sys.exit(main())
