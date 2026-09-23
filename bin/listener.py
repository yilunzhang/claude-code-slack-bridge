#!/usr/bin/env python3
"""listener 进程。两种模式:
- 无参 `python3 bin/listener.py`(插件 monitor 常驻):跟随本 CC 实例(ppid 链定位),等 DB/绑定出现即认领,
  绑定结束后回等待,实例确定死才退出。
- 有参 `python3 bin/listener.py <binding_id>`(手动 Monitor 回退):只跟一个绑定,绑定结束即退出。
职责见 lib/listener_core.py;本文件只做真实依赖装配 + 主循环。"""
import os
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import constants, ctl, db, paths, procs, util  # noqa: E402
from lib.clock import SystemClock  # noqa: E402
from lib.listener_core import InstanceFollower, ListenerCore  # noqa: E402

INSTANCE_LOOKUP_ATTEMPTS = 3  # 启动瞬时 ps 失败不能永久丢 listener(monitor 每 session 只 arm 一次)


def ensure_daemon():
    ctl_py = paths.pkg_root() / "bin" / "bridgectl.py"
    subprocess.Popen(
        [sys.executable, str(ctl_py), "ensure-daemon"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True)


def stdout_printer(s):
    print(s, flush=True)


def _run_loop(step, sleep):
    consecutive_errors = 0
    while True:
        try:
            if step() == "exit":
                break
            consecutive_errors = 0
        except BrokenPipeError:
            break  # Monitor 管道没了,进程无意义
        except Exception:
            consecutive_errors += 1
            if consecutive_errors >= 30:
                break  # 持续异常(如 DB 损坏):退出,由 daemon 判死收口
        sleep(constants.LISTENER_TICK_S)
    return 0


def main(argv=None, *, prober=None, start_pid=None, sleep=time.sleep, printer=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    emit = printer if printer is not None else stdout_printer
    prober = prober if prober is not None else procs.SystemProber()
    me_pid = os.getpid()
    ident = procs.self_identity(prober, me_pid)
    me_start = ident[1] if ident else f"unknown-{me_pid}"
    dbf = paths.db_path()

    def make_core(conn, binding_id):
        return ListenerCore(conn, binding_id, SystemClock(), prober,
                            me_pid=me_pid, me_start=me_start, printer=emit,
                            # 探活 = 锁+心跳新鲜(挂死 daemon 也触发自愈接管)
                            daemon_alive_probe=lambda: ctl.daemon_healthy(conn),
                            ensure_daemon=ensure_daemon)

    if argv:
        if not dbf.exists():
            emit(util.jdumps({"type": "farewell", "code": "no-db"}))
            return 0
        conn = db.connect(dbf, busy_timeout_ms=constants.BUSY_TIMEOUT_LISTENER_MS)
        return _run_loop(make_core(conn, argv[0]).step, sleep)

    # 无参:跟随本 CC 实例
    start = start_pid if start_pid is not None else os.getppid()
    inst = None
    for attempt in range(INSTANCE_LOOKUP_ATTEMPTS):
        inst = procs.find_cc_instance(prober, start)
        if inst is not None:
            break
        if attempt < INSTANCE_LOOKUP_ATTEMPTS - 1:
            sleep(constants.LISTENER_TICK_S)
    if inst is None:
        emit(util.jdumps({"type": "farewell", "code": "no-instance"}))
        return 0
    while not dbf.exists():
        if procs.probe_alive(prober, inst[0], inst[1]) == procs.DEAD:
            return 0  # 等 DB 期间实例确定死(如 CC 崩溃):别留孤儿,静默退出
        sleep(constants.LISTENER_TICK_S)  # 首次使用时 DB 由 bind 建,先于它存在就等
    conn = db.connect(dbf, busy_timeout_ms=constants.BUSY_TIMEOUT_LISTENER_MS)
    follower = InstanceFollower(conn, inst[0], inst[1], prober,
                                lambda binding_id: make_core(conn, binding_id))
    return _run_loop(follower.step, sleep)


if __name__ == "__main__":
    sys.exit(main())
