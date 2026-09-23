#!/usr/bin/env python3
"""bridge daemon:flock 单例;真实依赖装配;单线程事件循环(I2:唯一发送进程)。

装配(contracts §7 / §8 / §9):
- `ConfigSnapshot` 唯一共享对象,每循环 `refresh()` 原地更新;
- **唯一** `SlackClient(tokens_path=…, cooldown_store=DaemonStateCooldownStore(conn))`,gate 与出站共用
  (`_prepare` 校验的凭据快照 = `_transmit` 实际使用的快照);
- `FingerprintGate(conn, cfg, client, clock, notifier=…)`;
- consumer argv `[cfg.consumer_python or sys.executable, <root>/bin/slack_consumer.py]`;
  xapp(app_token)变化的**唯一**探测点是 `FingerprintGate.tick()`(它已经每 tick stat tokens.json),
  主循环在 `core.loop_iteration()`(内含 gate.tick)之后读一次性信号 `gate.app_token_changed()`
  → `mgr.restart(SOCKET_KEY, "app_token_changed")`(SIGTERM consumer,ConsumerManager 立即重拉,
  consumer 自读文件)。bot_token 变化只影响 gate / SlackClient,不重拉 consumer。"""
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
from lib.fingerprint import FingerprintGate  # noqa: E402
from lib.inbound import Inbound  # noqa: E402
from lib.outbound import Outbound  # noqa: E402
from lib.recovery import Recovery  # noqa: E402
from lib.slackapi import DaemonStateCooldownStore, SlackClient  # noqa: E402


_SECRETS = []   # 启动读到 tokens 后填入;daemon.log 每一行都过 util.redact_secrets(R1-M5)


def log_line(msg):
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        util.append_log_line(paths.daemon_log_path(),
                             "%s %s" % (ts, util.redact_secrets(msg, _SECRETS)))
    except OSError:
        pass


def restart_consumer_if_app_token_changed(gate, mgr, log=None):
    """主循环每轮一次(在 gate.tick 之后):gate 的一次性信号为 True → 主动重拉 consumer。→ 是否重拉。
    app_token 变化的探测只有这一处来源(FingerprintGate._check_tokens_file),不另开文件监视。"""
    if not gate.app_token_changed():
        return False
    (log or log_line)("app_token changed → restarting consumer")
    mgr.restart(constants.SOCKET_KEY, "app_token_changed")
    return True


def refresh_secrets_if_rotated(gate, seen, secrets=None, log=None):
    """主循环每轮一次:gate 看到的 tokens 版本变了 → 重读 tokens.json 刷新遮蔽表(secrets 列表**原地**更新,
    status writer / ConsumerManager / log_line 的 secrets_provider 都持同一引用)。读失败 → 保留旧表
    (形态兜底不依赖此表)。`seen` = {"version": …} 可变记录。→ 是否刷新。"""
    ver = getattr(gate, "tokens_version", None)
    if ver is None or ver == seen.get("version"):
        return False
    seen["version"] = ver
    target = _SECRETS if secrets is None else secrets
    try:
        tokens, _v = configmod.load_tokens(paths.tokens_path(), allow_env=False)
    except configmod.ConfigError:
        return False
    fresh = [v for v in tokens.values() if isinstance(v, str) and v]
    target[:] = list(dict.fromkeys(target + fresh))   # 旧 token 也留着:轮换后旧值仍可能出现在异常文本里
    (log or log_line)("secrets table refreshed for tokens_version=%s" % ver)
    return True


def build_consumer_argv(cfg, root):
    return [cfg.get("consumer_python") or sys.executable, str(root / "bin" / "slack_consumer.py")]


def main():
    paths.ensure_data_dir()
    # flock 单例:锁被持有 → 已有 daemon → 静默退出
    lock_fd = os.open(str(paths.lock_path()), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return 0
    try:
        cfg = configmod.ConfigSnapshot.load()
    except configmod.ConfigError as e:
        log_line("refuse start: %s" % e)
        return 2
    clock = SystemClock()
    conn = db.connect(paths.db_path(), busy_timeout_ms=constants.BUSY_TIMEOUT_DAEMON_MS)
    db.init_schema(conn, paths.schema_path())
    prober = procs.SystemProber()
    # 拿锁+身份落库后**立即**写首次心跳 + startup_state=probing:<gen>(gate 前)。
    generation = record_daemon_identity(
        conn, clock, prober, code_identity=version.code_identity_str())
    try:
        # 唯一 SlackClient:文件是真相(allow_env=False);冷却存储 = daemon_state(与 notify/probe 共用)
        client = SlackClient(tokens_path=paths.tokens_path(),
                             cooldown_store=DaemonStateCooldownStore(conn), clock=clock)
    except configmod.ConfigError as e:
        set_startup_state(conn, "refused", generation)
        log_line("refuse start: tokens: %s" % e)
        db.set_state(conn, "last_error", "tokens.json unusable — daemon refused to start")
        return 2
    try:   # 遮蔽表:bot/app token(daemon.log 每行 redact;失败不影响启动,形态兜底仍在)
        _SECRETS[:] = [v for v in configmod.load_tokens(paths.tokens_path(), allow_env=False)[0].values()
                       if isinstance(v, str) and v]
    except configmod.ConfigError:
        pass
    # 指纹/凭据门 fail-closed(缺字段≠ok;unknown/版本不符 → 出站停摆+退避重探)
    gate = FingerprintGate(conn, cfg, client, clock, notifier=None)
    state = gate.startup()
    if state == "mismatch":
        set_startup_state(conn, "refused", generation)  # refused 不算就绪
        log_line("refuse start: identity fingerprint mismatch (team/bot/app)")
        db.set_state(conn, "last_error", "fingerprint mismatch — daemon refused to start")
        return 3
    if state == "degraded":
        set_startup_state(conn, "degraded", generation)  # 就绪(出站停摆但 daemon 正常运行)
        log_line("degraded start: outbound gated (%s); 入站照常入库,带退避重探"
                 % db.get_state(conn, constants.GATE_KEY))
    else:
        set_startup_state(conn, "running", generation)  # 就绪

    def heartbeat():
        # 多点心跳:含网络条目处理完即 touch,长下载不会被误判挂死
        db.set_state(conn, "last_loop_at", clock.wall_ms())

    inbound = Inbound(conn, cfg, client, clock, paths.media_root(),
                      heartbeat=heartbeat, log=log_line)
    outbound = Outbound(conn, cfg, client, clock, heartbeat=heartbeat, log=log_line)
    approval = Approval(conn, cfg, clock, inbound=inbound)
    recovery = Recovery(conn, cfg, client, clock, inbound, prober)
    core = DaemonCore(conn, cfg, clock, inbound, approval, outbound, recovery,
                      log=log_line, gate=gate)  # gate.tick 在 loop 内先于出站

    root = paths.pkg_root()
    secrets_provider = lambda: _SECRETS  # noqa: E731 —— 状态入库 / stderr 行遮蔽的显式 token 表(R2-M5)
    on_status = make_status_writer(conn, log_line, secrets_provider=secrets_provider)  # ready 置位/清除同步 daemon_state
    mgr = ConsumerManager(clock, on_line=core.on_consumer_line, on_status=on_status,
                          argv_builder=lambda key: build_consumer_argv(cfg, root),
                          secrets_provider=secrets_provider)
    secrets_seen = {"version": gate.tokens_version}

    stop = {"flag": False}

    def on_signal(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    heartbeat()  # 进入主循环前再刷一次(身份+首心跳已在 gate.startup 之前落库)
    outbound.startup_scan()
    recovery.slow_tick()
    mgr.start_all()
    log_line("daemon started pid=%d team=%s tokens_version=%s"
             % (os.getpid(), cfg.get("team_id"), client.tokens_version))
    try:
        while True:
            mgr.poll(1.0)
            # 退出检查提到 loop_iteration/刷心跳**之前**:收到 SIGTERM 后不再刷新鲜心跳
            if stop["flag"]:
                break
            cfg.refresh()  # 配置原地更新(所有组件持同一引用)
            core.loop_iteration()  # 内含刷心跳 + drain + followup + gate.tick(先于出站)
            restart_consumer_if_app_token_changed(gate, mgr)  # gate.tick 之后读一次性信号
            refresh_secrets_if_rotated(gate, secrets_seen)     # 凭据轮换 → 遮蔽表跟着换
            mgr.tick()
    finally:
        # 安全点标记 stopping(finally 首步,mgr.shutdown 前)—— supervisor 由此可观测到"正在退出"
        try:
            set_startup_state(conn, "stopping", generation)
        except Exception:  # noqa: BLE001
            pass
        log_line("daemon shutting down")
        mgr.shutdown()
        mark_consumers_down(conn, list(mgr.consumers.keys()))  # 正常退出清 ready
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:  # noqa: BLE001
            pass
        conn.close()
        os.close(lock_fd)
    return 0


if __name__ == "__main__":
    sys.exit(main())
