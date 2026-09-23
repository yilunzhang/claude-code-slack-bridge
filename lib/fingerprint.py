"""修复项1:指纹/版本门 fail-closed(§5 指纹钉死的运行时执行者)。
- verify_identity:缺字段绝不算 ok(fail-closed);确证不符=mismatch;探测失败/缺字段=unknown。
- verify_cli_version:实际 CLI 版本 vs config.cli_version(必填)。
- FingerprintGate:身份 unknown / 版本不符 → daemon_state.outbound_gate=degraded:*
  (出站停摆;入站仍可入库),带退避重探;重探读盘上 config(doctor 重钉后自动放行)。"""
from . import config as configmod
from . import db, runner as runner_mod

PROBE_BACKOFF_START_MS = 30_000
PROBE_BACKOFF_MAX_MS = 10 * 60 * 1000
REVERIFY_INTERVAL_MS = 10 * 60 * 1000  # r2-M2:ok 状态的周期复检间隔

GATE_KEY = "outbound_gate"


def verify_identity(runner, cfg):
    """→ 'ok' | 'mismatch' | 'unknown'。缺字段=unknown(不是 ok)。"""
    res = runner.run(["auth", "status"], timeout_s=20)
    env = runner_mod.parse_envelope(res.stdout)
    if res.rc != 0 or not isinstance(env, dict):
        return "unknown"
    app_id = env.get("appId")
    owner = ((env.get("identities") or {}).get("user") or {}).get("openId")
    if not app_id or not owner:
        return "unknown"  # fail-closed:缺字段不放行
    if app_id != cfg.get("app_id") or owner != cfg.get("owner_open_id"):
        return "mismatch"
    return "ok"


def probe_cli_version(runner):
    # E1:--version 是顶层元命令,不吃全局 --profile(拖尾 → rc=2)→ 裸 argv
    res = runner.run(["--version"], timeout_s=10, no_profile=True)
    if res.rc != 0 or not (res.stdout or "").strip():
        return None
    return res.stdout.strip().splitlines()[0].strip()


def verify_cli_version(runner, cfg):
    actual = probe_cli_version(runner)
    if actual is None:
        return "unknown"
    pinned = cfg.get("cli_version")
    if not pinned:
        return "mismatch"  # cli_version 必填;缺=视为不符(require_config 也会拒)
    return "ok" if actual == pinned else "mismatch"


class FingerprintGate:
    """daemon 内的出站门。startup() → 'ok'|'mismatch'|'degraded';mismatch 由 daemon 拒启。
    degraded 期间 tick() 按退避重探;全 ok → 开门。
    r2-M2:ok 状态也按 REVERIFY_INTERVAL 周期复检(身份/版本漂移 → 下一循环发送前关门;
    daemon 循环里 gate.tick 排在 outbound tick 之前)。"""

    def __init__(self, conn, cfg, runner, clock, notifier=None):
        self.conn = conn
        self.cfg = cfg
        self.runner = runner
        self.clock = clock
        self._backoff = PROBE_BACKOFF_START_MS
        self._next_probe_at = 0
        # v1.5.0:通知走**本机桌面**(不经飞书 —— 出站门降级时飞书正是被堵的那条线)。
        # 延迟导入 + 可注入:测试注 fake;生产缺省用 notifyos。
        self._notifier = notifier
        self._last_notified_state = None   # 只在**跃迁**时通知,不每轮刷屏

    def _current_cfg(self):
        # doctor 重钉写盘;重探必须读盘上最新 config(内存 cfg 只作兜底)
        return configmod.load_config() or self.cfg

    def _evaluate(self):
        cfg = self._current_cfg()
        ident = verify_identity(self.runner, cfg)
        if ident == "mismatch":
            return "mismatch", "identity_mismatch"
        ver = verify_cli_version(self.runner, cfg)
        if ident == "ok" and ver == "ok":
            return "ok", None
        if ident != "ok":
            return "degraded", "identity_unverified"
        reason = "version_mismatch" if ver == "mismatch" else "version_unverified"
        if reason == "version_mismatch":
            # **就地关门,不等返回**(codex impl r1 M1 + 实测):此后调用方还要探版本、
            # 写标记、发自检(合计可达数十秒),期间门若仍是 `ok`,**另一个进程**的直发路径
            # (notify / StopFailure)会读到 ok,在未验证的 CLI 上把消息发出去。
            # 每个 set_state 各自 autocommit → 一判定就落盘,窗口才真正闭合。
            db.set_state(self.conn, GATE_KEY, "degraded:version_selfcheck")
        return "degraded", reason

    # ------------------------------------------------------------------ 通知(旁路)
    def _notify(self, title, message, subtitle=None):
        """**fail-open**:通知是旁路装饰,任何异常都吞掉 —— 绝不能炸掉门逻辑/daemon 循环。"""
        try:
            fn = self._notifier
            if fn is None:
                from . import notifyos
                fn = notifyos.notify
            fn(title, message, subtitle)
        except Exception:   # noqa: BLE001
            pass

    def _notify_transition(self, state, reason, extra=None, force=False):
        """只在**状态跃迁**时发一条(同状态连续多轮不刷屏)。
        `extra` 用于把「已自愈 x→y」「自检失败」并进同一条,**避免一次事故发两条**。

        **`force`= 发生了「事件」而非仅「状态变化」**(codex impl r2 实证的真 bug):
        运行中 CLI 被升级 → 自愈成功 → 状态从 ok 回到 ok,跃迁去重会把通知**整条吞掉** ——
        而那正是最常见的路径,"不能静默"在那里失效。故自愈有结论时无条件发。"""
        key = (state, reason)
        if key == self._last_notified_state and not force:
            return
        self._last_notified_state = key
        if state == "ok":
            msg = extra or "出站已恢复。"
            self._notify("feishu-bridge", msg, "出站恢复")
        else:
            msg = extra or ("出站已停摆(%s)。转发/通知都发不出去,需处理。" % (reason,))
            self._notify("feishu-bridge", msg, "出站停摆")

    # ------------------------------------------------------------------ 自愈
    def _heal_key(self, pinned, actual):
        """按 **pinned→actual** 记(codex impl r1 M2):只按 actual 记的话,
        「从 A 钉漂到 C」与「从 B 钉漂到 C」会被当成同一次尝试。"""
        return "selfheal_attempted:%s->%s" % (pinned, actual)

    def _try_self_heal(self, pinned, actual):
        """版本漂移 → 用**与真实转发同形**的自检验证新 CLI;通过则重钉放行。

        **只对 version_mismatch 自愈**(identity_mismatch = 换了 app/owner,必须停下问人)。

        **「只试一次」要持久化**(codex plan r1 #5):进程内变量在 daemon 重启即失忆,
        崩溃循环下最坏约 360 次/小时刷屏测试群。标记**联网前**写。

        **但瞬态失败必须清标记**(codex impl r1 M2 + 我自查同结论):否则
        「lark-cli 升级时恰好 VPN 断一下」= 标记永久留下 → **该版本永不再自愈** →
        owner 陷入永久停摆,只能人肉 doctor —— **正是本轮要消灭的症状**。
        判据复用出站已在用的信封分类(`type:network` / `error.retryable`),不新建重试框架。
        歧义结果(超时/信封不可解析/ok 但无 message_id)**保留标记**,宁可少试一次。

        返回 `(attempted, healed)` —— **必须两态分开**(codex impl r3 blocker#1):
        只回 False 的话,「这轮真跑了自检但失败」与「标记说跳过、根本没跑」不可区分,
        调用方就会在**每个退避窗**都强制发一条通知(codex 实测:1 次自检 → 4 条通知)。"""
        key = self._heal_key(pinned, actual)
        if db.get_state(self.conn, key):
            return (False, False)             # 没试(本 pinned→actual 试过了),别再发声
        db.set_state(self.conn, key, "1")     # 先落标记再联网(崩溃也不会重试刷屏)
        from . import selfcheck
        ok, retryable = selfcheck.verify_and_repin(
            self.runner, self.clock, self._current_cfg(), actual)
        if not ok and retryable:
            # 瞬态(网络/503/官方 retryable)→ 清标记,让既有退避在下一轮再试一次
            db.set_state(self.conn, key, "")
        return (True, ok)                     # 本轮确实跑了自检

    def _apply(self, state, reason):
        now = self.clock.mono_ms()  # r3-4:复检/退避调度用单调钟(墙钟回拨免疫)
        if state == "ok":
            db.set_state(self.conn, GATE_KEY, "ok")
            self._backoff = PROBE_BACKOFF_START_MS
            self._next_probe_at = now + REVERIFY_INTERVAL_MS  # ok 也定期复检(r2-M2)
        else:
            db.set_state(self.conn, GATE_KEY, f"degraded:{reason}")
            self._next_probe_at = now + self._backoff
            self._backoff = min(self._backoff * 2, PROBE_BACKOFF_MAX_MS)

    def startup(self):
        """**只检测,绝不联网自愈**(codex plan r1 #4):startup 跑在 daemon 就绪之前
        (bin/daemon.py),supervisor 约 52s 就会判失败;自愈那串网络动作会把它拖到 130s。
        → 照常置 degraded、让 daemon 正常就绪,首个 tick() 再自愈。"""
        state, reason = self._evaluate()
        if state == "mismatch":
            db.set_state(self.conn, GATE_KEY, "degraded:identity_mismatch")
            self._notify_transition("degraded", "identity_mismatch")
            return "mismatch"
        self._apply(state, reason)
        # **健康启动不通知**(codex impl r1 M3):否则每次 daemon 重启都弹一条"出站恢复",
        # 而根本没发生过恢复 = 噪音。也**不在此处**通知 version_mismatch —— 那条留给紧接着
        # 的首个 tick() 自愈,由它发**一条**合并结论(停摆/已自愈),避免"先停摆后自愈"双发。
        if state != "ok" and reason != "version_mismatch":
            self._notify_transition(state, reason)
        elif state == "ok":
            self._last_notified_state = ("ok", None)   # 记状态但不发声
        return state

    def tick(self):
        now = self.clock.mono_ms()  # r3-4:单调钟
        if now < self._next_probe_at:
            return
        state, reason = self._evaluate()
        if state == "mismatch":
            # 运行期发现确证不符:关死门(比 degraded 更硬的语义留给 daemon 重启决断)
            # **绝不自愈** —— 换了 app/owner 是"谁在发"变了,必须停下问人。
            db.set_state(self.conn, GATE_KEY, "degraded:identity_mismatch")
            self._next_probe_at = now + self._backoff
            self._backoff = min(self._backoff * 2, PROBE_BACKOFF_MAX_MS)
            self._notify_transition("degraded", "identity_mismatch")
            return
        # v1.5.0 自愈:**仅** version_mismatch。同步跑是安全的 —— loop_iteration 先写心跳
        # (daemon_core.py)再 gate.tick(),砍掉 recall 后只剩一次有界 send,远低于 180s 接管阈值。
        extra = None
        if reason == "version_mismatch":
            # 关门已由 `_evaluate()` **就地**完成(判定的那一刻,早于此处的二次探测与写标记)
            # —— 这里不再重复置位:两处都写会让"关门时机"的变异测不出来(实测踩过)。
            old = (self._current_cfg() or {}).get("cli_version")
            attempted, healed, actual = False, False, None
            try:
                actual = probe_cli_version(self.runner)
                if actual:
                    attempted, healed = self._try_self_heal(old, actual)
            except Exception:      # noqa: BLE001 —— 自愈失败绝不能炸掉 daemon 循环
                attempted, healed = False, False
            if not attempted:
                # 本轮没真跑自检(标记说跳过 / 探版本失败)→ **不制造 extra、不强制发声**,
                # 否则每个退避窗都会重发同一条(codex impl r3 实测:1 次自检 → 4 条通知)。
                pass
            elif healed:
                state, reason = self._evaluate()          # 重钉后重新评估
                if state == "ok":
                    extra = "已自愈:lark-cli %s → %s,出站恢复。" % (old, actual)
                else:
                    # 重钉了但仍不 ok(如身份同时也出问题)——**别说"恢复"**(codex M3)
                    extra = ("出站仍停摆:已重钉 lark-cli %s,但门未开(%s)。"
                             % (actual, reason))
            else:
                extra = ("出站已停摆:lark-cli 版本漂移(%s → %s),自检未通过 —— "
                         "可能是真回归,需人工处理。" % (old, actual or "?"))
        self._apply(state, reason)
        # extra 非空 = 本轮真发生了自愈事件(成功/失败)→ 强制发声,不被跃迁去重吞掉
        self._notify_transition(state, reason, extra=extra, force=extra is not None)
