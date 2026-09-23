"""版本漂移自愈 + 停摆必通知(v1.5.0)。

**真实事故(2026-07-30)**:lark-cli 被升级 1.0.66→1.0.80(非 owner 主动)→ 指纹门判
`version_mismatch` → **全部出站静默停摆**(入站不过此门,故"发进来的能到、转发出不去"),
积压 27 条。owner 靠"怎么没转发了"才发现;恢复要人肉 `bridgectl doctor --chat-id <群>`。
**notify/StopFailure 告警也走同一个门** = 报警器与被报警物共用一条线。

本模块钉住两件事:① 版本漂移**自动自检重钉**(失败才停摆);② **任何停摆/恢复都通知**
(走 osascript,不依赖飞书那条被堵的线)。

**🔑 自检 oracle 必须与真实转发同形**:`session_turn` 用 `--markdown`(outbound.py),
旧 doctor 自检却用 `--text` —— 2026-07-30 就是拿 `--text` 的通过放行了 `--markdown`
(而 v1.4.1 修的正是 markdown 渲染)。测了零件没测装配。
"""
import json

import pytest

from tests.conftest import PROFILE
from tests.helpers import FakeRunResult, FakeRunner, ok_envelope
from lib import config as configmod
from lib import constants, db as dbmod, fingerprint


def auth_resp(app_id="cli_testapp", owner="ou_owner"):
    return FakeRunResult(0, json.dumps(
        {"appId": app_id, "identities": {"user": {"available": True, "openId": owner}}}))


def _runner(*, version="9.9.9\n", auth=None, send=None):
    """默认:身份 ok、版本漂移(9.9.9 ≠ config 的 1.0.66)。"""
    r = FakeRunner(profile=PROFILE)
    r.on_prefix(["auth", "status"], lambda a, c: auth or auth_resp())
    r.on_prefix(["--version"], lambda a, c: FakeRunResult(0, version))
    r.on_prefix(["im", "+messages-send"],
                send or (lambda a, c: ok_envelope({"message_id": "om_selfcheck"})))
    return r


class _Notes:
    """收集通知调用。"""
    def __init__(self, raising=False):
        self.calls = []
        self.raising = raising

    def __call__(self, title, message, subtitle=None):
        if self.raising:
            raise RuntimeError("notification backend exploded")
        self.calls.append((title, message, subtitle))
        return True

    @property
    def texts(self):
        return [" ".join(str(x) for x in c if x) for c in self.calls]


def _gate(env, runner, notes=None):
    return fingerprint.FingerprintGate(env.conn, env.cfg, runner, env.clock,
                                       notifier=notes or _Notes())


# --------------------------------------------------------------- 自愈:oracle 与放行
class TestSelfHeal:
    def test_selfcheck_uses_markdown_like_real_forwarding(self, env):
        """**自检必须与 session_turn 同形**(codex plan r1 #1,今天真踩过)。

        旧 doctor 用 `--text`,真实转发用 `--markdown` —— 拿前者的通过放行后者 =
        测了零件没测装配。本条钉死自检命令的形状,且必须校验 message_id 契约。"""
        env.make_binding(status="active")   # 存在工作群绑定:自检也绝不能发那儿去
        r = _runner()
        g = _gate(env, r)
        assert g.startup() == "degraded"
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()
        sends = r.calls_matching("im", "+messages-send")
        assert len(sends) == 1, "应恰好自检发一条"
        argv = sends[0][0]
        assert "--markdown" in argv and "--text" not in argv
        assert argv[argv.index("--chat-id") + 1] == constants.SELFCHECK_CHAT_ID
        assert dbmod.get_state(env.conn, "outbound_gate") == "ok"   # 通过 → 开门
        assert configmod.load_config()["cli_version"] == "9.9.9"    # 已重钉

    def test_selfcheck_failure_keeps_gate_shut_and_pin_unchanged(self, env):
        """自检失败(真回归)→ 门保持 degraded,**cli_version 绝不被改**,且**恰一条**通知。

        **断言通知内容而非只数条数**(codex impl r1 Low:只数 1 条的话,startup 那条泛泛的
        「出站停摆」也算数 —— 而我们要的是**合并了自检结论**的那条 = 假绿)。"""
        r = _runner(send=lambda a, c: FakeRunResult(1, json.dumps({"ok": False})))
        notes = _Notes()
        g = _gate(env, r, notes)
        g.startup()
        assert notes.calls == [], "版本漂移不该在 startup 就发声(留给 tick 发合并结论)"
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()
        assert dbmod.get_state(env.conn, "outbound_gate").startswith("degraded")
        assert configmod.load_config()["cli_version"] == "1.0.66"   # 未重钉
        assert len(notes.calls) == 1                                 # 合并成一条,不双发
        assert "自检未通过" in notes.texts[0], "必须是合并了自检结论那条,不是泛泛的停摆"
        # **跨多个退避窗后仍只有那一条**(codex impl r3 blocker#1 实测:强制发声若不区分
        # "真跑了自检" vs "标记说跳过",会变成 1 次自检 → 每窗一条通知)。
        for _ in range(3):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g.tick()
        assert len(r.calls_matching("im", "+messages-send")) == 1, "只该自检一次"
        assert len(notes.calls) == 1, "没真跑自检的轮次不得重复发声"

    def test_transient_failure_retries_next_round(self, env):
        """**瞬态失败必须能再试**(codex impl r1 M2 + 自查):否则「lark-cli 升级时恰好
        VPN 断一下」= 标记永久留下 → 该版本**永不再自愈** → 永久停摆,只能人肉 doctor,
        **正是本轮要消灭的症状**。网络类失败 → 清标记 → 下一轮退避再试一次。"""
        calls = {"n": 0}

        def flaky(a, c):
            calls["n"] += 1
            if calls["n"] == 1:
                # **只给 type=network、不给 retryable**(codex impl r2:两者都给的话,
                # 任一分支坏掉这条都还是绿的 = 分不清是哪条在起作用)。
                return FakeRunResult(4, "", json.dumps(
                    {"ok": False, "error": {"type": "network", "code": 503}}))
            return ok_envelope({"message_id": "om_ok"})

        g = _gate(env, _runner(send=flaky))
        g.startup()
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()                                     # 第一次:瞬态失败
        assert dbmod.get_state(env.conn, "outbound_gate").startswith("degraded")
        env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
        g.tick()                                     # 第二次:应该再试并成功
        assert calls["n"] == 2, "瞬态失败后没有重试 → 陷入永久停摆"
        assert dbmod.get_state(env.conn, "outbound_gate") == "ok"

    def test_explicit_retryable_true_without_network_type(self, env):
        """**显式 `retryable:true` 单独也要生效**(codex impl r3 blocker#2):
        一个"只认 timeout 与显式 false、其余非 network 一律不重试"的变异能过 14/14 —— 因为
        没有一条用例给出**非超时、非 network、显式 true** 的组合。本条补上那个孤立信号。

        **码必须选 `RETRYABLE_FALLBACK_CODES` 之外的**(codex impl r4 blocker):我第一版用了
        `230020`,而它**本身就在**那张回退表里 → "忽略显式 true、只按数字回退重试"的变异照样
        绿 = 又一次把两个信号耦合在一条 fixture 里。`99991400` 只可能由显式字段驱动。"""
        calls = {"n": 0}

        def retryable_api_err(a, c):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeRunResult(1, "", json.dumps(
                    {"ok": False, "error": {"type": "api", "code": 99991400,
                                            "retryable": True}}))   # 非 network、非回退表
            return ok_envelope({"message_id": "om_ok"})

        g = _gate(env, _runner(send=retryable_api_err))
        g.startup()
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()
        env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
        g.tick()
        assert calls["n"] == 2, "显式 retryable:true 未被采信 → 该重试的没重试"
        assert dbmod.get_state(env.conn, "outbound_gate") == "ok"

    def test_explicit_retryable_false_wins_over_network_type(self, env):
        """**显式 `retryable:false` 优先于 `type:network`**(codex impl r2):传输层出错但
        服务端明确说别重试 —— 必须尊重 false,不能因为 type 是 network 就翻成可重试。"""
        calls = {"n": 0}

        def nonretryable_network(a, c):
            calls["n"] += 1
            return FakeRunResult(4, "", json.dumps(
                {"ok": False, "error": {"type": "network", "code": 400,
                                        "retryable": False}}))

        g = _gate(env, _runner(send=nonretryable_network))
        g.startup()
        for _ in range(3):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g.tick()
        assert calls["n"] == 1, "显式 retryable:false 被 network 覆盖了 → 会无谓重试"

    def test_timeout_is_ambiguous_even_with_retryable_envelope(self, env):
        """**超时一律歧义**(codex impl r2):超时可能是"已发出但没等到回执",重试会重复
        打扰测试群;即便响应里恰好带 `retryable:true` 也不采信。"""
        calls = {"n": 0}

        def timed_out(a, c):
            calls["n"] += 1
            r = FakeRunResult(1, "", json.dumps(
                {"ok": False, "error": {"type": "network", "retryable": True}}))
            r.timed_out = True
            return r

        g = _gate(env, _runner(send=timed_out))
        g.startup()
        for _ in range(3):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g.tick()
        assert calls["n"] == 1, "超时被当成可重试 → 可能重复发送"

    def test_ok_without_message_id_is_ambiguous_failure(self, env):
        """`ok:true` 但无 message_id **既不算通过、也不重试**(codex impl r3 blocker#3:
        原先拆成两条,各自都可被"把它当成功"这一个变异蒙混过关 —— 合并后一次钉死三件事)。"""
        calls = {"n": 0}

        def amb(a, c):
            calls["n"] += 1
            return ok_envelope({})                   # ok 但无 message_id

        g = _gate(env, _runner(send=amb))
        g.startup()
        for _ in range(3):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g.tick()
        assert calls["n"] == 1, "歧义失败不该重试"
        assert dbmod.get_state(env.conn, "outbound_gate").startswith("degraded"), "不得当成功放行"
        assert configmod.load_config()["cli_version"] == "1.0.66", "不得重钉"

    @pytest.mark.parametrize("version", ["1.0.66\n", "9.9.9\n"])
    def test_identity_mismatch_never_self_heals(self, env, version):
        """**换了 app/owner 绝不自愈** —— 那是"谁在发"变了,必须停下问人。
        走真实 startup/tick 入口,断言**零** send 调用。

        **必须同时覆盖「身份与版本都不符」**(codex impl r5):只测"身份错、版本对"的话,
        一个**先看版本**的实现会在两者都错时走进自愈分支 —— 拿着一个**身份存疑**的 CLI
        往外发消息。这是本功能唯一的硬安全性质,不能只测一半。"""
        r = _runner(auth=auth_resp(app_id="cli_evil"), version=version)
        g = _gate(env, r)
        g.startup()
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()
        assert r.calls_matching("im", "+messages-send") == [], "身份不符时绝不能发任何消息"
        assert dbmod.get_state(env.conn, "outbound_gate").startswith("degraded:identity")

    def test_attempt_persisted_across_gate_rebuild(self, env):
        """**「每版本只自愈一次」必须持久化**(codex #5):进程内变量在 daemon 重启即失忆,
        崩溃循环下最坏约 360 次/小时往测试群发消息。

        构造要点(codex 指定):doctor 必须**失败** + 跨多个退避窗 + **新建第二个 Gate 实例**
        —— 若 doctor 成功,该变异不可见。"""
        fail = lambda a, c: FakeRunResult(1, json.dumps({"ok": False}))  # noqa: E731
        r1 = _runner(send=fail)
        g1 = _gate(env, r1)
        g1.startup()
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g1.tick()
        assert len(r1.calls_matching("im", "+messages-send")) == 1
        # daemon 重启:全新 Gate 实例(进程内记忆已丢),同一版本不得再试
        r2 = _runner(send=fail)
        g2 = _gate(env, r2)
        g2.startup()
        for _ in range(3):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g2.tick()
        assert r2.calls_matching("im", "+messages-send") == [], "持久化抑制失效 → 会刷屏测试群"

    def test_gate_is_already_shut_during_selfcheck(self, env):
        """自愈**期间**门必须已是非 ok(codex #4):否则 notify/StopFailure 这类**直发**路径
        会读到旧的 `ok`,在**未验证的 CLI** 上把消息发出去。

        **必须从「门本来是 ok」的状态漂移过来**才有判别力 —— 若从 startup 的
        `degraded:version_mismatch` 起步,门本来就非 ok,断言对两种实现都成立 = 假绿
        (我第一版就栽在这)。故:先让它 ok,再让版本漂移,走周期复检那条路。"""
        seen = {}
        ver = {"v": "1.0.66\n"}

        def spy_send(a, c):
            seen["gate_during"] = dbmod.get_state(env.conn, "outbound_gate")
            return ok_envelope({"message_id": "om_x"})

        # **用第二条连接采样**(codex impl r1 Low):模拟 notify/StopFailure 那个**独立进程**
        # 看到的东西 —— 每个 set_state 各自 autocommit,同连接读不出跨进程可见性问题。
        # 采样点放在**探版本时**(关门之后、send 之前),这样能抓到"关门太晚"那 ≤10s 窗口。
        import sqlite3
        from lib import paths
        other = sqlite3.connect(str(paths.db_path()))
        other.row_factory = sqlite3.Row

        probes = []

        def probe_version(a, c):
            # 记下**每一次**探版本时、另一个连接(= 直发进程视角)看到的门状态。
            # 实测序列:auth→ok, version→ok(尚未判定,物理必然), version→已关门, send→已关门。
            if ver["v"] != "1.0.66\n":
                probes.append(dbmod.get_state(other, "outbound_gate"))
            return FakeRunResult(0, ver["v"])

        r = FakeRunner(profile=PROFILE)
        r.on_prefix(["auth", "status"], lambda a, c: auth_resp())
        r.on_prefix(["--version"], probe_version)
        r.on_prefix(["im", "+messages-send"], spy_send)
        g = _gate(env, r)
        assert g.startup() == "ok"
        assert dbmod.get_state(env.conn, "outbound_gate") == "ok"   # 门本来是开的
        ver["v"] = "9.9.9\n"                                        # CLI 被升级
        env.clock.tick(fingerprint.REVERIFY_INTERVAL_MS + 1)        # 周期复检
        g.tick()
        assert seen.get("gate_during") is not None, "自检没跑?"
        assert seen["gate_during"] != "ok", "自检期间门仍为 ok → 直发路径会用未验证 CLI 发出去"
        # **判定漂移之后的每一刻**,直发路径都不得再看到 ok。
        # 首次探测(尚未判定)为 ok 是物理必然;其后的每次探测都必须已关门 ——
        # 这段是**可消除**的窗口,若实现拖到 send 时才关门,这里就会红。
        assert len(probes) >= 2, "版本漂移路径应至少探两次版本"
        assert all(p != "ok" for p in probes[1:]), \
            "关门太晚:判定漂移后直发路径仍见 ok(观测序列 %r)" % (probes,)
        other.close()


# --------------------------------------------------------------- 通知
class TestNotify:
    def test_stall_notifies_once_not_every_tick(self, env):
        """停摆要通知,但**只在跃迁时**发一条 —— 后续 tick 不刷屏。

        构造要点(codex #7):必须**推进过每个退避 deadline**;紧邻两次 tick 不触发评估,
        那样即使实现每轮都通知也会假绿。"""
        notes = _Notes()
        # 身份不可验证(非版本漂移)→ 不可自愈的降级
        g = _gate(env, _runner(auth=FakeRunResult(1, ""), version="1.0.66\n"), notes)
        g.startup()
        assert len(notes.calls) == 1
        assert "停摆" in notes.texts[0] or "stall" in notes.texts[0].lower()
        for _ in range(3):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g.tick()
        assert len(notes.calls) == 1, "同状态重复通知 = 刷屏"

    def test_self_heal_notifies_exactly_once(self, env):
        """自愈成功 → **恰好一条**「已自愈 x→y」,不是"先停摆再恢复"两条。

        **别 join 全部文本再断言**(codex impl r1 Low:join 后两条也能过 = 假绿),
        要断言**条数**与那唯一一条的内容。

        **必须走「健康启动 → 运行中漂移」这条真实路径**(codex impl r2 实证的真 bug):
        从 degraded 起步测是假绿 —— 健康启动记了 `("ok", None)`,自愈成功又回到同一个 key,
        跃迁去重会把通知**整条吞掉**,而那正是最常见的场景("不能静默"在那儿失效)。"""
        notes = _Notes()
        ver = {"v": "1.0.66\n"}
        r = FakeRunner(profile=PROFILE)
        r.on_prefix(["auth", "status"], lambda a, c: auth_resp())
        r.on_prefix(["--version"], lambda a, c: FakeRunResult(0, ver["v"]))
        r.on_prefix(["im", "+messages-send"],
                    lambda a, c: ok_envelope({"message_id": "om_ok"}))
        g = _gate(env, r, notes)
        assert g.startup() == "ok"        # 健康启动(静默)
        assert notes.calls == []
        ver["v"] = "9.9.9\n"              # 运行中 CLI 被升级 —— 真实场景
        env.clock.tick(fingerprint.REVERIFY_INTERVAL_MS + 1)
        g.tick()                          # 自愈成功
        assert dbmod.get_state(env.conn, "outbound_gate") == "ok"
        assert len(notes.calls) == 1, "自愈成功却没通知(或发了多条)"
        assert "已自愈" in notes.texts[0]
        assert "1.0.66" in notes.texts[0] and "9.9.9" in notes.texts[0], "应含新旧版本"

    def test_notifier_exception_does_not_break_loop(self, env):
        """通知是**旁路装饰**:后端抛异常绝不能炸掉门逻辑 / daemon 循环。
        (codex #7:要跑真实 loop_iteration 两次,不能只测 helper。)"""
        from lib.daemon_core import DaemonCore
        g = fingerprint.FingerprintGate(env.conn, env.cfg, _runner(), env.clock,
                                        notifier=_Notes(raising=True))
        core = DaemonCore(env.conn, env.cfg, env.clock, env.inbound, env.approval,
                          env.outbound, env.recovery, gate=g)
        core.loop_iteration()                                   # 首轮:startup 未跑,tick 评估
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        core.loop_iteration()         # 第二轮仍不抛 = 通过(通知异常没炸掉循环)
        assert dbmod.get_state(env.conn, "outbound_gate") is not None
