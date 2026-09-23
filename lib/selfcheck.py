"""CLI 版本漂移自检 —— **与真实转发同形**的单次探针,通过则重钉 `cli_version`。

**为什么不复用 `ctl.doctor`**(2026-07-30 codex plan r1 #1,已实测):
doctor 自检用 `--text`(ctl.py),而真实转发 `session_turn` 用 `--markdown`(outbound.py)
—— **两条不同代码路径**。当天就是拿 `--text` 的通过放行了 `--markdown`,而 v1.4.1 修的
恰恰是 markdown 渲染 = 自检从没覆盖过的那条路。**测了零件没测装配。**

**判据与出站完全一致**:`ok` 信封 ∧ `data.message_id` 非空(缺字段 = 非成功)。

**不做 recall**(codex CUT):撤回证明不了正常出站,反而制造两种误判 —— DELETE 坏了会误停
全部出站,而 recall 好了不代表 `--markdown` 好。测试消息留在靶子群即可。

**🔑 已知取舍(codex plan r2 点名)—— false-green admission**:本探针只覆盖**主路径**
(session_turn)。卡片 / reaction / post 可能独立回归而本检查照样通过 → 会重钉一个"部分兼容"
的 CLI。这是用「自动恢复」换掉的安全性质:旧行为从不信任未钉版本的 CLI。接受理由 = 主路径
占绝大多数出站,其余路径各有失败处理;而停摆的代价(静默丢消息 + 人肉 doctor)已被 2026-07-30
的真实事故证明更痛。
"""
from . import config as configmod
from . import constants, runner as runner_mod, util

SEND_TIMEOUT_S = 30


def verify_and_repin(runner, clock, cfg, actual):
    """→ `(ok, retryable)`。`ok=True` = 通过自检且已重钉(调用方应重新评估门)。

    `retryable=True` 仅在**瞬态**失败时(网络/5xx/官方 `error.retryable`)—— 调用方据此
    清掉"已尝试"标记,让退避在下一轮再试。**歧义结果保留标记**(超时/信封不可解析/
    `ok` 但无 message_id):宁可少试一次,也不要在真回归时反复刷屏测试群。

    `actual` 由调用方传入(它刚探过)—— **不在这里重探**(codex impl r1 Low:重探既不能让
    可执行文件替换变原子,又多一次 10s 往返)。

    重钉**读盘上最新 config**:不拿调用方的旧副本 copy-save,免得覆盖并发写入的 allowlist。
    """
    # 与 outbound.py::_transmit 的 session_turn 分支同形:--markdown + 幂等键
    res = runner.run(
        ["im", "+messages-send", "--as", "bot",
         "--chat-id", constants.SELFCHECK_CHAT_ID,
         "--markdown", "feishu-bridge 版本自检 %s(CLI %s)" % (clock.wall_ms(), actual),
         "--idempotency-key", util.short_key(util.new_id())],
        timeout_s=SEND_TIMEOUT_S)
    env = runner_mod.parse_result(res)          # E4a:stdout 空则回退 stderr
    if not runner_mod.envelope_ok(env):
        return (False, _is_transient(env, res))
    if not runner_mod.data_of(env).get("message_id"):
        return (False, False)                   # ok 但无 id ≠ 成功;歧义 → 不重试
    disk = configmod.load_config()
    if disk is None:
        return (False, False)                   # config 不见了:别用旧副本把它复活
    disk["cli_version"] = actual
    configmod.save_config(disk)
    return (True, False)


def _is_transient(env, res):
    """判定序(codex impl r2:**顺序本身就是契约**,别调换):

      ① **超时 → 一律歧义**(不重试)。超时可能是"已发出但没等到回执",重试会重复打扰
         测试群;即便响应里恰好带着 `retryable:true` 也不采信(那是上一次的残留语义)。
      ② **官方 `error.retryable` 显式给了值 → 一律听它的**(true 重试 / **false 不重试**)。
         `network + retryable:false` 是"传输层出错但服务端明确说别重试" → 必须尊重 false,
         不能因为 type 是 network 就翻成可重试。
      ③ 都没有 → 回退看 `type == "network"`(5xx/传输类,通常瞬态)。
    """
    if getattr(res, "timed_out", False):
        return False                                   # ① 超时 = 歧义
    explicit = runner_mod.envelope_error_retryable(env)
    if explicit is not None:
        return bool(explicit)                          # ② 显式值优先,含显式 false
    return runner_mod.envelope_error_type(env) == "network"   # ③ 回退
