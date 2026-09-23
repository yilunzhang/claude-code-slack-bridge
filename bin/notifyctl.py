#!/usr/bin/env python3
"""notifyctl CLI(薄壳):agent 主动给「本 session 绑定的飞书群」发通知,系统自动前缀 @owner
(独立 `at` 节点,穿透免打扰),凸显"需 owner 决策/授权的 blocker"。

**核心发送逻辑已抽到 `lib/notify.py`**(`run_notify` 及硬化路径),供本 CLI 与 StopFailure hook
(`lib.hooklib.run_stop_failure`)共用同一路径、零逻辑复制。本文件只留 CLI I/O(读 stdin / 写 JSON)。

**消息经 stdin**:SKILL.md 指示 agent 用 Write 把正文写临时文件 → `notifyctl.py < 文件`
(避 shell 展开 + heredoc delimiter 碰撞);notifyctl 读全部 stdin。

输出:结构化 JSON 到 stdout(`{ok,sent,reason,...}`),**全程绝不裸 traceback**;`sent` 恒为
主信号(true / false / "unknown")。退出码:0=已通知或前置条件(空/未绑定);3=发送前拒绝
(确定未发);4=发送失败/被拒(确定未发,sent:false);5=不确定(sent:"unknown",看群别乱重试)。
"""
import json
import os
import pathlib
import sys

# 引导:CLI 从任意项目 cwd 调用 → 必须把 plugin 根加进 sys.path,否则 `from lib ...` 报
# ModuleNotFoundError(测试因 conftest 已插根抓不到此漏,但装好的 plugin 会真崩)。别删。
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import config as configmod  # noqa: E402  (测试 monkeypatch notifyctl.configmod.require_config)
from lib import procs, runner as runner_mod  # noqa: E402  (main() 注入 prober/make_runner 用)
# 再导出发送核心:测试经 importlib 直接引用 notifyctl.run_notify / OWNER_RE / _wire_argv / LARK_BIN;
# main() 用模块级名 run_notify(可被 monkeypatch rebind)。
from lib.notify import LARK_BIN, OWNER_RE, _wire_argv, run_notify  # noqa: E402,F401


def out(obj, code=0):
    # 直写 UTF-8 字节(绕过 text 层 encoding;codex impl MINOR:PYTHONIOENCODING=ascii 下不裸 traceback)。
    data = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8", "replace")
    try:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
    except Exception:
        pass
    sys.exit(code)


def main():
    # 非法 UTF-8 字节 → replace(不裸崩);但 **读取失败**(closed stdin / OSError)≠ 空消息
    # (codex impl r2 MINOR:别伪装成 empty-message exit 0)→ 独立 stdin-error exit 3(确定未发)。
    try:
        stdin_text = sys.stdin.buffer.read().decode("utf-8", "replace")
    except Exception as e:
        out({"ok": False, "sent": False, "reason": "stdin-error", "detail": str(e)}, 3)
        return  # out() 已 sys.exit;显式 return 兜底
    try:
        obj, code = run_notify(
            stdin_text=stdin_text,
            environ=os.environ,
            prober=procs.SystemProber(),
            start_pid=os.getppid(),
            make_runner=lambda profile: runner_mod.LarkRunner(profile),
        )
    except Exception as e:  # run_notify 已 total;此为最后防线,绝不裸 traceback
        obj, code = ({"ok": False, "sent": "unknown",
                      "reason": "internal-error", "detail": str(e)}, 5)
    out(obj, code)


if __name__ == "__main__":
    main()
