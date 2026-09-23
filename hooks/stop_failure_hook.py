#!/usr/bin/env python3
"""CC StopFailure hook 入口:stdin JSON → hooklib.stop_failure_entry。
一轮因 API 错误(429/529/5xx/auth… 任意类型)结束时,给本 session 绑定群发 @群主 告警。
永远 exit 0(绝不阻塞 CC);一切异常在 hooklib 内 fail-closed。"""
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import hooklib  # noqa: E402


def main():
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    # exit 0 是绝对契约:即便 hooklib 的 fail-closed 兜底(含 _fail_closed_drop 的时间戳/stderr 写)
    # 自身抛,也绝不让异常逃逸出本进程 → 始终 sys.exit(0),绝不阻塞 CC。
    try:
        hooklib.stop_failure_entry(payload, start_pid=os.getppid())
    except Exception:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
