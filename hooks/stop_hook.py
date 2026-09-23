#!/usr/bin/env python3
"""CC Stop hook 入口:stdin JSON → hooklib.stop_hook_entry。
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
    hooklib.stop_hook_entry(payload, start_pid=os.getppid())
    sys.exit(0)


if __name__ == "__main__":
    main()
