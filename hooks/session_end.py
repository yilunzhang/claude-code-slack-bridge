#!/usr/bin/env python3
"""CC SessionEnd hook 入口:快路径 close(plan 4.6)。永远 exit 0。"""
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
    hooklib.session_end_entry(payload, start_pid=os.getppid())
    sys.exit(0)


if __name__ == "__main__":
    main()
