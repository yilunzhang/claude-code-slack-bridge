"""脚本与日志的进程级共享状态(WebClient 与 SocketModeClient 都要读)。"""
import json
import os
import threading

_lock = threading.Lock()
_script = None


def load():
    global _script
    with _lock:
        if _script is None:
            raw = os.environ.get("FAKE_SLACK_SCRIPT") or "[]"
            items = json.loads(raw)
            if not isinstance(items, list):
                raise ValueError("FAKE_SLACK_SCRIPT must be a JSON list")
            _script = list(items)
        return _script


def peek_control():
    s = load()
    with _lock:
        if s and isinstance(s[0], dict) and "__control" in s[0]:
            return s[0]
        return None


def pop():
    s = load()
    with _lock:
        return s.pop(0) if s else None


def log(obj):
    """追加一行 JSON 到 FAKE_SLACK_ACK_LOG(缺 env 则丢弃)。"""
    path = os.environ.get("FAKE_SLACK_ACK_LOG")
    if not path:
        return
    line = json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n"
    with _lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
