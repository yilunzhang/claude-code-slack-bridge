#!/usr/bin/env python3
"""测试用假 download_worker(零网络):按 URL path 决定行为,协议与 bin/download_worker.py 一致
(stdin JSON → dest_tmp 写入 → stdout 单行 JSON → 退出码)。
  /ok/<n>        写 n 字节 → rc 0
  /rc/<code>     不写文件,rc=<code>(error=fake)
  /badsize       写 3 字节但报 nbytes=5 → rc 0(父进程应判不一致)
  /symlink       把 dest_tmp 做成符号链接后报 ok(父进程应拒绝)
  /hang          sleep 60(父进程 deadline 应 SIGTERM)
  /nojson        rc 0 但不输出 JSON
"""
import json
import os
import sys
import time


def main():
    req = json.loads(sys.stdin.read())
    url = req["url"]
    dest = req["dest_tmp"]
    path = url.split("://", 1)[-1].split("/", 1)[-1] if "/" in url.split("://", 1)[-1] else ""
    parts = path.split("/")
    if parts[0] == "ok":
        n = int(parts[1]) if len(parts) > 1 else 4
        with open(dest, "wb") as f:
            f.write(b"x" * n)
        print(json.dumps({"ok": True, "nbytes": n, "content_type": "application/octet-stream",
                          "http_status": 200, "error": None}))
        return 0
    if parts[0] == "rc":
        code = int(parts[1])
        print(json.dumps({"ok": False, "nbytes": None, "content_type": None,
                          "http_status": None, "error": "fake_rc_%d" % code}))
        return code
    if parts[0] == "badsize":
        with open(dest, "wb") as f:
            f.write(b"xyz")
        print(json.dumps({"ok": True, "nbytes": 5, "content_type": None, "http_status": 200,
                          "error": None}))
        return 0
    if parts[0] == "symlink":
        os.symlink("/etc/hosts", dest)
        print(json.dumps({"ok": True, "nbytes": os.path.getsize("/etc/hosts"),
                          "content_type": None, "http_status": 200, "error": None}))
        return 0
    if parts[0] == "hang":
        time.sleep(60)
        return 0
    if parts[0] == "nojson":
        with open(dest, "wb") as f:
            f.write(b"q")
        return 0
    print(json.dumps({"ok": False, "error": "unknown_path"}))
    return 2


if __name__ == "__main__":
    sys.exit(main())
