#!/usr/bin/env python3
"""sendfilectl CLI(薄壳):把本机一个文件上传并分享到「本 session 绑定的 Slack 会话」。
核心逻辑在 `lib/sendfile.py`(`run_sendfile`),出站门与 notify / StopFailure 同款(`lib/notify.open_gated_context`)。

用法:
  sendfilectl.py --path /abs/file [--title 标题] [--comment 一句说明]
  echo "一句说明" | sendfilectl.py --path /abs/file          # 说明也可走 stdin(沿用 notifyctl 惯例)
不给 --comment 且 stdin 不是终端时读 stdin 作为说明;两者都给以 --comment 为准。

输出:JSON `{ok, sent, ...}`,`sent` 恒为主信号(true / false / "unknown");退出码:0=已发或前置条件
(not-bound);3=发送前拒绝(输入/门);4=确定未分享(冷却/429/被拒/上传失败);5=不确定(看会话别乱重试)。
token 只在 0600 的 tokens.json,绝不进输出。"""
import argparse
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import procs  # noqa: E402
from lib.sendfile import default_make_client, default_upload, run_sendfile  # noqa: E402,F401


def out(obj, code=0):
    data = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8", "replace")
    try:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
    except Exception:
        pass
    sys.exit(code)


def parse_args(argv):
    ap = argparse.ArgumentParser(prog="sendfilectl", add_help=True)
    ap.add_argument("--path", required=True, help="要发送的本机文件(绝对路径)")
    ap.add_argument("--title", default=None, help="Slack 里显示的标题(默认文件名)")
    ap.add_argument("--comment", default=None, help="随文件发出的一句说明(也可走 stdin)")
    return ap.parse_args(argv)


def main(argv=None):
    try:
        args = parse_args(sys.argv[1:] if argv is None else argv)
    except SystemExit as e:  # argparse 已打印用法
        raise e
    comment = args.comment
    if comment is None:
        try:
            if not sys.stdin.isatty():
                comment = sys.stdin.buffer.read().decode("utf-8", "replace")
        except Exception as e:
            out({"ok": False, "sent": False, "reason": "stdin-error", "detail": str(e)}, 3)
            return
    try:
        obj, code = run_sendfile(
            path=args.path, title=args.title, comment_text=comment,
            environ=os.environ, prober=procs.SystemProber(), start_pid=os.getppid(),
            make_client=default_make_client, upload=default_upload)
    except Exception as e:  # 最后防线,绝不裸 traceback
        obj, code = ({"ok": False, "sent": "unknown", "reason": "internal-error",
                      "detail": str(e)}, 5)
    out(obj, code)


if __name__ == "__main__":
    main()
