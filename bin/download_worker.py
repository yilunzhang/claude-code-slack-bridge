#!/usr/bin/env python3
"""download_worker:单文件下载子进程(contracts §6;R3-M10 / R4-Q2 / R6-m1)。

stdin JSON  : {"url", "dest_tmp", "token", "max_bytes", "timeout_s"}(token 经 stdin,不上 argv)
stdout JSON : 单行 {"ok", "nbytes", "content_type", "http_status", "error"}
退出码      : 0 成功 / 2 参数错 / 3 永久拒绝(非 https、主机不在白名单、任何 3xx、text/html、超 max_bytes、
              HTTP 4xx 非 429)/ 4 瞬态网络错(DNS、拒连、超时、连接中断)/ 5 HTTP 429 或 5xx /
              124 自身到期(signal.alarm)/ 125 父亡(看门狗)。
自保:启动即起看门狗线程(每秒查 ppid;父亡 → os._exit(125));解析出 timeout_s 后
`signal.alarm(int(timeout_s)+WORKER_ALARM_SLACK_S)` → handler os._exit(124)。二者都不依赖网络读返回。
写入只落 dest_tmp(O_EXCL 新建);发布(rename)只由父进程做。"""
import http.client
import json
import os
import pathlib
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import constants  # noqa: E402

RC_OK = constants.WORKER_RC_OK
RC_ARGS = constants.WORKER_RC_ARGS
RC_PERMANENT = constants.WORKER_RC_PERMANENT
RC_TRANSIENT = constants.WORKER_RC_TRANSIENT
RC_HTTP_RETRY = constants.WORKER_RC_HTTP_RETRY
RC_DEADLINE = constants.WORKER_RC_DEADLINE
RC_ORPHAN = constants.WORKER_RC_ORPHAN
RC_UNEXPECTED = 1                      # run() 抛出未预期异常:不在 §6 列内 → 父进程计 worker_unexpected_exit
CHUNK = 64 * 1024
WATCHDOG_INTERVAL_S = 1.0
USER_AGENT = "slack-bridge-download-worker/1"


# ---------------------------------------------------------------- 自保
def _on_alarm(signum, frame):  # pragma: no cover - 进程级退出,由子进程测试覆盖
    try:
        os.write(2, b"[worker] exit 124 deadline\n")
    except OSError:
        pass
    os._exit(RC_DEADLINE)


def is_orphaned(ppid, initial_ppid):
    """父亡判定:被 init 收养(==1)或父 pid 变化(Linux 子收割者场景亦覆盖)。"""
    return ppid == 1 or ppid != initial_ppid


def watchdog_once(initial_ppid, getppid=os.getppid, exit=os._exit):
    """看门狗的一次检查(可测:注入 getppid/exit)。父亡 → exit(125)。"""
    if is_orphaned(getppid(), initial_ppid):
        try:
            os.write(2, b"[worker] exit 125 orphaned\n")
        except OSError:
            pass
        exit(RC_ORPHAN)
        return True
    return False


def _watchdog_loop(initial_ppid):  # pragma: no cover - 线程体
    while True:
        time.sleep(WATCHDOG_INTERVAL_S)
        watchdog_once(initial_ppid)


def start_watchdog():
    t = threading.Thread(target=_watchdog_loop, args=(os.getppid(),), name="ppid-watchdog",
                         daemon=True)
    t.start()
    return t


# ---------------------------------------------------------------- 校验
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # 任何 3xx → HTTPError(绝不跟随:token 不得流向别的主机)


def build_opener():
    return urllib.request.build_opener(_NoRedirect())


def host_allowed(host, extra_hosts=()):
    host = (host or "").lower().rstrip(".")
    if not host:
        return False
    if host in constants.FILE_HOSTS or host.endswith(tuple(constants.FILE_HOST_SUFFIXES)):
        return True
    return host in {h.lower() for h in (extra_hosts or ())}


def check_url(url, allow_plain_http_hosts=()):
    """→ None(合法)或拒绝原因字符串。仅 https;主机 ∈ FILE_HOSTS ∨ 后缀 ∈ FILE_HOST_SUFFIXES。
    allow_plain_http_hosts:**仅测试**(本地 http.server)——这些主机允许 http 且免白名单。"""
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return "bad_url"
    host = (parts.hostname or "").lower()
    if not host:
        return "no_host"
    if parts.scheme == "https":
        if host_allowed(host, allow_plain_http_hosts):
            return None
        return "host_not_allowed"
    if parts.scheme == "http" and host in {h.lower() for h in (allow_plain_http_hosts or ())}:
        return None
    return "scheme_not_https"


def parse_request(raw):
    """stdin JSON → (req, error)。任何缺失/类型错 → error(退出码 2)。"""
    try:
        obj = json.loads(raw)
    except ValueError:
        return None, "bad_json"
    if not isinstance(obj, dict):
        return None, "not_object"
    url = obj.get("url")
    dest = obj.get("dest_tmp")
    token = obj.get("token")
    max_bytes = obj.get("max_bytes")
    timeout_s = obj.get("timeout_s")
    if not isinstance(url, str) or not url:
        return None, "url"
    if not isinstance(dest, str) or not dest:
        return None, "dest_tmp"
    if not isinstance(token, str) or not token:
        return None, "token"
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        return None, "max_bytes"
    if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or timeout_s <= 0:
        return None, "timeout_s"
    extra = obj.get("allow_plain_http_hosts") or []
    if not isinstance(extra, list) or not all(isinstance(h, str) for h in extra):
        return None, "allow_plain_http_hosts"
    return {"url": url, "dest_tmp": dest, "token": token, "max_bytes": int(max_bytes),
            "timeout_s": float(timeout_s), "allow_plain_http_hosts": extra}, None


# ---------------------------------------------------------------- 下载
def _result(ok=False, nbytes=None, content_type=None, http_status=None, error=None):
    return {"ok": bool(ok), "nbytes": nbytes, "content_type": content_type,
            "http_status": http_status, "error": error}


def _unlink(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def run(req, opener=None):
    """执行下载 → (rc, result_dict)。opener 可注入(测试)。写入只落 req.dest_tmp。"""
    url, dest, token = req["url"], req["dest_tmp"], req["token"]
    max_bytes, timeout_s = req["max_bytes"], req["timeout_s"]
    bad = check_url(url, req.get("allow_plain_http_hosts") or ())
    if bad:
        return RC_PERMANENT, _result(error=bad)
    opener = opener or build_opener()
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + token, "User-Agent": USER_AGENT})
    try:
        resp = opener.open(request, timeout=timeout_s)
    except urllib.error.HTTPError as e:
        code = int(e.code)
        try:
            e.close()
        except Exception:
            pass
        if 300 <= code < 400:
            return RC_PERMANENT, _result(http_status=code, error="http_redirect")
        if code == 429 or code >= 500:
            return RC_HTTP_RETRY, _result(http_status=code, error="http_%d" % code)
        return RC_PERMANENT, _result(http_status=code, error="http_%d" % code)
    except (urllib.error.URLError, socket.timeout, http.client.HTTPException, OSError) as e:
        return RC_TRANSIENT, _result(error="network:%s" % (getattr(e, "reason", None) or e))
    except ValueError:
        # header 校验(token 含 \r/\n)/ 非法 URL:永久;**只给固定码**,异常文本含 `Bearer <token>`(R1-M5)
        return RC_PERMANENT, _result(error="bad_request")
    status = int(getattr(resp, "status", None) or resp.getcode() or 0)
    headers = resp.headers
    ctype = headers.get("Content-Type") if headers is not None else None
    ctype = ctype.strip() if isinstance(ctype, str) else None
    if 300 <= status < 400:
        _close(resp)
        return RC_PERMANENT, _result(http_status=status, content_type=ctype, error="http_redirect")
    if ctype and ctype.split(";", 1)[0].strip().lower() == "text/html":
        _close(resp)
        return RC_PERMANENT, _result(http_status=status, content_type=ctype, error="text_html")
    clen = headers.get("Content-Length") if headers is not None else None
    try:
        if clen is not None and int(clen) > max_bytes:
            _close(resp)
            return RC_PERMANENT, _result(http_status=status, content_type=ctype, error="too_large")
    except ValueError:
        pass
    try:
        fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as e:
        _close(resp)
        return RC_ARGS, _result(http_status=status, content_type=ctype, error="dest_tmp:%s" % e)
    n = 0
    try:
        with os.fdopen(fd, "wb") as f:
            while True:
                try:
                    chunk = resp.read(CHUNK)
                except (OSError, http.client.HTTPException, urllib.error.URLError, socket.timeout) as e:
                    _unlink(dest)
                    return RC_TRANSIENT, _result(http_status=status, content_type=ctype,
                                                 error="read:%s" % e)
                if not chunk:
                    break
                n += len(chunk)
                if n > max_bytes:
                    _unlink(dest)
                    return RC_PERMANENT, _result(http_status=status, content_type=ctype,
                                                 error="too_large")
                f.write(chunk)
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        _unlink(dest)
        return RC_TRANSIENT, _result(http_status=status, content_type=ctype, error="write:%s" % e)
    finally:
        _close(resp)
    if clen is not None:
        try:
            expected = int(clen)
        except ValueError:
            expected = None
        if expected is not None and n != expected:
            _unlink(dest)   # 正文提前结束 = 截断;绝不把残缺文件判成功(瞬态,走预算重试)
            return RC_TRANSIENT, _result(http_status=status, content_type=ctype,
                                         error="truncated:%d/%d" % (n, expected))
    return RC_OK, _result(ok=True, nbytes=n, content_type=ctype, http_status=status)


def _close(resp):
    try:
        resp.close()
    except Exception:
        pass


def emit(result, out=None):
    out = out or sys.stdout
    out.write(json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n")
    out.flush()


def main(stdin=None, out=None):
    start_watchdog()                          # 先于一切:父亡即退
    raw = (stdin or sys.stdin).read()
    req, err = parse_request(raw)
    if err:
        emit(_result(error="args:%s" % err), out)
        return RC_ARGS
    signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(int(req["timeout_s"]) + constants.WORKER_ALARM_SLACK_S)
    try:
        rc, result = run(req)
    except Exception as e:  # noqa: BLE001 —— 绝不让 traceback(可能含 token)落到 stderr(R1-M5)
        emit(_result(error="worker_exception:%s" % type(e).__name__), out)
        return RC_UNEXPECTED
    emit(result, out)
    return rc


if __name__ == "__main__":
    sys.exit(main())
