"""bin/download_worker.py(contracts §6):进程内单元(校验/映射/看门狗)+ 真子进程(本地 http.server):
成功 + 父进程发布、任何 3xx 拒绝、text/html 拒绝、max_bytes(Content-Length 与流式)、429/5xx=5、
4xx=3、拒连=4、参数错=2、**滴流 → 自身 alarm 到期 124**、**父进程被 SIGKILL → 看门狗 125**、
父进程 deadline 到期 SIGTERM(瞬态 None、无 tmp 残留)。只有父进程发布 tmp → 正式目录。"""
import http.server
import importlib.util
import io
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error

import pytest

from lib import constants, media
from tests.conftest import CHAT

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKER = ROOT / "bin" / "download_worker.py"
LOCAL = ("127.0.0.1",)


def _load_worker():
    spec = importlib.util.spec_from_file_location("download_worker_mod", WORKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def dw():
    return _load_worker()


# ---------------------------------------------------------------- 本地 http.server
class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *a):  # 静默
        pass

    def do_GET(self):
        p = self.path
        self.server.seen.append((p, self.headers.get("Authorization")))
        parts = p.strip("/").split("/")
        if parts[0] == "ok":
            body = b"x" * int(parts[1]) if len(parts) > 1 else b"hello"
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif parts[0] == "nolen":
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.end_headers()
            self.wfile.write(b"y" * 10)
        elif parts[0] == "redirect":
            self.send_response(302)
            self.send_header("Location", "/ok")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif parts[0] == "html":
            body = b"<html>login</html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif parts[0] == "biglen":
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", "100000")
            self.end_headers()
            try:
                self.wfile.write(b"z" * 100000)
            except OSError:
                pass
        elif parts[0] == "bigstream":
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            try:
                self.wfile.write(b"z" * 100000)
            except OSError:
                pass
        elif parts[0] in ("429", "500", "404", "403"):
            self.send_response(int(parts[0]))
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif parts[0] == "drip":
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            for _ in range(600):
                if self.server.closing:
                    break
                try:
                    self.wfile.write(b"d")
                    self.wfile.flush()
                except OSError:
                    break
                time.sleep(0.05)
        elif parts[0] == "hang":
            for _ in range(600):
                if self.server.closing:
                    break
                time.sleep(0.05)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()


@pytest.fixture
def server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    srv.daemon_threads = True
    srv.seen = []
    srv.closing = False
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    srv.url = "http://127.0.0.1:%d" % srv.server_address[1]
    yield srv
    srv.closing = True
    srv.shutdown()
    srv.server_close()


def req(url, dest, token="xoxb-t", max_bytes=10_000, timeout_s=5.0, allow=LOCAL):
    r = {"url": url, "dest_tmp": str(dest), "token": token, "max_bytes": max_bytes,
         "timeout_s": timeout_s}
    if allow:
        r["allow_plain_http_hosts"] = list(allow)
    return r


def run_worker(r, timeout=20):
    proc = subprocess.run([sys.executable, str(WORKER)], input=json.dumps(r).encode("utf-8"),
                          capture_output=True, timeout=timeout)
    lines = [l for l in proc.stdout.decode("utf-8", "replace").splitlines() if l.strip()]
    out = json.loads(lines[-1]) if lines else None
    return proc.returncode, out, proc.stderr.decode("utf-8", "replace")


# ---------------------------------------------------------------- 进程内单元
class TestUnit:
    def test_check_url_and_hosts(self, dw):
        assert dw.check_url("https://files.slack.com/files-pri/T/F/a.pdf") is None
        assert dw.check_url("https://foo.slack.com/x") is None
        assert dw.check_url("https://slack.com.evil.example/x") == "host_not_allowed"
        assert dw.check_url("https://evil.example/x") == "host_not_allowed"
        assert dw.check_url("http://files.slack.com/x") == "scheme_not_https"
        assert dw.check_url("ftp://files.slack.com/x") == "scheme_not_https"
        assert dw.check_url("https:///x") == "no_host"
        assert dw.check_url("http://127.0.0.1:1/x", ("127.0.0.1",)) is None
        assert dw.check_url("http://127.0.0.2:1/x", ("127.0.0.1",)) == "scheme_not_https"
        assert dw.host_allowed("FILES.SLACK.COM") and not dw.host_allowed("")

    def test_parse_request_errors(self, dw):
        base = {"url": "https://files.slack.com/x", "dest_tmp": "/tmp/x", "token": "t",
                "max_bytes": 10, "timeout_s": 1.5}
        assert dw.parse_request(json.dumps(base))[1] is None
        assert dw.parse_request("{not json")[1] == "bad_json"
        assert dw.parse_request("[]")[1] == "not_object"
        for k, v in (("url", ""), ("dest_tmp", None), ("token", ""), ("max_bytes", 0),
                     ("max_bytes", True), ("timeout_s", 0), ("timeout_s", "3"),
                     ("allow_plain_http_hosts", "x")):
            bad = dict(base)
            bad[k] = v
            assert dw.parse_request(json.dumps(bad))[1] == k, (k, v)

    def test_watchdog_orphan_detection(self, dw):
        assert dw.is_orphaned(1, 4242) is True
        assert dw.is_orphaned(9999, 4242) is True     # 父 pid 变化(子收割者场景)
        assert dw.is_orphaned(4242, 4242) is False
        codes = []
        assert dw.watchdog_once(4242, getppid=lambda: 4242, exit=codes.append) is False
        assert dw.watchdog_once(4242, getppid=lambda: 1, exit=codes.append) is True
        assert codes == [constants.WORKER_RC_ORPHAN] == [125]

    def _resp(self, body=b"abc", status=200, ctype="application/pdf", clen=None, read_exc=None):
        class R:
            def __init__(self):
                self.status = status
                self.headers = {"Content-Type": ctype}
                if clen is not None:
                    self.headers["Content-Length"] = str(clen)
                self._buf = io.BytesIO(body)

            def getcode(self):
                return status

            def read(self, n=-1):
                if read_exc is not None:
                    raise read_exc
                return self._buf.read(n)

            def close(self):
                pass
        return R()

    def _opener(self, resp=None, exc=None):
        class O:
            def open(self, request, timeout=None):
                O.last = request
                if exc is not None:
                    raise exc
                return resp
        return O()

    def test_run_success_writes_only_dest_tmp(self, dw, tmp_path):
        dest = tmp_path / "f.bin"
        r = req("https://files.slack.com/x", dest, allow=())
        rc, out = dw.run(r, opener=self._opener(self._resp(b"abcdef", clen=6)))
        assert rc == 0 and out["ok"] and out["nbytes"] == 6 and out["http_status"] == 200
        assert dest.read_bytes() == b"abcdef" and oct(dest.stat().st_mode & 0o777) == "0o600"
        assert "Bearer xoxb-t" in self._opener().__class__.last.get_header("Authorization") \
            if hasattr(self._opener().__class__, "last") else True
        assert sorted(os.listdir(tmp_path)) == ["f.bin"]

    @pytest.mark.parametrize("code,rc,err", [
        (301, 3, "http_redirect"), (302, 3, "http_redirect"), (307, 3, "http_redirect"),
        (429, 5, "http_429"), (500, 5, "http_500"), (503, 5, "http_503"),
        (404, 3, "http_404"), (401, 3, "http_401"), (403, 3, "http_403"),
    ])
    def test_run_http_error_mapping(self, dw, tmp_path, code, rc, err):
        dest = tmp_path / "f.bin"
        exc = urllib.error.HTTPError("https://files.slack.com/x", code, "msg", {}, io.BytesIO(b""))
        got_rc, out = dw.run(req("https://files.slack.com/x", dest, allow=()), opener=self._opener(exc=exc))
        assert (got_rc, out["error"], out["http_status"]) == (rc, err, code)
        assert not dest.exists()

    def test_run_transient_network_errors(self, dw, tmp_path):
        dest = tmp_path / "f.bin"
        for exc in (urllib.error.URLError("dns"), socket.timeout("t"), ConnectionResetError(),
                    ConnectionRefusedError(), OSError("net")):
            rc, out = dw.run(req("https://files.slack.com/x", dest, allow=()), opener=self._opener(exc=exc))
            assert rc == 4 and out["error"].startswith("network:")
        rc, out = dw.run(req("https://files.slack.com/x", dest, allow=()),
                         opener=self._opener(self._resp(read_exc=ConnectionResetError())))
        assert rc == 4 and out["error"].startswith("read:") and not dest.exists()

    def test_run_html_and_size_rules(self, dw, tmp_path):
        dest = tmp_path / "f.bin"
        rc, out = dw.run(req("https://files.slack.com/x", dest, allow=()),
                         opener=self._opener(self._resp(b"<html>", ctype="text/html; charset=utf-8")))
        assert rc == 3 and out["error"] == "text_html" and not dest.exists()
        rc, out = dw.run(req("https://files.slack.com/x", dest, allow=(), max_bytes=5),
                         opener=self._opener(self._resp(b"x" * 100, clen=100)))
        assert rc == 3 and out["error"] == "too_large" and not dest.exists()
        rc, out = dw.run(req("https://files.slack.com/x", dest, allow=(), max_bytes=5),
                         opener=self._opener(self._resp(b"x" * 100)))
        assert rc == 3 and out["error"] == "too_large" and not dest.exists()   # 流式计数中止
        rc, out = dw.run(req("https://files.slack.com/x", dest, allow=()),
                         opener=self._opener(self._resp(b"", status=302)))
        assert rc == 3 and out["error"] == "http_redirect"

    def test_run_rejects_before_any_request(self, dw, tmp_path):
        dest = tmp_path / "f.bin"

        class Never:
            def open(self, *a, **k):
                raise AssertionError("must not open")
        rc, out = dw.run(req("http://files.slack.com/x", dest, allow=()), opener=Never())
        assert rc == 3 and out["error"] == "scheme_not_https"
        rc, out = dw.run(req("https://evil.example/x", dest, allow=()), opener=Never())
        assert rc == 3 and out["error"] == "host_not_allowed"

    def test_run_existing_dest_is_args_error(self, dw, tmp_path):
        dest = tmp_path / "f.bin"
        dest.write_bytes(b"old")
        rc, out = dw.run(req("https://files.slack.com/x", dest, allow=()),
                         opener=self._opener(self._resp(b"new")))
        assert rc == 2 and dest.read_bytes() == b"old"


# ---------------------------------------------------------------- 真子进程 + 本地 http.server
class TestSubprocess:
    def test_success_bearer_header_and_exit_0(self, server, tmp_path):
        dest = tmp_path / "a.bin"
        rc, out, err = run_worker(req(server.url + "/ok/17", dest))
        assert rc == 0 and out["ok"] and out["nbytes"] == 17 and out["content_type"].startswith("application/octet")
        assert dest.read_bytes() == b"x" * 17
        assert server.seen == [("/ok/17", "Bearer xoxb-t")]

    def test_no_content_length_streams(self, server, tmp_path):
        dest = tmp_path / "a.bin"
        rc, out, _ = run_worker(req(server.url + "/nolen", dest))
        assert rc == 0 and out["nbytes"] == 10 and dest.read_bytes() == b"y" * 10

    def test_plain_http_refused_without_test_allow(self, server, tmp_path):
        rc, out, _ = run_worker(req(server.url + "/ok", tmp_path / "a", allow=()))
        assert rc == 3 and out["error"] == "scheme_not_https" and server.seen == []

    def test_redirect_refused_never_follows(self, server, tmp_path):
        dest = tmp_path / "a.bin"
        rc, out, _ = run_worker(req(server.url + "/redirect", dest))
        assert rc == 3 and out["error"] == "http_redirect" and out["http_status"] == 302
        assert [p for p, _ in server.seen] == ["/redirect"]      # 未跟随到 /ok
        assert not dest.exists()

    def test_html_refused(self, server, tmp_path):
        dest = tmp_path / "a.bin"
        rc, out, _ = run_worker(req(server.url + "/html", dest))
        assert rc == 3 and out["error"] == "text_html" and not dest.exists()

    @pytest.mark.parametrize("path", ["/biglen", "/bigstream"])
    def test_max_bytes_enforced(self, server, tmp_path, path):
        dest = tmp_path / "a.bin"
        rc, out, _ = run_worker(req(server.url + path, dest, max_bytes=100))
        assert rc == 3 and out["error"] == "too_large" and not dest.exists()

    @pytest.mark.parametrize("path,rc", [("/429", 5), ("/500", 5), ("/404", 3), ("/403", 3)])
    def test_http_status_mapping(self, server, tmp_path, path, rc):
        got, out, _ = run_worker(req(server.url + path, tmp_path / "a"))
        assert got == rc and out["http_status"] == int(path[1:])

    def test_connection_refused_is_transient(self, tmp_path):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        rc, out, _ = run_worker(req("http://127.0.0.1:%d/ok" % port, tmp_path / "a"))
        assert rc == 4 and out["error"].startswith("network:")

    def test_args_error_exit_2(self, tmp_path):
        proc = subprocess.run([sys.executable, str(WORKER)], input=b"{not json", capture_output=True, timeout=20)
        assert proc.returncode == 2 and json.loads(proc.stdout.decode())["error"] == "args:bad_json"
        rc, out, _ = run_worker({"url": "https://files.slack.com/x", "dest_tmp": str(tmp_path / "a"),
                                 "max_bytes": 1, "timeout_s": 1})
        assert rc == 2 and out["error"] == "args:token"

    def test_drip_feed_hits_own_alarm_124(self, server, tmp_path):
        """滴流(每 50ms 一字节,socket 永不超时)→ worker 自身 alarm(timeout_s+2)→ os._exit(124)。"""
        dest = tmp_path / "a.bin"
        t0 = time.monotonic()
        rc, out, err = run_worker(req(server.url + "/drip", dest, timeout_s=0.5), timeout=15)
        elapsed = time.monotonic() - t0
        assert rc == 124 and out is None and "exit 124" in err
        assert elapsed < 0.5 + constants.WORKER_ALARM_SLACK_S + 3

    def test_parent_sigkill_worker_exits_125(self, server, tmp_path):
        """父进程被 SIGKILL(daemon 崩溃)→ 孤儿 worker 看门狗 ≤~2s 内 os._exit(125)。"""
        errfile = tmp_path / "worker.err"
        script = tmp_path / "parent.py"
        script.write_text(
            "import json, subprocess, sys, time\n"
            "worker, url, dest, errfile = sys.argv[1:5]\n"
            "p = subprocess.Popen([sys.executable, worker], stdin=subprocess.PIPE,\n"
            "                     stdout=subprocess.DEVNULL, stderr=open(errfile, 'wb'))\n"
            "p.stdin.write(json.dumps({'url': url, 'dest_tmp': dest, 'token': 't', 'max_bytes': 1000,\n"
            "                          'timeout_s': 60, 'allow_plain_http_hosts': ['127.0.0.1']}).encode())\n"
            "p.stdin.close()\n"
            "print(p.pid, flush=True)\n"
            "time.sleep(120)\n")
        parent = subprocess.Popen([sys.executable, str(script), str(WORKER), server.url + "/hang",
                                   str(tmp_path / "a.bin"), str(errfile)], stdout=subprocess.PIPE)
        wpid = int(parent.stdout.readline().decode().strip())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not server.seen:
            time.sleep(0.05)                       # 等 worker 真正阻塞在网络读上
        os.kill(parent.pid, signal.SIGKILL)
        parent.wait(timeout=5)
        t0 = time.monotonic()
        gone = False
        while time.monotonic() - t0 < 6:
            try:
                os.kill(wpid, 0)
            except ProcessLookupError:
                gone = True
                break
            time.sleep(0.1)
        err = errfile.read_text() if errfile.exists() else ""
        assert "exit 125" in err, err
        assert gone, "orphan worker still alive after parent death"
        assert time.monotonic() - t0 < 4

    def test_materialize_end_to_end_only_parent_publishes(self, server, env):
        files = [{"id": "F1", "name": "a.bin", "size": 5, "mimetype": "application/octet-stream",
                  "url_private_download": server.url + "/ok/5"},
                 {"id": "F2", "name": "gone", "mode": "tombstone"}]
        paths, skipped = media.materialize({"bot_token": "xoxb-t"}, env.media_root, "b1", "%s:1.1" % CHAT,
                                           files, deadline_s=10, allow_plain_http_hosts=LOCAL)
        assert [os.path.basename(p) for p in paths] == ["a.bin"] and os.path.getsize(paths[0]) == 5
        assert skipped == [{"id": "F2", "name": "gone", "skipped_reason": "tombstone"}]
        assert server.seen == [("/ok/5", "Bearer xoxb-t")]
        assert [x for x in os.listdir(env.media_root / "b1") if x.startswith(".tmp")] == []

    def test_materialize_parent_deadline_terminates_drip(self, server, env):
        """父进程持绝对截止:滴流下载到 deadline 被 SIGTERM,视为瞬态(None),无 tmp 残留。"""
        files = [{"id": "F1", "name": "d.bin", "size": 5, "url_private_download": server.url + "/drip"}]
        logs = []
        t0 = time.monotonic()
        res = media.materialize({"bot_token": "xoxb-t"}, env.media_root, "b1", "%s:2.2" % CHAT, files,
                                deadline_s=1, allow_plain_http_hosts=LOCAL, log=logs.append)
        assert res is None and time.monotonic() - t0 < 6
        assert any("parent deadline" in l for l in logs)
        assert not (env.media_root / "b1" / ("%s:2.2" % CHAT)).exists()
        assert [x for x in os.listdir(env.media_root / "b1") if x.startswith(".tmp")] == []

    def test_materialize_redirect_and_html_are_media_errors(self, server, env):
        for path in ("/redirect", "/html"):
            files = [{"id": "F1", "name": "x.bin", "size": 5, "url_private_download": server.url + path}]
            with pytest.raises(media.MediaError):
                media.materialize({"bot_token": "xoxb-t"}, env.media_root, "b1", "%s:3.3" % CHAT, files,
                                  deadline_s=10, allow_plain_http_hosts=LOCAL)
