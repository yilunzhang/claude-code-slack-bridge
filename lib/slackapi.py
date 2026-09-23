"""Slack Web API 客户端(stdlib urllib;**无重试、不跟随重定向**)+ 方法级冷却存储 + 结果分类。

- `SlackClient.call(method, params, timeout_s)` → `CallResult`。每次 call **前**读 `cooldown:<method>`,
  未到期 → 不发请求,返回 `CallResult(cooldown_until=…)`(分类 `wait`,不是 not_sent;R3-P2);
  收到 429 → `cooldown_store.publish(method, until)` 取 max(旧, 新)(R3-P2)。
- daemon / notify / probe 共用 `DaemonStateCooldownStore`(daemon_state 键 `cooldown:<method>`)。
- `not_sent` 只在**请求写出之前**的本地错误(DNS / 拒连 / 网络不可达 / TLS 握手)成立;
  请求写出后的任何错误(超时、连接重置、5xx、不可解析)一律不能断言未发送 → unknown。
- token 只存于对象字段,绝不进日志/异常文本/返回值。下载不在此类(见 media / download_worker)。
契约:docs/contracts.md §3 / §4.4。"""
import errno
import http.client
import json
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

from . import config as configmod
from . import constants, db

USER_AGENT = "slack-bridge/0.1 (+stdlib urllib)"
DEFAULT_RETRY_AFTER_S = 1


@dataclass
class CallResult:
    """一次 Web API 调用的完整结果(分类由 classify_send_error 决定,本类只如实记录)。
    - ok:HTTP 200 ∧ body.ok==true
    - data:解析后的 JSON 对象(成功或失败都尽量给;不可解析 → None)
    - error:Slack error 字串 / 本地分类字串('timeout','dns','connection_refused','cooldown',
      'unparseable','http_5xx','urlerror:…')
    - http_status:HTTP 状态码(本地失败 → None)
    - retry_after:429 的 Retry-After 秒数(int;缺头 → 1)
    - timed_out:连接或读超时
    - exc:原始异常(本地/传输错误时)
    - not_sent:True = 可断言请求**未写出**
    - cooldown_until:非空 = 本地冷却未到期,请求未发出(墙钟 ms)"""
    ok: bool = False
    data: Optional[dict] = None
    error: Optional[str] = None
    http_status: Optional[int] = None
    retry_after: Optional[int] = None
    timed_out: bool = False
    exc: Optional[BaseException] = field(default=None, repr=False)
    not_sent: bool = False
    cooldown_until: Optional[int] = None

    def get(self, key, default=None):
        """便捷:data.get(key)。"""
        if isinstance(self.data, dict):
            return self.data.get(key, default)
        return default


@dataclass
class DownloadResult:
    """media.materialize / download_worker 父进程映射的单文件结果(contracts §6)。
    permanent=True → MediaError;ok=False ∧ permanent=False → 瞬态(走预算)。"""
    ok: bool = False
    nbytes: Optional[int] = None
    content_type: Optional[str] = None
    http_status: Optional[int] = None
    error: Optional[str] = None
    rc: Optional[int] = None
    permanent: bool = False
    path: Optional[str] = None


# ---------------------------------------------------------------- cooldown store
class CooldownStore:
    """协议(duck-typed):get(method) -> int|None(到期墙钟 ms);publish(method, until_ms) -> int(取 max 后的生效值)。"""

    def get(self, method):  # pragma: no cover - protocol
        raise NotImplementedError

    def publish(self, method, until_ms):  # pragma: no cover - protocol
        raise NotImplementedError


class InMemoryCooldownStore(CooldownStore):
    def __init__(self):
        self._m = {}

    def get(self, method):
        return self._m.get(method)

    def publish(self, method, until_ms):
        until_ms = int(until_ms)
        cur = self._m.get(method)
        eff = until_ms if cur is None or until_ms > cur else cur
        self._m[method] = eff
        return eff


class DaemonStateCooldownStore(CooldownStore):
    """daemon_state 键 `cooldown:<method>`,值 = 到期墙钟 ms。publish 用单条 UPSERT 取 max
    (autocommit 下原子;调用点在事务外 —— call 本就在事务外)。畸形值按 None。"""

    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def key(method):
        return constants.COOLDOWN_KEY_PREFIX + str(method)

    def get(self, method):
        v = db.get_state(self.conn, self.key(method))
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def publish(self, method, until_ms):
        until_ms = int(until_ms)
        self.conn.execute(
            "INSERT INTO daemon_state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value = CASE "
            "WHEN CAST(daemon_state.value AS INTEGER) >= CAST(excluded.value AS INTEGER) "
            "THEN daemon_state.value ELSE excluded.value END",
            (self.key(method), str(until_ms)))
        got = self.get(method)
        return got if got is not None else until_ms


# ---------------------------------------------------------------- transport
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # 任何 3xx → HTTPError(不跟随)


_OPENER = urllib.request.build_opener(_NoRedirect())


def _open(req, timeout_s):
    """真实网络入口(测试 monkeypatch 此函数)。"""
    return _OPENER.open(req, timeout=timeout_s)


def _retry_after(headers):
    try:
        v = headers.get("Retry-After") if headers is not None else None
        if v is None:
            return DEFAULT_RETRY_AFTER_S
        return max(0, int(float(str(v).strip())))
    except (TypeError, ValueError):
        return DEFAULT_RETRY_AFTER_S


def _parse_json(raw):
    if raw is None:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        obj = json.loads(raw)
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def _is_not_sent_reason(reason):
    """URLError.reason → (not_sent, label)。只有确定发生在请求写出**之前**的错误才 True。"""
    if isinstance(reason, socket.gaierror):
        return True, "dns"
    if isinstance(reason, ConnectionRefusedError):
        return True, "connection_refused"
    if isinstance(reason, ssl.SSLCertVerificationError):
        return True, "tls_cert"          # 证书校验失败发生在握手阶段,请求肯定未写出
    if isinstance(reason, ssl.SSLError):
        return False, "tls"              # 其它 TLS 错可能发生在响应读取阶段 → 不能断言未发送
    if isinstance(reason, (socket.timeout, TimeoutError)):
        return False, "timeout"
    if isinstance(reason, OSError) and getattr(reason, "errno", None) in (
            errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EADDRNOTAVAIL, errno.ENETDOWN):
        return True, "network_unreachable"
    return False, "urlerror:%s" % (type(reason).__name__ if reason is not None else "unknown")


class SlackClient:
    """`SlackClient(tokens_path=None, token=None, cooldown_store=None, timeout_s=SEND_TIMEOUT_S, clock=None)`
    - tokens_path 给定 → 从文件读 bot_token(load_tokens(allow_env=False)),tokens_version = 文件版本;
    - token 给定(CLI/测试)→ tokens_version = tokens_version 参数或 "env";
    - 两者都给 → 文件优先(文件是真相)。
    `reload_tokens(version=None)` 重读文件并返回新版本(无文件路径 → 原样返回当前版本)。"""

    def __init__(self, tokens_path=None, token=None, cooldown_store=None,
                 timeout_s=constants.SEND_TIMEOUT_S, clock=None, base_url=None,
                 tokens_version=None, environ=None):
        self.tokens_path = tokens_path
        self.cooldown_store = cooldown_store or InMemoryCooldownStore()
        self.timeout_s = timeout_s
        self.clock = clock
        self.base_url = base_url or constants.SLACK_API_BASE
        self._environ = environ
        self._token = None
        self._tokens_version = None
        if tokens_path is not None:
            self.reload_tokens()
        elif token is not None:
            self._token = token
            self._tokens_version = tokens_version or configmod.ENV_TOKENS_VERSION
        else:
            raise ValueError("SlackClient 需要 tokens_path 或 token")

    # ---- credentials ----
    @property
    def tokens_version(self):
        return self._tokens_version

    @property
    def has_token(self):
        return bool(self._token)

    def reload_tokens(self, version=None):
        """重读 tokens 文件(文件是真相);返回新版本字符串。`version` 仅作调用方期望值记录,
        实际以文件计算为准(两者不一致时以文件为准并照常返回文件版本)。"""
        if self.tokens_path is None:
            return self._tokens_version
        tokens, ver = configmod.load_tokens(self.tokens_path, allow_env=False,
                                            environ=self._environ)
        self._token = tokens["bot_token"]
        self._tokens_version = ver
        return ver

    def __repr__(self):
        return "<SlackClient version=%r>" % (self._tokens_version,)  # 绝不含 token

    # ---- time ----
    def _now_ms(self):
        if self.clock is not None:
            return self.clock.wall_ms()
        return int(time.time() * 1000)

    # ---- call ----
    def call(self, method, params=None, timeout_s=None):
        method = str(method)
        now = self._now_ms()
        until = self.cooldown_store.get(method)
        if until is not None and until > now:
            return CallResult(ok=False, error="cooldown", cooldown_until=int(until))
        if not self._token:
            return CallResult(ok=False, error="no_token", not_sent=True)
        body = json.dumps(params or {}, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            urllib.parse.urljoin(self.base_url, method), data=body, method="POST",
            headers={
                "Authorization": "Bearer " + self._token,
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": USER_AGENT,
            })
        to = self.timeout_s if timeout_s is None else timeout_s
        try:
            with _open(req, to) as resp:
                status = getattr(resp, "status", None) or resp.getcode()
                headers = resp.headers
                raw = resp.read()
        except urllib.error.HTTPError as e:
            return self._on_http_error(method, e)
        except ValueError:
            # http.client 的 header 校验(token 含 \r/\n 等)/ 非法 URL:请求根本没写出 → not_sent。
            # **不**附带异常对象/文本:这类 ValueError 的文本是 `Invalid header value b'Bearer <token>…'`(R1-M5)。
            return CallResult(ok=False, error="bad_request", not_sent=True)
        except urllib.error.URLError as e:
            reason = getattr(e, "reason", None)
            if isinstance(reason, (socket.timeout, TimeoutError)):
                return CallResult(ok=False, error="timeout", timed_out=True, exc=e)
            not_sent, label = _is_not_sent_reason(reason)
            return CallResult(ok=False, error=label, exc=e, not_sent=not_sent)
        except (socket.timeout, TimeoutError) as e:
            return CallResult(ok=False, error="timeout", timed_out=True, exc=e)
        except socket.gaierror as e:
            return CallResult(ok=False, error="dns", exc=e, not_sent=True)
        except ConnectionRefusedError as e:
            return CallResult(ok=False, error="connection_refused", exc=e, not_sent=True)
        except ssl.SSLCertVerificationError as e:
            return CallResult(ok=False, error="tls_cert", exc=e, not_sent=True)
        except ssl.SSLError as e:
            return CallResult(ok=False, error="tls", exc=e)   # 可能已写出 → unknown
        except (http.client.HTTPException, ConnectionError, OSError) as e:
            # 请求可能已写出(RemoteDisconnected / reset / broken pipe)→ 不能断言未发送
            return CallResult(ok=False, error="transport:%s" % type(e).__name__, exc=e)
        return self._on_response(method, status, headers, raw)

    def _on_http_error(self, method, e):
        status = int(getattr(e, "code", 0) or 0)
        headers = getattr(e, "headers", None)
        try:
            raw = e.read()
        except Exception:  # noqa: BLE001
            raw = None
        if status == 429:
            ra = _retry_after(headers)
            self.cooldown_store.publish(method, self._now_ms() + ra * 1000)
            return CallResult(ok=False, data=_parse_json(raw), error="ratelimited",
                              http_status=429, retry_after=ra, exc=e)
        data = _parse_json(raw)
        err = None
        if data is not None and data.get("ok") is False and isinstance(data.get("error"), str):
            err = data["error"]
        if err is None:
            if 300 <= status < 400:
                err = "http_redirect"
            elif 500 <= status < 600:
                err = "http_5xx"
            else:
                err = "http_%d" % status
        return CallResult(ok=False, data=data, error=err, http_status=status, exc=e)

    def _on_response(self, method, status, headers, raw):
        data = _parse_json(raw)
        if data is None:
            return CallResult(ok=False, error="unparseable", http_status=status)
        if data.get("ok") is True:
            return CallResult(ok=True, data=data, http_status=status)
        err = data.get("error") if isinstance(data.get("error"), str) else "error_without_code"
        if err == "ratelimited":
            ra = _retry_after(headers)
            self.cooldown_store.publish(method, self._now_ms() + ra * 1000)
            return CallResult(ok=False, data=data, error="ratelimited", http_status=status,
                              retry_after=ra)
        return CallResult(ok=False, data=data, error=err, http_status=status)


# ---------------------------------------------------------------- classification
def classify_send_error(res):
    """CallResult → 'sent' | 'failed' | 'ratelimited' | 'wait' | 'not_sent' | 'unknown'。
    顺序:ok → wait(本地冷却,未发出)→ ratelimited(429 / 'ratelimited')→ not_sent(本地写出前错误
    或 NOT_SENT_ERRORS 显式集)→ timeout/传输/5xx/AMBIGUOUS/不可解析 → unknown → PERMANENT → failed
    → 其余未知错误码 → unknown。**markdown_rejected 与 already_reacted 的特判在 Outbound 层**
    (它们需要 op_payload_kind / method 上下文),本函数只做上下文无关分类。"""
    if res.ok:
        return "sent"
    if res.cooldown_until is not None:
        return "wait"
    if res.http_status == 429 or res.error == "ratelimited":
        return "ratelimited"
    if res.not_sent:
        return "not_sent"
    if res.error in constants.NOT_SENT_ERRORS:
        return "not_sent"
    if res.timed_out:
        return "unknown"
    if res.http_status is not None and res.http_status >= 500:
        return "unknown"
    if res.error in constants.AMBIGUOUS_SEND_ERRORS:
        return "unknown"
    if res.error in constants.PERMANENT_SEND_ERRORS:
        return "failed"
    return "unknown"
