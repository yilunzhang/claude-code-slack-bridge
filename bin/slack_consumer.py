#!/usr/bin/env python3
"""Socket Mode consumer(daemon 子进程;**仓库中唯一导入 slack_sdk 的文件**)。契约:docs/contracts.md §9 / I6。

职责只有一条:把每个 Socket Mode 信封**持久化提交到 bridge.db 之后**再 ack;其余一概不做
(不解析业务、不联网发消息、stdout 不承载数据)。daemon 每 tick 只 drain `slack_events` 表。

- tokens:自读 `tokens.json`(`config.load_tokens(allow_env=False)`;文件是真相;尊重
  `SLACK_BRIDGE_DATA_DIR`);token 只在内存,**绝不进 stderr / stdout / 异常文本**:
  `status()` 对每一行 `util.redact_secrets`;凭据相关异常只打类型名(`_exc_label`,R1-M5)。
- 客户端(R2-N3,关键字参数):`WebClient(token=app_token, retry_handlers=[])`;
  `SocketModeClient(app_token=…, web_client=…, auto_reconnect_enabled=True, ping_interval=10)`。
- `on_request(client, req)`(sdk 线程池线程,每线程独立 `db.connect_short(CONSUMER_DB_BUSY_MS)`):
  `key = slackwire.event_key(req.type, req.payload)`(None → ack + 计数 staged_invalid);
  同事务 SELECT 钉 binding_id → `INSERT … ON CONFLICT(event_key) DO NOTHING`(rowcount 0 → 计数
  staged_dup,**仍 ack**)→ 提交 → ack;锁超时 / 任何异常 → **不 ack**(Slack 会重投)。
  其它信封类型(slash_commands 等)→ ack 后丢弃、不落库。
- stderr 状态行:`[socket] ready num_connections=N`(= CONSUMER_READY_SENTINEL 前缀)/
  `[socket] disconnect reason=…` / `[socket] warn …` / `[socket] fatal <code>` / `[socket] exit reason=…`;
  sdk **自身** logger(`slack_sdk.*`)经 `install_sdk_log_redaction` 挂的 handler 走同一 `status()`
  (`[sdk] <LEVEL> <logger>: <msg>`,WARNING+,不 propagate;exc_info 只留异常类型名)——
  否则无 handler 时 logging.lastResort 会把含 `Bearer <token>` 的 sdk 日志裸打到 stderr(R2-M5)。
- 退出码(constants.CONSUMER_RC_*):0 stdin EOF / SIGTERM / 看门狗断连超 CONSUMER_DISCONNECT_EXIT_S
  (均先 `client.close()`);2 tokens;3 致命鉴权(invalid_auth / link_disabled 等,或 hello 迟迟不来);
  4 缺 slack_sdk;5 其它(db 缺失/schema 不符/网络连接失败/未知异常)。
"""
import json
import logging
import os
import pathlib
import signal
import sqlite3
import sys
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib import config as configmod  # noqa: E402
from lib import constants, db, paths, slackwire, util  # noqa: E402

# 初次连接后等待服务端 hello 的上限;超时视为致命鉴权类(rc 3,ConsumerManager 直接最大退避)。
HELLO_TIMEOUT_S = 3 * constants.SOCKET_OP_TIMEOUT_S
ENV_HELLO_TIMEOUT = "SLACK_BRIDGE_CONSUMER_HELLO_TIMEOUT_S"        # 测试/排障用覆盖
ENV_DISCONNECT_EXIT = "SLACK_BRIDGE_CONSUMER_DISCONNECT_EXIT_S"    # 测试/排障用覆盖
POLL_S = 0.2

# apps.connections.open 的致命鉴权错误(→ rc 3);其余 Slack 错误(internal_error 等)→ rc 5。
FATAL_AUTH_ERRORS = frozenset(constants.NOT_SENT_ERRORS) | frozenset({
    "link_disabled", "missing_scope", "no_permission", "ekm_access_denied",
    "team_access_not_granted", "invalid_token", "invalid_app_token",
})

_status_lock = threading.Lock()
_SECRETS = []   # main() 读到 tokens 后填入(bot/app token);status() 对每一行遮蔽(R1-M5)


def status(line):
    """stderr 状态行(唯一输出通道;绝不含 token / payload)。每一行都过 util.redact_secrets:
    显式 token + Slack token 形态兜底 —— 即便某处不慎把异常文本拼进来也不会泄漏。"""
    with _status_lock:
        try:
            sys.stderr.write(util.redact_secrets(line, _SECRETS) + "\n")
            sys.stderr.flush()
        except (OSError, ValueError):
            pass


SDK_LOGGER_NAME = "slack_sdk"          # 真 sdk 全部 logger 都在这棵树下(logging.getLogger(__name__))
SDK_LOG_LEVEL = logging.WARNING
SDK_LOG_MAX_LEN = 300


class _SdkLogHandler(logging.Handler):
    """`slack_sdk` logger 树的唯一出口(R2-M5):记录经 status() 遮蔽后单行输出。
    - 只用 record.getMessage()(不走 Formatter 的 traceback 拼接):exc_info 只留异常类型名,
      真 sdk 常用 logger.exception(...) 把含 `Bearer <token>` 的 header ValueError 整段打出来;
    - 任何格式化异常都吞掉(handler 绝不能炸 sdk 线程)。"""

    def emit(self, record):
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001
            msg = "<unformattable>"
        try:
            if record.exc_info and record.exc_info[1] is not None:
                msg = "%s %s" % (msg, type(record.exc_info[1]).__name__)
            status("[sdk] %s %s: %s" % (record.levelname, record.name,
                                        msg.replace("\n", " ")[:SDK_LOG_MAX_LEN]))
        except Exception:  # noqa: BLE001
            pass


def install_sdk_log_redaction(level=SDK_LOG_LEVEL):
    """给 `slack_sdk` logger 挂 `_SdkLogHandler`(幂等,返回 handler),并关闭 propagate:
    sdk 日志不再落到 root / lastResort 裸打 stderr。在 _load_sdk 之前调用亦可(按名字取 logger)。"""
    logger = logging.getLogger(SDK_LOGGER_NAME)
    for h in logger.handlers:
        if isinstance(h, _SdkLogHandler):
            return h
    h = _SdkLogHandler(level=level)
    logger.addHandler(h)
    logger.setLevel(level)
    logger.propagate = False
    return h


def _env_float(name, default):
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _exc_label(e, with_text=False):
    """异常 → 安全短标签。缺省**只有类型名**(R1-M5:凭据相关路径 —— connect / sdk / 未知异常 —— 的
    异常文本可能含 `Bearer <token>`,一律不打);`with_text=True` 仅供 sqlite 路径(文本形如
    "database is locked",对排障有用),且仍经 status() 遮蔽。"""
    if not with_text:
        return type(e).__name__
    text = util.redact_secrets(str(e).replace("\n", " ")[:120], _SECRETS)
    return "%s:%s" % (type(e).__name__, text) if text else type(e).__name__


def _slack_error_code(e):
    resp = getattr(e, "response", None)
    code = None
    try:
        code = resp["error"] if resp is not None else None
    except (KeyError, TypeError, IndexError):
        code = None
    if not isinstance(code, str) or not code:
        data = getattr(resp, "data", None)
        if isinstance(data, dict) and isinstance(data.get("error"), str):
            code = data["error"]
    return code or "unknown_error"


class Consumer:
    def __init__(self, sdk, app_token, db_file):
        self.sdk = sdk
        self.app_token = app_token
        self.db_file = str(db_file)
        self.stop = threading.Event()
        self.exit_rc = constants.CONSUMER_RC_OK
        self.exit_reason = None
        self._exit_lock = threading.Lock()
        self.hello_seen = threading.Event()
        self._local = threading.local()
        self.client = None
        self.hello_timeout_s = _env_float(ENV_HELLO_TIMEOUT, HELLO_TIMEOUT_S)
        self.disconnect_exit_s = _env_float(ENV_DISCONNECT_EXIT, constants.CONSUMER_DISCONNECT_EXIT_S)

    # ------------------------------------------------------------------ 退出
    def request_stop(self, reason, rc):
        with self._exit_lock:
            if self.exit_reason is None:
                self.exit_reason = reason
                self.exit_rc = rc
        self.stop.set()

    # ------------------------------------------------------------------ DB(每线程)
    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = db.connect_short(self.db_file, constants.CONSUMER_DB_BUSY_MS)
            self._local.conn = conn
        return conn

    def _drop_conn(self):
        conn = getattr(self._local, "conn", None)
        self._local.conn = None
        if conn is not None:
            try:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def _count(self, key):
        """计数(小事务);失败只告警 —— 计数不是数据,不能因它拒绝 ack。"""
        try:
            conn = self._conn()
            with db.tx(conn):
                db.bump_counter(conn, key)
        except Exception as e:  # noqa: BLE001
            status("[socket] warn count_failed key=%s err=%s" % (key, _exc_label(e, with_text=True)))
            self._drop_conn()

    # ------------------------------------------------------------------ ack
    def _ack(self, client, req):
        try:
            client.send_socket_mode_response(
                self.sdk["SocketModeResponse"](envelope_id=req.envelope_id))
            return True
        except Exception as e:  # noqa: BLE001
            # 行已提交;ack 失败 → Slack 重投 → 重复键仍 ack。只告警。
            status("[socket] warn ack_failed envelope=%s err=%s" % (req.envelope_id, _exc_label(e)))
            return False

    # ------------------------------------------------------------------ 信封
    def on_request(self, client, req):
        etype = getattr(req, "type", None)
        if etype not in slackwire.ENVELOPE_TYPES:
            self._ack(client, req)  # 其它类型:ack 后丢弃,不落库
            return
        payload = req.payload if isinstance(req.payload, dict) else {}
        key = slackwire.event_key(etype, payload)
        if key is None:
            self._count("staged_invalid")
            self._ack(client, req)
            return
        chat = slackwire.chat_of(etype, payload)
        now = int(time.time() * 1000)
        try:
            conn = self._conn()
            with db.tx(conn):
                pinned = None
                if chat:
                    r = conn.execute(
                        "SELECT binding_id FROM bindings WHERE chat_id=? "
                        "AND status IN ('starting','active') ORDER BY binding_seq DESC LIMIT 1",
                        (chat,)).fetchone()
                    pinned = r[0] if r else None
                cur = conn.execute(
                    "INSERT INTO slack_events(envelope_type,event_key,chat_id,binding_id,"
                    "payload_json,received_at,state) VALUES(?,?,?,?,?,?,'staged') "
                    "ON CONFLICT(event_key) DO NOTHING",
                    (etype, key, chat, pinned, util.jdumps(payload), now))
                if cur.rowcount != 1:
                    db.bump_counter(conn, "staged_dup")
        except sqlite3.OperationalError as e:
            # busy / locked / I/O:未提交 → 不 ack(Slack 重投)
            status("[socket] warn db_locked envelope=%s err=%s" % (req.envelope_id, _exc_label(e, with_text=True)))
            self._drop_conn()
            return
        except Exception as e:  # noqa: BLE001
            status("[socket] warn db_error envelope=%s err=%s" % (req.envelope_id, _exc_label(e, with_text=True)))
            self._drop_conn()
            return
        self._ack(client, req)  # 只有提交之后才到这里

    # ------------------------------------------------------------------ 连接状态
    def on_raw(self, raw):
        """原始帧(sdk `on_message_listeners`):只看 hello / disconnect,绝不打印内容。"""
        try:
            msg = json.loads(raw) if isinstance(raw, str) and raw.startswith("{") else None
        except ValueError:
            return
        if not isinstance(msg, dict):
            return
        t = msg.get("type")
        if t == "hello":
            n = msg.get("num_connections")
            self.hello_seen.set()
            status("%s num_connections=%s" % (constants.CONSUMER_READY_SENTINEL,
                                              n if isinstance(n, int) else "?"))
        elif t == "disconnect":
            status("[socket] disconnect reason=%s" % (msg.get("reason") or "unknown"))

    def on_close(self, code, reason=None):
        status("[socket] disconnect reason=close code=%s %s" % (code, (reason or "")[:80]))

    def on_error(self, err):
        status("[socket] warn ws_error %s" % type(err).__name__)

    # ------------------------------------------------------------------ stdin / 信号
    def _watch_stdin(self):
        stream = getattr(sys.stdin, "buffer", None) or sys.stdin
        try:
            while True:
                data = stream.read(4096)
                if not data:
                    break
        except Exception:  # noqa: BLE001
            pass
        self.request_stop("stdin_eof", constants.CONSUMER_RC_OK)

    def _install_signals(self):
        def on_term(signum, frame):
            self.request_stop("sigterm" if signum == signal.SIGTERM else "sigint",
                              constants.CONSUMER_RC_OK)
        signal.signal(signal.SIGTERM, on_term)
        signal.signal(signal.SIGINT, on_term)

    # ------------------------------------------------------------------ 主流程
    def run(self):
        sdk = self.sdk
        web = sdk["WebClient"](token=self.app_token, retry_handlers=[])
        client = sdk["SocketModeClient"](app_token=self.app_token, web_client=web,
                                         auto_reconnect_enabled=True, ping_interval=10)
        self.client = client
        client.socket_mode_request_listeners.append(self.on_request)
        client.on_message_listeners.append(self.on_raw)
        client.on_close_listeners.append(self.on_close)
        client.on_error_listeners.append(self.on_error)
        self._install_signals()
        if sys.stdin is not None:
            threading.Thread(target=self._watch_stdin, name="stdin-watch", daemon=True).start()
        status("[socket] connecting")
        try:
            client.connect()
        except sdk["SlackApiError"] as e:
            code = _slack_error_code(e)
            rc = constants.CONSUMER_RC_AUTH if code in FATAL_AUTH_ERRORS else constants.CONSUMER_RC_OTHER
            status("[socket] fatal %s" % code)
            self._close(client)
            return rc
        except Exception as e:  # noqa: BLE001 —— 网络/DNS/TLS 等:瞬态,正常退避重拉
            status("[socket] fatal connect_failed %s" % _exc_label(e))
            self._close(client)
            return constants.CONSUMER_RC_OTHER
        connect_at = time.monotonic()
        disconnected_since = None
        rc = constants.CONSUMER_RC_OK
        try:
            while not self.stop.is_set():
                self.stop.wait(POLL_S)
                if self.stop.is_set():
                    break
                now = time.monotonic()
                if not self.hello_seen.is_set():
                    if now - connect_at > self.hello_timeout_s:
                        status("[socket] fatal no_hello timeout_s=%g" % self.hello_timeout_s)
                        rc = constants.CONSUMER_RC_AUTH
                        self.exit_reason = "no_hello"
                        break
                    continue
                connected = False
                try:
                    connected = bool(client.is_connected())
                except Exception:  # noqa: BLE001
                    connected = False
                if connected:
                    disconnected_since = None
                elif disconnected_since is None:
                    disconnected_since = now
                elif now - disconnected_since > self.disconnect_exit_s:
                    status("[socket] disconnect reason=watchdog exit_s=%g" % self.disconnect_exit_s)
                    rc = constants.CONSUMER_RC_OK
                    self.exit_reason = "watchdog"
                    break
            else:
                rc = self.exit_rc
        finally:
            self._close(client)
        status("[socket] exit reason=%s rc=%d" % (self.exit_reason or "stop", rc))
        return rc

    def _close(self, client):
        """有界 close(sdk 线程池 shutdown 可能等在途 on_request,最长 = busy 超时量级)。"""
        def _do():
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
        t = threading.Thread(target=_do, name="sdk-close", daemon=True)
        t.start()
        t.join(constants.SOCKET_OP_TIMEOUT_S)
        self._drop_conn()


def _load_sdk():
    from slack_sdk.errors import SlackApiError, SlackClientError
    from slack_sdk.socket_mode import SocketModeClient
    from slack_sdk.socket_mode.request import SocketModeRequest
    from slack_sdk.socket_mode.response import SocketModeResponse
    from slack_sdk.web import WebClient
    return {
        "SocketModeClient": SocketModeClient, "SocketModeRequest": SocketModeRequest,
        "SocketModeResponse": SocketModeResponse, "WebClient": WebClient,
        "SlackApiError": SlackApiError, "SlackClientError": SlackClientError,
    }


def main(argv=None):
    install_sdk_log_redaction()   # 先于导入 sdk:sdk 自身 logger 的输出全部经 status() 遮蔽(R2-M5)
    try:
        sdk = _load_sdk()
    except ImportError:
        status("[socket] fatal missing_dependency slack_sdk")
        return constants.CONSUMER_RC_NO_SDK
    try:
        tokens, _version = configmod.load_tokens(paths.tokens_path(), allow_env=False)
    except configmod.ConfigError as e:
        status("[socket] fatal tokens %s" % util.redact_secrets(str(e).replace("\n", " ")[:160]))
        return constants.CONSUMER_RC_TOKENS
    app_token = tokens.get("app_token")
    _SECRETS[:] = [v for v in tokens.values() if isinstance(v, str) and v]
    if not app_token:
        status("[socket] fatal tokens missing app_token")
        return constants.CONSUMER_RC_TOKENS
    db_file = paths.db_path()
    if not os.path.exists(str(db_file)):
        status("[socket] fatal db_missing")
        return constants.CONSUMER_RC_OTHER
    try:
        probe = db.connect_short(db_file, constants.CONSUMER_DB_BUSY_MS)
        try:
            db.check_schema(probe)
        finally:
            probe.close()
    except Exception as e:  # noqa: BLE001
        status("[socket] fatal db_schema %s" % _exc_label(e, with_text=True))
        return constants.CONSUMER_RC_OTHER
    consumer = Consumer(sdk, app_token, db_file)
    try:
        return consumer.run()
    except Exception as e:  # noqa: BLE001
        status("[socket] fatal unexpected %s" % _exc_label(e))
        return constants.CONSUMER_RC_OTHER


if __name__ == "__main__":
    _rc = main()
    try:
        sys.stderr.flush()
        sys.stdout.flush()
    except (OSError, ValueError):
        pass
    os._exit(int(_rc))  # 绝不被 sdk 残留非 daemon 线程拖住(ConsumerManager 只 SIGTERM,不 -9)
