"""假 builtin SocketModeClient:同真 sdk 的消息路径(_on_message → enqueue → process_message →
run_message_listeners → socket_mode_request_listeners(client, SocketModeRequest)),
帧由脚本注入而非 WebSocket。构造参数**关键字限定**(真 sdk 的 R2-N3 契约:必须以关键字构造)。"""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

from .. import _script
from ..web import WebClient
from .request import SocketModeRequest
from .response import SocketModeResponse


class SocketModeClient:
    def __init__(self, *, app_token, logger=None, web_client=None, auto_reconnect_enabled=True,
                 ping_interval=5, concurrency=10, **kwargs):
        self.app_token = app_token
        self.logger = logger
        self.web_client = web_client or WebClient(token=app_token)
        self.auto_reconnect_enabled = auto_reconnect_enabled
        self.ping_interval = ping_interval
        self.wss_uri = None
        self.closed = False
        self.message_queue = Queue()
        self.message_listeners = []
        self.socket_mode_request_listeners = []
        self.on_message_listeners = []
        self.on_error_listeners = []
        self.on_close_listeners = []
        self._connected = False
        self._futures = []
        self._futures_lock = threading.Lock()
        self.message_workers = ThreadPoolExecutor(max_workers=concurrency)
        self._processor = threading.Thread(target=self.process_messages, daemon=True)
        self._processor.start()
        self._feeder = None
        rh = getattr(self.web_client, "retry_handlers", "<missing>")
        _script.log({
            "__event": "ctor",
            "app_token_set": bool(app_token),
            "web_client_given": web_client is not None,
            "web_client_token_is_app_token": getattr(self.web_client, "token", None) == app_token,
            "web_client_retry_handlers": rh,
            "auto_reconnect_enabled": auto_reconnect_enabled,
            "ping_interval": ping_interval,
            "extra_kwargs": sorted(kwargs.keys()),
        })

    # ---- 对齐真 sdk 的公开方法 ----
    def issue_new_wss_url(self):
        return self.web_client.apps_connections_open(app_token=self.app_token)["url"]

    def is_connected(self):
        return (not self.closed) and self._connected

    def connect(self):
        if self.wss_uri is None:
            self.wss_uri = self.issue_new_wss_url()
        self._connected = True
        _script.log({"__event": "connect"})
        if self._feeder is None:
            self._feeder = threading.Thread(target=self._run_script, daemon=True)
            self._feeder.start()

    def connect_to_new_endpoint(self, force=False):
        """真 sdk 在收到 type=disconnect 帧时调用;这里模拟重连成功 + 服务端补发 hello。"""
        _script.log({"__event": "reconnect", "force": bool(force)})
        self._connected = True
        self._on_message(json.dumps({"type": "hello", "num_connections": 1,
                                     "debug_info": {"host": "fake"}}))

    def disconnect(self):
        self._connected = False

    def close(self):
        self.closed = True
        self.auto_reconnect_enabled = False
        self._connected = False
        self.message_queue.put(None)
        _script.log({"__event": "close"})
        try:
            self.message_workers.shutdown(wait=True)
        except Exception:  # noqa: BLE001
            pass

    def send_message(self, message):
        if not self._connected:
            raise RuntimeError("fake websocket not connected")
        _script.log(json.loads(message))

    def send_socket_mode_response(self, response):
        if isinstance(response, SocketModeResponse):
            self.send_message(json.dumps(response.to_dict()))
        else:
            self.send_message(json.dumps(response))

    def enqueue_message(self, message):
        self.message_queue.put(message)

    def process_message(self):
        raw = self.message_queue.get()
        if raw is None:
            return False
        message = json.loads(raw) if raw.startswith("{") else {}
        fut = self.message_workers.submit(self.run_message_listeners, message, raw)
        with self._futures_lock:
            self._futures.append(fut)
        return True

    def process_messages(self):
        while not self.closed:
            try:
                if not self.process_message():
                    return
            except Exception:  # noqa: BLE001
                pass

    def run_message_listeners(self, message, raw_message):
        if message.get("type") == "disconnect":
            self.connect_to_new_endpoint(force=True)
            return
        for listener in self.message_listeners:
            try:
                listener(self, message, raw_message)
            except Exception:  # noqa: BLE001
                pass
        if message.get("envelope_id") is not None:
            request = SocketModeRequest.from_dict(message)
            if request is not None:
                for listener in self.socket_mode_request_listeners:
                    try:
                        listener(self, request)
                    except Exception:  # noqa: BLE001
                        pass

    # ---- 内部:与真 builtin client 相同的 raw 入口 ----
    def _on_message(self, message):
        self.enqueue_message(message)
        for listener in self.on_message_listeners:
            try:
                listener(message)
            except Exception:  # noqa: BLE001
                pass

    def _on_close(self, code, reason=None):
        for listener in self.on_close_listeners:
            try:
                listener(code, reason)
            except Exception:  # noqa: BLE001
                pass

    def _drain_inflight(self, timeout_s=10.0):
        deadline = time.time() + timeout_s
        while time.time() < deadline and not self.message_queue.empty():
            time.sleep(0.01)
        with self._futures_lock:
            futs = list(self._futures)
        for f in futs:
            try:
                f.result(timeout=max(0.0, deadline - time.time()))
            except Exception:  # noqa: BLE001
                pass

    def _run_script(self):
        while not self.closed:
            item = _script.pop()
            if item is None:
                return
            if isinstance(item, dict) and "__control" in item:
                ctl = item["__control"]
                if ctl == "sleep":
                    time.sleep(float(item.get("seconds", 0.1)))
                elif ctl == "close":
                    self._connected = False
                    self._on_close(int(item.get("code", 1006)), item.get("reason"))
                    if item.get("reconnect") and self.auto_reconnect_enabled:
                        time.sleep(float(item.get("reconnect_after", 0.0)))
                        if not self.closed:
                            self.connect_to_new_endpoint(force=True)
                elif ctl == "done":
                    self._drain_inflight()
                    _script.log({"__event": "script_done"})
                elif ctl == "sdk_log":
                    # 模拟真 sdk **自身** logger(slack_sdk.socket_mode.builtin.client 等)把含
                    # `Bearer <token>` 的异常文本打进日志:{app_token} 占位符替换为真实 app token。
                    # 真 sdk 里 logger 无 handler 时由 logging.lastResort 裸打 stderr(R2-M5)。
                    import logging
                    text = str(item.get("text", "")).replace("{app_token}", str(self.app_token))
                    logging.getLogger(item.get("logger") or "slack_sdk.socket_mode.builtin.client").log(
                        logging.getLevelName(str(item.get("level", "WARNING")).upper()), text)
                else:
                    _script.log({"__event": "unknown_control", "control": ctl})
                continue
            self._on_message(json.dumps(item))
            time.sleep(0.005)
