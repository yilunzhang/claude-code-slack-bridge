"""假 WebClient:只实现 consumer 路径会碰到的 apps.connections.open;构造参数**只记录布尔特征**。"""
from .. import _script
from ..errors import SlackApiError, SlackClientError, _FakeResponse


class WebClient:
    def __init__(self, token=None, retry_handlers=None, **kwargs):
        self.token = token
        self.retry_handlers = list(retry_handlers) if retry_handlers is not None else None
        self.kwargs = dict(kwargs)

    def apps_connections_open(self, app_token=None, **kwargs):
        item = _script.peek_control()
        if item is not None and item.get("__control") == "connect_error":
            _script.pop()
            err = item.get("error") or "invalid_auth"
            resp = _FakeResponse({"ok": False, "error": err}, status_code=item.get("status", 200),
                                 headers=item.get("headers") or {})
            raise SlackApiError("The request to the Slack API failed.", resp)
        if item is not None and item.get("__control") == "connect_exception":
            _script.pop()
            kind = item.get("kind", "network")
            if kind == "header_valueerror":
                # 真 sdk 路径:token 含换行 → http.client.putheader 抛 ValueError,文本含整个 Bearer 值
                tok = app_token or self.token
                raise ValueError("Invalid header value %r" % (("Bearer " + str(tok) + "\n").encode(),))
            raise SlackClientError("simulated %s failure" % kind)
        return _FakeResponse({"ok": True, "url": "wss://fake.slack.invalid/link"})
