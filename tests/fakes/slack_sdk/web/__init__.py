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
            raise SlackClientError("simulated %s failure" % item.get("kind", "network"))
        return _FakeResponse({"ok": True, "url": "wss://fake.slack.invalid/link"})
