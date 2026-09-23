"""对齐 slack_sdk.socket_mode.response.SocketModeResponse。"""


class SocketModeResponse:
    def __init__(self, envelope_id, payload=None):
        self.envelope_id = envelope_id
        self.payload = payload

    def to_dict(self):
        d = {"envelope_id": self.envelope_id}
        if self.payload is not None:
            d["payload"] = self.payload
        return d
