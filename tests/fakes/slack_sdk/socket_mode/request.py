"""对齐 slack_sdk.socket_mode.request.SocketModeRequest。"""


class SocketModeRequest:
    def __init__(self, type, envelope_id, payload, accepts_response_payload=None,
                 retry_attempt=None, retry_reason=None):
        self.type = type
        self.envelope_id = envelope_id
        self.payload = payload
        self.accepts_response_payload = accepts_response_payload or False
        self.retry_attempt = retry_attempt
        self.retry_reason = retry_reason

    @classmethod
    def from_dict(cls, d):
        if all(k in d for k in ("type", "envelope_id", "payload")):
            return cls(type=d.get("type"), envelope_id=d.get("envelope_id"), payload=d.get("payload"),
                       accepts_response_payload=d.get("accepts_response_payload") or False,
                       retry_attempt=d.get("retry_attempt"), retry_reason=d.get("retry_reason"))
        return None

    def to_dict(self):
        d = {"type": self.type, "envelope_id": self.envelope_id, "payload": self.payload,
             "accepts_response_payload": self.accepts_response_payload}
        if self.retry_attempt is not None:
            d["retry_attempt"] = self.retry_attempt
        if self.retry_reason is not None:
            d["retry_reason"] = self.retry_reason
        return d
