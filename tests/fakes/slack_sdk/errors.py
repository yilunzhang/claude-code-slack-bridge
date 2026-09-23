"""对齐 slack_sdk.errors 的最小集合。"""


class SlackClientError(Exception):
    pass


class SlackRequestError(SlackClientError):
    pass


class _FakeResponse:
    """对齐 SlackResponse 的最小接口:status_code / headers / data / ["error"] / get()。"""

    def __init__(self, data, status_code=200, headers=None):
        self.data = dict(data or {})
        self.status_code = int(status_code)
        self.headers = dict(headers or {})

    def __getitem__(self, key):
        return self.data[key]

    def get(self, key, default=None):
        return self.data.get(key, default)


class SlackApiError(SlackClientError):
    def __init__(self, message, response):
        msg = "%s\nThe server responded with: %s" % (message, getattr(response, "data", response))
        super().__init__(msg)
        self.response = response
