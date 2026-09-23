"""Test fakes: clock, lark-cli runner, process prober. 不 mock 被测逻辑本身。"""
import json


class FakeClock:
    """wall/mono 独立可拨,支持时钟回拨测试。单位 ms。"""

    def __init__(self, wall=1_000_000_000_000, mono=50_000):
        self.wall = wall
        self.mono = mono

    def wall_ms(self):
        return self.wall

    def mono_ms(self):
        return self.mono

    def tick(self, ms):
        """正常流逝:两钟同进。"""
        self.wall += ms
        self.mono += ms

    def rewind_wall(self, ms):
        """墙钟回拨(单调钟继续走)。"""
        self.wall -= ms


class FakeRunResult:
    def __init__(self, rc=0, stdout="", stderr="", timed_out=False, exc=None):
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.exc = exc


def ok_envelope(data, notice=None):
    env = {"ok": True, "data": data}
    if notice:
        env["_notice"] = notice
    return FakeRunResult(0, json.dumps(env))


def err_envelope(code, msg="err"):
    return FakeRunResult(1, json.dumps({"ok": False, "code": code, "msg": msg}))


def stderr_err_envelope(code, subtype="invalid_parameters", msg="field validation failed"):
    """E4a 真机形状:错误信封打在 stderr(stdout 空),code 嵌套在 .error.code。"""
    return FakeRunResult(
        1, "", json.dumps({"ok": False, "error": {
            "type": "api", "subtype": subtype, "code": code, "message": msg}},
            ensure_ascii=False))


def network_err_envelope(code=503, retryable=True, subtype="server_error"):
    """真机 503/网络类错误形状(daemon.log 实证):错误信封在 stderr,
    error.type=network、error.retryable=true、error.code=5xx。"""
    err = {"type": "network", "subtype": subtype, "code": code, "message": f"HTTP {code}: "}
    if retryable is not None:
        err["retryable"] = retryable
    return FakeRunResult(4, "", json.dumps({"ok": False, "error": err}, ensure_ascii=False))


class FakeRunner:
    """可注入 lark-cli runner。responders: list of (predicate(args)->bool, fn(args, cwd)->FakeRunResult)。
    未匹配调用默认抛 AssertionError(离线铁律:绝不静默放过未预期的外呼)。
    按 LarkRunner 真实语义模拟 --profile 拖尾(E1:no_profile=True 不拖尾)。"""

    def __init__(self, profile="main"):
        self.profile = profile
        self.calls = []  # [(args, cwd)]
        self.responders = []
        self.default = None  # 若设置,未匹配时返回

    def on(self, predicate, fn):
        self.responders.append((predicate, fn))
        return self

    def on_prefix(self, prefix, fn):
        prefix = list(prefix)

        def pred(args):
            return list(args[: len(prefix)]) == prefix

        return self.on(pred, fn)

    def run(self, args, timeout_s=None, cwd=None, no_profile=False):
        args = list(args)
        if not no_profile:
            args = args + ["--profile", self.profile]
        self.calls.append((args, cwd))
        for pred, fn in self.responders:
            if pred(args):
                res = fn(args, cwd) if callable(fn) else fn
                return res
        if self.default is not None:
            return self.default(args, cwd) if callable(self.default) else self.default
        raise AssertionError(f"unexpected lark-cli call in offline test: {args}")

    def calls_matching(self, *prefix):
        prefix = list(prefix)
        return [c for c in self.calls if c[0][: len(prefix)] == prefix]


class FakeProber:
    """pid → (ppid, lstart, comm);缺席=进程不存在;raising=探测异常(UNKNOWN)。"""

    def __init__(self):
        self.table = {}
        self.raising = False

    def set(self, pid, ppid, lstart, comm):
        self.table[pid] = (ppid, lstart, comm)

    def remove(self, pid):
        self.table.pop(pid, None)

    def get(self, pid):
        if self.raising:
            raise RuntimeError("simulated ps failure")
        return self.table.get(pid)


def mget_snapshot(message_id, chat_id, sender_id, msg_type="text", text="hi",
                  mentions=(), sender_type="user", content=None, reply_to=None):
    """构造 +messages-mget 的 .data.messages[] 单条 —— **真实形状**(E3 真机样例):
    正文在顶层 `content`(lark-cli 已渲染的纯文本,mention 以 "@{name}" 内联);
    **没有** body.content raw-API 形状。text 参数=去掉 @ 前缀的消息本体,
    渲染 content 自动拼 "@{name} " 前缀(与真机 "@Yilun's agent e2e-2 …" 一致)。"""
    mentions = list(mentions)
    if content is None:
        prefix = "".join(f"@{m.get('name', '?')} " for m in mentions)
        if msg_type in ("text", "post"):
            content = prefix + text
        elif msg_type == "image":
            content = "[图片]"
        elif msg_type == "file":
            content = "(文件) a.pdf"
        else:
            content = ""
    snap = {
        "message_id": message_id,
        "chat_id": chat_id,
        "msg_type": msg_type,
        "sender": {"id": sender_id, "id_type": "open_id", "sender_type": sender_type},
        "content": content,
        "mentions": mentions,
    }
    # 真机:非回复的消息**根本没有 reply_to 键**(不是 null)—— 保持同形,
    # 否则「用 `in snap` 判定」的实现会被 fixture 惯出来的 null 键蒙混过关。
    if reply_to is not None:
        snap["reply_to"] = reply_to
    return snap


def raw_body_snapshot(message_id, chat_id, sender_id, msg_type="text", text="hi",
                      mentions=(), sender_type="user"):
    """旧 raw-API body.content 形状(双形状容忍的 fallback 路径专用)。"""
    if msg_type == "text":
        content = {"text": text}
    elif msg_type == "image":
        content = {"image_key": "img_k1"}
    elif msg_type == "file":
        content = {"file_key": "file_k1", "file_name": "a.pdf"}
    elif msg_type == "post":
        content = {"title": "t", "content": [[{"tag": "text", "text": text}]]}
    else:
        content = {}
    return {
        "message_id": message_id,
        "chat_id": chat_id,
        "msg_type": msg_type,
        "sender": {"id": sender_id, "id_type": "open_id", "sender_type": sender_type},
        "body": {"content": json.dumps(content, ensure_ascii=False)},
        "mentions": list(mentions),
    }


def bot_mention(app_id, key="@_user_1", name="TestBot"):
    return {"key": key, "id": app_id, "id_type": "app_id", "name": name}


def user_mention(open_id, key="@_user_2", name="Some One"):
    return {"key": key, "id": open_id, "id_type": "open_id", "name": name}
