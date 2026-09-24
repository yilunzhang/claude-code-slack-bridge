"""sendfile 发送核心 —— `bin/sendfilectl.py` 的逻辑层:把**本机一个文件**上传并分享到本 session 绑定的
Slack 会话(build-in-public 桥的第三个受门控直发例外;另两个是 notify 与 StopFailure hook)。

门与 notify **完全同款**(`lib.notify.open_gated_context`):三元组精确命中本 session 的 active 绑定、
`chat_allowlist`、`outbound_gate=="ok"`、tokens.json 版本 == daemon 已验证版本、方法级冷却
(`SlackClient.call` 内)。token 只在 0600 文件里读进本进程,绝不进输出/日志。

上传走 Slack 新接口三步(`files.upload` 已废弃):
  ① `files.getUploadURLExternal(filename, length)`(读类方法 → `SlackClient` 自动表单编码)
  ② HTTP POST 文件原始字节到 `upload_url`(不带 token;`upload` 可注入)
  ③ `files.completeUploadExternal(files=[{id,title}], channel_id=绑定 chat_id, initial_comment)`
    (表单编码,`files` 由 `slackapi.form_fields` 序列化为 JSON 字符串)
主信号 `sent`:① / ② 失败 = 文件**确定没出现**在会话(`sent:false`);③ 是线性化点,结果不确定
(超时 / 5xx / 传输错)= `sent:"unknown"`(文件可能已分享,重试前先看会话)。"""
import os
import pathlib
import urllib.error
import urllib.request

from . import config as configmod
from . import constants, db, util
from .notify import BROADCAST_RE, MAX_BODY_CHARS, open_gated_context
from .slackapi import USER_AGENT, DaemonStateCooldownStore, SlackClient, classify_send_error

MAX_FILE_BYTES = 20 * 1024 * 1024      # 大小上限(先定 20MB)
UPLOAD_TIMEOUT_S = 120                 # ② 字节上传(20MB 也够)
SEND_TIMEOUT_S = constants.SEND_TIMEOUT_S
TITLE_MAX_CHARS = 200
COMMENT_MAX_CHARS = min(4000, MAX_BODY_CHARS)   # initial_comment 是普通消息正文,保守上限


def default_make_client(tokens, version, cooldown_store):
    return SlackClient(token=tokens["bot_token"], cooldown_store=cooldown_store,
                       timeout_s=SEND_TIMEOUT_S, tokens_version=version)


def default_upload(url, data, timeout_s):
    """② 把原始字节 POST 到 Slack 给的 upload_url(无 token)。返回 http_status(int);
    HTTPError 也转成状态码返回;其它网络异常原样抛出(调用方归为 upload-failed,确定未分享)。"""
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/octet-stream", "User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return int(getattr(resp, "status", None) or resp.getcode())
    except urllib.error.HTTPError as e:
        return int(e.code)


def _validate_path(path):
    """→ (pathlib.Path, name, size) 或 (None, (obj, code))。确定未发的输入拒绝一律 exit 3。"""
    if not isinstance(path, str) or not path:
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "缺 --path"}, 3)
    if "\x00" in path:
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "路径含 NUL 字节"}, 3)
    if not os.path.isabs(path):
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "--path 必须是绝对路径"}, 3)
    p = pathlib.Path(path)
    if not p.exists():
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "文件不存在:%s" % path}, 3)
    if not p.is_file():
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "不是普通文件:%s" % path}, 3)
    size = p.stat().st_size
    if size <= 0:
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "文件为空:%s" % path}, 3)
    if size > MAX_FILE_BYTES:
        return None, ({"ok": False, "sent": False, "reason": "file-too-large",
                       "detail": "文件 %d 字节,超过上限 %d 字节" % (size, MAX_FILE_BYTES),
                       "size": size, "limit": MAX_FILE_BYTES}, 3)
    return (p, p.name, size), None


def _validate_text(comment_text, title, default_title):
    """→ (comment, title) 或 (None, (obj, code))。comment 可空;title 缺省 = 文件名。"""
    comment = (comment_text or "").strip()
    if comment:
        if BROADCAST_RE.search(comment):
            return None, ({"ok": False, "sent": False, "reason": "invalid-mention",
                           "detail": "说明含 `<!`(Slack 广播/特殊 mention 前缀,如 <!channel>),已拒绝"}, 3)
        if "\x00" in comment:
            return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                           "detail": "说明含 NUL 字节"}, 3)
        try:
            comment.encode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                           "detail": "说明含不可编码字符(孤立 surrogate)"}, 3)
        if len(comment) > COMMENT_MAX_CHARS:
            return None, ({"ok": False, "sent": False, "reason": "message-too-long",
                           "detail": "说明超过 %d 字符" % COMMENT_MAX_CHARS}, 3)
    t = (title or "").strip() or default_title
    if "\x00" in t or "\n" in t or "\r" in t:
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "标题含控制字符"}, 3)
    try:
        t.encode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "标题含不可编码字符"}, 3)
    if len(t) > TITLE_MAX_CHARS:
        return None, ({"ok": False, "sent": False, "reason": "invalid-input",
                       "detail": "标题超过 %d 字符" % TITLE_MAX_CHARS}, 3)
    return (comment, t), None


def _stage_failed(stage, res, cls):
    """①/② 阶段的非成功结果 → 文件确定没出现在会话(sent:false)。exit 4;冷却/429 带 retryable。"""
    base = {"ok": False, "sent": False, "stage": stage}
    if cls == "wait":
        base.update(reason="cooldown", retryable=True, cooldown_until=res.cooldown_until,
                    detail="方法级冷却未到期(此前被 429),请求未发出")
    elif cls == "ratelimited":
        base.update(reason="ratelimited", retryable=True, retry_after=res.retry_after,
                    detail="Slack 429,已发布冷却;稍后重试")
    elif cls == "not_sent":
        auth_family = res.error in constants.NOT_SENT_ERRORS
        base.update(reason="send-failed", error=res.error, retryable=not auth_family,
                    detail=("鉴权被拒" if auth_family else "请求写出前本地失败"))
    elif cls == "failed":
        base.update(reason="send-failed", error=res.error, retryable=False,
                    detail="Slack 明确拒绝")
    else:  # unknown —— 但文件尚未分享,如实标 sent:false
        base.update(reason="send-failed", error=res.error, retryable=True,
                    detail="取 upload_url 结果不确定(超时/5xx/传输错);文件未分享,可重试")
    return base, 4


def classify_complete(res, chat_id, file_id):
    """③ files.completeUploadExternal 的结果 → 主信号(与 notify.classify_send 同一套语义)。"""
    cls = classify_send_error(res)
    if cls == "sent":
        return ({"ok": True, "sent": True, "file_id": file_id, "chat_id": chat_id}, 0)
    if cls == "wait":
        return ({"ok": False, "sent": False, "stage": "complete", "reason": "cooldown",
                 "retryable": True, "cooldown_until": res.cooldown_until,
                 "detail": "files.completeUploadExternal 处于方法级冷却,请求未发出"}, 4)
    if cls == "ratelimited":
        return ({"ok": False, "sent": False, "stage": "complete", "reason": "ratelimited",
                 "retryable": True, "retry_after": res.retry_after,
                 "detail": "Slack 429,已发布冷却;稍后整体重试(重新上传)"}, 4)
    if cls == "not_sent":
        auth_family = res.error in constants.NOT_SENT_ERRORS
        return ({"ok": False, "sent": False, "stage": "complete", "reason": "send-failed",
                 "error": res.error, "retryable": not auth_family,
                 "detail": ("鉴权被拒(未分享)" if auth_family else "请求写出前本地失败(未分享)")}, 4)
    if cls == "failed":
        return ({"ok": False, "sent": False, "stage": "complete", "reason": "send-failed",
                 "error": res.error, "retryable": False,
                 "detail": "Slack 明确拒绝(未分享;如 not_in_channel → 让用户 /invite)"}, 4)
    return ({"ok": False, "sent": "unknown", "stage": "complete", "reason": "send-unknown",
             "error": res.error, "file_id": file_id,
             "message": "网络/服务端结果不确定,文件可能已分享到会话,重试前先看会话"}, 5)


def run_sendfile(*, path, title, comment_text, environ, prober, start_pid, make_client,
                 upload=default_upload):
    """纯逻辑(全依赖注入)→ (obj, exit_code)。顺序:输入校验(确定未发)→ 出站门(共用)→ 读文件
    → ① 取 upload_url → ② 上传字节 → ③ 完成并分享(线性化点 `may_have_sent`)。"""
    may_have_sent = False
    secrets = []

    def _detail(e):
        return util.redact_secrets(str(e), secrets)

    try:
        info, err = _validate_path(path)
        if err is not None:
            return err
        p, name, size = info
        texts, err = _validate_text(comment_text, title, name)
        if err is not None:
            return err
        comment, title = texts

        ctx, err = open_gated_context(environ=environ, prober=prober, start_pid=start_pid,
                                      secrets=secrets)
        if err is not None:
            return err
        try:
            data = p.read_bytes()
            if not data or len(data) > MAX_FILE_BYTES:   # TOCTOU:以实际读到的为准
                return ({"ok": False, "sent": False,
                         "reason": "file-too-large" if data else "invalid-input",
                         "detail": "文件读取后 %d 字节(上限 %d)" % (len(data), MAX_FILE_BYTES)}, 3)

            client = make_client(ctx.tokens, ctx.file_version, DaemonStateCooldownStore(ctx.conn))

            # ① upload_url
            res = client.call("files.getUploadURLExternal",
                              {"filename": name, "length": len(data)}, timeout_s=SEND_TIMEOUT_S)
            cls = classify_send_error(res)
            if cls != "sent":
                return _stage_failed("upload-url", res, cls)
            upload_url, file_id = res.get("upload_url"), res.get("file_id")
            if not isinstance(upload_url, str) or not upload_url.startswith("https://") \
                    or not isinstance(file_id, str) or not file_id:
                return ({"ok": False, "sent": False, "stage": "upload-url", "reason": "send-failed",
                         "error": "malformed-upload-url", "retryable": True,
                         "detail": "files.getUploadURLExternal 返回缺 upload_url/file_id 或非 https"}, 4)

            # ② 字节上传(不带 token)
            try:
                status = upload(upload_url, data, UPLOAD_TIMEOUT_S)
            except Exception as e:  # noqa: BLE001 —— 网络类异常:确定未分享
                return ({"ok": False, "sent": False, "stage": "upload", "reason": "upload-failed",
                         "retryable": True,
                         "detail": "上传字节失败:%s: %s" % (type(e).__name__, _detail(e))}, 4)
            if not (200 <= int(status) < 300):
                return ({"ok": False, "sent": False, "stage": "upload", "reason": "upload-failed",
                         "http_status": int(status), "retryable": int(status) >= 500 or int(status) == 429,
                         "detail": "upload_url 返回 HTTP %d(未分享)" % int(status)}, 4)

            # ③ 完成 + 分享(线性化点)
            params = {"files": [{"id": file_id, "title": title}], "channel_id": ctx.chat_id}
            if comment:
                params["initial_comment"] = comment
            may_have_sent = True
            res = client.call("files.completeUploadExternal", params, timeout_s=SEND_TIMEOUT_S)
            return classify_complete(res, ctx.chat_id, file_id)
        finally:
            ctx.conn.close()
    except configmod.ConfigError as e:
        return ({"ok": False, "sent": False, "reason": "config", "detail": _detail(e)}, 3)
    except db.SchemaMismatch as e:
        return ({"ok": False, "sent": False, "reason": "schema-mismatch", "detail": _detail(e)}, 3)
    except Exception as e:  # noqa: BLE001
        if may_have_sent:
            return ({"ok": False, "sent": "unknown", "reason": "internal-error-after-send",
                     "detail": "%s: %s" % (type(e).__name__, _detail(e))}, 5)
        return ({"ok": False, "sent": False, "reason": "internal-error",
                 "detail": "%s: %s" % (type(e).__name__, _detail(e))}, 3)
