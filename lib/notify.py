"""notify 发送核心 —— 供两个入口复用:
  ① notifyctl CLI(agent 主动 blocker 通知);② StopFailure hook(hooklib.run_stop_failure,API 错误告警)。

agent/hook 主动给「本 session 绑定的 Slack 会话」发一条 @owner 的通知(Block Kit:section mrkdwn
`<@owner>` + markdown 块正文),凸显"需 owner 决策/授权的 blocker"或"本轮 API 错误"。

**定位(关键假设,别 overdesign)**:个人轻量工具、单用户、会话内可信;agent/hook = 可信主体
(主动出站,不经审批门)。**不是 fail-open 装饰**:发送失败/未知**如实返回、不吞**;只有前置条件
(空消息 / 无绑定)才 exit 0 + ok:false。直发(非 daemon 队列),但发前尊重同款门
(contracts §0 I2 / §7):allowlist + `outbound_gate=="ok"` + **凭据版本**(tokens.json 当前版本必须
== `outbound_gate_tokens_version`,否则 `credentials-unverified`;先取 tokens 快照,再用**一条 SELECT**
联合读 gate 与版本,R1-M4)+ session 三元组 + owner/chat_id 校验;
方法级冷却由 `SlackClient.call` 统一处理 → `cooldown`。

run_notify 全依赖注入(stdin_text/environ/prober/start_pid/make_client)→ 纯逻辑、零真实网络/进程
副作用外呼;CLI 与 hook 共用同一硬化路径(零逻辑复制)。token 只在 0600 文件,绝不进输出/日志。"""
import re

from . import config as configmod
from . import constants, db, paths, procs, util
from .slackapi import DaemonStateCooldownStore, SlackClient, classify_send_error

# Slack user id 形态(U… / W…);fullmatch 而非 `^…$`(后者 `$` 会放过尾换行)。
OWNER_RE = re.compile(r"[UW][A-Z0-9]{2,32}")
# 会话 id:频道 C… / 私有频道 G… / DM D…
CHAT_RE = re.compile(r"[CGD][A-Z0-9]{2,32}")
# 拒绝正文里字面 `<!`(Slack 特殊 mention 前缀:<!channel> <!here> <!everyone> <!subteam^…> <!date…>)。
# markdown 块会真渲染 mrkdwn 特殊标记 → 这是阻止正文自行 @全员的**唯一**本地闸门,别删。
BROADCAST_RE = re.compile(r"<!")

SEND_TIMEOUT_S = constants.SEND_TIMEOUT_S
MAX_BODY_CHARS = constants.CHUNK_LIMIT   # markdown 块 / 单条消息正文上限(与 session_turn 同源)


def build_wire_params(chat_id, owner, msg):
    """chat.postMessage 参数(contracts / plan「控制面」):
    - `text` = 通知栏回退(含 owner mention);
    - `blocks[0]` = section mrkdwn `<@owner>`(独立块,mention 不受正文影响);
    - `blocks[1]` = markdown 块 `{type:"markdown", text: body}`(正文按标准 markdown 渲染);
    - `unfurl_links/unfurl_media=False`。"""
    return {
        "channel": chat_id,
        "text": "<@%s> %s" % (owner, msg),
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "<@%s>" % owner}},
            {"type": "markdown", "text": msg},
        ],
        "unfurl_links": False,
        "unfurl_media": False,
    }


def default_make_client(tokens, version, cooldown_store):
    """生产 client 工厂:token 来自已读取并核对过版本的 tokens.json 快照(不再读文件,版本即快照版本)。"""
    return SlackClient(token=tokens["bot_token"], cooldown_store=cooldown_store,
                       timeout_s=SEND_TIMEOUT_S, tokens_version=version)


def classify_send(res, chat_id):
    """CallResult → (obj, exit_code)。三态主信号 `sent`:True / False / "unknown"。
      sent(ok ∧ ts)→ sent:true exit 0;
      wait(本地冷却,请求未发出)→ sent:false reason=cooldown retryable exit 4;
      ratelimited(429)→ sent:false reason=ratelimited retryable exit 4;
      not_sent(写出前本地错 / NOT_SENT_ERRORS 鉴权族)→ sent:false reason=send-failed exit 4
        (本地网络错 retryable:true;鉴权族 retryable:false —— 门由 FingerprintGate 收);
      failed(PERMANENT_SEND_ERRORS)→ sent:false reason=send-failed exit 4;
      其余(超时 / 传输错 / 5xx / AMBIGUOUS / 不可解析 / ok 无 ts)→ sent:"unknown" exit 5(看会话别乱重试)。"""
    cls = classify_send_error(res)
    if cls == "sent":
        ts = res.get("ts")
        channel = res.get("channel") or chat_id
        if ts:
            return ({"ok": True, "sent": True, "message_id": "%s:%s" % (channel, ts),
                     "chat_id": chat_id}, 0)
        return ({"ok": False, "sent": "unknown", "reason": "send-unknown",
                 "detail": "ok 但无 ts", "message": "可能已发送,重试前先看会话"}, 5)
    if cls == "wait":
        return ({"ok": False, "sent": False, "reason": "cooldown", "retryable": True,
                 "cooldown_until": res.cooldown_until,
                 "detail": "chat.postMessage 处于方法级冷却(此前被 429),请求未发出"}, 4)
    if cls == "ratelimited":
        return ({"ok": False, "sent": False, "reason": "ratelimited", "retryable": True,
                 "retry_after": res.retry_after,
                 "detail": "Slack 429,已发布冷却;稍后重试"}, 4)
    if cls == "not_sent":
        auth_family = res.error in constants.NOT_SENT_ERRORS
        return ({"ok": False, "sent": False, "reason": "send-failed", "error": res.error,
                 "retryable": not auth_family,
                 "detail": ("鉴权被拒(消息未创建)" if auth_family
                            else "请求写出前本地失败(消息未创建)")}, 4)
    if cls == "failed":
        return ({"ok": False, "sent": False, "reason": "send-failed", "error": res.error,
                 "retryable": False, "detail": "Slack 明确拒绝(消息未创建)"}, 4)
    return ({"ok": False, "sent": "unknown", "reason": "send-unknown", "error": res.error,
             "message": "网络/服务端结果不确定,可能已发送,重试前先看会话"}, 5)


class GateContext:
    """open_gated_context 的结果:已打开的 conn(调用方负责 close)+ 本 session 绑定的 chat_id + 凭据快照。"""

    def __init__(self, conn, chat_id, cfg, owner, tokens, file_version, session_id):
        self.conn = conn
        self.chat_id = chat_id
        self.cfg = cfg
        self.owner = owner
        self.tokens = tokens
        self.file_version = file_version
        self.session_id = session_id


def open_gated_context(*, environ, prober, start_pid, secrets):
    """notify / sendfile 共用的出站门(contracts §0 I2 / §7),顺序固定:
    session id → config → cc 实例 → db 存在 → schema → 三元组 + active 绑定 → allowlist → tokens 快照
    → 身份门 + 凭据版本(一条 SELECT 联合读,R1-M4)→ owner 格式 → chat_id 格式。
    返回 `(ctx, None)`(ctx.conn 已打开,**调用方负责 close**)或 `(None, (obj, exit_code))`(conn 已关)。
    读到的 token 追加进 `secrets`(供调用方遮蔽异常文本)。ConfigError / SchemaMismatch / 其它异常原样抛出,
    由调用方统一映射(抛出前 conn 已关)。"""
    session_id = environ.get("CLAUDE_CODE_SESSION_ID")
    if not session_id:
        return None, ({"ok": False, "sent": False, "reason": "session-unresolved",
                       "detail": "环境缺 CLAUDE_CODE_SESSION_ID,无法锁定本 session"}, 3)

    cfg = configmod.require_config()

    inst = procs.find_cc_instance(prober, start_pid)
    if inst is None:
        return None, ({"ok": False, "sent": False, "reason": "instance-unresolved",
                       "detail": "无法从进程树解析 CC 实例"}, 3)
    cc_pid, cc_start = inst

    db_file = paths.db_path()
    if not db_file.exists():
        return None, ({"ok": False, "sent": False, "reason": "not-bound",
                       "detail": "本 session 未绑定任何 Slack 会话(无 bridge.db)"}, 0)
    conn = db.connect(db_file)
    try:
        ctx, err = _gate_in_conn(conn, cfg, session_id, cc_pid, cc_start, secrets)
    except BaseException:
        conn.close()
        raise
    if err is not None:
        conn.close()
        return None, err
    return ctx, None


def _gate_in_conn(conn, cfg, session_id, cc_pid, cc_start, secrets):
    db.check_schema(conn)
    row = conn.execute(
        "SELECT chat_id, binding_id FROM bindings "
        "WHERE session_id=? AND cc_pid=? AND cc_start=? AND status='active'",
        (session_id, cc_pid, cc_start)).fetchone()
    if row is None:
        return None, ({"ok": False, "sent": False, "reason": "not-bound",
                       "detail": "本 session 未绑定任何 Slack 会话"}, 0)
    chat_id = row["chat_id"]

    allow = cfg.get("chat_allowlist")
    if allow is None:
        pass
    elif not isinstance(allow, list) or not all(isinstance(x, str) for x in allow):
        return None, ({"ok": False, "sent": False, "reason": "invalid-config",
                       "detail": "chat_allowlist 畸形(须为字符串列表)"}, 3)
    elif allow and chat_id not in allow:
        return None, ({"ok": False, "sent": False, "reason": "chat-not-allowed",
                       "detail": "绑定会话不在 chat_allowlist 内"}, 3)

    try:
        tokens, file_version = configmod.load_tokens(allow_env=False)
    except configmod.ConfigError as e:
        return None, ({"ok": False, "sent": False, "reason": "credentials-unverified",
                       "detail": "tokens.json 不可用:%s" % util.redact_secrets(str(e), secrets)}, 3)
    secrets.extend(v for v in tokens.values() if isinstance(v, str))

    st = db.get_states(conn, (constants.GATE_KEY, constants.GATE_VERSION_KEY,
                              constants.TOKENS_VERSION_SEEN_KEY))
    gate = st.get(constants.GATE_KEY)
    gate_version = st.get(constants.GATE_VERSION_KEY)
    if gate != "ok":
        return None, ({"ok": False, "sent": False, "reason": "gate-degraded",
                       "detail": "出站身份门非 ok(%r,绑定版本 %r);身份未验证,拒绝直发"
                                 % (gate, gate_version)}, 3)
    if not gate_version or gate_version != file_version:
        return None, ({"ok": False, "sent": False, "reason": "credentials-unverified",
                       "detail": ("tokens.json 版本与 daemon 已验证版本不一致"
                                  "(文件 %s ≠ outbound_gate_tokens_version %s);"
                                  "等 daemon 重验后再试" % (file_version, gate_version))}, 3)

    owner = cfg.get("owner_user_id")
    if not isinstance(owner, str) or not OWNER_RE.fullmatch(owner):
        return None, ({"ok": False, "sent": False, "reason": "invalid-owner",
                       "detail": "owner_user_id 非法(防 @全员/注入)"}, 3)

    if not isinstance(chat_id, str) or not CHAT_RE.fullmatch(chat_id):
        return None, ({"ok": False, "sent": False, "reason": "invalid-binding",
                       "detail": "绑定 chat_id 格式非法"}, 3)

    return GateContext(conn, chat_id, cfg, owner, tokens, file_version, session_id), None


def run_notify(*, stdin_text, environ, prober, start_pid, make_client):
    """纯逻辑(全依赖注入,零真实网络/进程副作用外呼)→ (obj, exit_code)。
    线性化点 = `may_have_sent`:进 client.call 前所有参数源已校验干净才置位;此后任何未分类异常
    一律 `sent:"unknown"`(绝不降 sent:false 免重复 @);置位前异常 = 确定未发 → internal-error。
    `make_client(tokens, version, cooldown_store) -> client`(生产 = default_make_client)。
    出站门(session → 绑定 → allowlist → 凭据版本 → owner/chat)由 `open_gated_context` 提供,
    与 sendfile 共用同一硬化路径。"""
    may_have_sent = False
    secrets = []   # 已读到的 token(仅用于遮蔽异常文本;R1-M5)

    def _detail(e):
        return util.redact_secrets(str(e), secrets)

    try:
        # 1. 读消息(前置)。strip 只用于判空与去两端空白;<! / NUL / 超长 在进 call 前拒。
        msg = (stdin_text or "").strip()
        if not msg:
            return ({"ok": False, "sent": False, "reason": "empty-message"}, 0)
        if BROADCAST_RE.search(msg):  # search 非 match:任意偏移的 <! 都拒
            return ({"ok": False, "sent": False, "reason": "invalid-mention",
                     "detail": "消息正文含 `<!`(Slack 广播/特殊 mention 前缀,如 <!channel>),"
                               "已拒绝——请去掉后重发"}, 3)
        if "\x00" in msg:
            return ({"ok": False, "sent": False, "reason": "invalid-input",
                     "detail": "消息含 NUL 字节"}, 3)
        try:
            msg.encode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return ({"ok": False, "sent": False, "reason": "invalid-input",
                     "detail": "消息含不可编码字符(孤立 surrogate)"}, 3)
        if len(msg) > MAX_BODY_CHARS:
            return ({"ok": False, "sent": False, "reason": "message-too-long",
                     "detail": "正文超过 %d 字符,请精简后重发" % MAX_BODY_CHARS}, 3)

        # 2–8. 出站门(共用)。
        ctx, err = open_gated_context(environ=environ, prober=prober, start_pid=start_pid,
                                      secrets=secrets)
        if err is not None:
            return err
        try:
            params = build_wire_params(ctx.chat_id, ctx.owner, msg)

            # 9. 发送(线性化点)。冷却存储 = daemon_state(与 daemon/probe 同一份)。
            client = make_client(ctx.tokens, ctx.file_version, DaemonStateCooldownStore(ctx.conn))
            may_have_sent = True
            res = client.call("chat.postMessage", params, timeout_s=SEND_TIMEOUT_S)
            return classify_send(res, ctx.chat_id)
        finally:
            ctx.conn.close()
    except configmod.ConfigError as e:
        return ({"ok": False, "sent": False, "reason": "config", "detail": _detail(e)}, 3)
    except db.SchemaMismatch as e:
        return ({"ok": False, "sent": False, "reason": "schema-mismatch", "detail": _detail(e)}, 3)
    except Exception as e:  # noqa: BLE001 —— 绝不裸 traceback;按 may_have_sent 分主信号
        # detail = 类型名 + 遮蔽后的文本(输出会进模型上下文,token 绝不能出现;R1-M5)
        if may_have_sent:
            return ({"ok": False, "sent": "unknown", "reason": "internal-error-after-send",
                     "detail": "%s: %s" % (type(e).__name__, _detail(e))}, 5)
        return ({"ok": False, "sent": False, "reason": "internal-error",
                 "detail": "%s: %s" % (type(e).__name__, _detail(e))}, 3)
