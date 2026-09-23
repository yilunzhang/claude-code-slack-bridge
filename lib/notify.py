"""notify 发送核心 —— 从 bin/notifyctl.py 抽出为 lib 包模块,供两个入口复用:
  ① notifyctl CLI(agent 主动 blocker 通知);② StopFailure hook(hooklib.run_stop_failure,API 错误告警)。

agent/hook 主动给「本 session 绑定的飞书群」发通知,系统自动前缀 @owner
(独立 `at` 节点,穿透免打扰),凸显"需 owner 决策/授权的 blocker"或"本轮 API 错误"。

**定位(关键假设,别 overdesign)**:个人轻量工具、单用户、群内可信;agent/hook = 可信主体
(主动出站,不经审批门)。**不是 fail-open 装饰**:发送失败/未知**如实返回、不吞**;
只有前置条件(空消息 / 无绑定)才 exit 0 + ok:false。直发(非 daemon 队列),但发前尊重
同款门(allowlist + outbound_gate + session 三元组 + owner/chat_id/argv 校验)。

run_notify 全依赖注入(stdin_text/environ/prober/start_pid/make_runner)→ 纯逻辑、零真实
网络/进程副作用外呼;CLI 与 hook 共用同一硬化路径(零逻辑复制)。
"""
import json
import re

from . import config as configmod
from . import constants, db, paths, procs, runner as runner_mod, util

# owner_open_id 白名单形态(codex r3 B1 + r4 MINOR):**fullmatch** 而非 `re.match("^...$")`——
# 后者 `$` 会放过尾换行 `ou_owner\n`。拒 "all"(真 @全员)/ 引号 / `<`/`>`(闭标签注入)/ 空白 / 尾换行。
OWNER_RE = re.compile(r"ou_[A-Za-z0-9_-]{1,64}")
CHAT_RE = re.compile(r"oc_[A-Za-z0-9_-]{1,64}")
# 拒绝正文里字面 `<at`(真 mention 面):`<at` 后接空白或 `>`(精确命中 mention 标签,不误伤 "<atlas>")。
AT_RE = re.compile(r"<at[\s>]", re.IGNORECASE)

# 本地 pre-API 失败类型(参数/配置/登录/scope 缺失,消息 API 未执行)= 确定未发。
DETERMINISTIC_ERROR_TYPES = frozenset({"validation", "config", "authentication", "authorization"})

LARK_BIN = "lark-cli"
SEND_TIMEOUT_S = constants.SEND_TIMEOUT_S


def _post_content(owner, msg):
    """结构化 post content(codex impl MAJOR2):owner mention = **独立 at 节点**
    `{"tag":"at","user_id":owner}` —— 永远是真 mention,**不受正文畸形标签(如 `<b>`)影响**
    (旧 `--text` 里 `<at>前缀 + 正文` 会被畸形标签整段按原文渲染、mention 失效却仍返回 message_id)。

    正文 = 独立 **md 节点**(2026-07-27 修 bug:旧 `text` 节点是**字面文本**,markdown 不渲染 ——
    turn 转发走 `--markdown` 能渲染,而 notify/StopFailure 走 post `text` 节点不能,两条出站路径
    观感不一致;owner 看到的是生的 `## 标题` / `**粗体**`)。

    **两个段落**(外层 list 两个元素)而非同一段落:飞书文档明确 `md` 标签"独占整段、不能与其它
    标签同行"。实测同段落(at+md 并列)服务端也会把 mention 拆到自己一行、渲染正常,但那是依赖
    宽松的服务端拆分行为;按文档取两段落的显式形态,且不会像同段落那样在正文前留一个多余空格。

    **安全**(与 `text` 节点相比新增的面,均已覆盖):md 会**真的渲染** `<at user_id=...>` 成活
    mention(text 节点不会),故正文里字面 `<at` 的拒绝(步1 AT_RE)从"防内联正规化"升级为**承重
    守卫**——它是阻止正文自行 @全员的唯一闸门,别删。md 的图片语法只认**已上传的 image key**
    (`![](img_v3_xxx)`),不认 URL,故**不引入 lark-cli `--markdown` 那条从本机抓 URL 图片的
    SSRF 面**(那条是 turn 转发路径的已知取舍,见 outbound.py)。链接/自动链接会变成可点链接
    (渲染面,非抓取面)。正文来源=本机 agent/hook(可信主体),群内可信 → 接受。"""
    return {"zh_cn": {"content": [
        [{"tag": "at", "user_id": owner}],
        [{"tag": "md", "text": msg}],
    ]}}


def _wire_argv(profile, chat_id, owner, msg, wire_key):
    """完整 wire argv,与 LarkRunner.build_argv 同构(`lark_bin` 前缀 + 末尾 `--profile <profile>`)。
    发送用 `--msg-type post --content <json>`(结构化 at 节点隔离 mention 与正文)。8d 编码门校验此
    **完整** list(含 content_json 与追加的 profile);发送走 `runner.run(send_args)`。返回 (send_args, full_argv)。"""
    content_json = json.dumps(_post_content(owner, msg), ensure_ascii=False)
    send_args = ["im", "+messages-send", "--as", "bot",
                 "--chat-id", chat_id, "--msg-type", "post",
                 "--content", content_json, "--idempotency-key", wire_key]
    full_argv = [LARK_BIN] + list(send_args) + ["--profile", profile]
    return send_args, full_argv


def _env_retryable(env):
    """lark-cli 官方 error.retryable(稳定字段)→ bool;顶层或 `.error` 下(codex impl r3 MINOR)。"""
    if not isinstance(env, dict):
        return False
    err = env.get("error")
    if isinstance(err, dict) and err.get("retryable") is not None:
        return bool(err.get("retryable"))
    return bool(env.get("retryable"))


def _classify_send(res):
    """发送结果三态 → (obj, exit_code)。判定序(与 lark-cli 错误契约对齐):
      ① message_id → sent:true(不看 rc/timed_out);② res.exc(Popen 没起)→ sent:false;
      ③ **error.type=='network'(HTTP 5xx 等,POST 已发出、可能已落库)→ unknown**——**先于数字码**;
      ④ 有 numeric 业务码(非 network = API 业务拒绝 = 消息未创建)→ sent:false(retryable=官方 error.retryable
        ∨ 本地 TRANSIENT);⑤ 无码但 error.type 在确定集(本地 pre-API 失败)→ sent:false;⑥ 其余 → unknown。"""
    env = runner_mod.parse_result(res)  # stdout → 空/不可解析回退 stderr(E4a)
    # ① message_id 优先:不看 rc、不看 timed_out。
    if runner_mod.envelope_ok(env):
        mid = runner_mod.data_of(env).get("message_id")
        if mid:
            return ({"ok": True, "sent": True, "message_id": mid}, 0)
    # ② res.exc = Popen 启动失败 = 子进程没起来 = 确定未发。
    if res.exc is not None:
        return ({"ok": False, "sent": False, "reason": "send-failed",
                 "detail": "start-failure"}, 4)
    # ③ 网络/传输错误(HTTP 5xx 等)→ unknown。**必须先于数字码**(codex impl r3 MAJOR:lark-cli 把
    #    HTTP 5xx 输出成 {type:"network", code:500, retryable:true}——POST 已发出、不能证明服务端未落消息,
    #    归 sent:false 会让用户重试造成重复@)。
    #    **已知限制(codex impl r4 MAJOR)**:type:network 也可能来自"POST 根本没开始"(如 TAT 获取失败,
    #    确定未发、本可安全重试)——但 lark-cli 信封不带失败阶段信息,无法与"POST 已发出"区分。取保守
    #    unknown(最坏=用户看群后手动重发一次,绝不重复@)。修需 lark-cli 出 phase 字段或拆 token 预检
    #    (每次多一往返),对个人轻量工具过重,不做。
    etype = runner_mod.envelope_error_type(env)  # 顶层 type / 嵌套 .error.type
    if etype == "network":
        return ({"ok": False, "sent": "unknown", "reason": "send-unknown",
                 "message": "网络/服务端错误,可能已发送,重试前先看群"}, 5)
    # ④ 有 numeric 业务码(非 network = API 业务拒绝 = 消息未创建)→ sent:false。不必枚举每个失败码(打地鼠);
    #    得 message_id 才是成功(①已判),故任何非 network 业务码一律 sent:false。**retryable 纯信 lark-cli
    #    官方 error.retryable 字段**(codex impl r4 MINOR:本地 TRANSIENT 集里 99991661 实为 token_missing
    #    retryable:false、误标会造无依据重试;官方字段才权威。仅用它,不再 OR 本地集)。
    code = runner_mod.envelope_error_code(env)  # 顶层 code / 嵌套 .error.code 双形状
    if code is not None:
        try:
            icode = int(code)
        except (TypeError, ValueError):
            icode = None
        if icode is not None:
            if _env_retryable(env):
                return ({"ok": False, "sent": False, "retryable": True,
                         "reason": "send-rejected", "code": icode,
                         "message": "瞬态/限频拒绝,消息未创建;可稍后同幂等键重试"}, 4)
            return ({"ok": False, "sent": False, "reason": "send-failed", "code": icode}, 4)
        # code 存在但非数值(罕见)→ 落 type/unknown 判定
    # ⑤ 无数字码但 error.type 在确定集(本地 pre-API 失败:参数/配置/登录/scope)→ 确定未发。
    if etype in DETERMINISTIC_ERROR_TYPES:
        return ({"ok": False, "sent": False, "reason": "send-failed", "detail": etype}, 4)
    # ⑥ 其余(ok 无 message_id / 超时无 id / 信封不可解析 / api-unknown 含 keychain 无 code)→ unknown。
    return ({"ok": False, "sent": "unknown", "reason": "send-unknown",
             "message": "可能已发送,重试前先看群"}, 5)


def run_notify(*, stdin_text, environ, prober, start_pid, make_runner):
    """纯逻辑(全依赖注入,零真实网络/进程副作用外呼)→ (obj, exit_code)。
    线性化点 = `may_have_sent`:进 runner.run 前所有 argv 源已校验干净才置位;此后任何未分类异常
    一律 `sent:"unknown"`(绝不降 sent:false 免重复 @);置位前异常 = 确定未发 → internal-error。"""
    may_have_sent = False
    try:
        # 1. 读消息(前置)。strip 只用于判空与去两端空白;<at / NUL 在进 run() 前拒。
        msg = (stdin_text or "").strip()
        if not msg:
            return ({"ok": False, "sent": False, "reason": "empty-message"}, 0)
        if AT_RE.search(msg):  # search 非 match:任意偏移的 <at 都拒(BLOCKER3)
            return ({"ok": False, "sent": False, "reason": "invalid-mention",
                     "detail": "消息正文含 <at> 标签(会触发真实 @),已拒绝——请去掉后重发"}, 3)
        if "\x00" in msg:  # NUL 会在 Popen 抛 ValueError(非 res.exc);发送前拒=确定未发
            return ({"ok": False, "sent": False, "reason": "invalid-input",
                     "detail": "消息含 NUL 字节"}, 3)

        # 2. session id(BLOCKER2:锁定"本 session")。
        session_id = environ.get("CLAUDE_CODE_SESSION_ID")
        if not session_id:
            return ({"ok": False, "sent": False, "reason": "session-unresolved",
                     "detail": "环境缺 CLAUDE_CODE_SESSION_ID,无法锁定本 session"}, 3)

        # 3. config(ConfigError=缺文件/缺 REQUIRED_KEY;malformed JSON/IO 错 → 外层兜 internal-error)。
        cfg = configmod.require_config()

        # 4. cc 实例(从进程树上溯)。
        inst = procs.find_cc_instance(prober, start_pid)
        if inst is None:
            return ({"ok": False, "sent": False, "reason": "instance-unresolved",
                     "detail": "无法从进程树解析 CC 实例"}, 3)
        cc_pid, cc_start = inst

        # 5. db 文件不存在 → not-bound(**connect 前判**——connect 会建空库)。
        db_file = paths.db_path()
        if not db_file.exists():
            return ({"ok": False, "sent": False, "reason": "not-bound",
                     "detail": "本 session 未绑定任何飞书群(无 bridge.db)"}, 0)
        conn = db.connect(db_file)
        try:
            # 5b. schema 门(只读;绝不 init_schema 建空库)。missing 行/值不符都 fail-closed。
            db.check_schema(conn)
            # 6. 三元组 + status='active' 查绑定(session_id ∧ cc_pid ∧ cc_start ∧ active 缺一不可)。
            row = conn.execute(
                "SELECT chat_id, binding_id FROM bindings "
                "WHERE session_id=? AND cc_pid=? AND cc_start=? AND status='active'",
                (session_id, cc_pid, cc_start)).fetchone()
            if row is None:
                return ({"ok": False, "sent": False, "reason": "not-bound",
                         "detail": "本 session 未绑定任何飞书群"}, 0)
            chat_id = row["chat_id"]

            # 7. allowlist 重查(**类型优先、后看空值** —— 别写 `if allow:`,否则 ""/{}/false/0 被当"不限制")。
            allow = cfg.get("chat_allowlist")
            if allow is None:
                pass  # 合法"不限制"之一
            elif not isinstance(allow, list) or not all(isinstance(x, str) for x in allow):
                return ({"ok": False, "sent": False, "reason": "invalid-config",
                         "detail": "chat_allowlist 畸形(须为字符串列表)"}, 3)
            elif allow and chat_id not in allow:
                return ({"ok": False, "sent": False, "reason": "chat-not-allowed",
                         "detail": "绑定群不在 chat_allowlist 内"}, 3)
            # allow == [] → 空 list = 另一合法"不限制"

            # 8. 身份门(fail-closed,**不给默认值**——缺行=None 也拒;绝不 `get_state(...,"ok")`)。
            gate = db.get_state(conn, "outbound_gate")
            if gate != "ok":
                return ({"ok": False, "sent": False, "reason": "gate-degraded",
                         "detail": "出站身份门非 ok(%r);身份未验证,拒绝直发" % (gate,)}, 3)

            # 8b. owner 校验(防前缀自身 @全员/闭标签注入)。
            owner = cfg.get("owner_open_id")
            if not isinstance(owner, str) or not OWNER_RE.fullmatch(owner):
                return ({"ok": False, "sent": False, "reason": "invalid-owner",
                         "detail": "owner_open_id 非法(防 @全员/注入)"}, 3)

            # 8c. chat_id 格式(bind 不验格式;lark-cli Validate 阶段拒但无 numeric code → 先拦免错归 unknown)。
            if not isinstance(chat_id, str) or not CHAT_RE.fullmatch(chat_id):
                return ({"ok": False, "sent": False, "reason": "invalid-binding",
                         "detail": "绑定 chat_id 格式非法"}, 3)

            profile = cfg.get("profile")
            wire_key = util.short_key(util.new_id())  # 每次新幂等键

            # 8d. 完整 argv 编码门(总兜底:NUL/孤立 surrogate/非 str 都会在 Popen 启动前抛,非 res.exc)。
            #     owner 结构化进 at 节点、msg 进 md 节点(见 _post_content),整体在 content_json 里一并校验。
            send_args, full_argv = _wire_argv(profile, chat_id, owner, msg, wire_key)
            for e in full_argv:
                if not isinstance(e, str) or "\x00" in e:
                    return ({"ok": False, "sent": False, "reason": "invalid-argv",
                             "detail": "argv 元素非字符串或含 NUL"}, 3)
                try:
                    e.encode("utf-8")
                except (UnicodeEncodeError, UnicodeDecodeError):
                    return ({"ok": False, "sent": False, "reason": "invalid-argv",
                             "detail": "argv 元素含不可编码字符(孤立 surrogate)"}, 3)

            # 9. 发送(线性化点:置位后所有 argv 源已校验干净)。
            runner = make_runner(profile)
            may_have_sent = True
            res = runner.run(send_args, timeout_s=SEND_TIMEOUT_S)
            obj, code = _classify_send(res)
            if obj.get("sent") is True:
                obj["chat_id"] = chat_id
            return (obj, code)
        finally:
            conn.close()
    except configmod.ConfigError as e:
        # 缺文件/缺 REQUIRED_KEY(恒在发送前)→ 结构化 exit 3。
        return ({"ok": False, "sent": False, "reason": "config", "detail": str(e)}, 3)
    except db.SchemaMismatch as e:
        # schema 门(恒在发送前)→ 结构化 exit 3、零发送。
        return ({"ok": False, "sent": False, "reason": "schema-mismatch", "detail": str(e)}, 3)
    except Exception as e:  # noqa: BLE001 —— 绝不裸 traceback;按 may_have_sent 分主信号
        if may_have_sent:
            return ({"ok": False, "sent": "unknown", "reason": "internal-error-after-send",
                     "detail": str(e)}, 5)
        return ({"ok": False, "sent": False, "reason": "internal-error", "detail": str(e)}, 3)
