"""Stop / SessionEnd / StopFailure hook 核心(plan 4.7 / 4.1.6)。
I5:**Stop/SessionEnd hook 不联网,只读写 bridge.db**(busy_timeout 有界);异常总则=统一抑制 +
hook_drops.log 固定文案一行(不含正文)+ 计数镜像;双双不可写 → stderr 固定告警。
**例外:StopFailure hook 会联网**——一轮因 API 错误结束时,经 `lib.notify.run_notify`(受同款
allowlist+身份门+session 三元组门控的直发)给绑定群发 @群主 告警(见 stop_failure_entry;fail-closed,
exit 0,不写心跳)。"""
import datetime
import os
import re
import sqlite3
import sys
import time

from . import constants, db, jobs, lifecycle, paths, procs, texts, util


def _touch_hook_heartbeat(event):
    """plugin 化:每次 hook 运行都写各自哨兵(先于 no-db 早退)=「plugin hooks 已生效」正向信号,
    供 preflight/bind 检测(替代读 settings.json 判断手装 hooks)。记 event/时间/plugin version/
    pkg_root(MAJOR 2:防「另一 install/旧版本/仅 SessionEnd」假阳性)。绝不抛异常(hook 永不因此失败)。"""
    try:
        paths.ensure_data_dir()
        from . import version as versionmod
        root, ver = versionmod.install_identity()
        util.atomic_write(paths.hook_heartbeat_path(event), util.jdumps({
            "event": event, "ts": int(time.time() * 1000),
            "plugin_version": ver, "pkg_root": root}))
    except Exception:
        pass


# ---------------------------------------------------------------- 入口(fail-closed 包装)
def stop_hook_entry(payload, conn=None, prober=None, clock=None, start_pid=None):
    _touch_hook_heartbeat("stop")
    own_conn = None
    try:
        if conn is None:
            dbf = paths.db_path()
            if not dbf.exists():
                return {"suppressed": False, "reason": "no-db"}
            own_conn = db.connect(dbf, busy_timeout_ms=constants.BUSY_TIMEOUT_HOOK_MS)
            conn = own_conn
        if prober is None:
            prober = procs.SystemProber()
        if clock is None:
            from .clock import SystemClock
            clock = SystemClock()
        return run_stop_hook(payload, conn=conn, prober=prober, clock=clock,
                             start_pid=start_pid)
    except Exception as e:
        _fail_closed_drop(e, hook="stop")
        return {"suppressed": True, "reason": "exception"}
    finally:
        if own_conn is not None:
            try:
                own_conn.close()
            except Exception:
                pass


def session_end_entry(payload, conn=None, prober=None, clock=None, start_pid=None):
    _touch_hook_heartbeat("session_end")
    own_conn = None
    try:
        if conn is None:
            dbf = paths.db_path()
            if not dbf.exists():
                return {"closed": [], "reason": "no-db"}
            # minor③:SessionEnd 关绑定是 **best-effort**(daemon cc_gone 兜底,非安全问题);
            # 用更短的等锁超时 → 锁竞争时快速让路,绝不拖住 CC 退出。
            own_conn = db.connect(dbf, busy_timeout_ms=constants.BUSY_TIMEOUT_SESSION_END_MS)
            conn = own_conn
        if prober is None:
            prober = procs.SystemProber()
        if clock is None:
            from .clock import SystemClock
            clock = SystemClock()
        return run_session_end(payload, conn=conn, prober=prober, clock=clock,
                               start_pid=start_pid)
    except Exception as e:
        _fail_closed_drop(e, hook="session_end")
        return {"closed": [], "reason": "exception"}
    finally:
        if own_conn is not None:
            try:
                own_conn.close()
            except Exception:
                pass


def _bump_drop_counter():
    try:
        # minor②:可观测计数用**短等锁**(BUSY_TIMEOUT_OBS_MS)——否则 SessionEnd 首次等锁失败(1.5s)
        # 后又用 3s 等锁补 counter = 总 ~4.5s,会拖住 CC 退出。计数只是观测,拿不到锁就算了。
        c = db.connect(paths.db_path(), busy_timeout_ms=constants.BUSY_TIMEOUT_OBS_MS)
        try:
            db.bump_counter(c, "hook_drop_count")
        finally:
            c.close()
        return True
    except Exception:
        return False


def _fail_closed_drop(exc, hook="stop"):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logged = False
    try:
        util.append_log_line(paths.hook_drops_path(),
                             f"{ts} {hook}_hook drop code={type(exc).__name__}")
        logged = True
    except Exception:
        pass
    counted = _bump_drop_counter()
    if not logged and not counted:
        # 可观测性降级:stderr 固定一行(4.7 异常总则)
        print("feishu-bridge: hook fail-closed drop (observability degraded)",
              file=sys.stderr)


# ---------------------------------------------------------------- Stop(4.7)
def run_stop_hook(payload, *, conn, prober, clock, start_pid=None):
    now = clock.wall_ms()
    session_id = payload.get("session_id")
    msg = payload.get("last_assistant_message") or ""
    sha = bool(payload.get("stop_hook_active"))
    inst = procs.find_cc_instance(prober, start_pid)
    if inst is None:
        return {"suppressed": True, "reason": "no-instance"}  # S3 运行时失败 → fail-closed
    pid, lstart = inst

    # ① 抑制链闩(r5-B1):续写 Stop 全抑制;fresh Stop 先关闩再正常处理
    latches = conn.execute(
        "SELECT 1 FROM pending_bind WHERE cc_pid=? AND cc_start=? AND latch_open=1 LIMIT 1",
        (pid, lstart)).fetchone()
    if latches:
        if sha:
            return {"suppressed": True, "reason": "latch-continuation"}
        with db.tx(conn):
            conn.execute(
                "UPDATE pending_bind SET latch_open=0 "
                "WHERE cc_pid=? AND cc_start=? AND latch_open=1", (pid, lstart))

    # ② bind 握手(4.1.6)
    pend = conn.execute(
        "SELECT * FROM pending_bind WHERE cc_pid=? AND cc_start=? AND state='pending' "
        "ORDER BY created_at DESC LIMIT 1", (pid, lstart)).fetchone()
    if pend is not None and (pend["expires_at"] or 0) > now:
        if util.marker_for(pend["nonce"]) in msg:
            return _handshake(conn, pend, session_id, now)
        # nonce 不命中:本 turn 属 bind turn → failed(latch 仍置)+ close starting
        with db.tx(conn):
            if db.cas(conn,
                      "UPDATE pending_bind SET state='failed', latch_open=1 "
                      "WHERE request_id=? AND state='pending'", (pend["request_id"],)):
                lifecycle._terminate_in_tx(conn, pend["request_id"], "bind_failed", now)
        return {"suppressed": True, "reason": "bind-nonce-miss"}

    # ③ tombstone:任意状态 pending_bind 行的完整 marker ∈ 消息
    for r in conn.execute(
            "SELECT nonce FROM pending_bind WHERE cc_pid=? AND cc_start=?",
            (pid, lstart)).fetchall():
        if util.marker_for(r["nonce"]) in msg:
            return {"suppressed": True, "reason": "tombstone"}

    # ④ 前缀纵深(member 诱导只会抑制转发 = fail-closed 方向,无害)
    if constants.MARKER_PREFIX in msg:
        return {"suppressed": True, "reason": "marker-prefix"}

    # ⑤ 正常路径:active 绑定 ∧ session_id ∧ cc 实例全匹配
    if not session_id:
        return {"suppressed": False, "reason": "no-session-id"}
    b = conn.execute(
        "SELECT * FROM bindings WHERE session_id=? AND status='active'",
        (session_id,)).fetchone()
    if b is None:
        return {"suppressed": False, "reason": "no-binding"}
    if (b["cc_pid"], b["cc_start"]) != (pid, lstart):
        _fail_closed_drop(RuntimeError("instance-mismatch"), hook="stop")
        return {"suppressed": True, "reason": "instance-mismatch"}
    if not msg.strip():
        return {"suppressed": False, "reason": "empty-message"}
    chunks = util.chunk_text(msg, constants.CHUNK_LIMIT)
    # 每轮转发页脚(context/model/effort)= best-effort 装饰,完全隔离于转发关键路径(fail-open 铁律):
    #   延迟导入 → 即便 ctxmeter 损坏(如 3.9 导入错)也只丢页脚、不炸 hook(不在顶层 import,把导入面挡在关键路径外);
    #   本地 try → import/计算/拼接任何异常都不冒泡到 stop_hook_entry 的 fail-closed 包装。
    #   拼到 chunks[-1] → 页脚完整、只一次、落最大 index;(turn_group,chunk_index) 唯一索引不受影响。
    try:
        from . import ctxmeter
        footer = ctxmeter.footer_for(payload)
        if footer and chunks:
            chunks[-1] = chunks[-1] + footer
    except Exception:
        pass                    # chunks 不变 → 原样转发
    group = util.new_id()
    with db.tx(conn):
        cur = conn.execute("SELECT status FROM bindings WHERE binding_id=?",
                           (b["binding_id"],)).fetchone()
        if cur is None or cur["status"] != "active":
            return {"suppressed": False, "reason": "binding-terminated"}
        for i, c in enumerate(chunks):
            jobs.create_job(
                conn, kind="session_turn", chat_id=b["chat_id"],
                binding_id=b["binding_id"], idempotency_key=jobs.key_turn(group, i),
                turn_group=group, chunk_index=i, body=c, now=now)
    return {"suppressed": False, "reason": "enqueued",
            "turn_group": group, "chunks": len(chunks)}


def _handshake(conn, pend, session_id, now):
    """单事务:pending→consumed+开闩 + confirmed+回填 session_id + (listener 就绪则)激活。"""
    rid = pend["request_id"]
    if not session_id:
        with db.tx(conn):
            if db.cas(conn,
                      "UPDATE pending_bind SET state='failed', latch_open=1 "
                      "WHERE request_id=? AND state='pending'", (rid,)):
                lifecycle._terminate_in_tx(conn, rid, "bind_failed", now)
        return {"suppressed": True, "reason": "no-session-id-at-handshake"}
    try:
        with db.tx(conn):
            if not db.cas(conn,
                          "UPDATE pending_bind SET state='consumed', latch_open=1 "
                          "WHERE request_id=? AND state='pending' AND expires_at>?",
                          (rid, now)):
                return {"suppressed": True, "reason": "handshake-race"}
            b = conn.execute(
                "SELECT * FROM bindings WHERE binding_id=? AND status='starting'",
                (rid,)).fetchone()
            if b is None:
                db.cas(conn,
                       "UPDATE pending_bind SET state='failed' "
                       "WHERE request_id=? AND state='consumed'", (rid,))
                return {"suppressed": True, "reason": "binding-gone"}
            conn.execute(
                "UPDATE bindings SET bind_phase='confirmed', confirmed_at=?, session_id=? "
                "WHERE binding_id=? AND status='starting'", (now, session_id, rid))
            b2 = conn.execute("SELECT * FROM bindings WHERE binding_id=?", (rid,)).fetchone()
            # listener 排他已建立(当前 epoch 有新鲜心跳,非仅 pid 非空)→ 同事务激活
            if lifecycle.heartbeat_fresh(b2, now):
                if db.cas(conn,
                          "UPDATE bindings SET status='active', bound_at=? "
                          "WHERE binding_id=? AND status='starting' AND session_id IS NOT NULL",
                          (now, rid)):
                    jobs.create_job(
                        conn, kind="lifecycle_notice", chat_id=b2["chat_id"],
                        binding_id=rid, idempotency_key=jobs.key_lc(rid, "bound"),
                        expected_state="active", body=texts.LC_BOUND, now=now)
            return {"suppressed": True, "reason": "bind-handshake"}
    except sqlite3.IntegrityError:
        # b_sess 冲突:该 session 已绑他群 → close starting + pending failed + 失败通知
        with db.tx(conn):
            if db.cas(conn,
                      "UPDATE pending_bind SET state='failed', latch_open=1 "
                      "WHERE request_id=? AND state='pending'", (rid,)):
                lifecycle._terminate_in_tx(conn, rid, "bind_failed", now)
        return {"suppressed": True, "reason": "session-conflict"}


# ---------------------------------------------------------------- SessionEnd(4.6 快路径)
def run_session_end(payload, *, conn, prober, clock, start_pid=None):
    now = clock.wall_ms()
    sid = payload.get("session_id")
    closed = []
    if sid:
        for b in conn.execute(
                "SELECT binding_id FROM bindings WHERE session_id=? "
                "AND status IN ('starting','active')", (sid,)).fetchall():
            if lifecycle.terminate_binding(conn, b["binding_id"], "session_end", clock):
                closed.append(b["binding_id"])
    inst = procs.find_cc_instance(prober, start_pid)
    if inst is not None:
        pid, lstart = inst
        for r in conn.execute(
                "SELECT request_id FROM pending_bind WHERE cc_pid=? AND cc_start=? "
                "AND state='pending'", (pid, lstart)).fetchall():
            with db.tx(conn):
                if db.cas(conn,
                          "UPDATE pending_bind SET state='expired', latch_open=0 "
                          "WHERE request_id=? AND state='pending'", (r["request_id"],)):
                    if lifecycle._terminate_in_tx(conn, r["request_id"], "session_end", now):
                        closed.append(r["request_id"])
        with db.tx(conn):
            conn.execute(
                "UPDATE pending_bind SET latch_open=0 "
                "WHERE cc_pid=? AND cc_start=? AND latch_open=1", (pid, lstart))
    return {"closed": closed}


# ---------------------------------------------------------------- StopFailure(API 错误告警;联网例外)
# 一轮因 API 错误(429 rate_limit / 529 overloaded / 5xx server_error / auth… 任意类型)结束时,CC 触发
# StopFailure(与 Stop 互斥,每轮至多一次)。经 lib.notify.run_notify 给**本 session 绑定群**发一条
# @群主 告警(结构化 at 节点,穿透免打扰),与 notify skill 同格式、同门控。未绑定 session 静默(如实
# 不发)。**定位假设**:个人轻量工具、单用户、群内可信 → 告警正文含**有界的** error/error_details/cwd
# 路径,发进可信群(非"零敏感";按可信群假设接受)。fail-closed:一切异常吞掉、exit 0,绝不阻塞 CC。

_MAX_BODY_LEN = 800


def _sanitize_field(val, cap):
    """把一个动态字段净化成**可安全放进 post md 节点的单行文本**(R1-#4/R2-Low3)。
    注(2026-07-27 正文由 text 节点改 md 节点):净化后的字段进的是 **markdown 源** → 其中的
    `*`/`_`/`` ` ``/`[]()`/裸 URL 会被渲染(cwd 里 `/a_b_c` 的下划线变斜体;URL 变**可点链接**)。
    **有意不加转义层**(codex impl r1 Low 校正):① 本条已折叠换行 → 动态值造不出标题/列表/围栏/
    表格等块级结构,影响限于内联;② 这些字段来自 CC 的 StopFailure payload(本机错误类型/详情/cwd),
    非外部输入,个人轻量工具 + 群内可信 → 渲染跑偏可接受。**安全性质由步③ 的 `<at` 中和保证**
    (md 会真渲染 mention);md 的图片只认已上传 image key、不认 URL,故无从本机抓取的面。
    (本函数**只作用于 StopFailure 的动态字段**;notify skill 的正文不过这里,其 markdown 是有意的。)
    顺序关键:**先删毒字符(NUL/孤立 surrogate/其它控制符)、再中和 `<at`** —— 否则 `<\\x00at` /
    `<\\ud800at` 删毒后会还原成 `<at`,漏过。步骤:
      ① 删 NUL 与孤立 surrogate;CR/LF/Tab→空格;其它 C0/DEL 删。
      ② 折叠连续空白为单空格 + strip(保证一行)。
      ③ 中和 `<at`(大小写不敏感)→ `< at`,使 lib.notify.AT_RE 不命中 → 告警不被整条丢弃。
      ④ 截断到 cap(留 `…`)。
    结果**绝不含** NUL/换行/孤立 surrogate,且 AT_RE 不命中。"""
    s = str(val)
    chars = []
    for ch in s:
        o = ord(ch)
        if o == 0 or 0xD800 <= o <= 0xDFFF:   # NUL / 孤立 surrogate → 删
            continue
        if ch in "\r\n\t":                     # 换行/制表 → 空格
            chars.append(" ")
            continue
        if o < 0x20 or o == 0x7F:              # 其它 C0 控制符 / DEL → 删
            continue
        chars.append(ch)
    s = re.sub(r"\s+", " ", "".join(chars)).strip()
    s = re.sub(r"<at", "< at", s, flags=re.IGNORECASE)   # 中和(在删毒之后)
    if len(s) > cap:
        s = s[:cap - 1] + "…"
    return s


def compose_stop_failure_message(payload):
    """据 StopFailure payload 拼一行诚实告警正文(纯函数)。含 error 类型 + 可选 error_details + cwd。
    **不含 "已重试耗尽"**(用户选"所有错误类型",含非重试类如 auth/model-not-found)。@群主前缀由
    run_notify 自动加(独立 at 节点),此处**只出正文**,且保证正文永不含 `<at`/NUL/换行(经 _sanitize_field)。"""
    error = _sanitize_field(payload.get("error") or "unknown", 60) or "unknown"
    body = "⚠️ 本会话有一轮因 API 错误结束:" + error
    details = payload.get("error_details")
    if details:
        d = _sanitize_field(details, 200)
        if d:
            body += " — " + d
    cwd = payload.get("cwd")
    if cwd:
        c = _sanitize_field(cwd, 200)
        if c:
            body += "。cwd: " + c
    if len(body) > _MAX_BODY_LEN:            # 三段相加兜底
        body = body[:_MAX_BODY_LEN - 1] + "…"
    return body


def run_stop_failure(payload, *, environ=None, prober=None, start_pid=None,
                     notify_fn=None, make_runner=None):
    """组装告警正文 + 只用 payload 的 session_id,经 run_notify 发到本 session 绑定群。→ 观测摘要 dict。
    **env(R1-#2 关键)**:`os.environ if environ is None`(非 `or`,免 environ={} 回退)→ 拷贝 →
    **先 pop 继承的 CLAUDE_CODE_SESSION_ID**(CC 会在 hook env 里设它;payload 缺 sid 时若留着继承值会
    误发到当前绑定群)→ 仅当 payload 有合法 str sid 才注入。lib.notify 惰性导入(不扩 Stop/SessionEnd
    的 import 面;notify 模块导入回归也不会拖垮既有 hooks)。"""
    from . import notify, runner as runner_mod  # lazy import
    sid = payload.get("session_id")
    msg = compose_stop_failure_message(payload)
    base = os.environ if environ is None else environ
    env = dict(base)
    env.pop("CLAUDE_CODE_SESSION_ID", None)
    if isinstance(sid, str) and sid:
        env["CLAUDE_CODE_SESSION_ID"] = sid
    if notify_fn is None:
        notify_fn = notify.run_notify
    if prober is None:
        prober = procs.SystemProber()
    if make_runner is None:
        def make_runner(p):
            return runner_mod.LarkRunner(p)
    if start_pid is None:
        start_pid = os.getppid()
    obj, _code = notify_fn(stdin_text=msg, environ=env, prober=prober,
                           start_pid=start_pid, make_runner=make_runner)
    return {"sent": obj.get("sent"), "reason": obj.get("reason")}


def _log_stop_failure_outcome(res):
    """观测:告警 sent≠true 时落 hook_drops.log 一行(**payload-free**,只含固定 reason 枚举;R2-Med1
    诚实三态:sent==False→not-sent 确定未发;sent=='unknown'→delivery-unconfirmed 可能已发)。绝不 raise。"""
    try:
        sent = res.get("sent")
        if sent is True:
            return
        label = "delivery-unconfirmed" if sent == "unknown" else "not-sent"
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        util.append_log_line(
            paths.hook_drops_path(),
            "%s stop_failure alert %s sent=%r reason=%r" % (ts, label, sent, res.get("reason")))
    except Exception:
        pass


def stop_failure_entry(payload, *, prober=None, start_pid=None, notify_fn=None, make_runner=None):
    """StopFailure hook fail-closed 入口。**不写 hooks 心跳**:StopFailure 仅 API 错误时触发(罕见)、
    非 hooks 生效的可靠信号;bind/preflight 只认 stop 心跳,paths.hook_heartbeat_path 也只接受
    stop/session_end。一切异常 → _fail_closed_drop + 抑制(exit 0,绝不阻塞 CC)。"""
    try:
        res = run_stop_failure(payload, prober=prober, start_pid=start_pid,
                               notify_fn=notify_fn, make_runner=make_runner)
        _log_stop_failure_outcome(res)
        return res
    except Exception as e:
        _fail_closed_drop(e, hook="stop_failure")
        return {"suppressed": True, "reason": "exception"}
