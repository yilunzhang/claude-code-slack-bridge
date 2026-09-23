"""出站(plan 4.5):daemon 唯一发送者。线性化点 = pending→sending 的 per-kind 守卫 CAS
(rowcount=1 才发);网络在事务外;结果契约解析(F10:缺字段=UNKNOWN 非成功);
unknown 同 key 自动重试一次(S4);组内/组间/同 chat 通知有序。"""
from . import constants, db, jobs, runner as runner_mod, texts, util

NOTICE_KINDS = ("decision_notice", "lifecycle_notice", "unsupported_notice", "inbound_notice")


class Outbound:
    def __init__(self, conn, cfg, runner, clock, heartbeat=None, log=None):
        self.conn = conn
        self.cfg = cfg
        self.runner = runner
        self.clock = clock
        self.heartbeat = heartbeat  # r2-M1②:每次发送完 touch last_loop_at
        self.log = log              # E4a 可观测性:unknown/failed 留原始 rc/stdout/stderr 现场

    # ------------------------------------------------------------------
    def startup_scan(self):
        """启动扫描 + 升级收口。tick/_prepare 已不按 attempt_count 卡(retryable session_turn 需过
        第 2 次)→ 崩溃循环(每次发送前崩)的硬上限兜底 + 升级遗留态收口都在此,且**按 kind 保旧上限**。
        步骤(顺序不可乱,后步依赖前步已改状态):
        1. session_turn 达 TURN_RETRYABLE_MAX_ATTEMPTS(6)且处于 sending(崩溃)**或** unknown(schema
           合法的 over-cap 遗留态)→ failed **且发告警**(与 _finalize_unknown 耗尽分支一致,不静默丢失;
           codex R2 MAJOR-2 + R3 MINOR:覆盖 unknown 才堵住第 4 步重臂发第 7 次 / over-cap 永久 HOL)。
        2. 崩溃于 sending 的**非** session_turn 达 MAX_SEND_ATTEMPTS(2)→ 终态 unknown(next=NULL,
           **完全保持既有语义**)。
        3. 其余 sending → unknown 重臂(next=now)。
        4. 遗留终态 session_turn unknown(next=NULL,升级前旧实现放弃的)→ 重臂(next=now),交新策略
           收口(送达/持久重试/耗尽→failed+告警),使「不再永久 HOL」成为库级 invariant(codex R1 BLOCKER-2)。
        5. 遗留非 session_turn unknown 已达旧上限(ac>=2)但 next 非 NULL(旧 startup 重臂 + 旧 tick 的
           attempt cap 令其逻辑终态)→ 收为真终态 next=NULL,保持「≤2 次」旧语义(新 tick 已无 attempt
           cap,否则升级后会实际发第 3 次;codex R2 MAJOR-1)。
        稳态下 finalize 保证 unknown-with-next 恒 < 上限,故上述兜底只在崩溃/升级生效。"""
        now = self.clock.wall_ms()
        # 1) session_turn 达硬上限(崩溃于 sending,或 schema 合法的 over-cap unknown)→ failed + 告警
        #    (逐行,复用耗尽分支的告警入队)。**覆盖 unknown** 是为「六次硬上限」启动收口的完整性:
        #    否则 over-cap unknown 会被第 4 步重臂发第 7 次、或(next 遥远未来时)永久 HOL(codex R3 MINOR)。
        # `attempt_count<0` 是 schema 合法但代码不可达态(create_job=0、_prepare 只+1、从不减):
        # 仍 fail-closed 收口(否则外部篡改/损坏出的负计数会绕过六次上限,codex R4 M1)。
        for job in self.conn.execute(
                "SELECT * FROM outbound_jobs WHERE kind='session_turn' "
                "AND (attempt_count>=? OR attempt_count<0) AND state IN ('sending','unknown')",
                (constants.TURN_RETRYABLE_MAX_ATTEMPTS,)).fetchall():
            with db.tx(self.conn):
                moved = db.cas(
                    self.conn,
                    "UPDATE outbound_jobs SET state='failed', error=? "
                    "WHERE job_id=? AND state IN ('sending','unknown')",
                    (f"exhausted after {job['attempt_count']} attempts (startup reconcile)",
                     job["job_id"]))
                if moved and not self._is_alert_turn(job):
                    self._enqueue_send_failure_alert(job, now)
        # 2) 非 session_turn 崩溃达旧上限 → 终态 unknown(next=NULL,行为不变)。
        self.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', error='crashed-mid-send (exhausted)', "
            "next_attempt_at=NULL WHERE state='sending' AND kind!='session_turn' "
            "AND attempt_count>=?", (constants.MAX_SEND_ATTEMPTS,))
        # 3) 其余 sending → unknown 重臂。
        self.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', error='crashed-mid-send', "
            "next_attempt_at=? WHERE state='sending'", (now,))
        # 4) 遗留终态 session_turn unknown(next=NULL 且 ac<上限)→ 重臂交新策略(消除永久 HOL)。
        #    ac>=上限者已在第 1 步 failed → 此处限 ac<上限,重臂天然有界、绝不越过六次(codex R3 MINOR)。
        # 重臂条件精确为 0<=ac<上限(ac>=上限与 ac<0 都已在第 1 步 failed;此处再钉一次令重臂自洽有界)。
        self.conn.execute(
            "UPDATE outbound_jobs SET next_attempt_at=? WHERE state='unknown' "
            "AND kind='session_turn' AND next_attempt_at IS NULL "
            "AND attempt_count>=0 AND attempt_count<?",
            (now, constants.TURN_RETRYABLE_MAX_ATTEMPTS))
        # 5) 遗留非 session_turn unknown 已达旧上限但 next 非 NULL → 收为真终态(保旧「≤2 次」语义)。
        self.conn.execute(
            "UPDATE outbound_jobs SET next_attempt_at=NULL WHERE state='unknown' "
            "AND kind!='session_turn' AND attempt_count>=?", (constants.MAX_SEND_ATTEMPTS,))

    def tick(self, budget=constants.OUTBOUND_BATCH):
        now = self.clock.wall_ms()
        # r4-3:allowlist 列外 job 无论 fingerprint 门状态都确定性 cancelled(兑现 README
        # "一律 cancelled"契约)—— 排在 degraded 早返回之前。
        self._cancel_disallowed_jobs()
        # 修复项1:指纹/版本门 degraded → 出站停摆(入站照常入库;门由 FingerprintGate 管理)
        gate = db.get_state(self.conn, "outbound_gate", "ok") or "ok"
        if gate != "ok":
            return 0
        # unknown 的耗尽判定移至 _finalize_unknown(session_turn→failed / 其它→next_attempt_at=NULL);
        # 凡仍 unknown 且带 next_attempt_at 者 by-construction 未耗尽 → 此处不再按 attempt_count 卡。
        rows = self.conn.execute(
            "SELECT * FROM outbound_jobs WHERE (state='pending' "
            "AND (next_attempt_at IS NULL OR next_attempt_at<=?)) "
            "OR (state='unknown' AND next_attempt_at IS NOT NULL "
            "AND next_attempt_at<=?) ORDER BY job_seq",
            (now, now)).fetchall()
        sends = 0
        for job in rows:
            if sends >= budget:
                break
            if self._prepare(job) == "send":
                self._send_and_finalize(job["job_id"])
                sends += 1
                if self.heartbeat is not None:
                    try:
                        self.heartbeat()
                    except Exception:
                        pass
        return sends

    def _cancel_disallowed_jobs(self):
        allow = self.cfg.get("chat_allowlist")
        if not allow:
            return
        placeholders = ",".join("?" for _ in allow)
        self.conn.execute(
            f"UPDATE outbound_jobs SET state='cancelled' "
            f"WHERE state IN ('pending','unknown') AND chat_id NOT IN ({placeholders})",
            tuple(allow))

    # ------------------------------------------------------------------
    def _prepare(self, job):
        """短事务:重读状态 → 顺序门(skip)→ 守卫(不满足→cancelled)→ CAS →sending。"""
        with db.tx(self.conn):
            fresh = self.conn.execute(
                "SELECT * FROM outbound_jobs WHERE job_id=?", (job["job_id"],)).fetchone()
            if fresh is None or fresh["state"] not in ("pending", "unknown"):
                return "gone"
            # r3-1③(E2 盖全兜底):allowlist 生效且 job.chat_id 不在列 → cancelled
            allow = self.cfg.get("chat_allowlist")
            if allow and fresh["chat_id"] not in allow:
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='cancelled' "
                       "WHERE job_id=? AND state IN ('pending','unknown')",
                       (fresh["job_id"],))
                return "cancelled"
            # 硬门(单一 send chokepoint):session_turn 绝不发送超过 TURN_RETRYABLE_MAX_ATTEMPTS 次。
            # attempt_count 由代码保证 0 起、只增(create_job=0/_prepare+1),故正常流永不触发(prepare 入口
            # ac∈[0,5])。只对**外部篡改/损坏**出的异常计数(负 / 超上限 / 非整数,任何 state 含 pending)
            # 兜底 fail-closed → failed(放行后续),使「六次硬上限」在发送 chokepoint 一处对所有 schema
            # 合法态成立,免去在 startup_scan 逐态枚举(codex R4/R5)。
            ac = fresh["attempt_count"]
            if fresh["kind"] == "session_turn" and (
                    not isinstance(ac, int) or ac < 0
                    or ac >= constants.TURN_RETRYABLE_MAX_ATTEMPTS):
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='failed', error=? "
                       "WHERE job_id=? AND state IN ('pending','unknown')",
                       (f"attempt cap reached at prepare (ac={ac!r})", fresh["job_id"]))
                return "cancelled"
            now = self.clock.wall_ms()
            if fresh["state"] == "unknown":
                # 耗尽由 _finalize_unknown 决(session_turn→failed / 其它→next_attempt_at=NULL);
                # 此处只尊重退避到期,不再按 attempt_count 卡(否则 retryable session_turn 过不了第 2 次)。
                if fresh["next_attempt_at"] is None or fresh["next_attempt_at"] > now:
                    return "skip"
            elif fresh["next_attempt_at"] is not None and fresh["next_attempt_at"] > now:
                return "skip"  # 重臂退避(修复项3):pending 也尊重 next_attempt_at
            order = self._order_gate(fresh)
            if order == "skip":
                return "skip"
            if order == "cancel" or not self._guard_ok(fresh):
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='cancelled' "
                       "WHERE job_id=? AND state IN ('pending','unknown')",
                       (fresh["job_id"],))
                return "cancelled"
            db.cas(self.conn,
                   "UPDATE outbound_jobs SET state='sending', sending_at=?, "
                   "attempt_count=attempt_count+1 WHERE job_id=? AND state IN ('pending','unknown')",
                   (self.clock.wall_ms(), fresh["job_id"]))
            return "send"

    def _order_gate(self, job):
        # 组内 chunk 严格序:前块非 sent 后块不发;前块 failed/cancelled → 本块 cancel
        if job["turn_group"] is not None and (job["chunk_index"] or 0) > 0:
            prev = self.conn.execute(
                "SELECT state FROM outbound_jobs WHERE turn_group=? AND chunk_index=?",
                (job["turn_group"], job["chunk_index"] - 1)).fetchone()
            if prev is None:
                return "skip"
            if prev["state"] in ("failed", "cancelled"):
                return "cancel"
            if prev["state"] != "sent":
                return "skip"
        # 同 binding 的 turn_group 间有序:前组存在 unknown(或在发)后组不发
        if job["kind"] == "session_turn":
            blocker = self.conn.execute(
                "SELECT 1 FROM outbound_jobs WHERE kind='session_turn' AND binding_id=? "
                "AND job_seq<? AND turn_group IS NOT NULL AND turn_group!=? "
                "AND state IN ('unknown','sending') LIMIT 1",
                (job["binding_id"], job["job_seq"], job["turn_group"] or "")).fetchone()
            if blocker:
                return "skip"
        # 同 chat 通知按 job_seq
        if job["kind"] in NOTICE_KINDS:
            qs = ",".join("?" for _ in NOTICE_KINDS)
            blocker = self.conn.execute(
                f"SELECT 1 FROM outbound_jobs WHERE chat_id=? AND job_seq<? "
                f"AND kind IN ({qs}) AND state IN ('pending','sending','unknown') LIMIT 1",
                (job["chat_id"], job["job_seq"], *NOTICE_KINDS)).fetchone()
            if blocker:
                return "skip"
        return None

    def _guard_ok(self, job):
        """per-kind 守卫(I2/4.5;结构化字段,不解析 body)。"""
        kind = job["kind"]
        if kind == "session_turn":
            return self._binding_active(job["binding_id"])
        if kind == "approval_card":
            p = self._pending(job["ref_pending_id"])
            return (p is not None and p["state"] == "pending"
                    and p["card_message_id"] is None
                    and self._binding_active(job["binding_id"]))
        if kind == "decision_notice":
            exp = job["expected_state"]
            if exp in ("approved", "rejected", "expired"):
                p = self._pending(job["ref_pending_id"])
                return p is not None and p["state"] == exp
            if exp == "failed":
                r = self._inbox(job["ref_message_id"])
                return r is not None and r["state"] == "failed"
            return False
        if kind == "lifecycle_notice":
            b = self.conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                                  (job["binding_id"],)).fetchone()
            if b is None:
                return False
            exp = job["expected_state"] or ""
            if exp == "active":
                return b["status"] == "active"
            if ":" in exp:
                st, reason = exp.split(":", 1)
                return b["status"] == st and (b["close_reason"] or "") == reason
            return False
        if kind == "unsupported_notice":
            r = self._inbox(job["ref_message_id"])
            return r is not None and r["state"] == "unsupported"
        if kind == "inbound_notice":
            r = self._inbox(job["ref_message_id"])
            return r is not None and r["state"] == (job["expected_state"] or "")
        if kind == "receipt_reaction":
            d = self.conn.execute("SELECT state FROM deliveries WHERE delivery_seq=?",
                                  (job["ref_delivery_seq"],)).fetchone()
            return d is not None and d["state"] != "dropped"
        return False

    def _binding_active(self, binding_id):
        b = self.conn.execute("SELECT status FROM bindings WHERE binding_id=?",
                              (binding_id,)).fetchone()
        return b is not None and b["status"] == "active"

    def _pending(self, pending_id):
        return self.conn.execute("SELECT * FROM pendings WHERE pending_id=?",
                                 (pending_id,)).fetchone()

    def _inbox(self, message_id):
        return self.conn.execute("SELECT * FROM inbox WHERE message_id=?",
                                 (message_id,)).fetchone()

    # ------------------------------------------------------------------
    def _send_and_finalize(self, job_id):
        job = self.conn.execute("SELECT * FROM outbound_jobs WHERE job_id=?",
                                (job_id,)).fetchone()
        if job is None or job["state"] != "sending":
            return
        outcome, detail, retryable = self._transmit(job)  # 网络在事务外
        now = self.clock.wall_ms()
        with db.tx(self.conn):
            if outcome == "sent":
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='sent', sent_message_id=?, sent_at=?, "
                       "error=NULL WHERE job_id=? AND state='sending'",
                       (detail, now, job_id))
                if job["kind"] == "approval_card" and detail:
                    # 发出后回填 card_message_id(4.2.7)
                    self.conn.execute(
                        "UPDATE pendings SET card_message_id=? "
                        "WHERE pending_id=? AND card_message_id IS NULL",
                        (detail, job["ref_pending_id"]))
            elif outcome == "failed":
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='failed', error=? "
                       "WHERE job_id=? AND state='sending'", (detail, job_id))
            else:  # unknown
                self._finalize_unknown(job, detail, retryable, now)

    def _finalize_unknown(self, job, detail, retryable, now):
        """unknown 收口(在 _send_and_finalize 的写事务内调用)。
        - session_turn:retryable → 指数退避重试至 TURN_RETRYABLE_MAX_ATTEMPTS;非 retryable →
          MAX_SEND_ATTEMPTS 平退避。两者**耗尽都 → failed**(放行后续 turn,不再终态 unknown 队头阻塞),
          并对非告警 turn 入队一条可见告警(idempotency-key 保证重发去重,故转 failed 安全)。
        - 其它 kind:保持既有语义(retryable = attempt_count<MAX,耗尽→终态 unknown)。"""
        fresh = self.conn.execute(
            "SELECT attempt_count FROM outbound_jobs WHERE job_id=?",
            (job["job_id"],)).fetchone()
        if fresh is None:
            return
        ac = fresh["attempt_count"]
        if job["kind"] != "session_turn":
            still = ac < constants.MAX_SEND_ATTEMPTS
            db.cas(self.conn,
                   "UPDATE outbound_jobs SET state='unknown', error=?, next_attempt_at=? "
                   "WHERE job_id=? AND state='sending'",
                   (detail, (now + constants.UNKNOWN_RETRY_DELAY_MS) if still else None,
                    job["job_id"]))
            return
        cap = constants.TURN_RETRYABLE_MAX_ATTEMPTS if retryable else constants.MAX_SEND_ATTEMPTS
        if ac < cap:
            if retryable:
                delay = min(constants.TURN_RETRY_BACKOFF_MS * (2 ** max(ac - 1, 0)),
                            constants.TURN_RETRY_BACKOFF_MAX_MS)
            else:
                delay = constants.UNKNOWN_RETRY_DELAY_MS
            db.cas(self.conn,
                   "UPDATE outbound_jobs SET state='unknown', error=?, next_attempt_at=? "
                   "WHERE job_id=? AND state='sending'",
                   (detail, now + delay, job["job_id"]))
            return
        # 耗尽:转 failed(_order_gate 视 failed 为非阻塞终态 → 放行后续 turn);对非告警 turn 发告警。
        moved = db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='failed', error=? "
                       "WHERE job_id=? AND state='sending'",
                       (f"{detail} (exhausted after {ac} attempts)", job["job_id"]))
        if not moved:
            return
        alert = not self._is_alert_turn(job)
        if self.log is not None:
            try:
                self.log(f"send session_turn key={job['idempotency_key']} EXHAUSTED after "
                         f"{ac} attempts -> failed" + (" + alert" if alert else ""))
            except Exception:
                pass
        if alert:
            self._enqueue_send_failure_alert(job, now)

    @staticmethod
    def _is_alert_turn(job):
        """告警 turn 用哨兵 turn_group 标记(真实 turn_group=hex uuid,绝不以此前缀开头);
        防「告警自身耗尽再生告警」的级联。"""
        return (job["turn_group"] or "").startswith("__sendfail__:")

    def _enqueue_send_failure_alert(self, job, now):
        """复用 session_turn + 哨兵 turn_group 发告警(零 schema 迁移)。INSERT OR IGNORE +
        (turn_group,chunk_index)/idempotency_key 双 UNIQUE → 幂等;走 _binding_active 守卫 + --markdown。"""
        tg = "__sendfail__:" + job["job_id"]
        jobs.create_job(
            self.conn, kind="session_turn", chat_id=job["chat_id"],
            binding_id=job["binding_id"], idempotency_key=jobs.key_turn(tg, 0),
            turn_group=tg, chunk_index=0, body=texts.send_failure_alert_body(), now=now)

    def _transmit(self, job):
        """→ ('sent', message_id|None, False) | ('failed', err, False) | ('unknown', err, retryable)。
        retryable 仅对 unknown 有意义(超时/network/.error.retryable=true/瞬态码 → True),
        供 _finalize_unknown 决定 session_turn 用持久退避还是短退避。"""
        kind = job["kind"]
        if kind == "receipt_reaction":
            params = util.jdumps({"message_id": job["ref_message_id"]})
            data = util.jdumps({"reaction_type": {"emoji_type": job["body"] or "GLANCE"}})
            res = self.runner.run(
                ["im", "reactions", "create", "--as", "bot", "--params", params,
                 "--data", data], timeout_s=constants.SEND_TIMEOUT_S)
            env = runner_mod.parse_result(res)  # E4a:stderr 信封回退
            # 修复项9:成功必须解析出 .data.reaction_id(F10:缺字段=非成功)
            if res.rc == 0 and runner_mod.envelope_ok(env) \
                    and runner_mod.data_of(env).get("reaction_id"):
                return ("sent", None, False)
            self._log_failure(job, "failed", res)
            return ("failed", "reaction failed", False)  # 失败即 failed 不重试(4.5;S11 幂等)
        wire_key = util.short_key(job["idempotency_key"])  # E4b:wire 一律短键(≤40)
        if kind == "approval_card":
            # 成员消息预览=**不可信**文本 → 必须保持转义 interactive 卡片,绝不 markdown 渲染。
            argv = ["im", "+messages-reply", "--as", "bot",
                    "--message-id", job["reply_to"], "--msg-type", "interactive",
                    "--content", job["body"] or "{}",
                    "--idempotency-key", wire_key]
        elif kind == "session_turn":
            # session_turn 经 --markdown 渲染(让模型输出的 markdown 在飞书正常显示)。
            # **安全前提(2026-07-17 Yilun 定)**:群内只有可信人员 + 接受模型输出经 --markdown 可能
            # 自动抓取图片 URL(SSRF 面:lark-cli 会从本机抓 ![](url) 的地址,可达内网/localhost/云
            # 元数据)/ 解析 @(@全员面)。**若将来群向不可信成员开放,必须改回 --text,或写一个
            # md→安全 post 渲染器(不主动抓远程资源)。** 注:approval_card 的成员预览仍走转义
            # interactive,不受此影响;通知类(lifecycle/decision/inbound/unsupported)是我们的固定
            # 文案,继续走 --text。
            argv = ["im", "+messages-send", "--as", "bot",
                    "--chat-id", job["chat_id"], "--markdown", job["body"] or "",
                    "--idempotency-key", wire_key]
        else:
            # 通知类(lifecycle_notice/decision_notice/inbound_notice/unsupported_notice)=固定文案
            # → 保持 --text(无需渲染,也无 markdown 主动面)。
            argv = ["im", "+messages-send", "--as", "bot",
                    "--chat-id", job["chat_id"], "--text", job["body"] or "",
                    "--idempotency-key", wire_key]
        res = self.runner.run(argv, timeout_s=constants.SEND_TIMEOUT_S)
        env = runner_mod.parse_result(res)  # E4a:stdout → 空/不可解析回退 stderr
        if runner_mod.envelope_ok(env):
            mid = runner_mod.data_of(env).get("message_id")
            if mid:
                return ("sent", mid, False)
            self._log_failure(job, "unknown", res)
            # ok 但无 id:一般不宜盲重(非 retryable);但若**同时超时**,超时信号占先 → 可持久重试
            # (codex R2 MINOR-1:此分支曾在 timeout 判断前 return,把超时误标非 retryable)。
            return ("unknown", "ok-without-message_id", bool(res.timed_out))
        code = runner_mod.envelope_error_code(env)  # 顶层 code / 嵌套 .error.code 双形状
        if env is not None and env.get("ok") is False:
            # 永久码 → failed;其余(含**无 code** 的错误信封)→ unknown,retryable 由 type/retryable
            # 字段/回退集判定,**不**只看 numeric-code 分支(codex MINOR-2:无 code 的 network 信封应可重试)。
            if code is not None and classify_error_code(code) == "failed":
                self._log_failure(job, "failed", res)
                return ("failed", f"code={code} {runner_mod.envelope_error_msg(env)}".strip(), False)
            self._log_failure(job, "unknown", res)
            detail = ((f"code={code} " if code is not None else "")
                      + runner_mod.envelope_error_msg(env)).strip()
            return ("unknown", detail or "error-without-code", self._is_retryable(env, res))
        self._log_failure(job, "unknown", res)
        if res.timed_out:
            return ("unknown", "timeout", True)
        return ("unknown", f"rc={res.rc} unparseable-envelope", False)

    @staticmethod
    def _is_retryable(env, res):
        """unknown 是否值得持久重试。**官方 error.retryable 字段优先**(True/False 都尊重):
        显式 false(如 99991661 token 失效,官方不可重试)绝不因本地码表被翻成 retryable(codex MAJOR-2)。
        字段缺失时才回退启发:超时 / network 类(5xx)/ RETRYABLE_FALLBACK_CODES(仅频控,不含 token 类)。
        永久错误已在 classify_error_code 走 failed 分支,不入此。"""
        explicit = runner_mod.envelope_error_retryable(env)
        if explicit is True:
            return True
        if explicit is False:
            return False
        if res.timed_out:
            return True
        if runner_mod.envelope_error_type(env) == "network":
            return True
        code = runner_mod.envelope_error_code(env)
        try:
            if int(code) in constants.RETRYABLE_FALLBACK_CODES:
                return True
        except (TypeError, ValueError):
            pass
        return False

    def _log_failure(self, job, outcome, res):
        """E4a 可观测性:非 sent 结果把原始现场(rc/stdout/stderr 截断)记入 daemon.log。"""
        if self.log is None:
            return
        try:
            self.log(
                f"send {job['kind']} key={job['idempotency_key']} -> {outcome}: "
                f"rc={res.rc} timed_out={res.timed_out} "
                f"stdout={(res.stdout or '')[:300]!r} stderr={(res.stderr or '')[:300]!r}")
        except Exception:
            pass


def classify_error_code(code):
    """修复项4:表驱动错误分类。永久集→failed;瞬态集→unknown(同 key 自动重试仍≤1);
    未知 code→unknown(留人工,status 高亮)。"""
    try:
        code = int(code)
    except (TypeError, ValueError):
        return "unknown"
    if code in constants.PERMANENT_SEND_CODES:
        return "failed"
    if code in constants.TRANSIENT_SEND_CODES:
        return "unknown"
    return "unknown"
