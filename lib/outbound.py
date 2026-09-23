"""出站状态机(docs/contracts.md §2;plan「出站状态机」)。daemon 是唯一发送者(I2)。

结构:
- `op_for(job_view, cfg)`:**纯函数**,kind → (op_method, op_target, op_thread_ts, op_payload_kind);
  已冻结(op_method 非空)原样返回存值。`job_view` = outbound_jobs LEFT JOIN pendings 的投影(含 card_message_id)。
- `tick`:候选(pending 到期 / postMessage unknown 核验到期 / 幂等 unknown 重发到期,ORDER BY job_seq)
  → 冷却预检(小事务只写 next_attempt_at / verify_after = 冷却到期,**不进 sending**)→ 每频道节流
  → `_prepare`(线性化点:pending→sending 的守卫 CAS,同事务冻结 op_*;§2.4 顺序)
  → 事务外 `_transmit`(只用冻结 op_*)→ `_classify` → `_finalize`(§2.5 转移表)。
- `_verify_unknown`:postMessage 类 unknown **只核验**(metadata job_id 精确关联);三次**成功查询**未见
  且核验能力经本凭据确证才重发一次(§2.6);错误 / 冷却绝不凑数、绝不因冷却回到 pending。
- `startup_scan`:崩溃 / 遗留态收口(§2.9)。
告警 = session_turn + 哨兵 turn_group `__sendfail__:…`(幂等键去重;告警自身失败不再生告警)。
fail-closed:任何判定不满足 → 不发送;编程错误(op_* 冻结断言失败)也进 unconfirmed 而非发送。
"""
import json

from . import constants, db, jobs, texts, util
from .slackapi import CallResult, classify_send_error

NOTICE_KINDS = ("decision_notice", "lifecycle_notice", "unsupported_notice", "inbound_notice")
ALERT_GROUP_PREFIX = "__sendfail__:"
DEFAULT_TICK_BUDGET = 20
VERIFY_HISTORY = "conversations.history"
VERIFY_REPLIES = "conversations.replies"
CLS_MARKDOWN_REJECTED = constants.MARKDOWN_REJECTED_ERROR  # `not_sent` 子类(§2.5)

# job_view 投影(contracts §2.3):outbound_jobs ⋈ pendings,附 card_message_id(可 NULL)与 decided_by。
JOB_VIEW_SQL = (
    "SELECT o.*, p.card_message_id AS card_message_id, p.decided_by AS card_decided_by "
    "FROM outbound_jobs o LEFT JOIN pendings p ON p.pending_id = o.ref_pending_id")


def _g(view, key, default=None):
    """dict / sqlite3.Row 通用取值(Row 缺键抛 IndexError)。"""
    try:
        v = view[key]
    except (KeyError, IndexError):
        return default
    return default if v is None else v


# ======================================================================
# 纯函数:kind → 传输形态(§2.2 / §2.3)
# ======================================================================
def op_for(job_view, cfg):
    """→ (method, target, thread_ts|None, payload_kind)。纯函数:只看 job_view 与 cfg["markdown_mode"]。
    已冻结(op_method 非空)→ 原样返回存值(不重算)。未知 kind → ValueError(调用方 fail-closed)。"""
    if _g(job_view, "op_method") is not None:
        return (job_view["op_method"], _g(job_view, "op_target"), _g(job_view, "op_thread_ts"),
                _g(job_view, "op_payload_kind"))
    kind = _g(job_view, "kind")
    chat_id = _g(job_view, "chat_id")
    reply_to = _g(job_view, "reply_to")
    if kind == "session_turn":
        mode = cfg.get("markdown_mode") if hasattr(cfg, "get") else None
        if mode not in ("markdown_text", "text"):
            mode = constants.MARKDOWN_MODE_DEFAULT
        return ("chat.postMessage", chat_id, None, mode)
    if kind == "lifecycle_notice":
        return ("chat.postMessage", chat_id, None, "text")
    if kind in ("inbound_notice", "unsupported_notice"):
        return ("chat.postMessage", chat_id, reply_to, "text")
    if kind == "approval_card":
        return ("chat.postMessage", chat_id, reply_to, "blocks")
    if kind == "decision_notice":
        card = _g(job_view, "card_message_id")
        if card:
            return ("chat.update", card, None, "blocks")
        return ("chat.postMessage", chat_id, reply_to, "text")
    if kind == "receipt_reaction":
        return ("reactions.add", chat_id, None, "reaction")
    raise ValueError("op_for: unknown kind %r" % (kind,))


def category_of(method):
    """op_method → 'postMessage' | 'idempotent' | None(§2.2:类别由 op_method 决定)。"""
    if method in constants.POSTMESSAGE_METHODS:
        return "postMessage"
    if method in constants.IDEMPOTENT_METHODS:
        return "idempotent"
    return None


def cap_for(kind, category):
    """attempt cap(§2.4-3):幂等 3;turn 6;card 3;其余 postMessage 通知 3。"""
    if category == "idempotent":
        return constants.IDEMPOTENT_CAP
    if kind == "session_turn":
        return constants.TURN_CAP
    if kind == "approval_card":
        return constants.CARD_CAP
    return constants.NOTICE_CAP


def _verify_method_for(thread_ts):
    return VERIFY_REPLIES if thread_ts else VERIFY_HISTORY


def _sql_in(values):
    return ",".join("?" for _ in values)


# ======================================================================
class Outbound:
    def __init__(self, conn, cfg, client, clock, heartbeat=None, log=None):
        self.conn = conn
        self.cfg = cfg
        self.client = client
        self.clock = clock
        self.heartbeat = heartbeat  # 每次网络往返后 touch(daemon 存活心跳)
        self.log = log              # 可观测性:固定字段,绝不写正文 / token
        self._last_post_at = {}     # channel → 最近一次 postMessage 请求的墙钟 ms(每频道节流)

    # ------------------------------------------------------------------ helpers
    def _log(self, msg):
        if self.log is None:
            return
        try:
            self.log(msg)
        except Exception:  # noqa: BLE001
            pass

    def _beat(self):
        if self.heartbeat is None:
            return
        try:
            self.heartbeat()
        except Exception:  # noqa: BLE001
            pass

    def _job_view(self, job_id):
        return self.conn.execute(JOB_VIEW_SQL + " WHERE o.job_id=?", (job_id,)).fetchone()

    def _cooldown_until(self, method, now):
        """方法级冷却到期(未到期才返回值,否则 None)。优先 client 的 cooldown_store(与 call 同一份)。"""
        store = getattr(self.client, "cooldown_store", None)
        until = None
        if store is not None:
            try:
                until = store.get(method)
            except Exception:  # noqa: BLE001
                until = None
        else:
            raw = db.get_state(self.conn, constants.COOLDOWN_KEY_PREFIX + method)
            try:
                until = int(raw) if raw is not None else None
            except (TypeError, ValueError):
                until = None
        if until is not None and until > now:
            return int(until)
        return None

    def _verify_capability_ok(self):
        """§7 两处复验之一:verify_capability=="ok" ∧ 探测版本 == 本 client 的凭据版本(_transmit 用的同一对象)。"""
        cap = db.get_state(self.conn, constants.VERIFY_CAPABILITY_KEY)
        ver = db.get_state(self.conn, constants.VERIFY_CAPABILITY_VERSION_KEY)
        cur = getattr(self.client, "tokens_version", None)
        return cap == constants.VERIFY_CAP_OK and ver is not None and cur is not None and ver == cur

    @staticmethod
    def _is_alert_turn(job):
        return (_g(job, "turn_group") or "").startswith(ALERT_GROUP_PREFIX)

    def _paced(self, channel, now):
        last = self._last_post_at.get(channel)
        if last is None:
            return False
        if now < last:  # 墙钟回拨:重置,不永久阻塞
            self._last_post_at.pop(channel, None)
            return False
        return now - last < constants.POST_MIN_INTERVAL_MS

    # ------------------------------------------------------------------ startup_scan(§2.9)
    def startup_scan(self):
        now = self.clock.wall_ms()
        pm = tuple(constants.POSTMESSAGE_METHODS)
        im = tuple(constants.IDEMPOTENT_METHODS)
        with db.tx(self.conn):
            # 0) sending/unknown 却无 op_*(设计上不可达):先按自身 kind 冻结,后续按类别收口(fail-closed 到 kind)。
            for row in self.conn.execute(
                    JOB_VIEW_SQL + " WHERE o.state IN ('sending','unknown') AND o.op_method IS NULL "
                    "ORDER BY o.job_seq").fetchall():
                try:
                    m, t, th, pk = op_for(row, self.cfg)
                except ValueError as e:
                    self.conn.execute(
                        "UPDATE outbound_jobs SET state='cancelled', error=? WHERE job_id=?",
                        ("op_for: %s" % e, row["job_id"]))
                    continue
                self.conn.execute(
                    "UPDATE outbound_jobs SET op_method=?, op_target=?, op_thread_ts=?, op_payload_kind=? "
                    "WHERE job_id=?", (m, t, th, pk, row["job_id"]))
            # 1) sending ∧ postMessage → unknown, had_unknown=1, verify_after=now(到 cap 的仍只核验)。
            self.conn.execute(
                "UPDATE outbound_jobs SET state='unknown', had_unknown=1, verify_after=?, next_attempt_at=NULL, "
                "error='crashed-mid-send' WHERE state='sending' AND op_method IN (%s)" % _sql_in(pm),
                (now, *pm))
            # 2) sending ∧ 幂等:ac<cap → unknown, next=now;ac≥cap(或损坏负数)→ failed(R3-P3:不发第 cap+1 次)。
            self.conn.execute(
                "UPDATE outbound_jobs SET state='unknown', next_attempt_at=?, verify_after=NULL, "
                "error='crashed-mid-send' WHERE state='sending' AND op_method IN (%s) "
                "AND attempt_count>=0 AND attempt_count<?" % _sql_in(im),
                (now, *im, constants.IDEMPOTENT_CAP))
            self.conn.execute(
                "UPDATE outbound_jobs SET state='failed', error='crashed-mid-send (attempt cap)' "
                "WHERE state='sending' AND op_method IN (%s) AND (attempt_count>=? OR attempt_count<0)"
                % _sql_in(im), (*im, constants.IDEMPOTENT_CAP))
            # 3) unknown 无时序 → 按类别重臂(postMessage:核验;幂等:重发,到 cap → failed)。
            self.conn.execute(
                "UPDATE outbound_jobs SET verify_after=?, had_unknown=1, next_attempt_at=NULL "
                "WHERE state='unknown' AND op_method IN (%s) AND verify_after IS NULL" % _sql_in(pm),
                (now, *pm))
            self.conn.execute(
                "UPDATE outbound_jobs SET state='failed', error='attempt cap (startup reconcile)' "
                "WHERE state='unknown' AND op_method IN (%s) AND next_attempt_at IS NULL "
                "AND (attempt_count>=? OR attempt_count<0)" % _sql_in(im), (*im, constants.IDEMPOTENT_CAP))
            self.conn.execute(
                "UPDATE outbound_jobs SET next_attempt_at=?, verify_after=NULL "
                "WHERE state='unknown' AND op_method IN (%s) AND next_attempt_at IS NULL" % _sql_in(im),
                (now, *im))
            # 4) pending ∧ ac≥cap → 终态(按 had_unknown 选 failed / unconfirmed)。
            for row in self.conn.execute(
                    JOB_VIEW_SQL + " WHERE o.state='pending' ORDER BY o.job_seq").fetchall():
                ac = row["attempt_count"]
                try:
                    method = op_for(row, self.cfg)[0]
                except ValueError as e:
                    self.conn.execute(
                        "UPDATE outbound_jobs SET state='cancelled', error=? WHERE job_id=? AND state='pending'",
                        ("op_for: %s" % e, row["job_id"]))
                    continue
                cap = cap_for(row["kind"], category_of(method))
                if not isinstance(ac, int) or ac < 0 or ac >= cap:
                    self._terminal_by_had_unknown(
                        row, "pending", "attempt cap reached (ac=%r, startup reconcile)" % (ac,), now)

    # ------------------------------------------------------------------ tick
    def tick(self, budget=DEFAULT_TICK_BUDGET):
        now = self.clock.wall_ms()
        # allowlist 列外 job 无论门状态都确定性 cancelled(排在 gate 早返回之前)。
        self._cancel_disallowed_jobs()
        gate = db.get_state(self.conn, constants.GATE_KEY, "ok") or "ok"
        if gate != "ok":
            return 0
        rows = self.conn.execute(
            JOB_VIEW_SQL + " WHERE (o.state='pending' AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?)) "
            "OR (o.state='unknown' AND ((o.verify_after IS NOT NULL AND o.verify_after<=?) "
            "OR (o.next_attempt_at IS NOT NULL AND o.next_attempt_at<=?) "
            "OR (o.verify_after IS NULL AND o.next_attempt_at IS NULL))) ORDER BY o.job_seq",
            (now, now, now)).fetchall()
        sends = 0
        for job in rows:
            if budget is not None and sends >= budget:
                break
            job_id = job["job_id"]
            try:
                method, target, thread_ts, _pk = op_for(job, self.cfg)
            except ValueError as e:
                with db.tx(self.conn):
                    db.cas(self.conn,
                           "UPDATE outbound_jobs SET state='cancelled', error=? "
                           "WHERE job_id=? AND state IN ('pending','unknown')", ("op_for: %s" % e, job_id))
                continue
            cat = category_of(method)
            state = job["state"]
            if state == "unknown":
                if cat == "postMessage":
                    # 核验分支:postMessage unknown **只核验**,绝不从此处发送。
                    va = job["verify_after"]
                    if va is None:  # 无时序(遗留 / recovery 旧写法)→ 自愈为核验
                        with db.tx(self.conn):
                            db.cas(self.conn,
                                   "UPDATE outbound_jobs SET verify_after=?, had_unknown=1, next_attempt_at=NULL "
                                   "WHERE job_id=? AND state='unknown' AND verify_after IS NULL", (now, job_id))
                        job = self._job_view(job_id)
                        if job is None or job["state"] != "unknown":
                            continue
                    elif va > now:
                        continue
                    vmethod = _verify_method_for(thread_ts)
                    until = self._cooldown_until(vmethod, now)
                    if until is not None:  # 冷却预检:小事务只写 verify_after,不动计数
                        with db.tx(self.conn):
                            db.cas(self.conn,
                                   "UPDATE outbound_jobs SET verify_after=? WHERE job_id=? AND state='unknown' "
                                   "AND verify_round=?", (until, job_id, job["verify_round"]))
                        continue
                    self._verify_unknown(job)
                    self._beat()
                    continue
                if cat != "idempotent":
                    continue
                nxt = job["next_attempt_at"]
                if nxt is None:  # 幂等 unknown 无时序 → 自愈为立即重发(cap 由 _prepare 守)
                    with db.tx(self.conn):
                        db.cas(self.conn,
                               "UPDATE outbound_jobs SET next_attempt_at=?, verify_after=NULL "
                               "WHERE job_id=? AND state='unknown' AND next_attempt_at IS NULL", (now, job_id))
                elif nxt > now:
                    continue
            # 发送分支(pending 到期 / 幂等 unknown 到期):冷却预检在 _prepare **之前**(§2.4-7)
            until = self._cooldown_until(method, now)
            if until is not None:
                with db.tx(self.conn):
                    db.cas(self.conn,
                           "UPDATE outbound_jobs SET next_attempt_at=? WHERE job_id=? AND state=?",
                           (until, job_id, state))
                continue
            if cat == "postMessage" and self._paced(target, now):
                continue  # 每频道 POST_MIN_INTERVAL_MS,留待下一 tick
            if self._prepare(job) == "send":
                self._send_and_finalize(job_id)
                sends += 1
                self._beat()
        return sends

    def _cancel_disallowed_jobs(self):
        allow = self.cfg.get("chat_allowlist")
        if not allow:
            return
        self.conn.execute(
            "UPDATE outbound_jobs SET state='cancelled', error='chat-not-allowed' "
            "WHERE state IN ('pending','unknown') AND chat_id NOT IN (%s)" % _sql_in(allow),
            tuple(allow))

    # ------------------------------------------------------------------ _prepare(§2.4)
    def _prepare(self, job):
        """短事务,顺序固定(§2.4):gone → allowlist / 顺序门 / 守卫 → cap → 重发资格复验 → op_* 冻结断言
        → 冻结 op_* 并 CAS → sending。→ 'gone' | 'skip' | 'cancelled' | 'terminal' | 'send'。"""
        job_id = job["job_id"]
        now = self.clock.wall_ms()
        with db.tx(self.conn):
            fresh = self._job_view(job_id)
            if fresh is None or fresh["state"] not in ("pending", "unknown"):
                return "gone"
            state = fresh["state"]
            try:
                method, target, thread_ts, payload_kind = op_for(fresh, self.cfg)
            except ValueError as e:
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='cancelled', error=? WHERE job_id=? AND state=?",
                       ("op_for: %s" % e, job_id, state))
                return "cancelled"
            cat = category_of(method)
            if cat is None:
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='cancelled', error=? WHERE job_id=? AND state=?",
                       ("unknown op_method %r" % (method,), job_id, state))
                return "cancelled"
            nxt = fresh["next_attempt_at"]
            if state == "unknown":
                if cat != "idempotent":
                    return "skip"  # postMessage unknown 只核验(§2.5「绝不盲重发」)
                if nxt is None or nxt > now:
                    return "skip"
            elif nxt is not None and nxt > now:
                return "skip"
            # 2) allowlist / 顺序门 / 守卫
            allow = self.cfg.get("chat_allowlist")
            if allow and fresh["chat_id"] not in allow:
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='cancelled', error='chat-not-allowed' "
                       "WHERE job_id=? AND state=?", (job_id, state))
                return "cancelled"
            order = self._order_gate(fresh, now)
            if order == "skip":
                return "skip"
            if order == "group-cancelled":
                return "cancelled"
            if order == "cancel" or not self._guard_ok(fresh):
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='cancelled', error=? WHERE job_id=? AND state=?",
                       ("prev-chunk-failed" if order == "cancel" else "guard", job_id, state))
                return "cancelled"
            # 3) cap
            ac = fresh["attempt_count"]
            cap = cap_for(fresh["kind"], cat)
            if not isinstance(ac, int) or ac < 0 or ac >= cap:
                if cat == "idempotent":
                    db.cas(self.conn,
                           "UPDATE outbound_jobs SET state='failed', error=? WHERE job_id=? AND state=?",
                           ("attempt cap reached (ac=%r)" % (ac,), job_id, state))
                else:
                    # postMessage:禁止发送;此处无核验在途(state=pending)→ 终态按 had_unknown 选
                    self._terminal_by_had_unknown(fresh, state, "attempt cap reached (ac=%r)" % (ac,), now)
                return "terminal"
            if cat == "postMessage" and fresh["had_unknown"]:
                # 4) 重发资格复验(R5-S1):本次实际使用的凭据快照 = self.client.tokens_version
                if not self._verify_capability_ok():
                    self._set_unconfirmed(
                        fresh, state, "resend-refused: verify capability not confirmed for current credentials",
                        now)
                    return "terminal"
                # 5) op_* 冻结断言(R3-m1 / R4-m1):编程错误也 fail-closed
                stored = (fresh["op_method"], fresh["op_target"], fresh["op_thread_ts"], fresh["op_payload_kind"])
                if fresh["op_method"] is None or stored != (method, target, thread_ts, payload_kind):
                    self._set_unconfirmed(fresh, state, "op-freeze-mismatch", now)
                    return "terminal"
            # 6) 冻结 op_*(未冻结时)+ CAS → sending
            if fresh["op_method"] is None:
                self.conn.execute(
                    "UPDATE outbound_jobs SET op_method=?, op_target=?, op_thread_ts=?, op_payload_kind=? "
                    "WHERE job_id=? AND state=?", (method, target, thread_ts, payload_kind, job_id, state))
            moved = db.cas(self.conn,
                           "UPDATE outbound_jobs SET state='sending', sending_at=?, attempt_count=attempt_count+1 "
                           "WHERE job_id=? AND state=?", (now, job_id, state))
            return "send" if moved else "gone"

    def _order_gate(self, job, now):
        """§2.8。→ None(放行)| 'skip' | 'cancel'(前块 failed/cancelled)| 'group-cancelled'(前块 unconfirmed,
        本组余块已在本事务内取消)。"""
        tg = job["turn_group"]
        ci = job["chunk_index"] or 0
        if tg is not None and ci > 0:
            prev = self.conn.execute(
                "SELECT state FROM outbound_jobs WHERE turn_group=? AND chunk_index=?", (tg, ci - 1)).fetchone()
            if prev is None:
                return "skip"
            ps = prev["state"]
            if ps in ("failed", "cancelled"):
                return "cancel"
            if ps == "unconfirmed":
                self._cancel_group_after_unconfirmed(job, ci - 1, now)
                return "group-cancelled"
            if ps != "sent":
                return "skip"
        if job["kind"] == "session_turn":
            # 跨组:同 binding 更早组存在 pending/sending/unknown → 阻塞;终态放行
            blocker = self.conn.execute(
                "SELECT 1 FROM outbound_jobs WHERE kind='session_turn' AND binding_id=? AND job_seq<? "
                "AND turn_group IS NOT NULL AND turn_group!=? AND state IN ('pending','sending','unknown') LIMIT 1",
                (job["binding_id"], job["job_seq"], tg or "")).fetchone()
            if blocker:
                return "skip"
        if job["kind"] in NOTICE_KINDS:
            blocker = self.conn.execute(
                "SELECT 1 FROM outbound_jobs WHERE chat_id=? AND job_seq<? AND kind IN (%s) "
                "AND state IN ('pending','sending','unknown') LIMIT 1" % _sql_in(NOTICE_KINDS),
                (job["chat_id"], job["job_seq"], *NOTICE_KINDS)).fetchone()
            if blocker:
                return "skip"
        return None

    def _cancel_group_after_unconfirmed(self, job, unconfirmed_chunk_index, now):
        """前块 unconfirmed → 同一事务把本组剩余(未发)块 cancelled(error prev-unconfirmed),计数 +1,只发一条告警。"""
        tg = job["turn_group"]
        if tg is None:
            return 0
        cur = self.conn.execute(
            "UPDATE outbound_jobs SET state='cancelled', error='prev-unconfirmed' "
            "WHERE turn_group=? AND chunk_index>? AND state='pending'", (tg, unconfirmed_chunk_index))
        if cur.rowcount > 0:
            db.bump_counter(self.conn, "group_cancelled_after_unconfirmed")
            self._enqueue_alert(job, texts.group_cancelled_alert_body(), "group:" + tg, now)
        return cur.rowcount

    # ------------------------------------------------------------------ 守卫(per-kind;结构化字段,不解析 body)
    def _guard_ok(self, job):
        kind = job["kind"]
        if kind == "session_turn":
            return self._binding_active(job["binding_id"])
        if kind == "approval_card":
            p = self._pending(job["ref_pending_id"])
            return (p is not None and p["state"] == "pending" and p["card_message_id"] is None
                    and self._binding_active(job["binding_id"]))
        if kind == "decision_notice":
            return self._decision_guard(job)
        if kind == "lifecycle_notice":
            b = self.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (job["binding_id"],)).fetchone()
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

    def _decision_guard(self, job):
        """§5.5 六种 outcome 的守卫(expected_state ∉ DECISION_OUTCOMES → False,fail-closed)。"""
        exp = job["expected_state"]
        p = self._pending(job["ref_pending_id"])
        mid = p["message_id"] if p is not None else job["ref_message_id"]
        r = self._inbox(mid) if mid else None
        if exp == "delivered":
            return p is not None and p["state"] == "approved" and r is not None and r["state"] == "enqueued"
        if exp == "approved_pending_files":
            return p is not None and p["state"] == "approved" and r is not None and r["state"] == "materializing"
        if exp in ("rejected", "expired"):
            return p is not None and p["state"] == exp
        if exp == "attachment_failed":
            return r is not None and r["state"] == "failed"
        if exp == "closed_undelivered":
            return r is not None and r["state"] == "undeliverable"
        return False

    def _binding_active(self, binding_id):
        b = self.conn.execute("SELECT status FROM bindings WHERE binding_id=?", (binding_id,)).fetchone()
        return b is not None and b["status"] == "active"

    def _pending(self, pending_id):
        if pending_id is None:
            return None
        return self.conn.execute("SELECT * FROM pendings WHERE pending_id=?", (pending_id,)).fetchone()

    def _inbox(self, message_id):
        if message_id is None:
            return None
        return self.conn.execute("SELECT * FROM inbox WHERE message_id=?", (message_id,)).fetchone()

    # ------------------------------------------------------------------ 发送 → 分类 → 收口
    def _send_and_finalize(self, job_id):
        job = self._job_view(job_id)
        if job is None or job["state"] != "sending":
            return
        res = self._transmit(job)  # 网络在事务外;只用冻结 op_*
        now = self.clock.wall_ms()
        cls = self._classify(job, res)
        if category_of(job["op_method"]) == "postMessage" and cls != "wait":
            self._last_post_at[job["op_target"]] = now  # 每频道节流(请求已发出才计)
        self._log("send %s key=%s method=%s -> %s err=%s http=%s" % (
            job["kind"], job["idempotency_key"], job["op_method"], cls, res.error, res.http_status))
        self._finalize(job, res, cls, now)

    def _transmit(self, job):
        """→ CallResult。**只用冻结的 op_***(§2.7),不重读 cfg 重选形态。"""
        method = job["op_method"]
        target = job["op_target"]
        thread_ts = job["op_thread_ts"]
        pk = job["op_payload_kind"]
        body = job["body"]
        if method == "chat.postMessage":
            params = {
                "channel": target, "unfurl_links": False, "unfurl_media": False,
                "metadata": {"event_type": constants.METADATA_EVENT_TYPE,
                             "event_payload": {"job_id": job["job_id"]}},
            }
            if thread_ts:
                params["thread_ts"] = thread_ts
            if pk == "markdown_text":
                params["markdown_text"] = body or ""
            elif pk == "text":
                text = body or ""
                if not text and job["kind"] == "decision_notice" \
                        and job["expected_state"] in constants.DECISION_OUTCOMES:
                    text = texts.decision_notice_body(job["expected_state"])
                params["text"] = text
            elif pk == "blocks":
                card = self._load_json(body)
                if not isinstance(card, dict) or not isinstance(card.get("blocks"), list):
                    return CallResult(ok=False, error="invalid_blocks")  # 本地永久错(不上网)
                params["blocks"] = card["blocks"]
                params["text"] = card.get("text") or ""
            else:
                return CallResult(ok=False, error="no_text")  # 未知形态:本地永久错
        elif method == "chat.update":
            try:
                channel, ts = util.split_message_id(target)
            except ValueError:
                return CallResult(ok=False, error="message_not_found")
            outcome = job["expected_state"]
            extra = self._load_json(body)
            extra = extra if isinstance(extra, dict) else {}
            decided_by = extra.get("decided_by") or _g(job, "card_decided_by")
            try:
                blocks = texts.decision_update_blocks(outcome, decided_by, extra.get("preview"))
                text = texts.decision_update_text(outcome)
            except (ValueError, KeyError):
                return CallResult(ok=False, error="invalid_blocks")
            params = {"channel": channel, "ts": ts, "blocks": blocks, "text": text}
        elif method == "reactions.add":
            ref = job["ref_message_id"] or ""
            ts = ref.rpartition(":")[2] if ":" in ref else ref
            if not ts:
                return CallResult(ok=False, error="bad_timestamp")
            params = {"channel": target, "timestamp": ts, "name": constants.RECEIPT_REACTION}
        else:
            return CallResult(ok=False, error="invalid_arguments")
        return self.client.call(method, params, timeout_s=constants.SEND_TIMEOUT_S)

    @staticmethod
    def _load_json(body):
        if not body:
            return None
        try:
            return json.loads(body)
        except ValueError:
            return None

    def _classify(self, job, res):
        """slackapi.classify_send_error + Outbound 上下文特判(§2.5):already_reacted = sent;
        markdown_rejected = invalid_arguments ∧ op_payload_kind=='markdown_text'(优先于 PERMANENT)。"""
        base = classify_send_error(res)
        if base in ("sent", "wait", "ratelimited"):
            return base
        method = job["op_method"]
        if method == "reactions.add" and res.error in constants.ALREADY_DONE_ERRORS:
            return "sent"
        if method in constants.POSTMESSAGE_METHODS and job["op_payload_kind"] == "markdown_text" \
                and res.error == "invalid_arguments":
            return CLS_MARKDOWN_REJECTED
        return base

    def _finalize(self, job, res, cls, now):
        """§2.5 转移表;全部 CAS WHERE state='sending'。"""
        job_id = job["job_id"]
        cat = category_of(job["op_method"])
        had = bool(job["had_unknown"])
        err = res.error or cls
        if cls == CLS_MARKDOWN_REJECTED and cat == "postMessage" and not had:
            self._persist_markdown_text()  # 文件 IO 在事务外(失败退化为内存切换)
        with db.tx(self.conn):
            if cls == "wait":
                # 发送分支 wait:只撤销本次 sending 与 ac 增量;不动任何 count(易错点 ③)
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='pending', attempt_count=attempt_count-1, next_attempt_at=? "
                       "WHERE job_id=? AND state='sending'", (res.cooldown_until, job_id))
                db.bump_counter(self.conn, "cooldown_waits")
                return
            if cls == "sent":
                mid = self._sent_message_id(job, res)
                moved = db.cas(self.conn,
                               "UPDATE outbound_jobs SET state='sent', sent_message_id=?, sent_at=?, error=NULL, "
                               "verify_after=NULL, next_attempt_at=NULL WHERE job_id=? AND state='sending'",
                               (mid, now, job_id))
                if moved:
                    self._backfill_card(job, mid)
                return
            if cat == "postMessage":
                if cls == "failed":
                    self._terminal_by_had_unknown(job, "sending", err, now)
                elif cls == CLS_MARKDOWN_REJECTED:
                    if had:
                        self._set_unconfirmed(job, "sending", CLS_MARKDOWN_REJECTED, now)  # 不得改形态(R4-m1)
                    else:
                        # 同一 attempt 内不发第二个请求;ac-1 不计 transient;op_* 归零由下一次 _prepare 记 text
                        db.cas(self.conn,
                               "UPDATE outbound_jobs SET state='pending', next_attempt_at=?, "
                               "attempt_count=attempt_count-1, op_method=NULL, op_target=NULL, op_thread_ts=NULL, "
                               "op_payload_kind=NULL, error=? WHERE job_id=? AND state='sending'",
                               (now, CLS_MARKDOWN_REJECTED, job_id))
                elif cls == "ratelimited":
                    self._backoff_ratelimited(job, res, now)
                elif cls == "not_sent":
                    self._backoff_transient(job, err, now)
                else:  # unknown → 只核验,绝不盲重发
                    db.cas(self.conn,
                           "UPDATE outbound_jobs SET state='unknown', had_unknown=1, verify_after=?, "
                           "next_attempt_at=NULL, error=? WHERE job_id=? AND state='sending'",
                           (now + constants.VERIFY_SCHEDULE_MS[0], err, job_id))
                return
            # 幂等类
            if cls == "failed":
                self._set_failed(job, "sending", err, now, alert=False)
            elif cls == "ratelimited":
                self._backoff_ratelimited(job, res, now)
            elif cls in ("not_sent", CLS_MARKDOWN_REJECTED):
                self._backoff_transient(job, err, now)
            else:  # unknown → 直接重发(cap 由 _prepare 守)
                db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='unknown', next_attempt_at=?, verify_after=NULL, error=? "
                       "WHERE job_id=? AND state='sending'",
                       (now + constants.IDEMPOTENT_RETRY_DELAY_MS, err, job_id))

    def _persist_markdown_text(self):
        try:
            self.cfg.set_persist("markdown_mode", "text")
        except Exception as e:  # noqa: BLE001
            try:
                self.cfg["markdown_mode"] = "text"
            except Exception:  # noqa: BLE001
                pass
            self._log("markdown_mode persist failed (%s); switched in-memory only" % type(e).__name__)

    @staticmethod
    def _sent_message_id(job, res):
        method = job["op_method"]
        if method == "reactions.add" or not isinstance(res.data, dict):
            return None
        ts = res.get("ts")
        if not ts:
            msg = res.get("message")
            ts = msg.get("ts") if isinstance(msg, dict) else None
        if not ts:
            return None
        channel = res.get("channel")
        if not channel:
            if method == "chat.update":
                try:
                    channel = util.split_message_id(job["op_target"])[0]
                except ValueError:
                    channel = None
            else:
                channel = job["op_target"]
        if not channel:
            return None
        return util.message_id_of(channel, ts)

    def _backfill_card(self, job, mid):
        if job["kind"] == "approval_card" and mid and job["ref_pending_id"]:
            self.conn.execute(
                "UPDATE pendings SET card_message_id=? WHERE pending_id=? AND card_message_id IS NULL",
                (mid, job["ref_pending_id"]))

    def _backoff_ratelimited(self, job, res, now):
        """ratelimited:pending,next=max(now+RA·1000, cooldown[method]);ac-1;ratelimit_count+1;≥cap → 终态。"""
        job_id = job["job_id"]
        ra = res.retry_after if res.retry_after is not None else 1
        until = self._cooldown_until(job["op_method"], now) or 0
        nxt = max(now + int(ra) * 1000, until)
        count = int(job["ratelimit_count"] or 0) + 1
        db.bump_counter(self.conn, "ratelimit_hits")
        if count >= constants.RATELIMIT_CAP:
            self.conn.execute(
                "UPDATE outbound_jobs SET ratelimit_count=?, attempt_count=attempt_count-1 "
                "WHERE job_id=? AND state='sending'", (count, job_id))
            self._terminal_by_had_unknown(job, "sending", "ratelimited x%d (cap)" % count, now)
            return
        db.cas(self.conn,
               "UPDATE outbound_jobs SET state='pending', next_attempt_at=?, attempt_count=attempt_count-1, "
               "ratelimit_count=?, error='ratelimited' WHERE job_id=? AND state='sending'", (nxt, count, job_id))

    def _backoff_transient(self, job, err, now):
        """not_sent(其它):pending,next=now+min(5s·2^tc, 45s);ac-1;transient_count+1;≥cap → 终态。冷却不走此行。"""
        job_id = job["job_id"]
        tc = int(job["transient_count"] or 0)
        delay = min(constants.TRANSIENT_BACKOFF_MS * (2 ** tc), constants.TRANSIENT_BACKOFF_MAX_MS)
        count = tc + 1
        if count >= constants.TRANSIENT_CAP:
            self.conn.execute(
                "UPDATE outbound_jobs SET transient_count=?, attempt_count=attempt_count-1 "
                "WHERE job_id=? AND state='sending'", (count, job_id))
            self._terminal_by_had_unknown(job, "sending", "%s (transient cap x%d)" % (err, count), now)
            return
        db.cas(self.conn,
               "UPDATE outbound_jobs SET state='pending', next_attempt_at=?, attempt_count=attempt_count-1, "
               "transient_count=?, error=? WHERE job_id=? AND state='sending'", (now + delay, count, err, job_id))

    # ------------------------------------------------------------------ 终态 + 告警(调用方已在事务内)
    def _terminal_by_had_unknown(self, job, from_state, error, now):
        if job["had_unknown"]:
            return self._set_unconfirmed(job, from_state, error, now)
        return self._set_failed(job, from_state, error, now, alert=True)

    def _set_failed(self, job, from_state, error, now, alert=True):
        moved = db.cas(self.conn,
                       "UPDATE outbound_jobs SET state='failed', error=?, next_attempt_at=NULL, verify_after=NULL "
                       "WHERE job_id=? AND state=?", (error, job["job_id"], from_state))
        if moved and alert:
            self._enqueue_alert(job, texts.send_failure_alert_body(), job["job_id"], now)
        return moved

    def _set_unconfirmed(self, job, from_state, error, now, verify_round=None):
        """unconfirmed 诚实终态(I6):告警(turn)+ 同一事务取消本组余块(§2.8)。"""
        sql = ("UPDATE outbound_jobs SET state='unconfirmed', error=?, next_attempt_at=NULL, verify_after=NULL "
               "WHERE job_id=? AND state=?")
        params = [error, job["job_id"], from_state]
        if verify_round is not None:
            sql += " AND verify_round=?"
            params.append(verify_round)
        moved = db.cas(self.conn, sql, tuple(params))
        if moved:
            self._enqueue_alert(job, texts.unconfirmed_alert_body(), job["job_id"], now)
            if job["kind"] == "session_turn" and job["turn_group"] is not None:
                self._cancel_group_after_unconfirmed(job, job["chunk_index"] or 0, now)
        return moved

    def _enqueue_alert(self, job, body, suffix, now):
        """告警 = session_turn + 哨兵 turn_group(幂等键去重;走 _binding_active 守卫)。
        只对 session_turn 的失败/不确定发告警(card → status 高亮;通知类固定文案不告警);告警自身不再生告警。"""
        if job["kind"] != "session_turn" or self._is_alert_turn(job):
            return False
        tg = ALERT_GROUP_PREFIX + suffix
        return jobs.create_job(
            self.conn, kind="session_turn", chat_id=job["chat_id"], binding_id=job["binding_id"],
            idempotency_key=jobs.key_turn(tg, 0), turn_group=tg, chunk_index=0, body=body, now=now)

    # ------------------------------------------------------------------ 核验(§2.6)
    def _verify_unknown(self, job):
        """postMessage 类 unknown 的核验:replies(有 op_thread_ts)/ history;≤ VERIFY_MAX_PAGES 页;
        命中 = bot_id ∧ metadata.event_type ∧ event_payload.job_id;三分结果 + CAS WHERE state='unknown' AND verify_round=?。
        永久错细分:VERIFY_GLOBAL_DEGRADE_ERRORS → unconfirmed + verify_capability=degraded:<err>(全局);
        VERIFY_CHANNEL_ERRORS → 只本 job unconfirmed(不动能力);其余任何错误码 → error 分支(退避 / cap)。
        → 'hit' | 'absent' | 'resend' | 'unconfirmed' | 'error' | 'wait' | 'ratelimited' | 'stale'。"""
        job_id = job["job_id"]
        round_ = job["verify_round"]
        target = job["op_target"]
        thread_ts = job["op_thread_ts"]
        method = _verify_method_for(thread_ts)
        now0 = self.clock.wall_ms()
        sending_at = job["sending_at"] if job["sending_at"] is not None else (job["created_at"] or now0)
        params = {
            "channel": target,
            "oldest": "%.6f" % (max(0, sending_at - constants.VERIFY_LOOKBACK_MS) / 1000.0),
            "inclusive": True, "include_all_metadata": True, "limit": constants.VERIFY_PAGE_LIMIT,
        }
        if thread_ts:
            params["ts"] = thread_ts
        outcome = None
        hit_ts = None
        res = None
        for _page in range(constants.VERIFY_MAX_PAGES):
            res = self.client.call(method, params, timeout_s=constants.SEND_TIMEOUT_S)
            cls = classify_send_error(res)
            if cls == "sent":
                msgs = res.get("messages")
                if not isinstance(msgs, list):
                    outcome = "error"       # 响应没有 messages 列表 = 不完整查询,绝不算 absent
                    break
                hit_ts = self._find_hit(job_id, msgs)
                if hit_ts:
                    outcome = "hit"
                    break
                meta = res.get("response_metadata")
                cursor = meta.get("next_cursor") if isinstance(meta, dict) else None
                cursor = cursor if isinstance(cursor, str) and cursor else None
                if res.get("has_more") or cursor:
                    if not cursor:
                        outcome = "error"   # 声称还有更多却给不出 cursor → 无法翻页 → 不算 absent
                        break
                    params = dict(params, cursor=cursor)
                    continue  # 翻页;页数用尽仍 has_more → 循环结束 outcome None → error
                outcome = "absent"          # 只有完整翻完且未命中才是 absent
                break
            if cls == "wait":
                outcome = "wait"
            elif cls == "ratelimited":
                outcome = "ratelimited"
            elif res.error in constants.VERIFY_GLOBAL_DEGRADE_ERRORS:
                outcome = "permanent_global"    # 能力级永久错:degraded 全局 + 本 job unconfirmed
            elif res.error in constants.VERIFY_CHANNEL_ERRORS:
                outcome = "permanent_channel"   # 频道级永久错:只本 job unconfirmed,不动 verify_capability
            else:
                outcome = "error"               # 其余任何 ok:false / 传输错 → 退避 / cap
            break
        if outcome is None:
            outcome = "error"
        now = self.clock.wall_ms()
        err = (res.error if res is not None else None) or outcome
        self._log("verify %s key=%s method=%s round=%d -> %s err=%s" % (
            job["kind"], job["idempotency_key"], method, round_, outcome, err))
        with db.tx(self.conn):
            fresh = self._job_view(job_id)
            if fresh is None or fresh["state"] != "unknown" or fresh["verify_round"] != round_:
                return "stale"
            where = " WHERE job_id=? AND state='unknown' AND verify_round=?"
            if outcome == "hit":
                mid = util.message_id_of(target, hit_ts)
                moved = db.cas(self.conn,
                               "UPDATE outbound_jobs SET state='sent', sent_message_id=?, sent_at=?, error=NULL, "
                               "verify_after=NULL, next_attempt_at=NULL" + where, (mid, now, job_id, round_))
                if moved:
                    db.bump_counter(self.conn, "verify_hit")
                    self._backfill_card(fresh, mid)
                return "hit"
            if outcome == "absent":
                n = int(fresh["verify_absent_count"] or 0) + 1  # n 在同一事务内算出并分支(R6-m2)
                db.bump_counter(self.conn, "verify_absent")
                if n < constants.VERIFY_ABSENT_RESEND_AT:
                    idx = min(n, len(constants.VERIFY_SCHEDULE_MS) - 1)
                    db.cas(self.conn,
                           "UPDATE outbound_jobs SET verify_absent_count=?, verify_after=?, error=?" + where,
                           (n, now + constants.VERIFY_SCHEDULE_MS[idx], "verify-absent x%d" % n, job_id, round_))
                    return "absent"
                eligible = (constants.RESEND_ONCE and int(fresh["resend_count"] or 0) == 0
                            and self._verify_capability_ok())
                if eligible:
                    # 唯一切轮点:verify_round+1 并把两个计数与档位归零(R6-m2)
                    db.cas(self.conn,
                           "UPDATE outbound_jobs SET state='pending', next_attempt_at=?, resend_count=resend_count+1, "
                           "verify_round=verify_round+1, verify_absent_count=0, verify_error_count=0, "
                           "verify_after=NULL, error='verify-absent: resend'" + where, (now, job_id, round_))
                    db.bump_counter(self.conn, "verify_resent")
                    return "resend"
                self._set_unconfirmed(fresh, "unknown", "verify-absent x%d (no resend)" % n, now, verify_round=round_)
                db.bump_counter(self.conn, "verify_unconfirmed")
                return "unconfirmed"
            if outcome == "wait":
                db.cas(self.conn, "UPDATE outbound_jobs SET verify_after=?" + where,
                       (res.cooldown_until, job_id, round_))
                db.bump_counter(self.conn, "cooldown_waits")
                return "wait"
            if outcome == "ratelimited":
                ra = res.retry_after if res.retry_after is not None else 1
                until = max(now + int(ra) * 1000, self._cooldown_until(method, now) or 0)
                db.cas(self.conn, "UPDATE outbound_jobs SET verify_after=?" + where, (until, job_id, round_))
                db.bump_counter(self.conn, "ratelimit_hits")
                return "ratelimited"
            if outcome in ("permanent_global", "permanent_channel"):
                if outcome == "permanent_global":
                    db.set_state(self.conn, constants.VERIFY_CAPABILITY_KEY, "degraded:%s" % err)
                self._set_unconfirmed(fresh, "unknown", "verify-permanent: %s" % err, now, verify_round=round_)
                db.bump_counter(self.conn, "verify_unconfirmed")
                return "unconfirmed"
            # error:计 verify_error_count,不动 absent;cap / deadline → unconfirmed
            ve_old = int(fresh["verify_error_count"] or 0)
            ve = ve_old + 1
            if ve >= constants.VERIFY_ERROR_CAP or now - sending_at > constants.VERIFY_DEADLINE_MS:
                self.conn.execute("UPDATE outbound_jobs SET verify_error_count=?" + where, (ve, job_id, round_))
                self._set_unconfirmed(fresh, "unknown", "verify-error x%d: %s" % (ve, err), now,
                                      verify_round=round_)
                db.bump_counter(self.conn, "verify_unconfirmed")
                return "unconfirmed"
            delay = min(constants.VERIFY_ERROR_BACKOFF_MS * (2 ** ve_old), constants.VERIFY_ERROR_BACKOFF_MAX_MS)
            db.cas(self.conn,
                   "UPDATE outbound_jobs SET verify_error_count=?, verify_after=?, error=?" + where,
                   (ve, now + delay, "verify-error: %s" % err, job_id, round_))
            return "error"

    def _find_hit(self, job_id, messages):
        bot_id = self.cfg.get("bot_id") if hasattr(self.cfg, "get") else None
        if not bot_id or not isinstance(messages, list):
            return None
        for m in messages:
            if not isinstance(m, dict) or m.get("bot_id") != bot_id:
                continue
            meta = m.get("metadata")
            if not isinstance(meta, dict) or meta.get("event_type") != constants.METADATA_EVENT_TYPE:
                continue
            payload = meta.get("event_payload")
            if isinstance(payload, dict) and payload.get("job_id") == job_id and m.get("ts"):
                return m["ts"]
        return None
