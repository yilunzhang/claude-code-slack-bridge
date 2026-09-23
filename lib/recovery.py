"""恢复工人(plan 4.8):驱动一切非终态;重驱只用行上钉死的 binding_id;
幂等=DB 约束(唯一键 + 确定性 job 键 + 带旧状态 CAS)。
r7-②:waiting_binding 专用分支先行,通用 undeliverable/dropped 分支绝不截获 waiting 行
(通用清扫的 SELECT 按状态显式排除 waiting_binding,结构上不可能碰到)。"""
import json
import os
import shutil

from . import constants, db, jobs, lifecycle, texts, util


class Recovery:
    def __init__(self, conn, cfg, runner, clock, inbound, prober):
        self.conn = conn
        self.cfg = cfg
        self.runner = runner
        self.clock = clock
        self.inbound = inbound
        self.prober = prober

    # ---------------- 快节奏(每 loop ~1s / 判死 ~5s) ----------------
    def fast_tick(self, in_suspect_window=False):
        # confirmed starting → 激活 / 30s 超时
        for b in self.conn.execute(
                "SELECT binding_id FROM bindings WHERE status='starting' "
                "AND bind_phase='confirmed'").fetchall():
            lifecycle.activate_if_ready(self.conn, b["binding_id"], self.clock)
        # waiting_binding 专用分支(激活重过分流门 / 终止映射)
        self.inbound.drive_waiting_rows()
        # 判死
        lifecycle.death_scan(self.conn, self.prober, self.clock,
                             in_suspect_window=in_suspect_window)

    # ---------------- 慢节奏(启动 + 每 60s) ----------------
    def slow_tick(self):
        now = self.clock.wall_ms()
        # r7-②:waiting 专用分支先行
        self.inbound.drive_waiting_rows()
        self._redrive_resolving(now)
        self._replenish_cards(now)
        self._rearm_failed_cards(now)
        self._redrive_materializing(now)
        self._expire_pendings(now)
        lifecycle.expire_stale_pending_binds(self.conn, self.clock)
        self._close_orphan_starting(now)
        self._reclaim_leases(now)
        self._sweep_stranded_enqueued(now)
        self._legacy_sending(now)
        self._retention(now)

    # ------------------------------------------------------------------
    def _redrive_resolving(self, now):
        rows = self.conn.execute(
            "SELECT * FROM inbox WHERE state IN ('received','resolving')").fetchall()
        for r in rows:
            if r["ts"] is not None and now - r["ts"] > constants.RESOLVE_DEADLINE_MS:
                # 有限重试到期 → failed + 静默(未确认@bot 绝不回群,4.2.2)
                with db.tx(self.conn):
                    db.cas(self.conn,
                           "UPDATE inbox SET state='failed', ts=? "
                           "WHERE message_id=? AND state IN ('received','resolving')",
                           (now, r["message_id"]))
                    db.bump_counter(self.conn, "resolve_deadline_failed")
                continue
            self.inbound.drive_row(r)

    def _replenish_cards(self, now):
        """awaiting_approval 无卡片 job → 补发;已 sent 未回填 card_message_id → 补回填。"""
        rows = self.conn.execute(
            "SELECT p.*, i.chat_id AS chat_id, i.snapshot_json AS snapshot_json "
            "FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE p.state='pending'").fetchall()
        for p in rows:
            job = self.conn.execute(
                "SELECT * FROM outbound_jobs WHERE idempotency_key=?",
                (jobs.key_card(p["pending_id"]),)).fetchone()
            if job is None:
                try:
                    snap = json.loads(p["snapshot_json"] or "{}")
                except ValueError:
                    snap = {}
                from . import inbound as inbound_mod
                sender = (snap.get("sender") or {}).get("id") or "?"
                jobs.create_job(
                    self.conn, kind="approval_card", chat_id=p["chat_id"],
                    binding_id=p["binding_id"], reply_to=p["message_id"],
                    idempotency_key=jobs.key_card(p["pending_id"]),
                    ref_pending_id=p["pending_id"], expected_state="pending",
                    body=texts.build_approval_card(
                        p["pending_id"], p["nonce"], sender,
                        inbound_mod.extract_text(snap, self.cfg.get("app_id"))),
                    now=now)
            elif (p["card_message_id"] is None and job["state"] == "sent"
                    and job["sent_message_id"]):
                self.conn.execute(
                    "UPDATE pendings SET card_message_id=? "
                    "WHERE pending_id=? AND card_message_id IS NULL",
                    (job["sent_message_id"], p["pending_id"]))

    def _rearm_failed_cards(self, now):
        """修复项3:failed approval_card 重臂(修"member 消息悬挂到审批 TTL")。
        CAS failed→pending + 退避 next_attempt_at;总尝试上限后放弃并 status 高亮(计一次)。"""
        rows = self.conn.execute(
            "SELECT o.* FROM outbound_jobs o JOIN pendings p ON p.pending_id=o.ref_pending_id "
            "WHERE o.kind='approval_card' AND o.state='failed' "
            "AND p.state='pending' AND p.card_message_id IS NULL").fetchall()
        for j in rows:
            if (j["attempt_count"] or 0) >= constants.CARD_REARM_MAX_ATTEMPTS:
                with db.tx(self.conn):
                    if db.cas(self.conn,
                              "UPDATE outbound_jobs SET error='given-up' "
                              "WHERE job_id=? AND state='failed' "
                              "AND (error IS NULL OR error!='given-up')", (j["job_id"],)):
                        db.bump_counter(self.conn, "approval_card_given_up")
                continue
            delay = min(
                constants.CARD_REARM_BACKOFF_MS * (2 ** max((j["attempt_count"] or 1) - 1, 0)),
                constants.CARD_REARM_BACKOFF_MAX_MS)
            db.cas(self.conn,
                   "UPDATE outbound_jobs SET state='pending', next_attempt_at=? "
                   "WHERE job_id=? AND state='failed' "
                   "AND EXISTS(SELECT 1 FROM bindings b "
                   "  WHERE b.binding_id=outbound_jobs.binding_id AND b.status='active')",
                   (now + delay, j["job_id"]))  # r2-M3:绑定复验在同一语句内

    def _redrive_materializing(self, now):
        rows = self.conn.execute(
            "SELECT * FROM inbox WHERE state='approved_materializing'").fetchall()
        for r in rows:
            b = None
            if r["binding_id"]:
                b = self.conn.execute(
                    "SELECT status FROM bindings WHERE binding_id=?",
                    (r["binding_id"],)).fetchone()
            if b is None or b["status"] != "active":
                # 通用分支:绑定复验失败 → undeliverable(只针对非 waiting 状态,r7-②)
                with db.tx(self.conn):
                    db.cas(self.conn,
                           "UPDATE inbox SET state='undeliverable', ts=? "
                           "WHERE message_id=? AND state='approved_materializing'",
                           (now, r["message_id"]))
                continue
            if r["ts"] is not None and now - r["ts"] > constants.MATERIALIZE_DEADLINE_MS:
                p = self.conn.execute(
                    "SELECT * FROM pendings WHERE message_id=?",
                    (r["message_id"],)).fetchone()
                with db.tx(self.conn):
                    if db.cas(self.conn,
                              "UPDATE inbox SET state='failed', ts=? "
                              "WHERE message_id=? AND state='approved_materializing'",
                              (now, r["message_id"])) and p is not None:
                        jobs.create_job(
                            self.conn, kind="decision_notice", chat_id=r["chat_id"],
                            binding_id=r["binding_id"],
                            idempotency_key=jobs.key_dec(p["pending_id"], "failed"),
                            ref_pending_id=p["pending_id"], ref_message_id=r["message_id"],
                            expected_state="failed",
                            body=texts.decision_notice_body("failed"), now=now)
                continue
            self.inbound.drive_row(r)

    def _expire_pendings(self, now):
        rows = self.conn.execute(
            "SELECT p.*, i.chat_id AS chat_id FROM pendings p "
            "JOIN inbox i ON p.message_id=i.message_id "
            "WHERE p.state='pending' AND p.created_at IS NOT NULL AND p.created_at+?<?",
            (constants.PENDING_TTL_MS, now)).fetchall()
        for p in rows:
            with db.tx(self.conn):
                if not db.cas(self.conn,
                              "UPDATE pendings SET state='expired', decided_at=? "
                              "WHERE pending_id=? AND state='pending'",
                              (now, p["pending_id"])):
                    continue
                db.cas(self.conn,
                       "UPDATE inbox SET state='expired', ts=? "
                       "WHERE message_id=? AND state='awaiting_approval'",
                       (now, p["message_id"]))
                jobs.create_job(
                    self.conn, kind="decision_notice", chat_id=p["chat_id"],
                    binding_id=p["binding_id"],
                    idempotency_key=jobs.key_dec(p["pending_id"], "expired"),
                    ref_pending_id=p["pending_id"], expected_state="expired",
                    body=texts.decision_notice_body("expired"), now=now)

    def _close_orphan_starting(self, now):
        """安全网:unconfirmed starting 且其 pending_bind 已终态 → bind_timeout 终止。"""
        rows = self.conn.execute(
            "SELECT b.binding_id FROM bindings b "
            "LEFT JOIN pending_bind pb ON pb.request_id=b.binding_id AND pb.state='pending' "
            "WHERE b.status='starting' AND b.bind_phase='unconfirmed' "
            "AND pb.request_id IS NULL").fetchall()
        for r in rows:
            lifecycle.terminate_binding(self.conn, r["binding_id"], "bind_timeout", self.clock)

    def _reclaim_leases(self, now):
        """r2-M3:处置=单条 CAS,active 判定在同一语句内(EXISTS)——
        读-判-写不再分离,与并发 unbind 交错时必落 dropped 而非复活 enqueued。"""
        self.conn.execute(
            "UPDATE deliveries SET "
            "state = CASE WHEN EXISTS(SELECT 1 FROM bindings b "
            "  WHERE b.binding_id=deliveries.binding_id AND b.status='active') "
            "  THEN 'enqueued' ELSE 'dropped' END, "
            "lease_token=NULL, lease_epoch=NULL, lease_pid=NULL, lease_start=NULL, "
            "lease_until=NULL "
            "WHERE state='leased' AND lease_until IS NOT NULL AND lease_until<?",
            (now,))

    def _sweep_stranded_enqueued(self, now):
        """r2-M3 防御性:终态绑定上滞留的 enqueued(理论不应再有)→ dropped。"""
        cur = self.conn.execute(
            "UPDATE deliveries SET state='dropped' WHERE state='enqueued' "
            "AND EXISTS(SELECT 1 FROM bindings b WHERE b.binding_id=deliveries.binding_id "
            "AND b.status IN ('dead','closed'))")
        if cur.rowcount:
            db.bump_counter(self.conn, "stranded_enqueued_dropped", cur.rowcount)

    def _legacy_sending(self, now):
        self.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', error='stale-sending', "
            "next_attempt_at=? WHERE state='sending' AND sending_at IS NOT NULL "
            "AND sending_at<?", (now, now - 2 * constants.SEND_TIMEOUT_S * 1000))

    # ------------------------------------------------------------------
    def _retention(self, now):
        """终态行 retention:正文裁剪、骨架保留;终态 media TTL 删(非终态禁删)。"""
        cutoff = now - constants.RETENTION_MS
        qs = ",".join("?" for _ in constants.INBOX_TERMINAL_STATES)
        self.conn.execute(
            f"UPDATE inbox SET snapshot_json=NULL WHERE state IN ({qs}) "
            "AND ts IS NOT NULL AND ts<? AND snapshot_json IS NOT NULL",
            (*constants.INBOX_TERMINAL_STATES, cutoff))
        self.conn.execute(
            "UPDATE deliveries SET payload_json='{}' WHERE state IN ('emitted','dropped') "
            "AND enq_at IS NOT NULL AND enq_at<? AND payload_json!='{}'", (cutoff,))
        self.conn.execute(
            "UPDATE outbound_jobs SET body=NULL WHERE state IN ('sent','failed','cancelled') "
            "AND created_at IS NOT NULL AND created_at<? AND body IS NOT NULL", (cutoff,))
        # media:仅终态消息的目录可删
        media_root = self.inbound.media_root
        try:
            binding_dirs = os.listdir(media_root)
        except OSError:
            return
        for bdir in binding_dirs:
            bpath = os.path.join(media_root, bdir)
            # 修复项8:lstat 语义,不跟随 symlink(绝不清 media root 之外)
            if os.path.islink(bpath) or not os.path.isdir(bpath):
                continue
            for mdir in os.listdir(bpath):
                mpath = os.path.join(bpath, mdir)
                if os.path.islink(mpath) or not os.path.isdir(mpath) \
                        or mdir.startswith("."):
                    continue
                row = self.conn.execute(
                    "SELECT state, ts FROM inbox WHERE message_id=?", (mdir,)).fetchone()
                if row is None:
                    continue
                if row["state"] in constants.INBOX_TERMINAL_STATES \
                        and row["ts"] is not None and row["ts"] < cutoff:
                    shutil.rmtree(mpath, ignore_errors=True)
