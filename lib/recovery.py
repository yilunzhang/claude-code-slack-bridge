"""恢复工人(contracts §2.9 / §2.10 / §5.2 / §5.6):驱动一切非终态;重驱只用行上钉死的 binding_id;
幂等 = DB 约束(唯一键 + 确定性 job 键 + 带旧状态 CAS)。
- received/resolving/waiting_binding:无条件本地重驱,**永不因排队/停机而 failed**(无死线)。
- materializing:**不在这里驱动**(R1-m1)。materializing 行的调度入口只有主循环的
  `DaemonCore.run_followups → Inbound.drive_pending_rows(budget)`(按 `materialize_next_at` 到期 + 每 tick
  预算取行;预算列在库上,重启不重置)。slow_tick 若再领一份预算,单 tick 就会下载 2 条(契约上限 1)。
- `_expire_pendings`:**单条审批范围**(§5.6)—— 只碰该审批、其 inbox、其未发的 approval_card。
- `_replenish_cards`:只在**缺** card job 时创建;**绝不**复活终态 job(无 _rearm_failed_cards)。
- `_legacy_sending`:postMessage 类 → unknown/had_unknown=1/verify_after=now;幂等类按 cap 收口。
- `_retention`:终态正文裁剪(含 unconfirmed)、slack_events consumed/quarantined 清理、
  终态 media TTL、孤儿 `.tmp-*`。"""
import json
import os
import shutil
import time

from . import constants, db, inbound as inbound_mod, jobs, lifecycle, texts

MEDIA_TMP_ORPHAN_AGE_S = 2 * constants.DOWNLOAD_DEADLINE_S   # 孤儿 worker 最晚 deadline+2s 自退


class Recovery:
    def __init__(self, conn, cfg, client, clock, inbound, prober):
        self.conn = conn
        self.cfg = cfg
        self.client = client
        self.clock = clock
        self.inbound = inbound
        self.prober = prober

    # ---------------- 快节奏(每 loop ~1s / 判死 ~5s) ----------------
    def fast_tick(self, in_suspect_window=False):
        for b in self.conn.execute(
                "SELECT binding_id FROM bindings WHERE status='starting' "
                "AND bind_phase='confirmed'").fetchall():
            lifecycle.activate_if_ready(self.conn, b["binding_id"], self.clock)
        self.inbound.drive_local_rows()      # 激活后 waiting 行立即重过门(零网络)
        lifecycle.death_scan(self.conn, self.prober, self.clock,
                             in_suspect_window=in_suspect_window)

    # ---------------- 慢节奏(启动 + 每 60s) ----------------
    def slow_tick(self):
        now = self.clock.wall_ms()
        self._redrive_resolving(now)
        self._replenish_cards(now)
        # materializing 行**不在此重驱**:唯一调度入口是主循环 run_followups(单 tick 下载 ≤ 1 条)。
        self._expire_pendings(now)
        lifecycle.expire_stale_pending_binds(self.conn, self.clock)
        self._close_orphan_starting(now)
        self._reclaim_leases(now)
        self._sweep_stranded_enqueued(now)
        self._legacy_sending(now)
        self._retention(now)

    # ------------------------------------------------------------------
    def _redrive_resolving(self, now):
        """received/resolving/waiting_binding 本地重驱;无死线、零网络。"""
        return self.inbound.drive_local_rows()

    def _replenish_cards(self, now):
        """awaiting 中的审批**缺** card job → 补建(同键幂等);已 sent 未回填 card_message_id → 补回填。
        绝不改动任何既有 job 的状态。"""
        rows = self.conn.execute(
            "SELECT p.*, i.chat_id AS chat_id, i.snapshot_json AS snapshot_json, "
            "i.reply_thread_ts AS reply_thread_ts, i.sender_user_id AS sender_user_id "
            "FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE p.state='pending'").fetchall()
        bot = self.cfg.get("bot_user_id")
        for p in rows:
            job = self.conn.execute(
                "SELECT * FROM outbound_jobs WHERE idempotency_key=?",
                (jobs.key_card(p["pending_id"]),)).fetchone()
            if job is None:
                try:
                    snap = json.loads(p["snapshot_json"] or "{}")
                except ValueError:
                    snap = {}
                if not isinstance(snap, dict):
                    snap = {}
                sender = inbound_mod.sender_of(snap)[0] or p["sender_user_id"] or "?"
                jobs.create_job(
                    self.conn, kind="approval_card", chat_id=p["chat_id"],
                    binding_id=p["binding_id"], reply_to=p["reply_thread_ts"],
                    idempotency_key=jobs.key_card(p["pending_id"]),
                    ref_pending_id=p["pending_id"], ref_message_id=p["message_id"],
                    expected_state="pending",
                    body=texts.build_approval_card(
                        p["pending_id"], p["nonce"], sender,
                        inbound_mod.card_preview(snap, bot)),
                    now=now)
            elif (p["card_message_id"] is None and job["state"] == "sent"
                    and job["sent_message_id"]):
                self.conn.execute(
                    "UPDATE pendings SET card_message_id=? "
                    "WHERE pending_id=? AND card_message_id IS NULL",
                    (job["sent_message_id"], p["pending_id"]))

    def _expire_pendings(self, now):
        """审批过期(单条审批范围,contracts §5.6):该 pending → expired、其 inbox → expired、
        其未发的 approval_card(pending/unknown)→ cancelled、入队 decision_notice(expired)。
        不取消绑定的输出、不影响其它审批或正在物化的附件。"""
        rows = self.conn.execute(
            "SELECT p.*, i.chat_id AS chat_id, i.reply_thread_ts AS reply_thread_ts "
            "FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE p.state='pending' AND p.created_at IS NOT NULL AND p.created_at+?<?",
            (constants.PENDING_TTL_MS, now)).fetchall()
        n = 0
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
                self.conn.execute(
                    "UPDATE outbound_jobs SET state='cancelled', error=COALESCE(error,'pending-expired') "
                    "WHERE idempotency_key=? AND state IN ('pending','unknown')",
                    (jobs.key_card(p["pending_id"]),))
                lifecycle.create_decision_notice(
                    self.conn, pending_id=p["pending_id"], binding_id=p["binding_id"],
                    chat_id=p["chat_id"], message_id=p["message_id"],
                    reply_to=p["reply_thread_ts"], outcome="expired", now=now)
                n += 1
        return n

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
        """处置 = 单条 CAS,active 判定在同一语句内(EXISTS)—— 与并发 unbind 交错时必落 dropped。"""
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
        """防御性:终态绑定上滞留的 enqueued(理论不应再有)→ dropped。"""
        cur = self.conn.execute(
            "UPDATE deliveries SET state='dropped' WHERE state='enqueued' "
            "AND EXISTS(SELECT 1 FROM bindings b WHERE b.binding_id=deliveries.binding_id "
            "AND b.status IN ('dead','closed'))")
        if cur.rowcount:
            db.bump_counter(self.conn, "stranded_enqueued_dropped", cur.rowcount)

    def _legacy_sending(self, now):
        """contracts §2.9 末条:sending_at 过旧的 sending 行。postMessage 类(含 op_method 未冻结的)
        → unknown, had_unknown=1, verify_after=now(只核验,绝不盲重发);
        幂等类 → ac<cap → unknown, next=now;ac≥cap → failed。终态语义归 Outbound。"""
        stale = now - 2 * constants.SEND_TIMEOUT_S * 1000
        idem = ",".join("?" for _ in constants.IDEMPOTENT_METHODS)
        # 幂等类 = op_method ∈ IDEMPOTENT_METHODS,或 op_method 未冻结但 kind 为 receipt_reaction;
        # 其余(含未冻结的 session_turn/approval_card/*_notice)按 postMessage 类:只核验,绝不盲重发。
        is_idem = (f"(COALESCE(op_method,'') IN ({idem}) "
                   "OR (op_method IS NULL AND kind='receipt_reaction'))")   # NULL 安全
        self.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', had_unknown=1, verify_after=?, "
            "next_attempt_at=NULL, error='stale-sending' WHERE state='sending' "
            f"AND sending_at IS NOT NULL AND sending_at<? AND NOT {is_idem}",
            (now, stale, *constants.IDEMPOTENT_METHODS))
        self.conn.execute(
            "UPDATE outbound_jobs SET state='unknown', next_attempt_at=?, error='stale-sending' "
            "WHERE state='sending' AND sending_at IS NOT NULL AND sending_at<? "
            f"AND {is_idem} AND attempt_count<?",
            (now, stale, *constants.IDEMPOTENT_METHODS, constants.IDEMPOTENT_CAP))
        self.conn.execute(
            "UPDATE outbound_jobs SET state='failed', error='stale-sending-cap' "
            "WHERE state='sending' AND sending_at IS NOT NULL AND sending_at<? "
            f"AND {is_idem} AND attempt_count>=?",
            (stale, *constants.IDEMPOTENT_METHODS, constants.IDEMPOTENT_CAP))

    # ------------------------------------------------------------------
    def _retention(self, now):
        """终态行 retention:正文裁剪、骨架保留;slack_events 清理;终态 media TTL 删;孤儿 .tmp-* 清理。"""
        cutoff = now - constants.RETENTION_MS
        qs = ",".join("?" for _ in constants.INBOX_TERMINAL_STATES)
        self.conn.execute(
            f"UPDATE inbox SET snapshot_json=NULL WHERE state IN ({qs}) "
            "AND ts IS NOT NULL AND ts<? AND snapshot_json IS NOT NULL",
            (*constants.INBOX_TERMINAL_STATES, cutoff))
        self.conn.execute(
            "UPDATE deliveries SET payload_json='{}' WHERE state IN ('emitted','dropped') "
            "AND enq_at IS NOT NULL AND enq_at<? AND payload_json!='{}'", (cutoff,))
        ts_ = ",".join("?" for _ in constants.OUTBOUND_TERMINAL_STATES)
        self.conn.execute(
            f"UPDATE outbound_jobs SET body=NULL WHERE state IN ({ts_}) "
            "AND created_at IS NOT NULL AND created_at<? AND body IS NOT NULL",
            (*constants.OUTBOUND_TERMINAL_STATES, cutoff))
        self.conn.execute(
            "DELETE FROM slack_events WHERE state='consumed' AND consumed_at IS NOT NULL "
            "AND consumed_at<?", (now - constants.SLACK_EVENTS_RETENTION_MS,))
        self.conn.execute(
            "DELETE FROM slack_events WHERE state='quarantined' AND received_at<?",
            (now - constants.SLACK_EVENTS_QUARANTINE_RETENTION_MS,))
        self._retention_media(cutoff)

    def _retention_media(self, cutoff):
        media_root = self.inbound.media_root
        try:
            binding_dirs = os.listdir(media_root)
        except OSError:
            return
        wall = time.time()
        for bdir in binding_dirs:
            bpath = os.path.join(media_root, bdir)
            # lstat 语义,不跟随 symlink(绝不清 media root 之外)
            if os.path.islink(bpath) or not os.path.isdir(bpath):
                continue
            try:
                entries = os.listdir(bpath)
            except OSError:
                continue
            for mdir in entries:
                mpath = os.path.join(bpath, mdir)
                if os.path.islink(mpath) or not os.path.isdir(mpath):
                    continue
                if mdir.startswith("."):
                    # 孤儿 .tmp-*(崩溃残留):足够老才删(在途 worker 最晚 deadline+2s 自退)
                    if mdir.startswith(".tmp-"):
                        try:
                            age = wall - os.lstat(mpath).st_mtime
                        except OSError:
                            continue
                        if age > MEDIA_TMP_ORPHAN_AGE_S:
                            shutil.rmtree(mpath, ignore_errors=True)
                    continue
                row = self.conn.execute(
                    "SELECT state, ts FROM inbox WHERE message_id=?", (mdir,)).fetchone()
                if row is None:
                    continue
                if row["state"] in constants.INBOX_TERMINAL_STATES \
                        and row["ts"] is not None and row["ts"] < cutoff:
                    shutil.rmtree(mpath, ignore_errors=True)
