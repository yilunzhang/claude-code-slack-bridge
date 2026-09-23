"""入站管线(contracts §1 / §5.1 / §5.2 / §4.3)。

- `ingest_in_tx(conn, row)`:drain 持有事务(**本函数绝不** BEGIN/COMMIT/ROLLBACK);零网络。
  drop 序 → INSERT inbox(received)→ IntegrityError 分支(同一行 / 双投升级 / dup_message)。
- `Inbound.drive_pending_rows(budget)`:先零网络本地分流(received→resolving→决策;
  waiting_binding 激活/终止;不限量),再按预算做需要网络的物化(materializing;
  每 tick ≤ budget[0] 条带下载、≤ budget[1] 条纯文本),每条之后 heartbeat;多附件消息在
  **文件之间**也 heartbeat(media.materialize(heartbeat=…)),整条消息共享一个 DOWNLOAD_DEADLINE_S。
- 附件预算(§5.2)四条路径(owner / allowlist / approved / waiting 激活后)统一在
  `materializing` 的四列上,持久化在库、重启不重置。
决策与入队单事务内复验绑定 active(I3);投递判定零模型参与(I1)。"""
import json
import sqlite3

from . import config as configmod
from . import constants, db, jobs, lifecycle, media, senderallow, slackwire, texts, util

DROP_REASONS = ("foreign_team", "foreign_app", "event_type", "self", "subtype",
                "chat_not_allowed", "not_mentioned_unbound", "cap", "dup_message", "invalid")

# drain 只传 (conn, row);cfg/clock 由最近构造的 Inbound 登记(daemon 内唯一实例)。
_DEFAULTS = {"cfg": None, "clock": None}


def register_defaults(cfg, clock):
    _DEFAULTS["cfg"] = cfg
    _DEFAULTS["clock"] = clock


def _resolve(cfg, clock):
    if cfg is None:
        cfg = _DEFAULTS["cfg"]
        if cfg is None:
            cfg = configmod.ConfigSnapshot.load()
    if clock is None:
        clock = _DEFAULTS["clock"]
        if clock is None:
            from .clock import SystemClock
            clock = SystemClock()
    return cfg, clock


# ---------------------------------------------------------------- 快照辅助(纯函数)
def sender_of(snap):
    """→ (sender_user_id, sender_type)。sender_type = 'bot'(带 bot_id)| 'user'。"""
    snap = snap if isinstance(snap, dict) else {}
    uid = snap.get("user")
    uid = uid.strip() if isinstance(uid, str) and uid.strip() else None
    return uid, ("bot" if snap.get("bot_id") else "user")


def render_text(snap, bot_user_id):
    return slackwire.render_text(snap, bot_user_id)


def files_of(snap):
    snap = snap if isinstance(snap, dict) else {}
    return [f for f in (snap.get("files") or []) if isinstance(f, dict)] \
        if isinstance(snap.get("files"), list) else []


def _blocks_or_attachments(snap):
    b = snap.get("blocks")
    a = snap.get("attachments")
    return bool(isinstance(b, list) and b) or bool(isinstance(a, list) and a)


def trim_snapshot(snap):
    """ignored_not_mentioned 的即时裁剪(不保留未@我们的正文)。"""
    return util.jdumps({"type": snap.get("type"), "trimmed": True, "user": snap.get("user"),
                        "ts": snap.get("ts")})


def _row_get(row, key, default=None):
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def card_preview(snap, bot_user_id):
    """审批卡预览 = 正文(去 bot mention)+ 附件文件名(不可信文本,卡片 plain_text 转义)。"""
    text = render_text(snap, bot_user_id)
    names = [str(f.get("name") or f.get("title") or f.get("id") or "?") for f in files_of(snap)]
    if names:
        text = (text + "\n" if text else "") + "📎 " + ", ".join(names)
    return text


# ---------------------------------------------------------------- ingest(drain 事务内)
def _drop(conn, reason):
    db.bump_counter(conn, "event_dropped_%s" % reason)
    return ("dropped", reason)


def ingest_in_tx(conn, row, cfg=None, clock=None):
    """contracts §1 / §5.1。假定调用方已 BEGIN;零网络。row = slack_events 行(sqlite3.Row 或 mapping)。
    → ("handed", inbox_row) | ("dropped", reason ∈ DROP_REASONS)。"""
    cfg, clock = _resolve(cfg, clock)
    now = clock.wall_ms()
    try:
        payload = json.loads(_row_get(row, "payload_json") or "")
    except (ValueError, TypeError):
        return _drop(conn, "invalid")
    f = slackwire.events_fields(payload)
    if f is None:
        return _drop(conn, "invalid")
    if f["team_id"] is None or f["team_id"] != cfg.get("team_id"):
        return _drop(conn, "foreign_team")
    if f["api_app_id"] is None or f["api_app_id"] != cfg.get("app_id"):
        return _drop(conn, "foreign_app")
    if f["type"] not in constants.EVENT_TYPES_ACCEPTED:
        return _drop(conn, "event_type")
    ev = f["event"]
    if slackwire.is_self_event(ev, cfg):
        return _drop(conn, "self")
    if f["subtype"] not in constants.ACCEPT_SUBTYPES:
        return _drop(conn, "subtype")
    channel, ts = f["channel"], f["ts"]
    event_id = f["event_id"]
    if event_id is None:
        key = _row_get(row, "event_key") or ""
        event_id = key[3:] if key.startswith("ev:") else None
    if not channel or not ts or not event_id:
        return _drop(conn, "invalid")
    allow = cfg.get("chat_allowlist")
    if allow and channel not in allow:
        return _drop(conn, "chat_not_allowed")
    binding_id = _row_get(row, "binding_id")
    mentioned = slackwire.is_bot_mentioned(ev, cfg.get("bot_user_id"))
    dm = slackwire.is_dm(ev)
    if binding_id is None and not mentioned and not dm:
        return _drop(conn, "not_mentioned_unbound")
    sender, sender_type = sender_of(ev)
    if sender != cfg.get("owner_user_id"):
        qs = ",".join("?" for _ in constants.INBOX_NONTERMINAL_STATES)
        n = conn.execute("SELECT COUNT(*) FROM inbox WHERE state IN (%s)" % qs,
                         constants.INBOX_NONTERMINAL_STATES).fetchone()[0]
        if n >= constants.INBOX_NONTERMINAL_CAP:
            return _drop(conn, "cap")
    mid = util.message_id_of(channel, ts)
    thread_ts = f["thread_ts"]
    reply_thread_ts = thread_ts or ts
    try:
        conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,sender_user_id,sender_type,"
            "message_type,thread_ts,reply_thread_ts,snapshot_json,state,ts) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,'received',?)",
            (event_id, mid, channel, binding_id, sender, sender_type, f["type"], thread_ts,
             reply_thread_ts, util.jdumps(ev), now))
    except sqlite3.IntegrityError:
        by_e = conn.execute("SELECT * FROM inbox WHERE event_id=?", (event_id,)).fetchone()
        if by_e is not None:
            return ("handed", by_e)                       # 同一事件重投:已存在,继续驱动
        by_m = conn.execute("SELECT * FROM inbox WHERE message_id=?", (mid,)).fetchone()
        if by_m is None:
            raise                                          # 非两键冲突(如 FK):交给 drain 退避/隔离
        # 双投(app_mention + message 同一 ts):升级规则(contracts §1)
        if (by_m["message_type"] == "app_mention" and by_m["state"] in ("received", "resolving")
                and f["type"] == "message" and (f["files"] or f["blocks"])):
            if db.cas(conn,
                      "UPDATE inbox SET snapshot_json=?, sender_user_id=?, sender_type=?, "
                      "message_type='message', thread_ts=?, reply_thread_ts=?, ts=? "
                      "WHERE message_id=? AND state IN ('received','resolving')",
                      (util.jdumps(ev), sender, sender_type, thread_ts, reply_thread_ts, now, mid)):
                db.bump_counter(conn, "inbox_snapshot_upgraded")
                return ("handed", conn.execute("SELECT * FROM inbox WHERE message_id=?",
                                               (mid,)).fetchone())
        db.bump_counter(conn, "inbox_dup_message")
        return _drop(conn, "dup_message")
    return ("handed", conn.execute("SELECT * FROM inbox WHERE message_id=?", (mid,)).fetchone())


# ---------------------------------------------------------------- Inbound
class Inbound:
    def __init__(self, conn, cfg, client, clock, media_root, heartbeat=None, log=None):
        self.conn = conn
        self.cfg = cfg
        self.client = client
        self.clock = clock
        self.media_root = media_root
        self.heartbeat = heartbeat   # 含网络条目处理完 touch last_loop_at
        self.log = log
        self.materializer = None     # 测试注入;None → media.materialize
        self.worker_path = None      # 测试注入;None → bin/download_worker.py
        register_defaults(cfg, clock)

    # ---- 基础 ----
    def _log(self, msg):
        if self.log is None:
            return
        try:
            self.log(msg)
        except Exception:
            pass

    def _beat(self):
        if self.heartbeat is not None:
            try:
                self.heartbeat()
            except Exception:
                pass

    def _binding_of(self, row):
        if row["binding_id"] is None:
            return None
        return self.conn.execute(
            "SELECT * FROM bindings WHERE binding_id=?", (row["binding_id"],)).fetchone()

    def _row(self, message_id):
        return self.conn.execute("SELECT * FROM inbox WHERE message_id=?", (message_id,)).fetchone()

    def _snapshot_of(self, row):
        try:
            snap = json.loads(row["snapshot_json"] or "")
        except (ValueError, TypeError):
            return None
        return snap if isinstance(snap, dict) and not snap.get("trimmed") else None

    def ingest_in_tx(self, conn, row):
        return ingest_in_tx(conn, row, self.cfg, self.clock)

    # ---- 驱动入口 ----
    def drive_pending_rows(self, budget=constants.FOLLOWUP_BUDGET_PER_TICK):
        """contracts §1.2:本地分流不限量 → 预算内物化。→ 统计 dict。"""
        routed = self.drive_local_rows()
        stats = self.drive_materializing_rows(budget)
        stats["routed"] = routed
        return stats

    def drive_local_rows(self):
        """零网络:received/resolving → 决策;waiting_binding → 激活重过门 / 终止映射。"""
        rows = self.conn.execute(
            "SELECT * FROM inbox WHERE state IN ('received','resolving','waiting_binding') "
            "ORDER BY inbox_seq").fetchall()
        n = 0
        for r in rows:
            if self._route_local(r):
                n += 1
        return n

    def drive_waiting_rows(self):
        """兼容入口(旧 daemon_core / recovery):只推进 waiting_binding 行。"""
        rows = self.conn.execute(
            "SELECT * FROM inbox WHERE state='waiting_binding' ORDER BY inbox_seq").fetchall()
        n = 0
        for r in rows:
            if self._route_local(r):
                n += 1
        return n

    def drive_materializing_rows(self, budget=constants.FOLLOWUP_BUDGET_PER_TICK):
        """预算内物化:取 materializing ∧ (materialize_next_at IS NULL ∨ ≤ now),
        带下载 ≤ budget[0] 条、纯文本 ≤ budget[1] 条;每条之后 heartbeat。"""
        dl_budget, text_budget = int(budget[0]), int(budget[1])
        stats = {"downloads": 0, "text_only": 0, "skipped_budget": 0}
        if dl_budget <= 0 and text_budget <= 0:
            return stats
        now = self.clock.wall_ms()
        rows = self.conn.execute(
            "SELECT * FROM inbox WHERE state='materializing' "
            "AND (materialize_next_at IS NULL OR materialize_next_at<=?) ORDER BY inbox_seq",
            (now,)).fetchall()
        for r in rows:
            snap = self._snapshot_of(r) or {}
            heavy = media.needs_download(files_of(snap))
            if heavy:
                if dl_budget <= 0:
                    stats["skipped_budget"] += 1
                    continue
                dl_budget -= 1
                stats["downloads"] += 1
            else:
                if text_budget <= 0:
                    stats["skipped_budget"] += 1
                    continue
                text_budget -= 1
                stats["text_only"] += 1
            self._drive_materializing(r)
            self._beat()
        return stats

    def drive_row(self, row):
        """单行驱动(测试 / 恢复 / approval.run_followup):本地分流到底;materializing → 一次尝试。"""
        mid = row["message_id"]
        did = False
        for _ in range(4):
            row = self._row(mid)
            if row is None:
                return did
            st = row["state"]
            if st in ("received", "resolving", "waiting_binding"):
                if not self._route_local(row):
                    return did
                did = True
                continue
            if st == "materializing":
                did = self._drive_materializing(row) or did
                self._beat()
            return did
        return did

    # ---- 本地分流 ----
    def _route_local(self, row):
        """→ 是否推进了状态。"""
        st = row["state"]
        if st in ("received", "resolving"):
            return self._drive_resolving(row)
        if st == "waiting_binding":
            return self._drive_waiting(row)
        return False

    def _drive_resolving(self, row):
        mid = row["message_id"]
        with db.tx(self.conn):
            cur = self._row(mid)
            if cur is None or cur["state"] not in ("received", "resolving"):
                return False
            now = self.clock.wall_ms()
            if cur["state"] == "received":
                db.cas(self.conn,
                       "UPDATE inbox SET state='resolving', ts=? WHERE message_id=? AND state='received'",
                       (now, mid))
            snap = self._snapshot_of(cur)
            if snap is None:
                db.cas(self.conn,
                       "UPDATE inbox SET state='failed', ts=? WHERE message_id=? AND state='resolving'",
                       (now, mid))
                db.bump_counter(self.conn, "inbox_snapshot_invalid")
                return True
            self._decide_in_tx(cur, snap, "resolving")
        return True

    def _decide_in_tx(self, row, snap, from_state):
        """本地决策(事务内):未提及 ∧ 非 DM → ignored;绑定 NULL/终态 → inbound_notice;
        starting → waiting_binding;active → 门禁。"""
        now = self.clock.wall_ms()
        mid = row["message_id"]
        mentioned = (row["message_type"] == "app_mention"
                     or slackwire.is_bot_mentioned(snap, self.cfg.get("bot_user_id")))
        dm = slackwire.is_dm(snap)
        if not mentioned and not dm:
            db.cas(self.conn,
                   "UPDATE inbox SET state='ignored_not_mentioned', snapshot_json=?, ts=? "
                   "WHERE message_id=? AND state=?", (trim_snapshot(snap), now, mid, from_state))
            return
        binding = self._binding_of(row)
        if binding is None or binding["status"] in ("dead", "closed"):
            self._terminated_in_tx(row, snap, binding, from_state, now)
            return
        if binding["status"] == "starting":
            db.cas(self.conn,
                   "UPDATE inbox SET state='waiting_binding', ts=? WHERE message_id=? AND state=?",
                   (now, mid, from_state))
            return
        self._gate_active_in_tx(row, snap, binding, from_state, now)

    def _terminated_in_tx(self, row, snap, binding, from_state, now):
        """绑定 NULL/终态 → 4.2.4 映射终态 + inbound_notice(DM 且非 owner 时压制)。"""
        mid = row["message_id"]
        target = lifecycle.map_terminated_to_inbox_state(binding)
        if not db.cas(self.conn,
                      "UPDATE inbox SET state=?, ts=? WHERE message_id=? AND state=?",
                      (target, now, mid, from_state)):
            return
        sender, _ = sender_of(snap)
        if slackwire.is_dm(snap) and sender != self.cfg.get("owner_user_id"):
            db.bump_counter(self.conn, "dm_notice_suppressed")
            return
        jobs.create_inbound_notice(
            self.conn, chat_id=row["chat_id"], message_id=mid, code=target,
            binding_id=binding["binding_id"] if binding else None, now=now)

    def _gate_active_in_tx(self, row, snap, binding, from_state, now):
        """active 门禁(contracts §5.1):unsupported / owner / 白名单 / 成员审批。"""
        mid = row["message_id"]
        bot = self.cfg.get("bot_user_id")
        text = render_text(snap, bot)
        files = files_of(snap)
        sender, _ = sender_of(snap)
        if not text and not files and _blocks_or_attachments(snap):
            db.cas(self.conn,
                   "UPDATE inbox SET state='unsupported', ts=? WHERE message_id=? AND state=?",
                   (now, mid, from_state))
            jobs.create_job(
                self.conn, kind="unsupported_notice", chat_id=row["chat_id"],
                binding_id=binding["binding_id"], reply_to=row["reply_thread_ts"],
                idempotency_key=jobs.key_un(mid), ref_message_id=mid,
                expected_state="unsupported", body=texts.UNSUPPORTED_NOTICE, now=now)
            return
        if sender is not None and sender == self.cfg.get("owner_user_id"):
            if files:
                self._set_materializing_in_tx(mid, from_state, "owner", now)
            else:
                self._enqueue_in_tx(row, binding, snap, from_state, now)
            return
        # 白名单成员((chat_id, user_id) 双精确匹配)→ 免审批直投,但**信任级别不变**:
        # payload 仍 sender_is_owner=false + approved_by="allowlist"。判定每次读盘。
        if senderallow.is_allowed(row["chat_id"], sender):
            if files:
                self._set_materializing_in_tx(mid, from_state, "allowlist", now)
            else:
                self._enqueue_in_tx(row, binding, snap, from_state, now,
                                    approved_by=constants.APPROVED_BY_ALLOWLIST)
            return
        # member → 审批门(纯机械;绝不直投)
        reason = self._member_quota_reason(row["chat_id"], sender, now)
        if reason:
            db.cas(self.conn,
                   "UPDATE inbox SET state='failed', ts=? WHERE message_id=? AND state=?",
                   (now, mid, from_state))
            db.bump_counter(self.conn, "ratelimit_%s" % reason)
            return
        pending_id = util.new_id()
        nonce = util.new_nonce()
        self.conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,created_at) "
            "VALUES(?,?,?,?,'pending',?)",
            (pending_id, mid, binding["binding_id"], nonce, now))
        db.cas(self.conn,
               "UPDATE inbox SET state='awaiting_approval', ts=? WHERE message_id=? AND state=?",
               (now, mid, from_state))
        jobs.create_job(
            self.conn, kind="approval_card", chat_id=row["chat_id"],
            binding_id=binding["binding_id"], reply_to=row["reply_thread_ts"],
            idempotency_key=jobs.key_card(pending_id), ref_pending_id=pending_id,
            ref_message_id=mid, expected_state="pending",
            body=texts.build_approval_card(pending_id, nonce, sender or "?",
                                           card_preview(snap, bot)),
            now=now)

    def _set_materializing_in_tx(self, mid, from_state, reason, now):
        return db.cas(self.conn,
                      "UPDATE inbox SET state='materializing', materialize_reason=?, "
                      "materialize_started_at=NULL, materialize_attempts=0, materialize_next_at=NULL, "
                      "ts=? WHERE message_id=? AND state=?", (reason, now, mid, from_state))

    def _member_quota_reason(self, chat_id, sender_id, now):
        undecided = self.conn.execute(
            "SELECT COUNT(*) FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE i.chat_id=? AND p.state='pending'", (chat_id,)).fetchone()[0]
        if undecided >= constants.MAX_UNDECIDED_PER_CHAT:
            return "chat_pending_quota"
        if sender_id is None:
            return None
        last = self.conn.execute(
            "SELECT MAX(p.created_at) FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE i.sender_user_id=?", (sender_id,)).fetchone()[0]
        if last is not None and 0 <= now - last < constants.SENDER_COOLDOWN_MS:
            return "sender_cooldown"
        return None

    def build_payload(self, row, snap, paths=None, approved_by=None):
        """listener payload(contracts §4.3;`type`/`delivery_seq` 由 listener 加)。"""
        sender, _ = sender_of(snap)
        paths = list(paths or [])
        return {
            "message_id": row["message_id"],
            "chat_id": row["chat_id"],
            "ts": snap.get("ts") if isinstance(snap.get("ts"), str) else None,
            "thread_ts": snap.get("thread_ts") if isinstance(snap.get("thread_ts"), str) else None,
            "sender_user_id": sender,
            "sender_is_owner": sender is not None and sender == self.cfg.get("owner_user_id"),
            "approved_by": approved_by,
            "message_type": row["message_type"] or snap.get("type"),
            "text": render_text(snap, self.cfg.get("bot_user_id")),
            "media_paths": paths,
            "files": media.describe_files(files_of(snap), paths),
        }

    def _enqueue_in_tx(self, row, binding, snap, from_state, now,
                       paths=None, approved_by=None, create_receipt=True):
        """事务内复验绑定 active + deliveries 幂等入队(I3)+ receipt_reaction。→ 是否入队。"""
        b = self.conn.execute(
            "SELECT status FROM bindings WHERE binding_id=?", (binding["binding_id"],)).fetchone()
        if b is None or b["status"] != "active":
            return False
        mid = row["message_id"]
        payload = self.build_payload(row, snap, paths=paths, approved_by=approved_by)
        existing = self.conn.execute(
            "SELECT delivery_seq FROM deliveries WHERE binding_id=? AND message_id=?",
            (binding["binding_id"], mid)).fetchone()
        if existing:
            seq = existing[0]
        else:
            cur = self.conn.execute(
                "INSERT INTO deliveries(binding_id,message_id,payload_json,state,enq_at) "
                "VALUES(?,?,?,'enqueued',?)",
                (binding["binding_id"], mid, util.jdumps(payload), now))
            seq = cur.lastrowid
        db.cas(self.conn,
               "UPDATE inbox SET state='enqueued', ts=? WHERE message_id=? AND state=?",
               (now, mid, from_state))
        if create_receipt:
            jobs.create_job(
                self.conn, kind="receipt_reaction", chat_id=row["chat_id"],
                binding_id=binding["binding_id"], idempotency_key=jobs.key_rc(seq),
                ref_delivery_seq=seq, ref_message_id=mid, body=constants.RECEIPT_REACTION,
                now=now)
        return True

    def _drive_waiting(self, row):
        """waiting_binding:绑定仍 starting → 继续等;active → 重过门;终止 → 4.2.4 映射。"""
        b = self._binding_of(row)
        if b is not None and b["status"] == "starting":
            return False
        mid = row["message_id"]
        with db.tx(self.conn):
            cur = self._row(mid)
            if cur is None or cur["state"] != "waiting_binding":
                return False
            now = self.clock.wall_ms()
            b = self._binding_of(cur)          # 事务内重读:激活/终止只取其一
            if b is not None and b["status"] == "starting":
                return False
            snap = self._snapshot_of(cur)
            if snap is None:
                db.cas(self.conn,
                       "UPDATE inbox SET state='failed', ts=? WHERE message_id=? AND state='waiting_binding'",
                       (now, mid))
                db.bump_counter(self.conn, "inbox_snapshot_invalid")
                return True
            if b is not None and b["status"] == "active":
                self._gate_active_in_tx(cur, snap, b, "waiting_binding", now)
            else:
                self._terminated_in_tx(cur, snap, b, "waiting_binding", now)
        return True

    # ---- 物化(需要网络;事务外下载,单事务收口) ----
    def _client_tokens(self):
        """下载用 bot_token:客户端的 tokens 文件(文件是真相)→ 客户端内存 token → 默认 tokens.json。
        拿不到 → None(视为瞬态:走预算)。token 绝不进日志。"""
        c = self.client
        try:
            path = getattr(c, "tokens_path", None)
            if path is not None:
                return configmod.load_tokens(path, allow_env=False)[0]
            tok = getattr(c, "_token", None)
            if isinstance(tok, str) and tok:
                return {"bot_token": tok}
            return configmod.load_tokens(allow_env=False)[0]
        except configmod.ConfigError as e:
            self._log("materialize: tokens unavailable: %s" % e)
            return None

    def _materialize(self, **kw):
        fn = self.materializer or media.materialize
        return fn(**kw)

    def _drive_materializing(self, row):
        """materializing 行的一次尝试(contracts §5.2)。"""
        mid = row["message_id"]
        row = self._row(mid)
        if row is None or row["state"] != "materializing":
            return False
        now = self.clock.wall_ms()
        reason = row["materialize_reason"] or "owner"
        pending = None
        if reason == "approved":
            pending = self.conn.execute(
                "SELECT * FROM pendings WHERE message_id=?", (mid,)).fetchone()
        snap = self._snapshot_of(row)
        if snap is None:
            self._materialize_terminal(row, reason, pending, "snapshot_invalid")
            return True
        # 绑定非 active:不下载,直接收口(approved → undeliverable;其它 → 4.2.4 映射)
        b = self._binding_of(row)
        if b is None or b["status"] != "active":
            self._finalize_not_active(row, snap, reason, pending)
            return True
        started = row["materialize_started_at"]
        if started is not None and now - started > constants.MEDIA_RETRY_DEADLINE_MS:
            self._materialize_terminal(row, reason, pending, "budget")
            return True
        if started is None:
            db.cas(self.conn,
                   "UPDATE inbox SET materialize_started_at=? WHERE message_id=? "
                   "AND state='materializing' AND materialize_started_at IS NULL", (now, mid))
        files = files_of(snap)
        tokens = self._client_tokens() if media.needs_download(files) else {}
        stats = {}
        try:
            # tokens 为 None(拿不到凭据)也原样传入:media.materialize 对需要下载的消息返回 None(瞬态)
            res = self._materialize(
                client_tokens=tokens, media_root=self.media_root,
                binding_id=row["binding_id"], message_id=mid, files=files,
                deadline_s=constants.DOWNLOAD_DEADLINE_S, worker_path=self.worker_path,
                log=self.log, clock=self.clock, stats=stats, heartbeat=self._beat)
        except media.MediaError as e:
            self._log("materialize %s MediaError: %s" % (mid, e))
            self._materialize_terminal(row, reason, pending, "media_error")
            return True
        for k, v in stats.items():
            db.bump_counter(self.conn, k, v)
        if res is None:
            self._materialize_transient(row)
            return True
        paths, skipped = res
        self._finalize_success(row, snap, reason, pending, paths)
        return True

    def _materialize_transient(self, row):
        now = self.clock.wall_ms()
        n = int(row["materialize_attempts"] or 0)
        delay = min(constants.MEDIA_RETRY_BACKOFF_MS * (2 ** n), constants.MEDIA_RETRY_BACKOFF_MAX_MS)
        db.cas(self.conn,
               "UPDATE inbox SET materialize_attempts=materialize_attempts+1, materialize_next_at=? "
               "WHERE message_id=? AND state='materializing'", (now + delay, row["message_id"]))

    def _materialize_terminal(self, row, reason, pending, why):
        """预算耗尽 / MediaError → 终态:owner/allowlist → failed(静默计数);
        approved → failed + decision_notice(attachment_failed)。"""
        mid = row["message_id"]
        now = self.clock.wall_ms()
        with db.tx(self.conn):
            if not db.cas(self.conn,
                          "UPDATE inbox SET state='failed', ts=? WHERE message_id=? AND state='materializing'",
                          (now, mid)):
                return
            db.bump_counter(self.conn, "media_budget_exhausted" if why == "budget" else "media_failed")
            if reason == "approved" and pending is not None:
                lifecycle.create_decision_notice(
                    self.conn, pending_id=pending["pending_id"], binding_id=row["binding_id"],
                    chat_id=row["chat_id"], message_id=mid, reply_to=row["reply_thread_ts"],
                    outcome="attachment_failed", now=now)
        self._log("materialize %s terminal (%s, reason=%s)" % (mid, why, reason))

    def _finalize_not_active(self, row, snap, reason, pending):
        mid = row["message_id"]
        now = self.clock.wall_ms()
        with db.tx(self.conn):
            cur = self._row(mid)
            if cur is None or cur["state"] != "materializing":
                return
            b = self._binding_of(cur)
            if b is not None and b["status"] == "active":
                return                      # 竞态:又 active 了(不可能,但 fail-safe:留给下轮)
            if reason == "approved":
                if db.cas(self.conn,
                          "UPDATE inbox SET state='undeliverable', ts=? WHERE message_id=? "
                          "AND state='materializing'", (now, mid)) and pending is not None:
                    lifecycle.create_decision_notice(
                        self.conn, pending_id=pending["pending_id"], binding_id=cur["binding_id"],
                        chat_id=cur["chat_id"], message_id=mid, reply_to=cur["reply_thread_ts"],
                        outcome="closed_undelivered", now=now)
            else:
                self._terminated_in_tx(cur, snap, b, "materializing", now)

    def _finalize_success(self, row, snap, reason, pending, paths):
        """单事务:复验绑定 active → 入队(approved_by 按 reason);approved 再入队 delivered。"""
        mid = row["message_id"]
        with db.tx(self.conn):
            cur = self._row(mid)
            if cur is None or cur["state"] != "materializing":
                return
            now = self.clock.wall_ms()
            b = self._binding_of(cur)
            if b is None or b["status"] != "active":
                if reason == "approved":
                    if db.cas(self.conn,
                              "UPDATE inbox SET state='undeliverable', ts=? WHERE message_id=? "
                              "AND state='materializing'", (now, mid)) and pending is not None:
                        lifecycle.create_decision_notice(
                            self.conn, pending_id=pending["pending_id"], binding_id=cur["binding_id"],
                            chat_id=cur["chat_id"], message_id=mid, reply_to=cur["reply_thread_ts"],
                            outcome="closed_undelivered", now=now)
                else:
                    self._terminated_in_tx(cur, snap, b, "materializing", now)
                return
            if reason == "approved":
                approved_by = pending["decided_by"] if pending is not None else None
            elif reason == "allowlist":
                approved_by = constants.APPROVED_BY_ALLOWLIST
            else:
                approved_by = None
            ok = self._enqueue_in_tx(cur, b, snap, "materializing", now, paths=paths,
                                     approved_by=approved_by, create_receipt=(reason != "approved"))
            if ok and reason == "approved" and pending is not None:
                lifecycle.create_decision_notice(
                    self.conn, pending_id=pending["pending_id"], binding_id=cur["binding_id"],
                    chat_id=cur["chat_id"], message_id=mid, reply_to=cur["reply_thread_ts"],
                    outcome="delivered", now=now)
