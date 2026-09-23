"""审批回调(contracts §1 / §5.4):执行门 = 纯机械(owner 比对 + 单事务 CAS),零模型判断。

`process_in_tx(conn, payload)`:假定已在 drain 持有的事务内;**绝不** BEGIN。
校验序:`slackwire.interactive_fields` 非 None ∧ team==cfg.team_id → INSERT callback_events(dup → dropped)
→ pending 存在 → chat 在 allowlist → hmac.compare_digest(nonce) → user==owner → inbox 存在
→ channel==inbox.chat_id → card_ts 合法 → card_message_id 已回填则须 == channel:card_ts
(**通过且未回填 → 回填,并把该 pending 的 card job 若处于 unknown/pending/sending CAS 为 sent,
error='confirmed-by-click'**)→ CAS pending→approved|rejected(失败 → "late")→ 副作用。
dropped 的回调也 INSERT OR IGNORE callback_events(裸去重),由本函数在同一事务内完成。
→ ("handed", followup_row|None) | ("dropped", reason ∈ skipped/dup/invalid/late)。"""
import hmac
import json
import re
import sqlite3

from . import config as configmod
from . import db, inbound as inbound_mod, jobs, lifecycle, slackwire, util

TS_RE = re.compile(r"^[0-9]{1,20}\.[0-9]{1,9}$")

_DEFAULTS = {"cfg": None, "clock": None, "inbound": None}


def register_defaults(cfg, clock, inbound):
    _DEFAULTS["cfg"] = cfg
    _DEFAULTS["clock"] = clock
    _DEFAULTS["inbound"] = inbound


def _resolve(cfg, clock, inbound):
    if cfg is None:
        cfg = _DEFAULTS["cfg"]
        if cfg is None:
            cfg = configmod.ConfigSnapshot.load()
    if clock is None:
        clock = _DEFAULTS["clock"]
        if clock is None:
            from .clock import SystemClock
            clock = SystemClock()
    if inbound is None:
        inbound = _DEFAULTS["inbound"]
        if inbound is None:
            # fail-closed:没有 Inbound 就无法入队;抛错让 drain 退避/隔离,而不是静默判 undeliverable
            raise RuntimeError("approval.process_in_tx: no Inbound registered "
                               "(construct Approval(conn, cfg, clock, inbound) first)")
    return cfg, clock, inbound


def valid_card_ts(ts):
    return isinstance(ts, str) and TS_RE.match(ts) is not None


def _bare(conn, key, now):
    if key:
        conn.execute("INSERT OR IGNORE INTO callback_events(event_id,seen_at) VALUES(?,?)",
                     (key, now))


def process_in_tx(conn, payload, cfg=None, clock=None, inbound=None):
    cfg, clock, inbound = _resolve(cfg, clock, inbound)
    now = clock.wall_ms()
    key = slackwire.event_key(slackwire.INTERACTIVE, payload)
    f = slackwire.interactive_fields(payload)
    if f is None or f["team"] != cfg.get("team_id"):
        _bare(conn, key, now)
        return ("dropped", "skipped")
    try:
        conn.execute("INSERT INTO callback_events(event_id,seen_at) VALUES(?,?)", (key, now))
    except sqlite3.IntegrityError:
        return ("dropped", "dup")
    ctx = _validate(conn, cfg, f)
    if ctx is None:
        return ("dropped", "invalid")
    pending, inbox_row, act, card_mid = ctx
    pid = pending["pending_id"]
    # 通过且卡片身份未回填 → 回填 + card job confirmed-by-click(先于 CAS;迟到点击同样确认卡片存在)
    if pending["card_message_id"] is None:
        db.cas(conn, "UPDATE pendings SET card_message_id=? WHERE pending_id=? AND card_message_id IS NULL",
               (card_mid, pid))
        conn.execute(
            "UPDATE outbound_jobs SET state='sent', error='confirmed-by-click', "
            "sent_message_id=COALESCE(sent_message_id,?), sent_at=COALESCE(sent_at,?) "
            "WHERE idempotency_key=? AND state IN ('unknown','pending','sending')",
            (card_mid, now, jobs.key_card(pid)))
    outcome = "approved" if act == "approve" else "rejected"
    if not db.cas(conn,
                  "UPDATE pendings SET state=?, decided_by=?, decided_event_id=?, decided_at=? "
                  "WHERE pending_id=? AND state='pending'",
                  (outcome, f["user"], key, now, pid)):
        return ("dropped", "late")
    mid = inbox_row["message_id"]
    followup = None
    if act == "approve":
        try:
            snap = json.loads(inbox_row["snapshot_json"] or "{}")
        except ValueError:
            snap = {}
        files = inbound_mod.files_of(snap)
        if files:
            if db.cas(conn,
                      "UPDATE inbox SET state='materializing', materialize_reason='approved', "
                      "materialize_started_at=NULL, materialize_attempts=0, materialize_next_at=NULL, "
                      "ts=? WHERE message_id=? AND state='awaiting_approval'", (now, mid)):
                lifecycle.create_decision_notice(
                    conn, pending_id=pid, binding_id=pending["binding_id"], chat_id=inbox_row["chat_id"],
                    message_id=mid, reply_to=inbox_row["reply_thread_ts"],
                    outcome="approved_pending_files", now=now)
                followup = conn.execute("SELECT * FROM inbox WHERE message_id=?", (mid,)).fetchone()
        else:
            binding = conn.execute("SELECT * FROM bindings WHERE binding_id=?",
                                   (pending["binding_id"],)).fetchone()
            ok = False
            if binding is not None:
                ok = inbound._enqueue_in_tx(inbox_row, binding, snap, "awaiting_approval", now,
                                            approved_by=f["user"], create_receipt=False)
            if ok:
                lifecycle.create_decision_notice(
                    conn, pending_id=pid, binding_id=pending["binding_id"], chat_id=inbox_row["chat_id"],
                    message_id=mid, reply_to=inbox_row["reply_thread_ts"], outcome="delivered", now=now)
            elif db.cas(conn,
                        "UPDATE inbox SET state='undeliverable', ts=? WHERE message_id=? "
                        "AND state='awaiting_approval'", (now, mid)):
                # 绑定已非 active(级联漏掉的窗口):approved 但不可投递 → closed_undelivered
                lifecycle.create_decision_notice(
                    conn, pending_id=pid, binding_id=pending["binding_id"], chat_id=inbox_row["chat_id"],
                    message_id=mid, reply_to=inbox_row["reply_thread_ts"],
                    outcome="closed_undelivered", now=now)
    else:
        db.cas(conn, "UPDATE inbox SET state='rejected', ts=? WHERE message_id=? AND state='awaiting_approval'",
               (now, mid))
        lifecycle.create_decision_notice(
            conn, pending_id=pid, binding_id=pending["binding_id"], chat_id=inbox_row["chat_id"],
            message_id=mid, reply_to=inbox_row["reply_thread_ts"], outcome="rejected", now=now)
    return ("handed", followup)


def _validate(conn, cfg, f):
    """机械校验(事务内读到的就是被 CAS 保护的同一视图);fail-closed:任一不符 → None。
    → (pending, inbox_row, act, card_message_id)。"""
    v = f["value"]
    pending = conn.execute("SELECT * FROM pendings WHERE pending_id=?", (v["pending_id"],)).fetchone()
    if pending is None:
        return None
    allow = cfg.get("chat_allowlist")
    if allow:
        b = conn.execute("SELECT chat_id FROM bindings WHERE binding_id=?",
                         (pending["binding_id"],)).fetchone()
        if b is None or b["chat_id"] not in allow:
            return None
    try:
        same = hmac.compare_digest(str(pending["nonce"]).encode("utf-8"),
                                   str(v["nonce"]).encode("utf-8"))
    except (UnicodeError, TypeError):
        same = False
    if not same:
        return None
    if not f["user"] or f["user"] != cfg.get("owner_user_id"):
        return None
    inbox_row = conn.execute("SELECT * FROM inbox WHERE message_id=?", (pending["message_id"],)).fetchone()
    if inbox_row is None:
        return None
    if not f["channel"] or f["channel"] != inbox_row["chat_id"]:
        return None
    if not valid_card_ts(f["card_ts"]):
        return None                                   # card_ts 非法:dropped(invalid),不回填
    card_mid = util.message_id_of(f["channel"], f["card_ts"])
    if pending["card_message_id"] is not None and pending["card_message_id"] != card_mid:
        return None
    return pending, inbox_row, v["act"], card_mid


class Approval:
    """构造签名冻结(contracts §8):Approval(conn, cfg, clock, inbound)。"""

    def __init__(self, conn, cfg, clock, inbound):
        self.conn = conn
        self.cfg = cfg
        self.clock = clock
        self.inbound = inbound
        register_defaults(cfg, clock, inbound)

    def process_in_tx(self, conn, payload):
        return process_in_tx(conn, payload, self.cfg, self.clock, self.inbound)

    def run_followup(self, row):
        """approve 且带 files 的 inbox 行:立即做一次物化尝试(网络在事务外;失败由预算接手)。"""
        if row is None:
            return False
        return self.inbound.drive_row(row)
