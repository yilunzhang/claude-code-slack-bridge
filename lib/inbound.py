"""入站管线(plan 4.2):去重 → 详情快照(钉死 binding)→ 结构化@判定 → 门禁 → deliveries。
写事务内零网络/零子进程;快照与物化在事务外,决策与入队单事务内复验绑定 active(I3)。"""
import json
import sqlite3

from . import (constants, db, jobs, lifecycle, media, runner as runner_mod,
               senderallow, texts, util)


def normalize_receive(obj):
    if not isinstance(obj, dict):
        return None
    cands = [obj] + [obj.get(k) for k in ("event", "payload", "data")
                     if isinstance(obj.get(k), dict)]
    for c in cands:
        if c.get("message_id") and c.get("event_id") and c.get("chat_id"):
            return c
    return None


def is_bot_mentioned(snap, app_id):
    for m in snap.get("mentions") or []:
        mid = m.get("id")
        if mid == app_id:
            return True
        if isinstance(mid, dict) and app_id in mid.values():
            return True
    return False


def extract_text(snap, app_id=None):
    """正文提取(E3 真机实锤):真实 `+messages-mget` 的正文在**顶层 `content`**
    (lark-cli 已渲染的纯文本,mention 以 "@{name}" 内联;file/image 亦为渲染文本,
    形如 "(文件) 名字" / "[图片]")→ 优先直接采用。
    兼容 fallback:旧 raw-API `body.content` JSON 形状(双形状容忍,与 list_chats
    的 items|chats 同风格)。
    app_id 给定时:剥掉指向**本 bot** 的 mention 渲染片段("@{name}"),
    其他人的 mention 保留原样;首尾空白规整。"""
    top = snap.get("content")
    if isinstance(top, str) and top:
        t = top
    else:
        # fallback:raw body.content JSON 形状
        mtype = snap.get("msg_type")
        try:
            content = json.loads((snap.get("body") or {}).get("content") or "{}")
        except ValueError:
            content = {}
        if mtype == "text":
            t = content.get("text") or ""
        elif mtype == "post":
            parts = []
            if content.get("title"):
                parts.append(str(content["title"]))
            for para in content.get("content") or []:
                runs = [r.get("text") for r in (para or [])
                        if isinstance(r, dict) and r.get("text")]
                if runs:
                    parts.append("".join(runs))
            t = "\n".join(parts)
        elif mtype == "image":
            t = "[图片]"
        elif mtype == "file":
            t = f"(文件) {content.get('file_name', '')}".strip()
        else:
            t = ""
        for m in snap.get("mentions") or []:
            key, name = m.get("key"), m.get("name")
            if key:
                t = t.replace(key, f"@{name}" if name else "@?")
    if app_id:
        for m in snap.get("mentions") or []:
            mid = m.get("id")
            is_bot = mid == app_id or (isinstance(mid, dict) and app_id in mid.values())
            if is_bot and m.get("name"):
                t = t.replace(f"@{m['name']}", "")
        t = t.strip()
    return t


def _media_keys(text):
    """从已渲染正文提取附件句柄 → [{key,type}],**保序去重**。
    key 前缀定 type(`img_*`→image、`file_*`→file),与飞书资源 API 契约一致。"""
    out, seen = [], set()
    for m in constants.MEDIA_KEY_RE.finditer(text or ""):
        k = m.group(0)
        if k in seen:
            continue
        seen.add(k)
        out.append({"key": k, "type": "image" if k.startswith("img_") else "file"})
    return out


def trim_snapshot(snap):
    return util.jdumps({"msg_type": snap.get("msg_type"), "trimmed": True,
                        "sender": snap.get("sender"),
                        "mention_count": len(snap.get("mentions") or [])})


def sender_of(snap):
    s = snap.get("sender") or {}
    return s.get("id"), s.get("sender_type") or "user"


class Inbound:
    def __init__(self, conn, cfg, runner, clock, media_root, heartbeat=None, log=None):
        self.conn = conn
        self.cfg = cfg
        self.runner = runner
        self.clock = clock
        self.media_root = media_root
        self.heartbeat = heartbeat  # r2-M1②:含网络条目处理完 touch last_loop_at
        self.log = log              # r3-6:mget/物化失败留原始现场

    def _log_io_failure(self, what, res):
        if self.log is None:
            return
        try:
            self.log(f"{what}: rc={res.rc} timed_out={res.timed_out} "
                     f"stdout={(res.stdout or '')[:300]!r} stderr={(res.stderr or '')[:300]!r}")
        except Exception:
            pass

    def _beat(self):
        if self.heartbeat is not None:
            try:
                self.heartbeat()
            except Exception:
                pass

    # ---------------- 入站事件 ----------------
    def process_event(self, ev):
        ev = normalize_receive(ev)
        if not ev:
            return
        if ev.get("chat_type") and ev.get("chat_type") != "group":
            return  # 只管 group
        # E2:chat_allowlist 灰度门 —— 任何 inbox/notice 之前,零副作用零回复
        allow = self.cfg.get("chat_allowlist")
        if allow and ev.get("chat_id") not in allow:
            return
        sender = ev.get("sender_id")
        if sender in (self.cfg.get("bot_open_id"), self.cfg.get("app_id")):
            return  # F9:显式过滤 bot 自发
        chat_id, mid, eid = ev["chat_id"], ev["message_id"], ev["event_id"]
        live = self.conn.execute(
            "SELECT 1 FROM bindings WHERE chat_id=? AND status IN ('starting','active')",
            (chat_id,)).fetchone()
        if not live and "@" not in (ev.get("content") or ""):
            return  # 4.2.0 廉价预滤(正确性不依赖)
        if sender != self.cfg.get("owner_open_id"):
            qs = ",".join("?" for _ in constants.INBOX_NONTERMINAL_STATES)
            n = self.conn.execute(
                f"SELECT COUNT(*) FROM inbox WHERE state IN ({qs})",
                constants.INBOX_NONTERMINAL_STATES).fetchone()[0]
            if n >= constants.INBOX_NONTERMINAL_CAP:
                db.bump_counter(self.conn, "inbox_cap_drops")
                return
        row = self._insert_or_resume(chat_id, mid, eid, ev)
        if row is not None:
            self.drive_row(row)

    def _insert_or_resume(self, chat_id, mid, eid, ev):
        now = self.clock.wall_ms()
        try:
            with db.tx(self.conn):
                pinned = self.conn.execute(
                    "SELECT binding_id FROM bindings WHERE chat_id=? "
                    "ORDER BY binding_seq DESC LIMIT 1", (chat_id,)).fetchone()
                self.conn.execute(
                    "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,"
                    "sender_open_id,message_type,state,ts) VALUES(?,?,?,?,?,?,'received',?)",
                    (eid, mid, chat_id, pinned[0] if pinned else None,
                     ev.get("sender_id"), ev.get("message_type"), now))
        except sqlite3.IntegrityError:
            by_e = self.conn.execute(
                "SELECT * FROM inbox WHERE event_id=?", (eid,)).fetchone()
            by_m = self.conn.execute(
                "SELECT * FROM inbox WHERE message_id=?", (mid,)).fetchone()
            if by_e is not None and by_m is not None and by_e["inbox_seq"] == by_m["inbox_seq"]:
                return by_e  # 崩溃恢复:同一行,继续驱动
            db.bump_counter(self.conn, "inbox_conflict_alerts")  # 两键命中不同行:fail-closed
            return None
        return self.conn.execute("SELECT * FROM inbox WHERE message_id=?", (mid,)).fetchone()

    # ---------------- 状态机驱动(恢复工人复用同一入口) ----------------
    def drive_row(self, row):
        mid = row["message_id"]
        for _ in range(4):
            row = self.conn.execute(
                "SELECT * FROM inbox WHERE message_id=?", (mid,)).fetchone()
            if row is None:
                return
            st = row["state"]
            if st == "received":
                db.cas(self.conn,
                       "UPDATE inbox SET state='resolving', ts=? WHERE message_id=? AND state='received'",
                       (self.clock.wall_ms(), mid))
                continue
            if st == "resolving":
                self._drive_resolving(row)
                self._beat()
                return
            if st == "waiting_binding":
                self._drive_waiting(row)
                self._beat()
                return
            if st == "approved_materializing":
                self._drive_materializing(row)
                self._beat()
                return
            return  # 终态或 awaiting_approval(回调驱动)

    def drive_waiting_rows(self):
        """激活后 / 每 tick:推进全部 waiting_binding(r7-②:此专用分支先于任何通用收口)。"""
        rows = self.conn.execute(
            "SELECT * FROM inbox WHERE state='waiting_binding' ORDER BY inbox_seq").fetchall()
        n = 0
        for r in rows:
            b = self._binding_of(r)
            if b is not None and b["status"] == "starting":
                continue  # 继续等
            self.drive_row(r)
            n += 1
        return n

    # ---------------- 内部步骤 ----------------
    def _binding_of(self, row):
        if row["binding_id"] is None:
            return None
        return self.conn.execute(
            "SELECT * FROM bindings WHERE binding_id=?", (row["binding_id"],)).fetchone()

    def _fetch_snapshot(self, message_id):
        res = self.runner.run(
            ["im", "+messages-mget", "--as", "bot", "--message-ids", message_id,
             "--no-reactions"], timeout_s=constants.MGET_TIMEOUT_S)
        env = runner_mod.parse_envelope(res.stdout)
        if res.rc != 0 or not runner_mod.envelope_ok(env):
            self._log_io_failure(f"mget {message_id} failed", res)  # r3-6
            return None
        for m in runner_mod.data_of(env).get("messages") or []:
            if m.get("message_id") == message_id:
                return m
        # r4-4:ok:true 但结果里没有目标 message(非静默 None)
        if self.log is not None:
            ids = [m.get("message_id") for m in runner_mod.data_of(env).get("messages") or []]
            try:
                self.log(f"mget {message_id}: ok but no target message not found in {ids[:5]}")
            except Exception:
                pass
        return None

    def _snapshot_of(self, row):
        if row["snapshot_json"]:
            try:
                snap = json.loads(row["snapshot_json"])
                if not snap.get("trimmed"):
                    return snap
            except ValueError:
                pass
        return self._fetch_snapshot(row["message_id"])

    def _drive_resolving(self, row):
        snap = self._snapshot_of(row)  # 网络在事务外
        if snap is None:
            return  # 瞬态失败:保持 resolving,恢复工人按 deadline 收口
        need_media = False
        with db.tx(self.conn):
            cur = self.conn.execute(
                "SELECT state FROM inbox WHERE message_id=?", (row["message_id"],)).fetchone()
            if cur is None or cur["state"] != "resolving":
                return
            need_media = self._decide_in_tx(row, snap)
        if need_media:
            self._materialize_then_finalize(row, snap, from_state="resolving")

    def _decide_in_tx(self, row, snap, from_state="resolving"):
        """resolving → 终点(单事务)。返回是否 owner 媒体需物化(保持原状态)。"""
        now = self.clock.wall_ms()
        mid = row["message_id"]
        sender_id, sender_type = sender_of(snap)
        mtype = snap.get("msg_type")
        self.conn.execute(
            "UPDATE inbox SET snapshot_json=?, sender_open_id=?, sender_type=?, message_type=? "
            "WHERE message_id=?",
            (util.jdumps(snap), sender_id, sender_type, mtype, mid))
        if not is_bot_mentioned(snap, self.cfg["app_id"]):
            self.conn.execute(
                "UPDATE inbox SET state='ignored_not_mentioned', snapshot_json=?, ts=? "
                "WHERE message_id=? AND state=?",
                (trim_snapshot(snap), now, mid, from_state))
            return False
        binding = self._binding_of(row)
        if binding is None or binding["status"] in ("dead", "closed"):
            target = lifecycle.map_terminated_to_inbox_state(binding)
            db.cas(self.conn,
                   "UPDATE inbox SET state=?, ts=? WHERE message_id=? AND state=?",
                   (target, now, mid, from_state))
            jobs.create_inbound_notice(
                self.conn, chat_id=row["chat_id"], message_id=mid, code=target,
                binding_id=binding["binding_id"] if binding else None, now=now)
            return False
        if binding["status"] == "starting":
            db.cas(self.conn,
                   "UPDATE inbox SET state='waiting_binding', ts=? WHERE message_id=? AND state=?",
                   (now, mid, from_state))
            return False
        # active:正常分流门(4.2.6/4.2.7)
        return self._gate_active_in_tx(row, snap, binding, from_state, now)

    def _gate_active_in_tx(self, row, snap, binding, from_state, now):
        mid = row["message_id"]
        mtype = snap.get("msg_type")
        sender_id, _ = sender_of(snap)
        if mtype not in constants.SUPPORTED_MSG_TYPES:
            db.cas(self.conn,
                   "UPDATE inbox SET state='unsupported', ts=? WHERE message_id=? AND state=?",
                   (now, mid, from_state))
            jobs.create_job(
                self.conn, kind="unsupported_notice", chat_id=row["chat_id"],
                binding_id=binding["binding_id"], idempotency_key=jobs.key_un(mid),
                ref_message_id=mid, expected_state="unsupported",
                body=texts.UNSUPPORTED_NOTICE, now=now)
            return False
        if sender_id == self.cfg["owner_open_id"]:
            if mtype in constants.MEDIA_MSG_TYPES:
                return True  # 保持原状态,事务外物化后 finalize
            self._enqueue_in_tx(row, binding, snap, from_state, now)
            return False
        # 白名单成员((chat_id, open_id) 双精确匹配)→ 免审批直投,但**信任级别不变**:
        # payload 仍 sender_is_owner=false + approved_by="allowlist",agent 侧照旧当
        # 不可信输入。判定每次读盘 → owner 让 agent 改完文件下一条消息即生效。
        if senderallow.is_allowed(row["chat_id"], sender_id):
            if mtype in constants.MEDIA_MSG_TYPES:
                return True  # 与 owner 同路:事务外物化后 finalize
            self._enqueue_in_tx(row, binding, snap, from_state, now,
                                approved_by=constants.APPROVED_BY_ALLOWLIST)
            return False
        # member → 审批门(纯机械;绝不直投)
        reason = self._member_quota_reason(row["chat_id"], sender_id, now)
        if reason:
            db.cas(self.conn,
                   "UPDATE inbox SET state='failed', ts=? WHERE message_id=? AND state=?",
                   (now, mid, from_state))
            db.bump_counter(self.conn, f"ratelimit_{reason}")
            return False
        pending_id = util.new_id()
        nonce = util.new_nonce()
        self.conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,created_at) "
            "VALUES(?,?,?,?,'pending',?)",
            (pending_id, mid, binding["binding_id"], nonce, now))
        db.cas(self.conn,
               "UPDATE inbox SET state='awaiting_approval', ts=? WHERE message_id=? AND state=?",
               (now, mid, from_state))
        sender_label = sender_id or "?"
        jobs.create_job(
            self.conn, kind="approval_card", chat_id=row["chat_id"],
            binding_id=binding["binding_id"], reply_to=mid,
            idempotency_key=jobs.key_card(pending_id), ref_pending_id=pending_id,
            expected_state="pending",
            body=texts.build_approval_card(pending_id, nonce, sender_label,
                                           extract_text(snap, self.cfg.get("app_id"))),
            now=now)
        return False

    def _member_quota_reason(self, chat_id, sender_id, now):
        undecided = self.conn.execute(
            "SELECT COUNT(*) FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE i.chat_id=? AND p.state='pending'", (chat_id,)).fetchone()[0]
        if undecided >= constants.MAX_UNDECIDED_PER_CHAT:
            return "chat_pending_quota"
        last = self.conn.execute(
            "SELECT MAX(p.created_at) FROM pendings p JOIN inbox i ON p.message_id=i.message_id "
            "WHERE i.sender_open_id=?", (sender_id,)).fetchone()[0]
        if last is not None and 0 <= now - last < constants.SENDER_COOLDOWN_MS:
            return "sender_cooldown"
        return None

    def _enqueue_in_tx(self, row, binding, snap, from_state, now,
                       media_paths=None, approved_by=None, create_receipt=True):
        """事务内复验绑定 active + deliveries 幂等入队(I3)。"""
        b = self.conn.execute(
            "SELECT status FROM bindings WHERE binding_id=?",
            (binding["binding_id"],)).fetchone()
        if b is None or b["status"] != "active":
            return False
        mid = row["message_id"]
        sender_id, sender_type = sender_of(snap)
        payload = {
            "message_id": mid,
            "chat_id": row["chat_id"],
            "sender_open_id": sender_id,
            "sender_type": sender_type,
            "sender_is_owner": sender_id == self.cfg["owner_open_id"],
            "approved_by": approved_by,
            "message_type": snap.get("msg_type"),
            "text": extract_text(snap, self.cfg.get("app_id")),
            "media_paths": media_paths or [],
        }
        # 非纯文本 → 附「附件可自取」句柄。**判定看 msg_type,不看正文有没有 key**:
        # 「扫到 key 才加」会漏掉 image/file(它们渲染成 `[图片]`/`(文件) 名字`、根本不含 key),
        # 也会给纯文本里粘了 img_v3_… 的代码误加。keys 独立填(可为空,空则给 mget 兜底)。
        # 新字段而非拼进 text:text 是用户原话,污染它会混淆「用户说了什么」与「系统提示」;
        # 新字段对既有消费者 additive,经 listener 的 line.update(payload) 原样到达 agent。
        # **只扫未剥 mention 的原始顶层 `content`;拿不到就不扫**(codex impl r1 Low1 + r2 实证):
        # `extract_text` 把指向本 bot 的 `@name` 替换成**空串**,故 `img@TestBot_v12_fake` 会被
        # 拼成正文里原本不存在的 key `img_v12_fake`(实测,legacy `body.content` 形状同样复现)。
        # 后果仅是 agent 拿到假 key、下载失败(charset 无 `/`/`.`/shell 元字符,无安全面),但
        # 没必要留着。**故意不回退扫 `payload["text"]`**(那会让假 key 在 legacy 形状上复活),
        # 也不去扫序列化后的 `body.content`(JSON 的字段名/转义会带来新误判面)。
        # **代价**:legacy 形状(顶层 `content` 缺失,lark-cli 正常渲染下不出现)keys 为空 —— 但
        # hint 仍在 + 给 mget 兜底,agent 照样能看原始结构。不递归解析原始 post 节点(过度设计)。
        if snap.get("msg_type") != "text":
            raw = snap.get("content")
            keys = _media_keys(raw) if isinstance(raw, str) else []
            payload["media_keys"] = keys
            payload["fetch_hint"] = texts.media_fetch_hint(mid, keys, self.cfg["profile"])
        # 回复/引用 → 附「被引用消息可自取」句柄。**闸门与上面的媒体分支相互独立**:
        # 带引用的消息 msg_type 仍是 `text`,套用 `msg_type != "text"` 会整个漏掉。
        # 「图 + 引用」两个 hint 都要有,故用两个独立字段、不共用一个键。
        reply_to = snap.get("reply_to")
        if isinstance(reply_to, str) and reply_to:
            payload["reply_to"] = reply_to
            payload["reply_hint"] = texts.reply_fetch_hint(reply_to, self.cfg["profile"])
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
                ref_delivery_seq=seq, ref_message_id=mid, body="GLANCE", now=now)
        return True

    def _materialize_then_finalize(self, row, snap, from_state, approved_pending=None):
        """owner 媒体 / 白名单成员媒体 / 已批准 member 媒体:
        网络物化(事务外)→ 单事务复验+入队。

        `approved_by` 三种来源必须区分开(否则白名单成员的图会**长得和 owner 本人发的一样**):
        点按钮 → 该 owner 的 operator_id;白名单 → `"allowlist"`;owner 本人 → None。
        """
        mid = row["message_id"]
        now = self.clock.wall_ms()
        try:
            paths = media.materialize(
                self.runner, self.media_root, row["binding_id"], mid, log=self.log)
        except media.MediaError:
            with db.tx(self.conn):
                db.cas(self.conn,
                       "UPDATE inbox SET state='failed', ts=? WHERE message_id=? AND state=?",
                       (now, mid, from_state))
                if approved_pending is not None:
                    jobs.create_job(
                        self.conn, kind="decision_notice", chat_id=row["chat_id"],
                        binding_id=row["binding_id"],
                        idempotency_key=jobs.key_dec(approved_pending["pending_id"], "failed"),
                        ref_pending_id=approved_pending["pending_id"], ref_message_id=mid,
                        expected_state="failed",
                        body=texts.decision_notice_body("failed"), now=now)
                db.bump_counter(self.conn, "media_failed")
            return
        if paths is None:
            return  # 瞬态:保持现状态,deadline 由恢复工人收口
        with db.tx(self.conn):
            b = self._binding_of(row)
            now = self.clock.wall_ms()
            if b is not None and b["status"] == "active":
                sender_id, _ = sender_of(snap)
                if approved_pending is not None:
                    approved_by = approved_pending["decided_by"]
                elif sender_id != self.cfg["owner_open_id"]:
                    # 非 owner 又走到这里 ⟹ 只可能是白名单直投(审批路径必带 pending)
                    approved_by = constants.APPROVED_BY_ALLOWLIST
                else:
                    approved_by = None
                self._enqueue_in_tx(
                    row, b, snap, from_state, now, media_paths=paths,
                    approved_by=approved_by,
                    create_receipt=approved_pending is None)
                if approved_pending is not None:
                    jobs.create_job(
                        self.conn, kind="decision_notice", chat_id=row["chat_id"],
                        binding_id=row["binding_id"],
                        idempotency_key=jobs.key_dec(approved_pending["pending_id"], "approved"),
                        ref_pending_id=approved_pending["pending_id"],
                        expected_state="approved",
                        body=texts.decision_notice_body("approved"), now=now)
            else:
                if approved_pending is not None:
                    # 4.8 通用分支:绑定复验失败 → undeliverable(仅非 waiting 状态)
                    db.cas(self.conn,
                           "UPDATE inbox SET state='undeliverable', ts=? "
                           "WHERE message_id=? AND state=?", (now, mid, from_state))
                else:
                    target = lifecycle.map_terminated_to_inbox_state(b)
                    if db.cas(self.conn,
                              "UPDATE inbox SET state=?, ts=? WHERE message_id=? AND state=?",
                              (target, now, mid, from_state)):
                        jobs.create_inbound_notice(
                            self.conn, chat_id=row["chat_id"], message_id=mid,
                            code=target, binding_id=row["binding_id"], now=now)

    def _drive_waiting(self, row):
        """waiting_binding 专用分支(r7-②):激活 → 重过正常分流门;终止 → 4.2.4 映射。"""
        b = self._binding_of(row)
        if b is not None and b["status"] == "starting":
            return
        snap = self._snapshot_of(row)
        if snap is None:
            return
        need_media = False
        with db.tx(self.conn):
            cur = self.conn.execute(
                "SELECT state FROM inbox WHERE message_id=?", (row["message_id"],)).fetchone()
            if cur is None or cur["state"] != "waiting_binding":
                return
            now = self.clock.wall_ms()
            b = self._binding_of(row)  # 事务内重读:激活/终止只取其一(r7-③)
            if b is not None and b["status"] == "active":
                need_media = self._gate_active_in_tx(row, snap, b, "waiting_binding", now)
            elif b is None or b["status"] in ("dead", "closed"):
                target = lifecycle.map_terminated_to_inbox_state(b)
                if db.cas(self.conn,
                          "UPDATE inbox SET state=?, ts=? WHERE message_id=? AND state=?",
                          (target, now, row["message_id"], "waiting_binding")):
                    jobs.create_inbound_notice(
                        self.conn, chat_id=row["chat_id"], message_id=row["message_id"],
                        code=target, binding_id=row["binding_id"], now=now)
        if need_media:
            self._materialize_then_finalize(row, snap, from_state="waiting_binding")

    def _drive_materializing(self, row):
        """approved_materializing:重驱物化(4.8);绑定复验失败 → undeliverable。"""
        pending = self.conn.execute(
            "SELECT * FROM pendings WHERE message_id=? AND state='approved'",
            (row["message_id"],)).fetchone()
        if pending is None:
            return
        snap = self._snapshot_of(row)
        if snap is None:
            return
        self._materialize_then_finalize(
            row, snap, from_state="approved_materializing", approved_pending=pending)
