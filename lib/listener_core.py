"""listener 核心(plan 4.4;bin/listener.py 驱动)。
- 父自检:cc 实例确定死 → 静默 exit。
- 接管/身份:单条 CAS(epoch 比对 + 观察旧元组;探测 UNKNOWN 一律按存活处理)。
- 心跳即租约:CAS 带我的 epoch,rowcount=0 → 被接管 exit。
- 领取:短 IMMEDIATE 事务;**提交后**才 print(flush);stdout 永远在事务外。
- daemon 自愈:flock 可得=daemon 死 → singleflight+退避 ensure-daemon;连续失败才一条固定 daemon_alert。"""
from . import constants, db, procs, util

DAEMON_CHECK_INTERVAL_MS = 10_000
ENSURE_BACKOFF_START_MS = 5_000
ENSURE_BACKOFF_MAX_MS = 80_000
ALERT_AFTER_FAILS = 3


class ListenerCore:
    def __init__(self, conn, binding_id, clock, prober, me_pid, me_start,
                 printer, daemon_alive_probe, ensure_daemon):
        self.conn = conn
        self.binding_id = binding_id
        self.clock = clock
        self.prober = prober
        self.me_pid = me_pid
        self.me_start = me_start
        self.printer = printer
        self.daemon_alive_probe = daemon_alive_probe
        self.ensure_daemon = ensure_daemon
        self.my_epoch = None
        self._farewell_sent = False
        self._alert_sent = False
        self._ensure_fails = 0
        self._next_ensure_at = 0
        self._ensure_backoff = ENSURE_BACKOFF_START_MS
        self._last_daemon_check = None

    # ------------------------------------------------------------------
    def step(self):
        row = self.conn.execute(
            "SELECT * FROM bindings WHERE binding_id=?", (self.binding_id,)).fetchone()
        if row is None:
            self._farewell("gone")
            return "exit"
        if row["status"] in ("dead", "closed"):
            self._farewell(row["close_reason"] or row["status"])
            return "exit"
        # 父自检(确定死才退;UNKNOWN=存活)
        if procs.probe_alive(self.prober, row["cc_pid"], row["cc_start"]) == procs.DEAD:
            return "exit"
        now = self.clock.wall_ms()
        if self.my_epoch is None or not self._identity_is_me(row):
            outcome = self._takeover(row, now)
            if outcome == "exit":
                return "exit"
            if outcome == "wait":
                self._daemon_check(now)
                return "ok"
        else:
            if not db.cas(self.conn,
                          "UPDATE bindings SET listener_beat_at=?, suspect_since=NULL "
                          "WHERE binding_id=? AND listener_epoch=?",
                          (now, self.binding_id, self.my_epoch)):
                return "exit"  # 被接管:静默退出(4.4.3)
        if row["status"] == "active" and self.my_epoch is not None:
            self._drain(now)
        self._daemon_check(now)
        return "ok"

    # ------------------------------------------------------------------
    def _identity_is_me(self, row):
        return (row["listener_pid"], row["listener_start"]) == (self.me_pid, self.me_start) \
            and row["listener_epoch"] == self.my_epoch

    def _takeover(self, row, now):
        old_epoch = row["listener_epoch"]
        holder = (row["listener_pid"], row["listener_start"])
        if row["listener_pid"] is None:
            cond, params = "AND listener_pid IS NULL", ()
        elif holder == (self.me_pid, self.me_start):
            cond, params = "AND listener_pid=? AND listener_start=?", holder
        else:
            beat = row["listener_beat_at"]
            fresh = beat is not None and (now - beat) <= constants.HEARTBEAT_FRESH_MS
            if fresh:
                return "exit"  # 行有他人新鲜心跳 → 多余副本退出
            if procs.probe_alive(self.prober, holder[0], holder[1]) != procs.DEAD:
                return "wait"  # 旧进程未确定死(UNKNOWN 按存活)→ 等 daemon 判死
            cond, params = "AND listener_pid=? AND listener_start=?", holder
        ok = db.cas(
            self.conn,
            "UPDATE bindings SET listener_pid=?, listener_start=?, listener_epoch=?, "
            "listener_beat_at=?, suspect_since=NULL "
            f"WHERE binding_id=? AND status IN ('starting','active') AND listener_epoch=? {cond}",
            (self.me_pid, self.me_start, old_epoch + 1, now, self.binding_id,
             old_epoch, *params))
        if ok:
            self.my_epoch = old_epoch + 1
            return "ok"
        return "wait"  # CAS 输了:下轮重读

    # ------------------------------------------------------------------
    def _drain(self, now):
        while True:
            item = self._claim_one(now)
            if item is None:
                return
            d, token = item
            try:
                payload = util.jdumps(self._compose_line(d))
            except Exception:
                payload = util.jdumps({"type": "feishu_message",
                                       "delivery_seq": d["delivery_seq"],
                                       "message_id": d["message_id"]})
            self.printer(payload)  # 提交后才 print;print 崩溃 → 保持 leased(至多重复不丢)
            db.cas(self.conn,
                   "UPDATE deliveries SET state='emitted', emitted_at=? "
                   "WHERE delivery_seq=? AND state='leased' AND lease_token=?",
                   (self.clock.wall_ms(), d["delivery_seq"], token))

    def _claim_one(self, now):
        with db.tx(self.conn):
            d = self.conn.execute(
                "SELECT * FROM deliveries WHERE binding_id=? AND state='enqueued' "
                "ORDER BY delivery_seq LIMIT 1", (self.binding_id,)).fetchone()
            if d is None:
                return None
            live = self.conn.execute(
                "SELECT 1 FROM bindings WHERE binding_id=? AND status='active' "
                "AND listener_epoch=?", (self.binding_id, self.my_epoch)).fetchone()
            if live is None:
                return None
            token = util.new_id()
            if not db.cas(self.conn,
                          "UPDATE deliveries SET state='leased', lease_pid=?, lease_start=?, "
                          "lease_token=?, lease_epoch=?, lease_until=?, attempts=attempts+1 "
                          "WHERE delivery_seq=? AND state='enqueued'",
                          (self.me_pid, self.me_start, token, self.my_epoch,
                           now + constants.LEASE_MS, d["delivery_seq"])):
                return None
        return d, token

    def _compose_line(self, d):
        import json
        try:
            payload = json.loads(d["payload_json"] or "{}")
        except ValueError:
            payload = {}
        line = {"type": "feishu_message", "delivery_seq": d["delivery_seq"]}
        line.update(payload)
        line.setdefault("message_id", d["message_id"])
        return line

    # ------------------------------------------------------------------
    def _farewell(self, code):
        if self._farewell_sent:
            return
        self._farewell_sent = True
        self.printer(util.jdumps({"type": "farewell", "code": code}))  # 固定文案(I1)

    def _daemon_check(self, now):
        if self._last_daemon_check is not None \
                and now - self._last_daemon_check < DAEMON_CHECK_INTERVAL_MS:
            return
        self._last_daemon_check = now
        try:
            alive = bool(self.daemon_alive_probe())
        except Exception:
            alive = True  # 探测失败按存活处理(fail-safe:别乱拉)
        if alive:
            self._ensure_fails = 0
            self._alert_sent = False
            self._ensure_backoff = ENSURE_BACKOFF_START_MS
            return
        if now < self._next_ensure_at:
            return
        try:
            self.ensure_daemon()  # singleflight:退避窗口内不重复 spawn(不经模型)
        except Exception:
            pass
        self._ensure_fails += 1
        self._next_ensure_at = now + self._ensure_backoff
        self._ensure_backoff = min(self._ensure_backoff * 2, ENSURE_BACKOFF_MAX_MS)
        if self._ensure_fails >= ALERT_AFTER_FAILS and not self._alert_sent:
            self._alert_sent = True
            self.printer(util.jdumps({"type": "daemon_alert", "code": "daemon_down"}))


class InstanceFollower:
    """跟随一个 CC 实例的常驻 listener(插件 monitor 无参模式):
    - 无 core 时:实例确定死 → exit(UNKNOWN 按活);本实例有 starting/active 绑定 → 为它建一个新 core 认领;否则 idle。
    - 持有 core 时:只驱动 core(父自检由 core 自己做);core 退出(farewell 已由它打印)→ 清空回等待,不退出,
      再次 bind 会被下一个新 core 接住。
    多个 follower 并存时,多余者建 core → 看到持有者心跳新鲜 → 立即退出清空,不加协调层。"""

    def __init__(self, conn, cc_pid, cc_start, prober, core_factory):
        self.conn = conn
        self.cc_pid = cc_pid
        self.cc_start = cc_start
        self.prober = prober
        self.core_factory = core_factory
        self.core = None

    def step(self):
        if self.core is None:
            if procs.probe_alive(self.prober, self.cc_pid, self.cc_start) == procs.DEAD:
                return "exit"
            row = self.conn.execute(
                "SELECT binding_id FROM bindings WHERE cc_pid=? AND cc_start=? "
                "AND status IN ('starting','active')", (self.cc_pid, self.cc_start)).fetchone()
            if row is None:
                return "idle"
            self.core = self.core_factory(row["binding_id"])
        if self.core.step() == "exit":
            self.core = None
            return "idle"
        return "ok"
