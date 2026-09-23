"""判死矩阵(plan 4.6):两条独立 CAS;睡眠宽限;时钟回拨;pid 活但心跳停;探测 UNKNOWN=存活。"""
from tests.conftest import CC_PID, CC_START
from lib import lifecycle, constants


def get(env, bid):
    return env.conn.execute("SELECT * FROM bindings WHERE binding_id=?", (bid,)).fetchone()


class TestCcGone:
    def test_cc_definitively_dead_closes_immediately(self, env):
        bid = env.make_binding(status="active",
                               listener_beat_at=env.clock.wall_ms())  # 心跳新鲜也照关
        env.prober.remove(CC_PID)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        b = get(env, bid)
        assert b["status"] == "closed" and b["close_reason"] == "cc_gone"

    def test_pid_reuse_counts_as_dead(self, env):
        bid = env.make_binding(status="active")
        env.prober.set(CC_PID, 1, "Wed Jul 15 00:00:00 2026", "claude")  # lstart 变了=pid 复用
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert get(env, bid)["close_reason"] == "cc_gone"

    def test_probe_unknown_treated_alive(self, env):
        bid = env.make_binding(status="active")
        env.prober.raising = True
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert get(env, bid)["status"] == "active"

    def test_starting_binding_cc_gone_also_closes(self, env):
        bid = env.make_binding(status="starting", bind_phase="unconfirmed", session_id=None)
        env.prober.remove(CC_PID)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert get(env, bid)["close_reason"] == "cc_gone"


class TestListenerLeaseExpired:
    def _stale_active(self, env):
        bid = env.make_binding(
            status="active",
            listener_beat_at=env.clock.wall_ms() - constants.HEARTBEAT_GRACE_MS - 1000)
        return bid

    def test_two_phase_suspect_then_dead_even_if_pid_alive(self, env):
        bid = self._stale_active(env)
        env.prober.set(7777, 1, "Tue Jul 14 09:01:00 2026", "python3")  # listener pid 仍活
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        b = get(env, bid)
        assert b["status"] == "active" and b["suspect_since"] == env.clock.wall_ms()
        env.clock.tick(constants.SUSPECT_CONFIRM_MS + 1)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        b = get(env, bid)
        # 心跳即租约:pid 存活不豁免
        assert b["status"] == "dead" and b["close_reason"] == "listener_gone"

    def test_beat_resume_clears_suspect(self, env):
        bid = self._stale_active(env)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert get(env, bid)["suspect_since"] is not None
        env.conn.execute("UPDATE bindings SET listener_beat_at=? WHERE binding_id=?",
                         (env.clock.wall_ms(), bid))
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        b = get(env, bid)
        assert b["suspect_since"] is None and b["status"] == "active"

    def test_sleep_gap_suspends_judgment(self, env):
        bid = self._stale_active(env)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        env.clock.tick(constants.SUSPECT_CONFIRM_MS + 1)
        # 睡眠恢复窗内:不判死,等 listener 自愈
        lifecycle.death_scan(env.conn, env.prober, env.clock, in_suspect_window=True)
        assert get(env, bid)["status"] == "active"
        # 窗结束仍陈旧 → 判死
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert get(env, bid)["status"] == "dead"

    def test_clock_rewind_beat_in_future_is_fresh(self, env):
        bid = env.make_binding(status="active", listener_beat_at=env.clock.wall_ms())
        env.clock.rewind_wall(60_000)  # 墙钟回拨:beat_at 在"未来"
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        b = get(env, bid)
        assert b["status"] == "active" and b["suspect_since"] is None

    def test_starting_not_subject_to_lease_expiry(self, env):
        bid = env.make_binding(status="starting", bind_phase="confirmed", session_id="s1",
                               listener_epoch=1,
                               listener_beat_at=env.clock.wall_ms() - 10 * constants.HEARTBEAT_GRACE_MS,
                               confirmed_at=env.clock.wall_ms())
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        env.clock.tick(constants.SUSPECT_CONFIRM_MS + 1)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert get(env, bid)["status"] == "starting"  # 由激活超时路径负责,判死不管

    def test_death_cascades_termination(self, env):
        bid = self._stale_active(env)
        env.conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) "
            "VALUES('ev_w','om_w', ?, ?, 'waiting_binding', 0)",
            (env.conn.execute("SELECT chat_id FROM bindings WHERE binding_id=?", (bid,)).fetchone()[0], bid))
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        env.clock.tick(constants.SUSPECT_CONFIRM_MS + 1)
        lifecycle.death_scan(env.conn, env.prober, env.clock)
        assert env.inbox_row("om_w")["state"] == "session_closed"
