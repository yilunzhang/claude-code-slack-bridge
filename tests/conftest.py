import json
import os
import pathlib
import sys

import pytest

SKILL_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_ROOT))

from tests.helpers import FakeClock, FakeRunner, FakeProber  # noqa: E402

APP_ID = "cli_testapp"
BOT_OPEN_ID = "ou_bot"
OWNER = "ou_owner"
MEMBER = "ou_member"
CHAT = "oc_chat1"
PROFILE = "main"

CC_PID = 4242
CC_START = "Tue Jul 14 09:00:00 2026"


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "bridge-data"
    monkeypatch.setenv("FEISHU_BRIDGE_DATA_DIR", str(d))
    # 测试铁律:绝不触碰真实 ~/.claude/settings.json
    monkeypatch.setenv("FEISHU_BRIDGE_SETTINGS_PATH", str(tmp_path / "settings.json"))
    return d


@pytest.fixture
def cfg(data_dir):
    from lib import config as configmod
    from lib import paths

    paths.ensure_data_dir()
    c = {
        "profile": PROFILE,
        "app_id": APP_ID,
        "bot_open_id": BOT_OPEN_ID,
        "bot_name": "TestBot",
        "owner_open_id": OWNER,
        "cli_version": "1.0.66",
        "created_at": 0,
    }
    configmod.save_config(c)
    return c


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def conn(data_dir):
    from lib import db as dbmod
    from lib import paths

    paths.ensure_data_dir()
    c = dbmod.connect(paths.db_path())
    dbmod.init_schema(c, paths.schema_path())
    yield c
    c.close()


@pytest.fixture
def runner():
    return FakeRunner(profile=PROFILE)


@pytest.fixture
def prober():
    p = FakeProber()
    p.set(CC_PID, 1, CC_START, "claude")
    return p


class Env:
    """一站式已接线组件(全部 fake 注入,零网络)。"""

    def __init__(self, conn, cfg, clock, runner, prober, data_dir):
        from lib import paths
        from lib.inbound import Inbound
        from lib.outbound import Outbound
        from lib.approval import Approval
        from lib.recovery import Recovery

        self.conn = conn
        self.cfg = cfg
        self.clock = clock
        self.runner = runner
        self.prober = prober
        self.data_dir = data_dir
        self.media_root = paths.media_root()
        self.inbound = Inbound(conn, cfg, runner, clock, self.media_root)
        self.outbound = Outbound(conn, cfg, runner, clock)
        self.approval = Approval(conn, cfg, clock, inbound=self.inbound)
        self.recovery = Recovery(conn, cfg, runner, clock, self.inbound, prober)

    # ---- 便捷工厂 ----
    def make_binding(self, status="active", chat_id=CHAT, session_id="sess-1",
                     cc_pid=CC_PID, cc_start=CC_START, binding_id=None,
                     close_reason=None, listener_epoch=1, listener_beat_at=None,
                     bind_phase="confirmed", confirmed_at=None, listener_pid=7777,
                     listener_start="Tue Jul 14 09:01:00 2026"):
        from lib import util
        bid = binding_id or util.new_id()
        now = self.clock.wall_ms()
        if listener_beat_at is None and status == "active":
            listener_beat_at = now
        if status in ("dead", "closed") and close_reason is None:
            close_reason = "user_unbind"
        if status == "starting" and bind_phase == "unconfirmed":
            session_id = None
        if status in ("starting",) and bind_phase == "unconfirmed":
            listener_epoch = 0
            listener_pid = None
            listener_start = None
        self.conn.execute(
            "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,cc_start,cwd,"
            "status,bind_phase,confirmed_at,listener_pid,listener_start,listener_epoch,"
            "listener_beat_at,bound_at,closed_at,close_reason) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (bid, chat_id, "测试群", session_id, cc_pid, cc_start, "/tmp/proj",
             status, bind_phase, confirmed_at, listener_pid, listener_start, listener_epoch,
             listener_beat_at, now if status == "active" else None,
             now if status in ("dead", "closed") else None, close_reason))
        return bid

    def recv_event(self, message_id="om_1", event_id=None, chat_id=CHAT,
                   sender_id=OWNER, content="@TestBot hi", message_type="text",
                   chat_type="group"):
        ev = {
            "type": "im.message.receive_v1",
            "event_id": event_id or ("ev_" + message_id),
            "message_id": message_id,
            "chat_id": chat_id,
            "chat_type": chat_type,
            "sender_id": sender_id,
            "message_type": message_type,
            "content": content,
        }
        self.inbound.process_event(ev)
        return ev

    def arm_mget(self, snapshot_rows):
        """让 +messages-mget 返回给定 messages(不含 --download-resources 调用)。"""
        from tests.helpers import ok_envelope

        def fn(args, cwd):
            return ok_envelope({"messages": snapshot_rows})

        self.runner.on(
            lambda a: a[:2] == ["im", "+messages-mget"] and "--download-resources" not in a,
            fn)

    def inbox_row(self, message_id):
        return self.conn.execute("SELECT * FROM inbox WHERE message_id=?", (message_id,)).fetchone()

    def jobs(self, kind=None):
        if kind:
            return self.conn.execute(
                "SELECT * FROM outbound_jobs WHERE kind=? ORDER BY job_seq", (kind,)).fetchall()
        return self.conn.execute("SELECT * FROM outbound_jobs ORDER BY job_seq").fetchall()

    def deliveries(self, binding_id=None):
        if binding_id:
            return self.conn.execute(
                "SELECT * FROM deliveries WHERE binding_id=? ORDER BY delivery_seq",
                (binding_id,)).fetchall()
        return self.conn.execute("SELECT * FROM deliveries ORDER BY delivery_seq").fetchall()

    def pendings(self):
        return self.conn.execute("SELECT * FROM pendings ORDER BY created_at").fetchall()


@pytest.fixture
def env(conn, cfg, clock, runner, prober, data_dir):
    return Env(conn, cfg, clock, runner, prober, data_dir)
