"""pytest 装配:数据目录重定向(SLACK_BRIDGE_*)、config.json/tokens.json、真实 SQLite、
FakeSlackClient、Env(一站式接线)。旧 Feishu fixture(runner / recv_event / arm_mget)标 LEGACY,WP5 删。"""
import pathlib
import sys
import types

import pytest

SKILL_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SKILL_ROOT))

from tests.helpers import (  # noqa: E402
    APP_ID, BOT_ID, BOT_USER, CHAT, DM, MEMBER, OWNER, TEAM,
    FakeClock, FakeProber, FakeRunner, FakeSlackClient, block_action, next_ts,
)

# LEGACY-FEISHU: remove in WP5(旧测试 `from tests.conftest import BOT_OPEN_ID, PROFILE`)
BOT_OPEN_ID = BOT_USER  # LEGACY-FEISHU: remove in WP5
PROFILE = "main"        # LEGACY-FEISHU: remove in WP5

CC_PID = 4242
CC_START = "Tue Jul 14 09:00:00 2026"

__all__ = ["TEAM", "BOT_USER", "BOT_ID", "APP_ID", "OWNER", "MEMBER", "CHAT", "DM",
           "BOT_OPEN_ID", "PROFILE", "CC_PID", "CC_START", "Env"]


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "bridge-data"
    monkeypatch.setenv("SLACK_BRIDGE_DATA_DIR", str(d))
    # 测试铁律:绝不触碰真实 ~/.claude/settings.json
    monkeypatch.setenv("SLACK_BRIDGE_SETTINGS_PATH", str(tmp_path / "settings.json"))
    # 测试铁律:env token 覆盖不得泄入(load_tokens 缺省 allow_env=True)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_APP_TOKEN", raising=False)
    return d


@pytest.fixture
def cfg(data_dir):
    """写 config.json(全部必填键 + owner_dm_id + markdown_mode)→ ConfigSnapshot(dict 子类)。"""
    from lib import config as configmod
    from lib import paths

    paths.ensure_data_dir()
    c = {
        "team_id": TEAM,
        "bot_user_id": BOT_USER,
        "bot_id": BOT_ID,
        "app_id": APP_ID,
        "owner_user_id": OWNER,
        "owner_dm_id": DM,
        "markdown_mode": "markdown_text",
        "created_at": 0,
    }
    configmod.save_config(c)
    return configmod.ConfigSnapshot.load()


@pytest.fixture
def tokens(data_dir):
    """写 tokens.json(0600)→ SimpleNamespace(path, tokens, version)。"""
    from lib import config as configmod
    from lib import paths

    paths.ensure_data_dir()
    t = {"bot_token": "xoxb-test-bot-token", "app_token": "xapp-test-app-token"}
    version = configmod.save_tokens(t)
    return types.SimpleNamespace(path=paths.tokens_path(), tokens=t, version=version)


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
def client(conn, clock):
    """FakeSlackClient,冷却存储 = daemon_state(与真 daemon / notify / probe 同一份)。"""
    from lib.slackapi import DaemonStateCooldownStore
    return FakeSlackClient(cooldown_store=DaemonStateCooldownStore(conn), clock=clock)


@pytest.fixture
def runner():  # LEGACY-FEISHU: remove in WP5
    return FakeRunner(profile=PROFILE)


@pytest.fixture
def prober():
    p = FakeProber()
    p.set(CC_PID, 1, CC_START, "claude")
    return p


class Env:
    """一站式已接线组件(全部 fake 注入,零网络)。构造签名冻结于 docs/contracts.md §8:
    Inbound(conn, cfg, client, clock, media_root) / Outbound(conn, cfg, client, clock) /
    Approval(conn, cfg, clock, inbound) / Recovery(conn, cfg, client, clock, inbound, prober) /
    DaemonCore(conn, cfg, clock, inbound, approval, outbound, recovery)。
    WP2/WP3 前旧模块只是把 client 存进 `self.runner` 字段,不影响构造。"""

    def __init__(self, conn, cfg, clock, client, prober, data_dir, runner=None):
        from lib import paths
        from lib.approval import Approval
        from lib.daemon_core import DaemonCore
        from lib.inbound import Inbound
        from lib.outbound import Outbound
        from lib.recovery import Recovery

        self.conn = conn
        self.cfg = cfg
        self.clock = clock
        self.client = client
        self.prober = prober
        self.data_dir = data_dir
        self.runner = runner  # LEGACY-FEISHU: remove in WP5
        self.media_root = paths.media_root()
        self.inbound = Inbound(conn, cfg, client, clock, self.media_root)
        self.outbound = Outbound(conn, cfg, client, clock)
        self.approval = Approval(conn, cfg, clock, inbound=self.inbound)
        self.recovery = Recovery(conn, cfg, client, clock, self.inbound, prober)
        self.core = DaemonCore(conn, cfg, clock, self.inbound, self.approval, self.outbound,
                               self.recovery)

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
            listener_epoch = 0
            listener_pid = None
            listener_start = None
        self.conn.execute(
            "INSERT INTO bindings(binding_id,chat_id,chat_name,session_id,cc_pid,cc_start,cwd,"
            "status,bind_phase,confirmed_at,listener_pid,listener_start,listener_epoch,"
            "listener_beat_at,bound_at,closed_at,close_reason) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (bid, chat_id, "测试频道", session_id, cc_pid, cc_start, "/tmp/proj",
             status, bind_phase, confirmed_at, listener_pid, listener_start, listener_epoch,
             listener_beat_at, now if status == "active" else None,
             now if status in ("dead", "closed") else None, close_reason))
        return bid

    # ---- Slack 暂存区(与 consumer 同一写法) ----
    def stage(self, envelope_type, payload, received_at=None):
        """像 consumer 那样写一行 slack_events:event_key + chat_of + **接收时钉死 binding_id**
        (chat 当前最新 starting/active 绑定或 NULL),ON CONFLICT DO NOTHING。→ sqlite3.Row(首个)。"""
        from lib import db as dbmod, slackwire, util
        key = slackwire.event_key(envelope_type, payload)
        assert key is not None, "stage(): event_key is None —— fixture 非法"
        chat = slackwire.chat_of(envelope_type, payload)
        now = self.clock.wall_ms() if received_at is None else received_at
        with dbmod.tx(self.conn):
            pinned = None
            if chat:
                r = self.conn.execute(
                    "SELECT binding_id FROM bindings WHERE chat_id=? "
                    "AND status IN ('starting','active') ORDER BY binding_seq DESC LIMIT 1",
                    (chat,)).fetchone()
                pinned = r[0] if r else None
            self.conn.execute(
                "INSERT INTO slack_events(envelope_type,event_key,chat_id,binding_id,"
                "payload_json,received_at,state,next_drain_at) VALUES(?,?,?,?,?,?,'staged',?) "
                "ON CONFLICT(event_key) DO NOTHING",
                (envelope_type, key, chat, pinned, util.jdumps(payload), now, now))
        return self.conn.execute("SELECT * FROM slack_events WHERE event_key=?", (key,)).fetchone()

    def drain(self):
        """= daemon 每 tick 的 drain_staging()(drain 拥有事务)。**WP1 落地前抛 NotImplementedError**:
        daemon_core.DaemonCore.drain_staging 尚不存在;WP1 合入后本方法无需改动即可用
        (DaemonCore 构造签名冻结,见 contracts §8)。"""
        fn = getattr(type(self.core), "drain_staging", None)
        if fn is None:
            raise NotImplementedError(
                "Env.drain(): lib.daemon_core.DaemonCore.drain_staging 由 WP1 提供")
        return self.core.drain_staging()

    def click(self, pending_id=None, nonce=None, act="approve", user=OWNER, channel=None,
              card_ts=None, action_ts=None, team=TEAM, **kw):
        """owner(或他人)点击卡片按钮 → stage 一条 interactive 行(不 drain)。
        pending_id/nonce 缺省取唯一一条 pendings;card_ts 缺省取其 card_message_id 的 ts。"""
        from lib import util
        if pending_id is None or nonce is None:
            rows = self.pendings()
            assert len(rows) == 1, "click(): 需显式 pending_id/nonce(pendings 不唯一)"
            pending_id = pending_id or rows[0]["pending_id"]
            nonce = nonce or rows[0]["nonce"]
        p = self.conn.execute("SELECT * FROM pendings WHERE pending_id=?", (pending_id,)).fetchone()
        if channel is None:
            if p is not None:
                channel = self.inbox_row(p["message_id"])["chat_id"]
            else:
                channel = CHAT
        if card_ts is None:
            if p is not None and p["card_message_id"]:
                card_ts = util.split_message_id(p["card_message_id"])[1]
            else:
                card_ts = next_ts()
        payload = block_action(pending_id, nonce, act=act, user=user, channel=channel,
                               card_ts=card_ts, action_ts=action_ts, team=team, **kw)
        return self.stage("interactive", payload)

    # ---- 查询 ----
    def slack_events(self, state=None):
        if state:
            return self.conn.execute(
                "SELECT * FROM slack_events WHERE state=? ORDER BY seq", (state,)).fetchall()
        return self.conn.execute("SELECT * FROM slack_events ORDER BY seq").fetchall()

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
        return self.conn.execute("SELECT * FROM pendings ORDER BY created_at, pending_id").fetchall()

    # ---- LEGACY-FEISHU: remove in WP5 ----
    def recv_event(self, message_id="om_1", event_id=None, chat_id=CHAT,
                   sender_id=OWNER, content="@TestBot hi", message_type="text",
                   chat_type="group"):  # LEGACY-FEISHU: remove in WP5
        ev = {
            "type": "im.message.receive_v1",
            "event_id": event_id or ("ev_" + message_id),
            "message_id": message_id, "chat_id": chat_id, "chat_type": chat_type,
            "sender_id": sender_id, "message_type": message_type, "content": content,
        }
        self.inbound.process_event(ev)
        return ev

    def arm_mget(self, snapshot_rows):  # LEGACY-FEISHU: remove in WP5
        from tests.helpers import ok_envelope

        def fn(args, cwd):
            return ok_envelope({"messages": snapshot_rows})

        assert self.runner is not None
        self.runner.on(
            lambda a: a[:2] == ["im", "+messages-mget"] and "--download-resources" not in a, fn)


@pytest.fixture
def env(conn, cfg, clock, client, prober, data_dir, runner):
    return Env(conn, cfg, clock, client, prober, data_dir, runner=runner)
