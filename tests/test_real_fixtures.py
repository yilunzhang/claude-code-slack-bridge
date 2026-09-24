"""真机样本回放(tests/fixtures/real,2026-09-24 采集,已脱敏):把真实 Socket Mode `events_api` 信封原样
灌进 staging → drain,断言入站契约在真实形状上成立(双投去重、未 @ 忽略、file_share 带 files)。
样本里的占位符(T0REAL / C0REAL)在加载时映射到测试 cfg 的 TEAM / CHAT,其余 id 与 helpers 常量一致。"""
import json
import pathlib

from lib import db as dbmod
from tests.helpers import CHAT, OWNER, TEAM

OWNER_DM = "D0OWNERDM"

REAL = pathlib.Path(__file__).parent / "fixtures" / "real"


def load_real(name):
    doc = json.loads((REAL / name).read_text(encoding="utf-8"))
    raw = json.dumps(doc["payload"], ensure_ascii=False).replace("T0REAL", TEAM).replace("C0REAL", CHAT).replace("D0REAL", OWNER_DM)
    return doc["envelope_type"], json.loads(raw)


def _stage_real(env, name):
    et, payload = load_real(name)
    row = env.stage(et, payload)
    assert row is not None, name
    return payload["event"]


def test_real_mention_message_then_app_mention_delivers_once(env):
    bid = env.make_binding(status="active", chat_id=CHAT)
    ev = _stage_real(env, "channel_owner_mention__message.json")       # 真机顺序:message 先到
    _stage_real(env, "channel_owner_mention__app_mention.json")
    env.drain()
    env.inbound.drive_pending_rows()
    mid = "%s:%s" % (CHAT, ev["ts"])
    row = env.inbox_row(mid)
    assert row is not None and row["state"] == "enqueued" and row["sender_user_id"] == OWNER
    assert row["message_type"] == "message"
    assert len(env.deliveries(bid)) == 1
    keys = {r["event_key"]: r["error"] for r in env.slack_events("consumed")}
    assert keys["ev:Ev0REAL3"] == "dup_message"                         # app_mention 是重复投递
    assert int(dbmod.get_state(env.conn, "inbox_dup_message", "0")) == 1
    assert int(dbmod.get_state(env.conn, "inbox_snapshot_upgraded", "0")) == 0
    payload = json.loads(env.deliveries(bid)[0]["payload_json"])   # listener 发出时才加 type=slack_message
    assert payload["sender_is_owner"] is True and payload["message_type"] == "message"
    assert "hi from owner" in payload["text"] and "<@" not in payload["text"]  # bot mention 已剥离


def test_real_no_mention_message_is_ignored(env):
    bid = env.make_binding(status="active", chat_id=CHAT)
    ev = _stage_real(env, "channel_owner_no_mention__message.json")
    env.drain()
    env.inbound.drive_pending_rows()
    row = env.inbox_row("%s:%s" % (CHAT, ev["ts"]))
    assert row is not None and row["state"] == "ignored_not_mentioned"
    assert env.deliveries(bid) == []


def test_real_file_share_message_then_app_mention_keeps_files(env):
    env.make_binding(status="active", chat_id=CHAT)
    ev = _stage_real(env, "channel_owner_file_share__message.json")    # subtype=file_share,先到
    assert ev["subtype"] == "file_share" and ev["files"][0]["url_private_download"]
    _stage_real(env, "channel_owner_file_share__app_mention.json")
    env.drain()
    row = env.inbox_row("%s:%s" % (CHAT, ev["ts"]))
    assert row is not None
    snap = json.loads(row["snapshot_json"])                          # snapshot_json = event 本体
    files = snap["files"]
    assert files and files[0]["name"] == "bridge-test.txt" and files[0]["mimetype"] == "text/plain"
    keys = {r["event_key"]: r["error"] for r in env.slack_events("consumed")}
    assert keys["ev:Ev0REAL5"] == "dup_message"
    assert int(dbmod.get_state(env.conn, "inbox_dup_message", "0")) == 1


def test_real_dm_owner_message_delivered_and_bot_echo_dropped(env):
    env.cfg["owner_dm_id"] = OWNER_DM
    bid = env.make_binding(status="active", chat_id=OWNER_DM)
    ev = _stage_real(env, "dm_owner_message.json")
    echo = _stage_real(env, "dm_bot_self_echo__message.json")
    assert ev.get("channel_type") == "im" and echo["bot_id"] and echo["user"] != OWNER
    env.drain()
    env.inbound.drive_pending_rows()
    row = env.inbox_row("%s:%s" % (OWNER_DM, ev["ts"]))
    assert row is not None and row["state"] == "enqueued"          # DM 无需 @
    assert env.inbox_row("%s:%s" % (OWNER_DM, echo["ts"])) is None   # 自身回流不进 inbox
    keys = {r["event_key"]: r["error"] for r in env.slack_events("consumed")}
    assert keys["ev:Ev0REAL7"] == "self"
    assert len(env.deliveries(bid)) == 1
