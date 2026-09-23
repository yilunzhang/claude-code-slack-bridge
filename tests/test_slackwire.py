"""lib/slackwire 纯函数契约:event_key / chat_of / interactive_fields / events_fields /
is_dm / is_self_event / is_bot_mentioned / render_text。含 None 安全与 app_mention/message 双投样本。
不依赖 conftest fixture(纯函数),但复用 helpers 的构造器。"""
import json

import pytest

from lib import constants, slackwire as w
from tests.conftest import APP_ID, BOT_ID, BOT_USER, CHAT, DM, MEMBER, OWNER, TEAM
from tests.helpers import app_mention_event, block_action, envelope, message_event


# ---------------------------------------------------------------- event_key
def test_event_key_events_api():
    assert w.event_key("events_api", envelope(message_event(), event_id="Ev123")) == "ev:Ev123"


@pytest.mark.parametrize("payload", [
    None, "str", [], {}, {"event_id": ""}, {"event_id": None}, {"event_id": 5},
    {"event_id": "a:b"},
])
def test_event_key_events_api_none_safety(payload):
    assert w.event_key("events_api", payload) is None


def test_event_key_interactive_shape():
    p = block_action("p1", "n" * 32, act="approve", user=OWNER, channel=CHAT,
                     card_ts="1700000000.000100", action_ts="1700000001.000200", team=TEAM)
    assert w.event_key("interactive", p) == \
        "act:%s:%s:1700000000.000100:%s:sb_approve:1700000001.000200" % (TEAM, CHAT, OWNER)


def test_event_key_interactive_channel_and_card_ts_fallbacks():
    p = block_action("p1", "n", channel=CHAT, card_ts="1.1")
    # channel 只在 container.channel_id;card_ts 只在 message.ts
    del p["channel"]
    p["container"]["channel_id"] = DM
    del p["container"]["message_ts"]
    p["message"] = {"ts": "9.9"}
    assert w.event_key("interactive", p) == \
        "act:%s:%s:9.9:%s:sb_approve:%s" % (TEAM, DM, OWNER, p["actions"][0]["action_ts"])
    assert w.chat_of("interactive", p) == DM
    assert w.interactive_card_ts(p) == "9.9"


@pytest.mark.parametrize("mutate", [
    lambda p: p.pop("team"),
    lambda p: p.__setitem__("team", {}),
    lambda p: (p.pop("channel"), p["container"].pop("channel_id")),
    lambda p: (p["container"].pop("message_ts"), p.pop("message", None)),
    lambda p: p.pop("user"),
    lambda p: p["actions"][0].pop("action_id"),
    lambda p: p["actions"][0].pop("action_ts"),
    lambda p: p.__setitem__("actions", []),
    lambda p: p.__setitem__("actions", p["actions"] * 2),
    lambda p: p.__setitem__("type", "view_submission"),
    lambda p: p["user"].__setitem__("id", "U:BAD"),
])
def test_event_key_interactive_none_when_any_part_missing(mutate):
    p = block_action("p1", "n")
    mutate(p)
    assert w.event_key("interactive", p) is None


def test_event_key_interactive_ignores_value_validity():
    """键只看六段;value 畸形仍可去重入暂存(由 approval 判 skipped)。"""
    p = block_action("p1", "n", value="not json")
    assert w.event_key("interactive", p) is not None
    assert w.interactive_fields(p) is None


def test_event_key_unknown_envelope_type():
    assert w.event_key("slash_commands", {"event_id": "x"}) is None
    assert w.event_key(None, {"event_id": "x"}) is None


# ---------------------------------------------------------------- chat_of
def test_chat_of_events_api_and_none():
    assert w.chat_of("events_api", envelope(message_event(channel=CHAT))) == CHAT
    assert w.chat_of("events_api", {"event": {}}) is None
    assert w.chat_of("events_api", {}) is None
    assert w.chat_of("events_api", None) is None
    assert w.chat_of("interactive", block_action("p", "n", channel=CHAT)) == CHAT
    assert w.chat_of("interactive", {"type": "block_actions"}) is None


# ---------------------------------------------------------------- interactive_fields
def test_interactive_fields_full():
    p = block_action("pid1", "nonce1", act="reject", user=OWNER, channel=CHAT, card_ts="1.1",
                     action_ts="2.2")
    f = w.interactive_fields(p)
    assert f == {
        "team": TEAM, "channel": CHAT, "user": OWNER, "action_id": constants.ACTION_REJECT,
        "action_ts": "2.2", "card_ts": "1.1",
        "value": {"pending_id": "pid1", "nonce": "nonce1", "act": "reject"},
        "response_url": p.get("response_url"),
    }


def test_interactive_fields_value_as_dict_accepted():
    p = block_action("pid1", "nonce1")
    p["actions"][0]["value"] = {"pending_id": "pid1", "nonce": "nonce1", "act": "approve"}
    assert w.interactive_fields(p)["value"]["act"] == "approve"


@pytest.mark.parametrize("value", [
    "garbage", json.dumps([1, 2]), json.dumps({"pending_id": "p", "nonce": "n"}),
    json.dumps({"pending_id": "p", "nonce": "", "act": "approve"}),
    json.dumps({"pending_id": "", "nonce": "n", "act": "approve"}),
    json.dumps({"pending_id": "p", "nonce": 5, "act": "approve"}),
    json.dumps({"pending_id": "p", "nonce": "n", "act": "reject"}),   # act 与 sb_approve 不一致
    json.dumps({"pending_id": "p", "nonce": "n", "act": "APPROVE"}),
    None,
])
def test_interactive_fields_none_on_bad_value(value):
    p = block_action("p", "n", act="approve", value=value)
    if value is None:
        p["actions"][0].pop("value", None)
    assert w.interactive_fields(p) is None


def test_interactive_fields_rejects_foreign_action_id():
    p = block_action("p", "n", action_id="other_button")
    assert w.event_key("interactive", p) is not None   # 键可算
    assert w.interactive_fields(p) is None             # 但不是我们的按钮


def test_interactive_fields_none_safety():
    for bad in (None, [], "x", {}, {"type": "block_actions"}):
        assert w.interactive_fields(bad) is None


# ---------------------------------------------------------------- events_fields
def test_events_fields_flatten():
    ev = message_event(text="hi", channel=CHAT, user=OWNER, ts="1.1", thread_ts="0.9",
                       files=[{"id": "F1"}], blocks=[{"type": "rich_text"}])
    f = w.events_fields(envelope(ev, event_id="EvX"))
    assert f["team_id"] == TEAM and f["api_app_id"] == APP_ID and f["event_id"] == "EvX"
    assert f["type"] == "message" and f["channel"] == CHAT and f["user"] == OWNER
    assert f["ts"] == "1.1" and f["thread_ts"] == "0.9" and f["subtype"] == "file_share"  # files ⇒ file_share
    assert f["files"] == [{"id": "F1"}] and f["blocks"] == [{"type": "rich_text"}]
    assert f["event"] is ev


def test_events_fields_none_safety():
    assert w.events_fields(None) is None
    assert w.events_fields({}) is None
    assert w.events_fields({"event": "x"}) is None
    assert w.events_fields({"event": {}}) is None
    f = w.events_fields({"event": {"type": "message", "text": 5, "files": "no", "blocks": [1]}})
    assert f["text"] == "" and f["files"] == [] and f["blocks"] == []


# ---------------------------------------------------------------- is_dm / is_self_event
def test_is_dm():
    assert w.is_dm(message_event(channel=DM))
    assert not w.is_dm(message_event(channel=CHAT))
    assert w.is_dm({"channel": DM})                       # 无 channel_type → 前缀
    assert not w.is_dm({"channel": DM, "channel_type": "channel"})  # channel_type 优先
    assert not w.is_dm(None) and not w.is_dm({})


def test_is_self_event_three_comparisons_each_require_both_nonempty():
    cfg = {"bot_id": BOT_ID, "bot_user_id": BOT_USER, "app_id": APP_ID}
    assert w.is_self_event({"bot_id": BOT_ID}, cfg)
    assert w.is_self_event({"user": BOT_USER}, cfg)
    assert w.is_self_event({"app_id": APP_ID}, cfg)
    assert not w.is_self_event({"user": OWNER}, cfg)
    # None 安全:cfg 缺值 / 事件缺值 → 绝不命中
    assert not w.is_self_event({"user": None, "bot_id": None, "app_id": None}, cfg)
    assert not w.is_self_event({"user": OWNER}, {"bot_id": None, "bot_user_id": None, "app_id": None})
    assert not w.is_self_event({}, {})
    assert not w.is_self_event(None, None)
    assert not w.is_self_event({"user": ""}, {"bot_user_id": ""})


# ---------------------------------------------------------------- mention / render
def test_is_bot_mentioned_by_type_regex_and_rich_text():
    assert w.is_bot_mentioned(app_mention_event(), BOT_USER)
    assert w.is_bot_mentioned(app_mention_event(), None)   # app_mention 不依赖 bot_user_id
    assert w.is_bot_mentioned(message_event(text="<@%s> hi" % BOT_USER), BOT_USER)
    assert w.is_bot_mentioned(message_event(text="<@%s|bot> hi" % BOT_USER), BOT_USER)
    assert not w.is_bot_mentioned(message_event(text="<@%s> hi" % MEMBER), BOT_USER)
    assert not w.is_bot_mentioned(message_event(text="<@%sX> hi" % BOT_USER), BOT_USER)  # 前缀不算
    rich = [{"type": "rich_text", "elements": [{"type": "rich_text_section", "elements": [
        {"type": "user", "user_id": BOT_USER}, {"type": "text", "text": " hi"}]}]}]
    assert w.is_bot_mentioned(message_event(text="plain", blocks=rich), BOT_USER)
    # bot_user_id 缺失 → 只认 app_mention
    assert not w.is_bot_mentioned(message_event(text="<@%s> hi" % BOT_USER), None)
    assert not w.is_bot_mentioned(None, BOT_USER)


def test_render_text_strips_mention_unescapes_and_strips():
    ev = message_event(text="  <@%s|bot> a &lt;b&gt; &amp; c <@%s>  " % (BOT_USER, MEMBER))
    assert w.render_text(ev, BOT_USER) == "a <b> & c <@%s>" % MEMBER
    assert w.render_text({"text": "<@%s>" % BOT_USER}, BOT_USER) == ""
    assert w.render_text({"text": 5}, BOT_USER) == ""
    assert w.render_text({}, None) == ""
    assert w.render_text({"text": "&amp;lt;"}, None) == "&lt;"   # 单次解码


# ---------------------------------------------------------------- 双投样本
def test_app_mention_and_message_duplication_fixture():
    """频道 @bot:同一条消息以 message 与 app_mention 两个 event_id 双投;message_id 相同、
    files/blocks 只在 message 上(不对称)。"""
    ts = "1700000000.000100"
    m = message_event(text="<@%s> look" % BOT_USER, channel=CHAT, user=MEMBER, ts=ts,
                      files=[{"id": "F1", "name": "a.pdf"}], blocks=[{"type": "rich_text"}])
    a = app_mention_event(text="<@%s> look" % BOT_USER, channel=CHAT, user=MEMBER, ts=ts)
    em, ea = envelope(m, event_id="Ev_msg"), envelope(a, event_id="Ev_mention")
    assert w.event_key("events_api", em) != w.event_key("events_api", ea)
    fm, fa = w.events_fields(em), w.events_fields(ea)
    assert (fm["channel"], fm["ts"]) == (fa["channel"], fa["ts"])
    assert fm["files"] and not fa["files"]
    assert w.is_bot_mentioned(m, BOT_USER) and w.is_bot_mentioned(a, BOT_USER)
    assert w.render_text(m, BOT_USER) == w.render_text(a, BOT_USER) == "look"
