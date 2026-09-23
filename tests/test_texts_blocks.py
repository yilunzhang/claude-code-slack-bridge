"""lib/texts:Block Kit 审批卡 / 六种 decision 更新块 / 文案改名 / 预览净化与转义。"""
import json

import pytest

from lib import constants, texts


def card(preview="hello", sender="U0MEMBER", pid="p" * 32, nonce="n" * 32):
    return json.loads(texts.build_approval_card(pid, nonce, sender, preview))


def test_card_top_level_shape():
    c = card("hi there")
    assert set(c) == {"text", "blocks"}
    assert c["text"].startswith("成员消息待审批:") and "hi there" in c["text"]
    types = [b["type"] for b in c["blocks"]]
    assert types == ["section", "section", "actions"]
    assert c["blocks"][0]["text"]["type"] == "plain_text"
    assert "U0MEMBER" in c["blocks"][0]["text"]["text"]
    assert c["blocks"][1]["text"] == {"type": "plain_text", "text": "hi there", "emoji": False}


def test_card_buttons_action_ids_and_values():
    pid, nonce = "p" * 32, "n" * 32
    c = card(pid=pid, nonce=nonce)
    btns = c["blocks"][2]["elements"]
    assert [b["action_id"] for b in btns] == [constants.ACTION_APPROVE, constants.ACTION_REJECT]
    assert [b["type"] for b in btns] == ["button", "button"]
    assert [b["style"] for b in btns] == ["primary", "danger"]
    for b, act in zip(btns, ("approve", "reject")):
        v = json.loads(b["value"])
        assert v == {"pending_id": pid, "nonce": nonce, "act": act}
        assert len(b["value"].encode("utf-8")) <= constants.CARD_VALUE_MAX_BYTES
    assert c["blocks"][2]["block_id"] == "sb_actions:" + pid


def test_card_value_too_large_raises():
    with pytest.raises(ValueError):
        texts.build_approval_card("p" * 1500, "n" * 600, "x", "y")


def test_card_preview_escaped_and_control_stripped():
    c = card("<!channel> a & b\x00c\x07d\nline2 e")
    body = c["blocks"][1]["text"]["text"]
    assert body == "&lt;!channel&gt; a &amp; b c d\nline2 e"
    assert "<!channel>" not in json.dumps(c)


def test_card_preview_truncated_to_limit_after_escape():
    c = card("<" * 5000)
    body = c["blocks"][1]["text"]["text"]
    assert len(body) <= constants.CARD_PREVIEW_LIMIT == 2900
    assert body.endswith("…") and body.startswith("&lt;")
    c2 = card("x" * 2899)
    assert c2["blocks"][1]["text"]["text"] == "x" * 2899
    c3 = card("x" * 2901)
    assert len(c3["blocks"][1]["text"]["text"]) == 2900


def test_card_empty_preview_and_non_string():
    assert card("")["blocks"][1]["text"]["text"] == "(空)"
    assert card(None)["blocks"][1]["text"]["text"] == "None"
    assert card(123)["blocks"][1]["text"]["text"] == "123"


def test_card_sender_label_escaped_in_header():
    c = card(sender="<@U0BOT>")
    assert "&lt;@U0BOT&gt;" in c["blocks"][0]["text"]["text"]


def test_sanitize_preview_legacy_semantics():
    assert texts.sanitize_preview("a\x01b") == "a b"
    assert texts.sanitize_preview("x" * 10, limit=5) == "xxxx…"
    assert texts.sanitize_preview("a\ud800b") == "ab"   # 孤立 surrogate 删


# ---------------------------------------------------------------- decision blocks
@pytest.mark.parametrize("outcome", constants.DECISION_OUTCOMES)
def test_decision_update_blocks_six_outcomes_no_buttons(outcome):
    blocks = texts.decision_update_blocks(outcome, "U0OWNER")
    assert isinstance(blocks, list) and blocks
    assert all(b["type"] != "actions" for b in blocks)
    assert all(el.get("type") != "button" for b in blocks for el in b.get("elements", []))
    ctx = blocks[-1]
    assert ctx["type"] == "context" and ctx["block_id"] == "sb_decision:" + outcome
    assert texts.DECISION_TEXT[outcome] in ctx["elements"][0]["text"]
    json.dumps(blocks)  # 可序列化


def test_decision_texts_chinese_per_plan():
    t = texts.DECISION_TEXT
    assert t["delivered"] == "✅ 已投递给 CC session。"
    assert t["approved_pending_files"] == "✅ 已批准,附件处理中…"
    assert t["rejected"] == "🚫 已忽略。"
    assert t["expired"] == "⌛ 审批超时,已自动忽略。"
    assert t["attachment_failed"] == "⚠️ 附件获取失败,该消息未投递。"
    assert t["closed_undelivered"] == "🔌 绑定已结束,附件未投递。"
    assert tuple(t) == constants.DECISION_OUTCOMES
    for o in constants.DECISION_OUTCOMES:
        assert texts.decision_notice_body(o) == t[o] == texts.decision_update_text(o)


def test_decision_decided_by_mention_only_when_valid():
    ok = texts.decision_update_blocks("delivered", "U0OWNER")[-1]["elements"][0]["text"]
    assert "<@U0OWNER>" in ok
    bad = texts.decision_update_blocks("delivered", "<!channel>")[-1]["elements"][0]["text"]
    assert "<!channel>" not in bad and "<@" not in bad
    none = texts.decision_update_blocks("expired", None)[-1]["elements"][0]["text"]
    assert none == texts.DECISION_TEXT["expired"]
    # 系统性结论(expired/closed_undelivered)不署名
    sys_ = texts.decision_update_blocks("closed_undelivered", "U0OWNER")[-1]["elements"][0]["text"]
    assert "<@" not in sys_


def test_decision_update_blocks_optional_preview():
    blocks = texts.decision_update_blocks("rejected", "U0OWNER", preview="<x> & y")
    assert [b["type"] for b in blocks] == ["section", "section", "context"]
    assert blocks[1]["text"]["text"] == "&lt;x&gt; &amp; y"


def test_decision_update_blocks_rejects_unknown_outcome():
    with pytest.raises(ValueError):
        texts.decision_update_blocks("approved", "U0OWNER")


# ---------------------------------------------------------------- 文案改名
def _all_notice_strings():
    out = list(texts.INBOUND_NOTICE.values()) + list(texts.LC_CLOSED.values())
    out += [texts.LC_BOUND, texts.BIND_BANNER, texts.UNSUPPORTED_NOTICE,
            texts.lifecycle_close_body("???"), texts.send_failure_alert_body(),
            texts.unconfirmed_alert_body(), texts.group_cancelled_alert_body()]
    out += list(texts.DECISION_TEXT.values())
    return out


def test_notices_say_slack_bridge_and_never_feishu():
    strings = _all_notice_strings()
    assert any("/slack-bridge:bridge" in s for s in strings)
    for s in strings:
        assert "feishu" not in s.lower() and "飞书" not in s and "lark" not in s.lower(), s
    for k in ("user_unbind", "cc_gone", "session_end", "listener_gone", "listener_never_ready",
              "bind_failed", "bind_timeout"):
        assert "/slack-bridge:bridge" in texts.LC_CLOSED[k], k
    for k in ("unbound", "session_closed"):
        assert "/slack-bridge:bridge" in texts.INBOUND_NOTICE[k], k
    assert "/slack-bridge:bridge unbind" in texts.LC_BOUND and "/slack-bridge:bridge unbind" in texts.BIND_BANNER


def test_alert_bodies_are_fixed_and_honest():
    assert "无法确认" in texts.unconfirmed_alert_body()
    assert "重复" in texts.unconfirmed_alert_body()
    assert "未能发出" in texts.send_failure_alert_body()
    assert "取消" in texts.group_cancelled_alert_body()


def test_legacy_feishu_helpers_removed():
    """WP5:飞书时代的取件提示与 approved/failed 旧键已删;decision_notice_body 只认六种 outcome。"""
    for name in ("DECISION_NOTICE", "media_fetch_hint", "reply_fetch_hint"):
        assert not hasattr(texts, name), name
    for legacy in ("approved", "failed"):
        with pytest.raises(KeyError):
            texts.decision_notice_body(legacy)
