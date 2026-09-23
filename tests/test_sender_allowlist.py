"""成员直投白名单:`(chat_id, user_id)` 在名单里 → 跳过审批卡直投。

**这是唯一一处放宽"member 必经 owner 审批"的口子**,所以判定必须是两个字段的精确合取:
少任一条件就变成"整个会话放行"或"该用户在所有会话放行",都超出 owner 授权范围。
信任级别不随之提升:payload 仍 `sender_is_owner=false`,且 `approved_by="allowlist"`(不是 None)。
"""
import json

import pytest

from lib import paths, senderallow
from tests.conftest import CHAT, MEMBER, OWNER
from tests.helpers import envelope, message_event, slack_file
from tests.test_inbound import ingest, mention, mid_of, ok_materializer

OTHER_CHAT = "C_OTHER_CHAT"
OTHER_MEMBER = "U_OTHER_MEMBER"


def _write_allowlist(entries):
    paths.ensure_data_dir()
    paths.allowlist_path().write_text(json.dumps({"entries": entries}, ensure_ascii=False), encoding="utf-8")


def _deliver_member(env, *, chat_id=CHAT, sender=MEMBER, files=None):
    """投一条 member 消息,返回 (deliveries, pendings)。"""
    if not env.conn.execute("SELECT 1 FROM bindings WHERE chat_id=? AND status='active'", (chat_id,)).fetchone():
        env.make_binding(status="active", chat_id=chat_id)
    ev = message_event(text=mention("跑个任务"), channel=chat_id, user=sender, files=files)
    assert ingest(env, envelope(ev))[0] == "handed"
    env.inbound.drive_pending_rows()
    return env.deliveries(), env.pendings()


class TestGate:
    def test_listed_member_delivers_without_approval(self, env):
        _write_allowlist([{"chat_id": CHAT, "user_id": MEMBER}])
        deliveries, pendings = _deliver_member(env)
        assert len(deliveries) == 1 and pendings == []

    @pytest.mark.parametrize("entry,desc", [
        ({"chat_id": OTHER_CHAT, "user_id": MEMBER}, "同人不同会话"),
        ({"chat_id": CHAT, "user_id": OTHER_MEMBER}, "同会话不同人"),
        ({"chat_id": CHAT}, "缺 user_id(整会话放行)"),
        ({"user_id": MEMBER}, "缺 chat_id(该人全局放行)"),
        ({"chat_id": CHAT[:2], "user_id": MEMBER[:2]}, "两字段都是真 id 的前缀"),
        ({"chat_id": CHAT + "_suffix", "user_id": MEMBER + "_x"}, "两字段都是真 id 的超串"),
        ({"chat_id": CHAT, "open_id": MEMBER}, "旧键名 open_id 不再认"),
    ])
    def test_partial_match_does_not_grant(self, env, entry, desc):
        _write_allowlist([entry])
        deliveries, pendings = _deliver_member(env)
        assert deliveries == [], desc
        assert len(pendings) == 1, desc

    def test_waiting_binding_files_still_gate_unlisted_member(self, env):
        """第四条入口 × 附件:未入名单成员的附件在 starting 期到达 → 激活后重过门,绝不直投。"""
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        env.inbound.materializer = ok_materializer()
        ev = message_event(text=mention("img"), user=MEMBER, files=[slack_file()])
        ingest(env, envelope(ev))
        env.inbound.drive_pending_rows()
        assert env.inbox_row(mid_of(ev))["state"] == "waiting_binding"
        env.conn.execute(
            "UPDATE bindings SET status='active', session_id='s1', bind_phase='confirmed', "
            "listener_beat_at=? WHERE binding_id=?", (env.clock.wall_ms(), bid))
        env.inbound.drive_pending_rows()
        assert env.deliveries() == [] and len(env.pendings()) == 1

    def test_absent_or_corrupt_file_falls_back_to_approval(self, env):
        p = paths.allowlist_path()
        paths.ensure_data_dir()
        pair = '{"chat_id": "%s", "user_id": "%s"}' % (CHAT, MEMBER)
        for content in (None, "{ not json" + pair, '{"entries": %s}' % pair, '[%s]' % pair,
                        '{"allow": [%s]}' % pair, '{"entries": [[%s]]}' % pair):
            if content is None:
                p.unlink(missing_ok=True)
            else:
                p.write_text(content, encoding="utf-8")
            assert senderallow.is_allowed(CHAT, MEMBER) is False, content

    @pytest.mark.parametrize("chat_id,user_id", [
        (None, MEMBER), (CHAT, None), ("", MEMBER), (CHAT, ""), (None, None),
    ])
    def test_empty_query_ids_never_match(self, env, chat_id, user_id):
        _write_allowlist([{"chat_id": CHAT}, {"user_id": MEMBER}, {}])
        assert senderallow.is_allowed(chat_id, user_id) is False


class TestTrustLevel:
    def test_payload_marks_allowlist_not_owner(self, env):
        _write_allowlist([{"chat_id": CHAT, "user_id": MEMBER}])
        deliveries, _ = _deliver_member(env)
        p = json.loads(deliveries[-1]["payload_json"])
        assert p["sender_is_owner"] is False and p["approved_by"] == "allowlist"
        assert p["sender_user_id"] == MEMBER

    def test_owner_payload_still_has_null_approved_by(self, env):
        deliveries, _ = _deliver_member(env, sender=OWNER)
        p = json.loads(deliveries[-1]["payload_json"])
        assert p["approved_by"] is None and p["sender_is_owner"] is True

    def test_files_from_listed_member_marked_allowlist(self, env):
        _write_allowlist([{"chat_id": CHAT, "user_id": MEMBER}])
        env.inbound.materializer = ok_materializer()
        deliveries, pendings = _deliver_member(env, files=[slack_file()])
        assert deliveries and pendings == []
        p = json.loads(deliveries[-1]["payload_json"])
        assert p["media_paths"] and p["approved_by"] == "allowlist" and p["sender_is_owner"] is False


class TestStore:
    def test_add_then_allowed_without_restart(self, env):
        assert senderallow.is_allowed(CHAT, MEMBER) is False
        senderallow.add_entry(CHAT, MEMBER, note="张三")
        assert senderallow.is_allowed(CHAT, MEMBER) is True

    def test_external_write_takes_effect_without_restart(self, env):
        assert senderallow.is_allowed(CHAT, MEMBER) is False
        _write_allowlist([{"chat_id": CHAT, "user_id": MEMBER}])
        assert senderallow.is_allowed(CHAT, MEMBER) is True
        _write_allowlist([])
        assert senderallow.is_allowed(CHAT, MEMBER) is False

    def test_add_is_idempotent(self, env):
        senderallow.add_entry(CHAT, MEMBER)
        entries, added = senderallow.add_entry(CHAT, MEMBER)
        assert added is False and len(entries) == 1

    def test_remove_only_targets_exact_pair(self, env):
        senderallow.add_entry(CHAT, MEMBER)
        senderallow.add_entry(CHAT, OTHER_MEMBER)
        senderallow.add_entry(OTHER_CHAT, MEMBER)
        _, removed = senderallow.remove_entry(CHAT, MEMBER)
        assert removed is True
        assert senderallow.is_allowed(CHAT, MEMBER) is False
        assert senderallow.is_allowed(CHAT, OTHER_MEMBER) is True
        assert senderallow.is_allowed(OTHER_CHAT, MEMBER) is True

    def test_add_rejects_missing_id(self, env):
        for bad in ((CHAT, ""), ("", MEMBER), (None, MEMBER), (CHAT, None)):
            with pytest.raises(ValueError):
                senderallow.add_entry(*bad)

    def test_note_is_cosmetic_only(self, env):
        senderallow.add_entry(CHAT, MEMBER, note="张三")
        assert senderallow.is_allowed(CHAT, MEMBER) is True
        entries = senderallow.load_entries()
        assert entries[0]["note"] == "张三" and set(entries[0]) == {"chat_id", "user_id", "note"}

    def test_cli_add_list_remove_round_trip(self, env, monkeypatch, capsys):
        """经真实 bridgectl `allow` 子命令走一遍(`--user-id`;CLI 归 WP4,未切换前 skip)。"""
        import importlib.util
        import pathlib as _pl
        import sys as _sys
        root = _pl.Path(__file__).resolve().parents[1]
        src = (root / "bin" / "bridgectl.py").read_text(encoding="utf-8")
        if "--user-id" not in src:
            pytest.skip("bin/bridgectl.py allow 子命令尚未切到 --user-id(WP4)")
        spec = importlib.util.spec_from_file_location("bridgectl_cli_mod", root / "bin" / "bridgectl.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        def run(*argv):
            monkeypatch.setattr(_sys, "argv", ["bridgectl", *argv])
            with pytest.raises(SystemExit) as ei:
                mod.main()
            return json.loads(capsys.readouterr().out), ei.value.code

        res, code = run("allow", "add", "--chat-id", CHAT, "--user-id", MEMBER)
        assert code == 0 and res["added"] is True
        assert senderallow.is_allowed(CHAT, MEMBER) is True
        res, _ = run("allow", "list")
        assert [(e["chat_id"], e["user_id"]) for e in res["entries"]] == [(CHAT, MEMBER)]
        res, _ = run("allow", "remove", "--chat-id", CHAT, "--user-id", MEMBER)
        assert res["removed"] is True and senderallow.is_allowed(CHAT, MEMBER) is False
        res, code = run("allow", "add", "--chat-id", CHAT)
        assert code != 0 and res["ok"] is False and senderallow.load_entries() == []

    def test_config_json_untouched(self, env):
        before = paths.config_path().read_text(encoding="utf-8")
        senderallow.add_entry(CHAT, MEMBER)
        assert paths.config_path().read_text(encoding="utf-8") == before
        assert paths.allowlist_path().exists()
