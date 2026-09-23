"""成员直投白名单:`(chat_id, open_id)` 在名单里 → 跳过审批卡直投。

**这是唯一一处放宽"member 必经 owner 审批"的口子**,所以判定必须是两个字段的
精确合取:少任一条件就变成"整个群放行"或"该用户在所有群放行",都超出 owner 授权范围。

信任级别不随之提升:payload 仍 `sender_is_owner=false`,且 `approved_by="allowlist"`
(**不是 None** —— None 是 owner 本人的语义)。
"""
import json

import pytest

from tests.conftest import APP_ID, CHAT, MEMBER, OWNER
from tests.helpers import bot_mention, mget_snapshot, ok_envelope
from lib import paths, senderallow

OTHER_CHAT = "oc_other_chat"
OTHER_MEMBER = "ou_other_member"


def _write_allowlist(entries):
    paths.ensure_data_dir()
    paths.allowlist_path().write_text(
        json.dumps({"entries": entries}, ensure_ascii=False), encoding="utf-8")


def _deliver_member(env, *, chat_id=CHAT, sender=MEMBER, msg_type="text",
                    message_id="om_1"):
    """投一条 member 消息,返回 (deliveries, pendings)。"""
    env.make_binding(status="active", chat_id=chat_id)
    env.arm_mget([mget_snapshot(message_id, chat_id, sender, msg_type=msg_type,
                                text="跑个任务", mentions=[bot_mention(APP_ID)])])
    env.recv_event(message_id=message_id, chat_id=chat_id, sender_id=sender,
                   message_type=msg_type)
    return env.deliveries(), env.pendings()


class TestGate:
    def test_listed_member_delivers_without_approval(self, env):
        """名单命中 → 直投,且**不产生 pending**(没有卡片要点)。
        (基线「未入名单必经审批」由既有 test_inbound.py::test_member_text_goes_to_approval
        钉住,不在此重测。)"""
        _write_allowlist([{"chat_id": CHAT, "open_id": MEMBER}])
        deliveries, pendings = _deliver_member(env)
        assert len(deliveries) == 1
        assert pendings == []

    @pytest.mark.parametrize("entry,desc", [
        ({"chat_id": OTHER_CHAT, "open_id": MEMBER}, "同人不同群"),
        ({"chat_id": CHAT, "open_id": OTHER_MEMBER}, "同群不同人"),
        ({"chat_id": CHAT}, "缺 open_id(整群放行)"),
        ({"open_id": MEMBER}, "缺 chat_id(该人全局放行)"),
        # **前缀/超串**:防"精确 == 换成 startswith/in"——`"oc_"` 是所有群 id 的前缀,
        # 一条这样的条目在宽松匹配下就是通配符(codex 实证:不加这两条,前缀匹配全绿)。
        ({"chat_id": CHAT[:4], "open_id": MEMBER[:4]}, "两字段都是真 id 的前缀"),
        ({"chat_id": CHAT + "_suffix", "open_id": MEMBER + "_x"}, "两字段都是真 id 的超串"),
    ])
    def test_partial_match_does_not_grant(self, env, entry, desc):
        """**两个字段必须同时精确相等**。任一维度不符 / 缺失 / 只是前缀或超串都不得放行。"""
        _write_allowlist([entry])
        deliveries, pendings = _deliver_member(env)
        assert deliveries == [], desc
        assert len(pendings) == 1, desc

    def test_waiting_binding_media_still_gates_unlisted_member(self, env):
        """**第四条入口 × 媒体**:未入名单成员的**图片**在绑定 `starting` 期到达 →
        挂 `waiting_binding`,激活后经 `_drive_waiting` 重过门。

        为什么必须是**图片**而不是文本:文本那半已被 test_waiting_binding.py 钉住;
        而媒体走的是 `_materialize_then_finalize`,那里「非 owner + 无 pending ⟹ 白名单」
        的推断若被 waiting 路径绕过,这张图会**直投且被标成 `approved_by="allowlist"`**
        —— codex 实证该变异下**全套 628 条全绿**,是真正的越权洞。"""
        bid = env.make_binding(status="starting", bind_phase="unconfirmed")
        snap = mget_snapshot("om_wi", CHAT, MEMBER, msg_type="image",
                             mentions=[bot_mention(APP_ID)])
        env.arm_mget([snap])
        env.runner.on(lambda a: "--download-resources" in a,
                      lambda a, cwd: ok_envelope({"messages": [snap]}))
        env.recv_event(message_id="om_wi", sender_id=MEMBER, message_type="image")
        assert env.inbox_row("om_wi")["state"] == "waiting_binding"
        env.conn.execute(
            "UPDATE bindings SET status='active', session_id='s1', bind_phase='confirmed', "
            "listener_beat_at=? WHERE binding_id=?", (env.clock.wall_ms(), bid))
        env.inbound.drive_row(env.inbox_row("om_wi"))
        assert env.deliveries() == []                 # 未在名单 → 不得直投
        assert len(env.pendings()) == 1               # 应进审批门

    def test_absent_or_corrupt_file_falls_back_to_approval(self, env):
        """文件不存在 / 不是 JSON / 形状不对 → **fail-closed 回到审批门**,不得放行也不得炸。

        **坏形状必须携带一个"若被宽松解析就会命中"的 pair**(codex 实证):否则把
        `entries` 是个 dict 的情况宽松转成 `[那个 dict]` 的实现照样全绿 —— 测试根本没给
        它可命中的内容。故下面每个坏形状里都埋着真实的 (CHAT, MEMBER)。"""
        p = paths.allowlist_path()
        paths.ensure_data_dir()
        pair = '{"chat_id": "%s", "open_id": "%s"}' % (CHAT, MEMBER)
        for content in (
            None,                                    # 文件不存在
            "{ not json" + pair,                     # 非 JSON(但含可命中的字面量)
            '{"entries": %s}' % pair,                # entries 是 dict 而非 list
            '[%s]' % pair,                           # 根是 list 而非 dict
            '{"allow": [%s]}' % pair,                # 键名不对
            '{"entries": [[%s]]}' % pair,            # 条目被多包了一层 list
        ):
            if content is None:
                p.unlink(missing_ok=True)
            else:
                p.write_text(content, encoding="utf-8")
            assert senderallow.is_allowed(CHAT, MEMBER) is False, content

    @pytest.mark.parametrize("chat_id,open_id", [
        (None, MEMBER), (CHAT, None), ("", MEMBER), (CHAT, ""), (None, None),
    ])
    def test_empty_query_ids_never_match(self, env, chat_id, open_id):
        """**查询侧**的空 id 也必须拒(不只是条目侧)。

        真实场景:快照的 `sender.id` 缺失 → 传进来的 `sender_open_id` 是 None,而名单里
        恰好有一条畸形条目也缺 `open_id` ⇒ `None == None` 成立 → **直投**。
        codex 实证:删掉 `is_allowed` 开头那两行空值拒绝,原有测试全绿。"""
        _write_allowlist([{"chat_id": CHAT}, {"open_id": MEMBER}, {}])
        assert senderallow.is_allowed(chat_id, open_id) is False


class TestTrustLevel:
    def test_payload_marks_allowlist_not_owner(self, env):
        """信任级别**不随直投提升**:`sender_is_owner=false` +
        `approved_by="allowlist"`。**approved_by 绝不能是 None** —— 那是 owner 本人的
        语义,混用会让 agent 把白名单成员的消息当成 owner 亲发。"""
        _write_allowlist([{"chat_id": CHAT, "open_id": MEMBER}])
        deliveries, _ = _deliver_member(env)
        p = json.loads(deliveries[-1]["payload_json"])
        assert p["sender_is_owner"] is False
        assert p["approved_by"] == "allowlist"
        assert p["sender_open_id"] == MEMBER

    def test_owner_payload_still_has_null_approved_by(self, env):
        """对照:owner 本人的 `approved_by` 仍是 None —— 两者必须可区分。"""
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_o2", CHAT, OWNER, text="hi",
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_id="om_o2", sender_id=OWNER)
        p = json.loads(env.deliveries()[-1]["payload_json"])
        assert p["approved_by"] is None and p["sender_is_owner"] is True

    def test_media_from_listed_member_marked_allowlist(self, env):
        """**媒体走另一条代码路径**(`_materialize_then_finalize`,先下载再入队)。
        那里 `approved_pending is None`,若照搬"无 pending ⟹ owner"就会把白名单成员的
        图标成 owner 亲发(approved_by=None)。"""
        _write_allowlist([{"chat_id": CHAT, "open_id": MEMBER}])
        env.make_binding(status="active")
        snap = mget_snapshot("om_img", CHAT, MEMBER, msg_type="image",
                             mentions=[bot_mention(APP_ID)])
        env.arm_mget([snap])

        def dl(args, cwd):
            import pathlib
            d = pathlib.Path(cwd) / "lark-im-resources"
            d.mkdir(parents=True, exist_ok=True)
            (d / "a.png").write_bytes(b"PNG")
            return ok_envelope({"messages": [snap]})

        env.runner.on(lambda a: "--download-resources" in a, dl)
        env.recv_event(message_id="om_img", sender_id=MEMBER, message_type="image")
        rows = env.deliveries()
        assert rows, "白名单成员的媒体消息应直投"
        p = json.loads(rows[-1]["payload_json"])
        assert p["media_paths"]                      # 确认真走了物化路径
        assert p["approved_by"] == "allowlist"       # 不是 None
        assert p["sender_is_owner"] is False


class TestStore:
    def test_add_then_allowed_without_restart(self, env):
        """**加完立刻生效**:同一进程内先判后加再判 —— 若实现缓存了名单(启动时读一次),
        第二次断言会红。这正是"owner 让 agent 加白名单,下一条消息就直投"的前提。"""
        assert senderallow.is_allowed(CHAT, MEMBER) is False
        senderallow.add_entry(CHAT, MEMBER, note="张三")
        assert senderallow.is_allowed(CHAT, MEMBER) is True

    def test_external_write_takes_effect_without_restart(self, env):
        """**别的进程改盘也必须立刻生效**(`False → True → False`)。

        真实拓扑:判定发生在 **daemon** 进程,而 `allow add/remove` 由 **agent 的 bridgectl**
        进程执行 —— 两个进程。若 daemon 按路径缓存了名单,`allow remove` 之后**下一条消息
        仍会直投**(撤权失效,比加权失效更危险)。上面那条只走本进程 add,对这种缓存无判别力
        (codex 实证:按路径缓存的实现下,上面那条全绿)。"""
        assert senderallow.is_allowed(CHAT, MEMBER) is False
        _write_allowlist([{"chat_id": CHAT, "open_id": MEMBER}])      # 模拟另一进程写盘
        assert senderallow.is_allowed(CHAT, MEMBER) is True
        _write_allowlist([])                                          # 另一进程撤权
        assert senderallow.is_allowed(CHAT, MEMBER) is False

    def test_add_is_idempotent(self, env):
        senderallow.add_entry(CHAT, MEMBER)
        entries, added = senderallow.add_entry(CHAT, MEMBER)
        assert added is False and len(entries) == 1

    def test_remove_only_targets_exact_pair(self, env):
        """删除只命中该 (群,人) 对,不误删同群其他人 / 同人其他群。"""
        senderallow.add_entry(CHAT, MEMBER)
        senderallow.add_entry(CHAT, OTHER_MEMBER)
        senderallow.add_entry(OTHER_CHAT, MEMBER)
        _, removed = senderallow.remove_entry(CHAT, MEMBER)
        assert removed is True
        assert senderallow.is_allowed(CHAT, MEMBER) is False
        assert senderallow.is_allowed(CHAT, OTHER_MEMBER) is True
        assert senderallow.is_allowed(OTHER_CHAT, MEMBER) is True

    def test_add_rejects_missing_id(self, env):
        """空 id 在**写入侧**就拒 —— 别把通配条目落进文件等判定侧兜。"""
        for bad in ((CHAT, ""), ("", MEMBER), (None, MEMBER), (CHAT, None)):
            with pytest.raises(ValueError):
                senderallow.add_entry(*bad)

    def test_note_is_cosmetic_only(self, env):
        """note 只给人看,不参与判定(改 note 不影响放行)。"""
        senderallow.add_entry(CHAT, MEMBER, note="张三")
        assert senderallow.is_allowed(CHAT, MEMBER) is True
        entries = senderallow.load_entries()
        assert entries[0]["note"] == "张三"

    def test_cli_add_list_remove_round_trip(self, env, monkeypatch, capsys):
        """**经真实 `main()` 走一遍 CLI**(codex:整个 `allow` 子命令此前零覆盖 ——
        删掉子命令注册、把 add/remove 接反、或参数分发写错,原有测试全绿)。
        逐步断言 argparse 注册 + action 分发 + 参数传递 + 真实落盘。"""
        import importlib.util
        import pathlib as _pl
        import sys as _sys
        root = _pl.Path(__file__).resolve().parents[1]
        spec = importlib.util.spec_from_file_location(
            "bridgectl_cli_mod", root / "bin" / "bridgectl.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        def run(*argv):
            monkeypatch.setattr(_sys, "argv", ["bridgectl", *argv])
            with pytest.raises(SystemExit) as ei:
                mod.main()
            return json.loads(capsys.readouterr().out), ei.value.code

        res, code = run("allow", "add", "--chat-id", CHAT, "--open-id", MEMBER)
        assert code == 0 and res["added"] is True
        assert senderallow.is_allowed(CHAT, MEMBER) is True    # 真落盘,不只是返回值

        res, _ = run("allow", "list")
        assert [(e["chat_id"], e["open_id"]) for e in res["entries"]] == [(CHAT, MEMBER)]

        res, _ = run("allow", "remove", "--chat-id", CHAT, "--open-id", MEMBER)
        assert res["removed"] is True
        assert senderallow.is_allowed(CHAT, MEMBER) is False   # 撤权真的生效

        # 缺参数 → 非0 退出,且**不落任何条目**(别把半个通配条目写进文件)
        res, code = run("allow", "add", "--chat-id", CHAT)
        assert code != 0 and res["ok"] is False
        assert senderallow.load_entries() == []

    def test_config_json_untouched(self, env):
        """白名单**绝不写进 config.json** —— 那里是 bootstrap 钉死的指纹,
        且是 FingerprintGate 的比对基准,碰坏会让出站停摆。"""
        before = paths.config_path().read_text(encoding="utf-8") \
            if paths.config_path().exists() else None
        senderallow.add_entry(CHAT, MEMBER)
        after = paths.config_path().read_text(encoding="utf-8") \
            if paths.config_path().exists() else None
        assert after == before
        assert paths.allowlist_path().exists()
