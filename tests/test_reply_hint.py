"""回复/引用消息 → 给 agent 自取被引用内容的句柄(reply_to / reply_hint)。

飞书把「回复/引用」放在快照**顶层 `reply_to`**,被引用的内容**一个字都不在正文里**。
媒体 hint 的闸门是 `msg_type != "text"`,而带引用的消息 `msg_type` 仍是 `text`
→ 套用那个闸门会整个漏掉,故两者闸门相互独立、字段也分开(可同时出现)。
"""
import json

from tests.conftest import APP_ID, CHAT, MEMBER, OWNER, PROFILE
from tests.helpers import bot_mention, mget_snapshot

# 两个不同的引用目标 —— 全套测试都用一对而非一个:单值 fixture 对「把 reply_to
# 写死成常量」这一变异零判别力(实测该变异下 14 条曾全绿)。
Q1 = "om_x100b687f25f2c4a0c07ddaca0cf2c7d"   # 真机:被引用的 merge_forward
Q2 = "om_x100b687f18636ca8df9e99570781d0f"   # 真机:另一条被引用消息
IMG_KEY = "img_v3_02143_6ea09006-e575-47a2-89e7-4a683ace737g"


def _deliver(env, *, reply_to=None, msg_type="text", text="hi", content=None,
             message_id="om_1"):
    """走真实入站管线投递一条 owner 消息 → 返回 payload dict。"""
    env.make_binding(status="active")
    env.arm_mget([mget_snapshot(message_id, CHAT, OWNER, msg_type=msg_type, text=text,
                                content=content, reply_to=reply_to,
                                mentions=[bot_mention(APP_ID)])])
    env.recv_event(message_id=message_id, message_type=msg_type)
    rows = env.deliveries()
    assert rows, "消息未投递(先确认前置门:绑定/mention/类型)"
    return json.loads(rows[-1]["payload_json"])


class TestReplyFields:
    def test_text_reply_carries_fields_keyed_to_its_own_target(self, env):
        """**核心回归 + 防硬编码**:两条消息各引用不同目标,断言各自拿到自己的那个。

        两件事一起钉:① 带引用的消息 `msg_type` 是 `text` —— 沿用 `msg_type != "text"`
        闸门则整条红(这正是此前整个漏掉的原因);② `reply_to` 与命令里的 id 都随消息变
        —— 把它写死成某个常量时,单一 fixture 的测试是全绿的(实测)。"""
        env.make_binding(status="active")
        # arm_mget 是**追加**匹配器、先注册先命中 → 必须一次装两条,循环里重复 arm 无效
        env.arm_mget([mget_snapshot(mid, CHAT, OWNER, msg_type="text", reply_to=q,
                                    mentions=[bot_mention(APP_ID)])
                      for mid, q in (("om_a", Q1), ("om_b", Q2))])
        got = {}
        for mid in ("om_a", "om_b"):
            env.recv_event(message_id=mid)
            got[mid] = json.loads(env.deliveries()[-1]["payload_json"])
        assert got["om_a"]["reply_to"] == Q1 and got["om_b"]["reply_to"] == Q2
        assert "--message-ids %s" % Q1 in got["om_a"]["reply_hint"]
        assert "--message-ids %s" % Q2 in got["om_b"]["reply_hint"]
        assert Q1 not in got["om_b"]["reply_hint"]      # 互不串台
        # 命令里是**被引用那条**的 id,不是当前消息的 id(写反了照样"有一条命令")
        assert "--message-ids om_a" not in got["om_a"]["reply_hint"]

    def test_non_reply_message_gets_no_fields(self, env):
        """不是回复 → 零噪音。真机形状下非回复消息**根本没有 `reply_to` 键**
        (不是 null),故本条同时杀死 `snap["reply_to"]` 直取写法。"""
        p = _deliver(env, reply_to=None, text="就是普通一句话")
        assert "reply_to" not in p and "reply_hint" not in p

    def test_hint_does_not_pollute_user_text(self, env):
        """`text` 是**用户原话**,hint 是独立字段 —— 拼进 `text` 会混淆「用户说了什么」
        与「系统提示」(自查变异实证:不加这条,追加进 text 时全绿)。"""
        p = _deliver(env, reply_to=Q1, text="看下这条回复里的 thread")
        assert p["text"] == "看下这条回复里的 thread"      # 精确等值,非 in
        assert "lark-cli" not in p["text"] and Q1 not in p["text"]

    def test_member_post_with_reply_carries_both_hints(self, env):
        """**member + post + 引用**三者同时 —— 一条覆盖两个洞:

        ① `_enqueue_in_tx` 的第二个调用点(approval.py 批准后入队);
        ② 「图 + 引用」两个 hint 并存、不共用一个键互相覆盖。
        实测:少了这条,`sender==owner or msg_type=="text"` 这个错误闸门是全绿的
        —— 因为 member 测试用 text、post 测试用 owner,组合从没被走过。"""
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot("om_m1", CHAT, MEMBER, msg_type="post", reply_to=Q2,
                                    content="![Image](%s) 看这个" % IMG_KEY,
                                    mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_id="om_m1", sender_id=MEMBER, message_type="post")
        row = env.pendings()[-1]
        assert env.approval.process_event({
            "type": "card.action.trigger", "event_id": "cb_reply1",
            "operator_id": OWNER, "message_id": "om_card_x", "chat_id": CHAT,
            "host": "im_message",
            "action_value": json.dumps({"pending_id": row["pending_id"],
                                        "nonce": row["nonce"], "act": "approve"}),
        }) == "applied"
        p = json.loads(env.deliveries()[-1]["payload_json"])
        assert p["reply_to"] == Q2
        assert "--message-ids %s" % Q2 in p["reply_hint"]
        assert IMG_KEY in p["fetch_hint"]                  # 媒体那条命令还在
        assert p["fetch_hint"] != p["reply_hint"]

    def test_materialized_media_path_carries_fields(self, env):
        """`_enqueue_in_tx` 的**第三个调用点**:`image`/`file` 先物化、再入队。
        实测 `and not media_paths` 这个错误条件在没有本条时全绿。
        (注:生产 507 条引用中未见 image/file+reply,但这是**调用链**覆盖、不是消息类型
        笛卡尔积 —— 三个调用点都得走一遍。)"""
        env.make_binding(status="active")
        snap = mget_snapshot("om_f1", CHAT, OWNER, msg_type="file", reply_to=Q1,
                             mentions=[bot_mention(APP_ID)])
        env.arm_mget([snap])

        def dl(args, cwd):
            import pathlib
            from tests.helpers import ok_envelope
            d = pathlib.Path(cwd) / "lark-im-resources"
            d.mkdir(parents=True, exist_ok=True)
            (d / "a.pdf").write_bytes(b"PDF")
            return ok_envelope({"messages": [snap]})

        env.runner.on(lambda a: "--download-resources" in a, dl)
        env.recv_event(message_id="om_f1", message_type="file")
        p = json.loads(env.deliveries()[-1]["payload_json"])
        assert p["media_paths"]                            # 确认真走了物化路径
        assert p["reply_to"] == Q1
        assert "--message-ids %s" % Q1 in p["reply_hint"]


class TestReplyHintCommand:
    def test_command_is_complete_and_runnable(self, env):
        """钉死整条命令:命令名 / **复数** `--message-ids`(单数 flag 不存在,照跑必失败)/
        `--as bot` / `--profile`。**profile 用非默认值**:fixture 的 PROFILE 恰是 `"main"`,
        硬编码 `--profile main` 会全绿 —— 桥在每条 argv 末尾追加 `cfg["profile"]`,
        hint 里不一致 = agent 用另一个身份的 bot 去下载,那 bot 可能不在群里。"""
        env.cfg["profile"] = "other-profile-xyz"
        p = _deliver(env, reply_to=Q1)
        h = p["reply_hint"]
        assert "lark-cli im +messages-mget --message-ids %s" % Q1 in h
        assert "--as bot" in h
        assert "--profile other-profile-xyz" in h and "--profile main" not in h

    def test_nested_resource_note_binds_download_id_to_quoted(self, env):
        """嵌套资源(merge_forward 里的图)必须用**被引用那条**的 id 下载。

        **断言要把 id 和"下载时"这句话绑在一起**:只查「hint 里出现过 Q1」是无效的
        —— 前面那条 mget 命令本来就含 Q1,把这句改成指向别的 id 照样全绿(codex 实测)。
        指错 id = agent 必然取不到资源,正是本功能要防的失败。"""
        p = _deliver(env, reply_to=Q1)
        assert "下载时 --message-id 用 %s" % Q1 in p["reply_hint"]
