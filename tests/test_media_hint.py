"""非纯文本消息 → 给 agent 自取附件的句柄(fetch_hint / media_keys)。

**背景(真机定位,2026-07-29)**:飞书按**发送方式**分 msg_type —— 图片单发=`image`
(走 materialize 自动下载),**图+文字=`post`**(`MEDIA_MSG_TYPES` 不含 post → 从不下载)。
真机 `om_x100b6998f9dddca8c0b0accad8ac994`:lark-cli **已把 image_key 交给我们**
(顶层 content 里 `![Image](img_v3_…)`),但没人告诉 agent 那串 key 可取 → agent 当占位符跳过。
本模块钉住"告诉它"这件事:**post 零下载**、只给可直接跑的命令;`image`/`file` 仍照旧自动下载
(放 `media_paths`),故 hint 措辞必须先指向 `media_paths`、不能谎称桥从不下载。
"""
import json

from tests.conftest import APP_ID, CHAT, OWNER, PROFILE
from tests.helpers import bot_mention, mget_snapshot, raw_body_snapshot
from lib import constants

# 真机实测的 image_key(含连字符、末位是 g —— 正则别把它切断)
REAL_KEY = "img_v3_02143_6ea09006-e575-47a2-89e7-4a683ace737g"
# 真机实测的完整渲染正文(顶层 content 原样)
REAL_CONTENT = (
    "![Image](%s)\n"
    "我想在这里加一个链接，内容大概是修改agent的名字或者图标，然后点击以后跳转到 "
    "[https://open.feishu.cn/app](https://open.feishu.cn/app)\n"
    "（你能看见我发的图片吗）\n@cc" % REAL_KEY
)


def _deliver(env, *, msg_type="post", text="hi", content=None, message_id="om_1"):
    """走真实入站管线投递一条 owner 消息 → 返回 payload dict。"""
    env.make_binding(status="active")
    env.arm_mget([mget_snapshot(message_id, CHAT, OWNER, msg_type=msg_type, text=text,
                                content=content, mentions=[bot_mention(APP_ID)])])
    env.recv_event(message_id=message_id, message_type=msg_type)
    rows = env.deliveries()
    assert rows, "消息未投递(先确认前置门:绑定/mention/类型)"
    return json.loads(rows[-1]["payload_json"])


# --------------------------------------------------------------- 判定:msg_type 而非扫描
class TestHintGating:
    def test_plain_text_gets_no_hint(self, env):
        """纯文本零噪音:既无 fetch_hint 也无 media_keys。"""
        p = _deliver(env, msg_type="text", text="就是普通一句话")
        assert "fetch_hint" not in p and "media_keys" not in p

    def test_plain_text_containing_keylike_string_still_no_hint(self, env):
        """**判定看 msg_type,不看正文**(codex plan r1 M2)。

        若实现改成「扫到 key 才加」,这条会红 —— 用户粘一段含 img_v3_… 的代码进纯文本,
        不该被当成有附件。反向那半(image/file 正文不含 key 仍要有 hint)见下面两条。"""
        p = _deliver(env, msg_type="text",
                     text="日志里有 %s 这串,你看下" % REAL_KEY)
        assert "fetch_hint" not in p and "media_keys" not in p

    # 注:`image` 类型「正文无 key 但仍要有 hint」由**真投递路径**上的
    # test_waiting_binding.py::test_owner_media_materializes_on_activation 钉住
    # (那里断言 media_keys==[] 且 hint 指向 media_paths)—— 不在此处用 helper 重测一遍。

    def test_real_file_message_gets_hint(self, env):
        """**真 `file` 类型消息**走完整物化+投递路径,也必须带 hint 字段。

        codex impl r3 实证:此前 `file` 只在 helper 层测过 `extract_text`,**没有一条真投递**
        用到它 → 把闸门收窄成 `("post","image")` 时 **578 全绿**。这条补上那个洞。"""
        env.make_binding(status="active")
        snap = mget_snapshot("om_f1", CHAT, OWNER, msg_type="file",
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
        assert p["media_paths"]                       # 桥已自动下好
        assert p["media_keys"] == []                  # 渲染正文 `(文件) a.pdf` 认不出 key
        assert "media_paths" in p["fetch_hint"]       # hint 先指向已下载的那份


# --------------------------------------------------------------- key 提取
class TestKeyExtraction:
    def test_post_image_key_extracted(self, env):
        p = _deliver(env, content="![Image](%s) 看这个" % REAL_KEY)
        assert p["media_keys"] == [{"key": REAL_KEY, "type": "image"}]

    def test_post_file_key_extracted_as_file_type(self, env):
        key = "file_v2_abc-123_XY"
        p = _deliver(env, content="附件 [a.pdf](%s)" % key)
        assert p["media_keys"] == [{"key": key, "type": "file"}]

    def test_unversioned_keys_extracted(self, env):
        """飞书也有不带版本号的 key(codex plan r1 M1:原正则要求 v\\d+ 会漏)。
        **漏判会重现原 bug,误判只是一条取不到的提示** → 不对称,往宽取。"""
        p = _deliver(env, content="![](img_plainkey123) 和 [x](file_plainkey456)")
        assert p["media_keys"] == [
            {"key": "img_plainkey123", "type": "image"},
            {"key": "file_plainkey456", "type": "file"},
        ]

    def test_multiple_keys_all_extracted_ordered_deduped(self, env):
        """多 key 全提取、保序、去重(防「只取第一个」)。"""
        k2 = "img_v3_second_key-0001a"
        p = _deliver(env, content="![](%s) ![](%s) 又一次 ![](%s)" % (REAL_KEY, k2, REAL_KEY))
        assert [d["key"] for d in p["media_keys"]] == [REAL_KEY, k2]

    def test_stripped_mention_does_not_synthesize_key(self, env):
        """剥 mention **不得拼出正文里不存在的 key**(codex impl r1 Low1 实证)。

        `extract_text` 把指向本 bot 的 `@name` 换成**空串** → 若在剥完之后才扫,
        `img@TestBot_v12_fake` 会被拼成 `img_v12_fake`(一个原文里没有的 key)。
        故扫描必须用**未剥 mention 的原始正文**。"""
        p = _deliver(env, content='img@TestBot_v12_fake 看这个')
        assert p["media_keys"] == []

    def test_legacy_body_content_shape_does_not_synthesize_key(self, env):
        """legacy `body.content` 形状(顶层 `content` 缺失)**也不能**拼出假 key
        (codex impl r2:此前回退去扫剥过 mention 的 text,假 key 在这条路上复活)。
        代价是这种形状下 keys 为空 —— 可接受,hint 仍给 mget 兜底。"""
        env.make_binding(status="active")
        # text 必须**含能被剥 mention 拼出假 key 的内容**,否则两种实现都得空 keys、
        # 这条测试对该变异零判别力(fake-green —— 我第一版就栽在默认 text="hi")。
        env.arm_mget([raw_body_snapshot("om_legacy", CHAT, OWNER, msg_type="post",
                                        text="img@_user_1_v12_fake 看这个",
                                        mentions=[bot_mention(APP_ID)])])
        env.recv_event(message_id="om_legacy", message_type="post")
        p = json.loads(env.deliveries()[-1]["payload_json"])
        assert p["media_keys"] == []            # 不猜、不编造(回退扫 text 会得 img_v12_fake)
        assert "+messages-mget" in p["fetch_hint"]   # 但仍给兜底

    def test_keyless_post_fabricates_nothing(self, env):
        """post 可以是富文本但无附件 → hint 在(中性),但绝不编造 key。"""
        p = _deliver(env, text="只是加粗的一段话,没有附件")
        assert p["media_keys"] == []
        assert "img_" not in p["fetch_hint"] and "file_" not in p["fetch_hint"]


# --------------------------------------------------------------- hint 命令语义
class TestHintCommand:
    def test_profile_comes_from_config_not_hardcoded(self, env):
        """**profile 必须取自 cfg**(codex impl r1 M3):fixture 的 PROFILE 恰是 `"main"`,
        所以把 `--profile main` 硬编码进模板**全套仍绿**。改成一个明显不同的 profile 再验。

        为什么这条重要:桥在每条 argv 末尾追加 `cfg["profile"]`;hint 里 profile 不对 =
        agent 用**另一个身份的 bot** 去下载,而那个 bot 可能不在群里 → 取不到。"""
        env.cfg["profile"] = "other-profile-xyz"
        p = _deliver(env, content="![](%s)" % REAL_KEY)
        assert "--profile other-profile-xyz" in p["fetch_hint"]
        assert "--profile main" not in p["fetch_hint"]

    def test_per_key_output_paths_are_distinct(self, env):
        """多 key 的 `--output` 必须各不相同(codex impl r1 M3):若所有 key 共用同一
        `--output ./x`,agent 顺序跑完只剩最后一张,前面的被覆盖 —— 而只断言"有两条命令"是绿的。"""
        k2 = "img_v3_second_key-0001a"
        p = _deliver(env, content="![](%s) ![](%s)" % (REAL_KEY, k2))
        h = p["fetch_hint"]
        assert "--output ./feishu-media-%s " % REAL_KEY in h + " "
        assert "--output ./feishu-media-%s " % k2 in h + " "

    def test_file_key_command_uses_file_type(self, env):
        """`file_*` 的命令必须是 `--type file`(codex impl r1 M3:原先只断言 media_keys 的
        type 字段,模板里恒发 `--type image` 也全绿)。"""
        key = "file_v2_abc-1"
        p = _deliver(env, content="附件 [a.pdf](%s)" % key)
        assert "--file-key %s --type file" % key in p["fetch_hint"]
        assert "--type image" not in p["fetch_hint"]

    def test_keyed_command_is_complete_and_runnable(self, env):
        """**钉死整条命令**(codex plan r1 M3 + r2 M1 + r3):命令名/‑‑file-key/‑‑type/
        ‑‑as bot/相对 ‑‑output/‑‑profile 任一写错或缺失都必须变红。

        `--profile` 是**本机看不出来的坑**:桥显式钉住 cfg["profile"](runner.py 在每条 argv
        末尾追加);hint 不带则 agent 用默认 active profile —— 本机恰好一致故肉眼无感,换机/换
        profile 就是用错身份的 bot 去下载,而那个 bot 可能不在群里 → 取不到。
        **本条断言投递出来的 payload(非直接调 helper)**,故 `_enqueue_in_tx` 漏传 profile 也会红。"""
        p = _deliver(env, content="![Image](%s)" % REAL_KEY, message_id="om_real")
        h = p["fetch_hint"]
        assert "lark-cli im +messages-resources-download" in h   # 命令名本身
        assert "--message-id om_real" in h                        # 动态 id,非硬编码
        assert "--file-key %s" % REAL_KEY in h
        assert "--type image" in h
        assert "--as bot" in h
        assert "--profile %s" % PROFILE in h                      # 与桥同 profile
        assert "--output ./" in h                                 # 相对路径(拒绝绝对路径)
        assert "/tmp/" not in h and "--output /" not in h
        # lark-cli 会按 Content-Type 自动补扩展名 → 落盘路径 ≠ 传入路径,必须提示读 saved_path
        assert "saved_path" in h

    def test_keyless_fallback_command_is_complete(self, env):
        """兜底命令同样要能直接跑:**复数 `--message-ids`**(单数 flag 不存在,照着跑必失败)。

        **动态值同样要钉**(codex impl r3:此前只有"有 key"那条分支钉了 id/profile,兜底分支
        把 id 硬编码成 `om_nokey`、或 profile 硬编码成 `"main"`,**578 全绿** —— codex 实跑
        两个变异各自证明)。故这里用非默认 profile + 两个不同 id。"""
        env.cfg["profile"] = "fallback-profile-zzz"
        env.make_binding(status="active")
        env.arm_mget([mget_snapshot(mid, CHAT, OWNER, msg_type="post", text="富文本无附件",
                                    mentions=[bot_mention(APP_ID)])
                      for mid in ("om_nokey1", "om_nokey2")])
        hints = {}
        for mid in ("om_nokey1", "om_nokey2"):
            env.recv_event(message_id=mid, message_type="post")
            hints[mid] = json.loads(env.deliveries()[-1]["payload_json"])["fetch_hint"]
        h = hints["om_nokey1"]
        assert "lark-cli im +messages-mget" in h
        assert "--message-ids om_nokey1" in h
        assert "--message-id " not in h.replace("--message-ids ", "")  # 不是单数形
        assert "--no-reactions" in h
        assert "--as bot" in h
        assert "--profile fallback-profile-zzz" in h and "--profile main" not in h
        # id 随消息变(防硬编码成某个固定 id)
        assert "--message-ids om_nokey2" in hints["om_nokey2"]
        assert "om_nokey1" not in hints["om_nokey2"]

    def test_one_command_per_key(self, env):
        """多 key → 每个各一条命令(防「多 key 只给一条、其余无从取」)。"""
        k2 = "img_v3_second_key-0001a"
        p = _deliver(env, content="![](%s) ![](%s)" % (REAL_KEY, k2))
        h = p["fetch_hint"]
        assert h.count("+messages-resources-download") == 2
        assert "--file-key %s" % REAL_KEY in h and "--file-key %s" % k2 in h

    def test_message_id_is_dynamic_not_hardcoded(self, env):
        """**同一绑定投两条不同 id 的消息**,断言各自 hint 里的 id 随之变 —— 单条 fixture
        的等值断言对「把 id 硬编码成 fixture 那个值」是绿的(codex plan r1 Low)。"""
        env.make_binding(status="active")
        # 一次装两条快照:`arm_mget` 是**追加**匹配器,先注册的先命中 → 循环里重复 arm 无效
        # (第二条会拿到第一条的快照而卡在 resolving)。
        env.arm_mget([mget_snapshot(mid, CHAT, OWNER, msg_type="post",
                                    content="![](%s)" % REAL_KEY,
                                    mentions=[bot_mention(APP_ID)])
                      for mid in ("om_first", "om_second")])
        payloads = {}
        for mid in ("om_first", "om_second"):
            env.recv_event(message_id=mid, message_type="post")
            payloads[mid] = json.loads(env.deliveries()[-1]["payload_json"])
        assert "--message-id om_first" in payloads["om_first"]["fetch_hint"]
        assert "--message-id om_second" in payloads["om_second"]["fetch_hint"]
        assert "om_first" not in payloads["om_second"]["fetch_hint"]


# --------------------------------------------------------------- 真机回归
class TestRealSnapshotRegression:
    def test_real_production_message_yields_key_and_verbatim_text(self, env):
        """用真机那条 content 做 fixture:既钉 key 提取,也钉 **`text` 逐字未被污染**
        (精确等值,非 `in`)—— hint 是独立字段,绝不能拼进用户原话。"""
        p = _deliver(env, content=REAL_CONTENT, message_id="om_prod")
        assert p["media_keys"] == [{"key": REAL_KEY, "type": "image"}]
        # 逐字保留(此 fixture 的 mention 名不是本 bot,故 extract_text 不剥任何东西)
        assert p["text"] == REAL_CONTENT.strip()
        assert "lark-cli" not in p["text"]      # 提示没混进原话


# --------------------------------------------------------------- 正则单元
class TestMediaKeyRegex:
    def test_does_not_match_ordinary_prose(self):
        """不误伤日常词句。注:**故意不测 `profile_v2_x` 这类"内嵌"串** —— 它里面确实含
        `file_v2_x`,正则会命中。那是**放宽取向的已知代价**(误判只多一条取不到的提示,
        漏判则重现原 bug),不是缺陷。"""
        for s in ("image_of_cat", "a file here", "imgur.com/x", "filename"):
            assert constants.MEDIA_KEY_RE.search(s) is None, s
