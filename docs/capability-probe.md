# Slack 能力探测记录(capability probe)

> 由 `python3 scripts/capability_probe.py --chat-id <测试频道> [--write-config]`(或 `bridgectl probe`)产出。
> **不需要 daemon 在跑**;需要 tokens.json(0600)或 env `SLACK_BOT_TOKEN`。probe 是验证步骤本身,
> 不受 `outbound_gate` 版本门约束,但**遵守方法级冷却**(`daemon_state.cooldown:<method>`)。
> 探测会在测试频道发两条消息(顶层 + 线程)并随即 `chat.delete`;失败时可能残留,需手动清理。

## 结论摘要(2026-09-24 真机确证;下文 id 均为占位符)

| 能力 | 结果 | 依据 |
|---|---|---|
| `auth.test` 身份与 config 一致(team_id / user_id==bot_user_id / bot_id) | ✅ CONFIRMED | `identity_ok` |
| `chat.postMessage` 接受 `markdown_text` 参数 | ✅ CONFIRMED(bot token 可用) | `markdown_text_ok`;被 `invalid_arguments` 拒 → `markdown_mode=text` |
| 顶层消息的 `metadata` 经 `conversations.history(include_all_metadata=true)` 可读回 | ✅ CONFIRMED | `metadata_history` |
| 线程内消息的 `metadata` 经 `conversations.replies(include_all_metadata=true)` 可读回 | ✅ CONFIRMED(**必须表单编码**,见下) | `metadata_replies` |
| `chat.delete` 清理成功 | ✅ CONFIRMED | `cleanup_ok` |
| 结果对应的凭据版本 | ✅ 已写入 | `tokens_version`(写入 `daemon_state.verify_capability_tokens_version`) |

`verify_capability`:`metadata_history ∧ metadata_replies` → `ok`;否则 `degraded:<首个失败项>`。
**daemon 只在 `verify_capability_tokens_version == 当前凭据版本` 时采信 `ok`**;换 tokens.json 后自动回到
`unverified`(自动重发关闭),需重跑 probe。

## 运行记录

| 日期 | 运行者 | workspace | tokens_version | 原始 JSON |
|---|---|---|---|---|
| 2026-09-24 | Claude(用户浏览器建 app)| 真实 workspace(id 已脱敏),测试频道 C0EXAMPLE | `<mtime_ns>:<sha256[:16]>` | `{"identity_ok": true, "markdown_text_ok": true, "metadata_history": true, "metadata_replies": true, "cleanup_ok": true, "tokens_version": "<mtime_ns>:<sha256[:16]>", "markdown_mode": "markdown_text", "verify_capability": "ok", "chat_id": "C0EXAMPLE", "probe_id": "510b39878e40", "complete": true, "errors": [], "written": {"markdown_mode": "markdown_text", "verify_capability": "ok", "verify_capability_tokens_version": "<mtime_ns>:<sha256[:16]>"}}` |
| 2026-09-24(修复前) | 同上 | 同上 | 同上 | `metadata_replies=null, errors=["conversations.replies:failed:invalid_arguments"], complete=false` —— 见下「真机发现」 |

## 真机发现(2026-09-24)

- **Slack 读类方法不接受 `application/json` 请求体**:`conversations.replies` 对 JSON body 返回
  `invalid_arguments`,`response_metadata.messages = ["[ERROR] missing required field: channel", "... ts"]`;
  同一参数用 `application/x-www-form-urlencoded` POST 或查询串 GET 均正常。`conversations.history` 两种都收
  (所以只有线程核验受影响)。修复:`SlackClient.call` 只对 `constants.JSON_BODY_METHODS`(chat.postMessage /
  chat.update / chat.delete / reactions.* / conversations.open / views.*)发 JSON,其余方法一律表单编码
  (bool → `true`/`false`,嵌套值 → JSON 字符串)。回归测试:`tests/test_slackapi.py::test_request_shape_read_methods_form_encoded`,
  `tests/test_capability_probe.py` 的假服务端也按真机行为对读类方法的 JSON body 返回 `invalid_arguments`。
- probe 自己发的两条消息与两次 `chat.delete` 会经 Socket Mode 回流到 daemon,被正确丢弃
  (`event_dropped_self` / `event_dropped_subtype` 各 +3),说明自身回流判定与 subtype 过滤在真机成立。
- 免费版 workspace 有 **10 个已安装 app 上限**(含第三方);到 10 个就不能再建/装新 app,需先卸一个。

## 待确认的真机事实(填完删掉 UNCONFIRMED)

- [x] `markdown_text` 对 bot token 可用(2026-09-24 真机)。
- [x] `metadata.event_type` 不需要预先声明 schema:自定义 `slack_bridge` + `event_payload.job_id` 直接可发、可读回(2026-09-24)。
- [ ] `conversations.history` 的 `include_all_metadata` 对 `im`(D…)是否同样返回 metadata(DM 绑定的回复均一次 `sent`,未触发核验路径,未单独验证)。
- [x] `conversations.replies` 返回父消息 + 回复,回复的 metadata 可读回(2026-09-24;父消息是否带 metadata 未单独核对,核验不依赖)。
- [ ] 429 时 `Retry-After` 头的实际取值范围(影响 `cooldown:<method>` 时长)。
- [x] 频道 `@bot` 恰好双投(`message` 先、`app_mention` 后,同 `ts`,不同 `event_id`);带附件时**两者都带 `files[]`**;
      样本已存 `tests/fixtures/real/`(2026-09-24)。
- [x] DM 内 bot 自身消息回流为 `message`,带 `bot_id`、`app_id` 且 `user`=bot user id → 三组比较足够(`event_dropped_self`);owner DM 消息无需 @ 即投递(2026-09-24)。
