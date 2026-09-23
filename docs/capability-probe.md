# Slack 能力探测记录(capability probe)

> 由 `python3 scripts/capability_probe.py --chat-id <测试频道> [--write-config]`(或 `bridgectl probe`)产出。
> **不需要 daemon 在跑**;需要 tokens.json(0600)或 env `SLACK_BOT_TOKEN`。probe 是验证步骤本身,
> 不受 `outbound_gate` 版本门约束,但**遵守方法级冷却**(`daemon_state.cooldown:<method>`)。
> 探测会在测试频道发两条消息(顶层 + 线程)并随即 `chat.delete`;失败时可能残留,需手动清理。

## 结论摘要(UNCONFIRMED = 尚未在真机跑)

| 能力 | 结果 | 依据 |
|---|---|---|
| `auth.test` 身份与 config 一致(team_id / user_id==bot_user_id / bot_id) | UNCONFIRMED | `identity_ok` |
| `chat.postMessage` 接受 `markdown_text` 参数 | UNCONFIRMED | `markdown_text_ok`;被 `invalid_arguments` 拒 → `markdown_mode=text` |
| 顶层消息的 `metadata` 经 `conversations.history(include_all_metadata=true)` 可读回 | UNCONFIRMED | `metadata_history` |
| 线程内消息的 `metadata` 经 `conversations.replies(include_all_metadata=true)` 可读回 | UNCONFIRMED | `metadata_replies` |
| `chat.delete` 清理成功 | UNCONFIRMED | `cleanup_ok` |
| 结果对应的凭据版本 | UNCONFIRMED | `tokens_version`(写入 `daemon_state.verify_capability_tokens_version`) |

`verify_capability`:`metadata_history ∧ metadata_replies` → `ok`;否则 `degraded:<首个失败项>`。
**daemon 只在 `verify_capability_tokens_version == 当前凭据版本` 时采信 `ok`**;换 tokens.json 后自动回到
`unverified`(自动重发关闭),需重跑 probe。

## 运行记录

| 日期 | 运行者 | workspace | tokens_version | 原始 JSON |
|---|---|---|---|---|
| (待填) | | | | |

## 待确认的真机事实(填完删掉 UNCONFIRMED)

- [ ] `markdown_text` 是否对 bot token 可用(2025 起公开;部分 workspace 可能未开放)。
- [ ] `metadata.event_type` 是否要求预先在 app 里声明 schema(目前按「任意自定义 event_type + event_payload」发送)。
- [ ] `conversations.history` 的 `include_all_metadata` 对 `im`(D…)是否同样返回 metadata。
- [ ] `conversations.replies` 返回的父消息是否也带 metadata(核验只看 `ts==sending 的那条`,父消息不影响)。
- [ ] 429 时 `Retry-After` 头的实际取值范围(影响 `cooldown:<method>` 时长)。
- [ ] 频道 `@bot` 是否恰好双投(`message` + `app_mention`,同 `ts`),files/blocks 是否只在 `message` 上
      → 保存脱敏样本到 `tests/fixtures/real/`。
- [ ] DM 内 bot 自身消息是否回流为 `message`(带 `bot_id`/`app_id`)→ `is_self_event` 三组比较是否够。
