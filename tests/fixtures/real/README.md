# 真机样本(脱敏)

来源:2026-09-24 在 一个真实 Slack workspace用 slack-bridge app 真机采集的 Socket Mode `events_api` 信封
`payload`(即 `slack_events.payload_json`),已把 team/channel/user/app/event id 替换为稳定占位符
(`T0REAL / C0REAL / U0OWNER / U0BOT / A0APP / Ev0REALn`),`ts` 保留原值。

| 文件 | 说明 |
|---|---|
| `channel_owner_mention__message.json` | owner 在频道里 `@bot …`:`message` 事件(**先到**,`_order_seen=1`),带 `blocks`(rich_text 含 `user` 元素)与 `channel_type` |
| `channel_owner_mention__app_mention.json` | 同一条消息的 `app_mention` 事件(**后到**,`_order_seen=3`,与 `message` 同 `ts`,**无** `channel_type`;`blocks` 同形) |
| `channel_owner_no_mention__message.json` | owner 不 @ 的普通消息:只有 `message` 事件,daemon 判为 `ignored_not_mentioned` |
| `channel_owner_file_share__message.json` | owner 带附件 `@bot`:`message` 事件 `subtype=file_share`(先到),`files[]` 带 `url_private_download`/`mimetype`/`size`,另有 `upload`/`display_as_bot` |
| `dm_owner_message.json` | owner 在自己 DM 里发的普通消息(`channel_type=im`,无 @):直接投递 |
| `dm_bot_self_echo__message.json` | bot 自己在 DM 里发的消息回流:`message` 事件带 `bot_id`/`app_id`,`user`=bot user id → `event_dropped_self` |
| `channel_owner_file_share__app_mention.json` | 同一条的 `app_mention`(后到,`dup_message` 丢弃):**同样带 `files[]`**,无 `subtype`/`channel_type` |

真机事实:频道 @bot 恰好双投(`message` + `app_mention`,同 `channel:ts`,不同 `event_id`),本次 `message` 先于
`app_mention` 到达;两者 `text`/`blocks` 相同(纯文本消息上没有 files/attachments 差异)。daemon 计数
`inbox_dup_message=1`,只投递一次。

带附件的双投同样是 `message`(subtype `file_share`)先到、`app_mention` 后到;两者都带 `files[]`,因此 §契约里的
「`app_mention` 先到且无 files → 用后到的 `message` 升级快照」路径在本次未触发(计数 `inbox_snapshot_upgraded=0`,
`inbox_dup_message=2`)。文件下载经 `download_worker` 落到 `media/<binding_id>/`,投递 payload 带 `local_path`。

DM 事实:owner DM 里 bot 的每条消息(bound 通知、session 回复)都会回流为 `message` 事件,同时带 `bot_id`、`app_id`
与 `user`(= bot user id),`is_self_event` 的三组比较任一命中即丢弃;owner 的 DM 消息不带 `bot_id`/`app_id`。
