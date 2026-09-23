---
name: notify
description: 遇到"需要 owner 决策或授权才能继续"的 blocker 时,主动给「本 session 已绑定的 Slack 会话」发一条 @owner 的通知,把待决的决策/待授权的操作发过去,凸显阻塞、请人来拍板。当你(agent)卡在需要用户批准/选择/提供密钥或权限/确认高风险操作、而用户可能不在终端前时使用。也可在长任务的重要里程碑/转向时用它主动知会。无绑定会话或发送失败会如实返回(不会假装已通知)。仅 slack-bridge 已 bind 的 session 有效。
---

# slack-bridge notify:给绑定会话发 @owner 通知

用途:**你**(agent)在本 session 里撞到需要 owner 决策/授权的 blocker 时,主动把它推送到本 session 绑定的 Slack 会话,并自动 @owner,让人及时来拍板——而不是干等在终端。

前提:本 session 已经用 `/slack-bridge:bridge bind` 绑定了一个会话。**没绑定不会发**,会返回 `not-bound`,你如实告诉用户即可。

## 怎么调用(消息经 stdin,别塞进命令行)

分两步,**别把消息正文直接写进 shell 命令**(避免 `$()`、反引号、引号被 shell 展开或截断):

1. 用 **Write 工具**把通知正文逐字写入一个临时文件,例如 `/tmp/slack-notify.md`。正文写你要让用户看到的原话(可中文、可多行)。**正文按标准 markdown 渲染**(Block Kit `markdown` 块):标题/粗体/列表/代码块像平时那样写即可。**不要**写 Slack 的 mrkdwn 特殊标记(`<!channel>` 之类会被拒;`<@U…>` 用户 mention 可以但通常不需要 —— @owner 是系统加的)。正文 ≤ 12000 字符。
2. 运行(每条 Bash 命令内联完整路径;`< 文件` 把正文喂给 stdin):
   ```bash
   python3 "${CLAUDE_SKILL_DIR}/../../bin/notifyctl.py" < /tmp/slack-notify.md
   ```

系统会以 bot 身份发出一条消息:第一块是 `@owner`(独立 section),第二块是你的正文(markdown 块);通知栏回退文本也带 owner mention。**你不需要、也不该自己写 `<@…>` / `<!…>`** —— 前缀是系统加的;正文里若出现字面 `<!` 会被整条拒绝(防误触发 @channel/@here)。

> 为什么用临时文件 + `< 文件` 而不是 heredoc / `echo`:正文经 stdin 逐字传入,不过 shell 解析,`$()`、反引号、引号、换行全部原样进消息,也不会有 heredoc 结束符碰撞。

## 读返回的 JSON(据此如实转述给用户)

输出是一段 JSON,`sent` 是主信号:

- `{"ok":true,"sent":true,"message_id":"C…:ts","chat_id":"C…"}` → **已通知**(@到 owner 了)。告诉用户"已在 Slack 里 @你了,待你决策",然后按需要停下等回复。
- `{"ok":false,"sent":false,"reason":…}` → **确定没发出去**,如实告诉用户原因,别假装已通知:
  - `not-bound` / `empty-message` → 本 session 没绑会话 / 正文是空的(exit 0,属正常前置,不是错误)。
  - `credentials-unverified`(exit 3)→ `tokens.json` 刚换过、daemon 还没重验(或 daemon 没在跑)。让用户等 daemon 下一 tick(几秒)或先 `bridgectl ensure-daemon` / `status`,再重发。
  - `gate-degraded`(exit 3)→ 出站身份门非 ok(`status.outbound_gate`):token 被撤销 / 身份不符 / 网络;转述 `detail`。
  - `cooldown`(exit 4,`retryable:true`,带 `cooldown_until` 毫秒墙钟)→ 此前被 Slack 429,方法级冷却未到期,**请求没发出**;到期后重发即可。
  - `ratelimited`(exit 4,`retryable:true`,`retry_after` 秒)→ 这次被 429,已发布冷却;稍后重发。
  - `send-failed`(exit 4;`error` 是 Slack 错误码,`retryable` 说明可否重试)→ 如 `channel_not_found`/`not_in_channel`(bot 不在会话,让用户 `/invite`)、`invalid_auth`(凭据问题)、`dns`(本机网络)。
  - `invalid-mention` / `invalid-input` / `message-too-long` / `chat-not-allowed` / `invalid-*` / `config` / `instance-unresolved` / `session-unresolved` / `schema-mismatch`(exit 3)→ 各自含义见 `reason`/`detail`,转述给用户。
- `{"ok":false,"sent":"unknown","reason":"send-unknown"}`(exit 5)→ **不确定发没发出去**(超时/5xx/传输错)。**别急着重发**——让用户看一眼会话里是否已出现该通知,避免重复 @打扰。

## 什么时候用它(判断)

- 需要用户批准/授权才能继续的高风险或不可逆操作(删数据、改配置、外发、花钱…)。
- 需要用户做方向选择、提供密钥/权限/缺失信息,而你无法自行决定。
- 长任务跑到重要里程碑或需要转向,值得主动知会一声。

不该用它:把最终答案再发一遍到会话里(绑定 session 的每轮输出**已自动**转发,别重复);或用它冒充桥的正常回复。它只用于**主动推送 blocker/知会**。

## 边界与安全

- 只对**本 session 用三元组(session_id + 进程实例 + active)精确命中的绑定**发;不会误发到别的 session 或旧绑定。
- 发前尊重与普通桥出站同款的门:`chat_allowlist`、出站身份门(`outbound_gate=="ok"`)、**凭据版本**(`tokens.json` 当前版本必须等于 daemon 验证过的 `outbound_gate_tokens_version`)、方法级冷却。门不通=不发、如实返回。
- 正文进会话会被成员看到:**不要把密钥/token/内网凭证写进通知正文**。
- 这是 daemon 唯一发送者之外的**受门控显式直发例外**之一(另一个是 StopFailure API 错误告警 hook,底座同为 `lib/notify.py`);别用任何其它方式往 Slack 发消息。
