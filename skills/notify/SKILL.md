---
name: notify
description: 遇到"需要 owner(群主)决策或授权才能继续"的 blocker 时,主动给「本 session 已绑定的飞书群」发一条 @群主 的通知(穿透免打扰),把待决的决策/待授权的操作发过去,凸显阻塞、请人来拍板。当你(agent)卡在需要用户批准/选择/提供密钥或权限/确认高风险操作、而用户可能不在终端前时使用。也可在长任务的重要里程碑/转向时用它主动知会。无绑定群或发送失败会如实返回(不会假装已通知)。仅群桥(feishu-bridge)已 bind 的 session 有效;DM 桥请用 feishu-chat。
---

# feishu-bridge notify:给绑定群发 @群主 通知

用途:**你**(agent)在本 session 里撞到需要 owner 决策/授权的 blocker 时,主动把它推送到本 session 绑定的飞书群,并自动 @群主(穿透免打扰),让人及时来拍板——而不是干等在终端。

前提:本 session 已经用 `/feishu-bridge:bridge bind` 绑定了一个飞书群。**没绑定不会发**,会返回 `not-bound`,你如实告诉用户即可(它不是必须先绑;只是没绑就没群可发)。

## 怎么调用(消息经 stdin,别塞进命令行)

分两步,**别把消息正文直接写进 shell 命令**(避免 `$()`、反引号、引号被 shell 展开或截断):

1. 用 **Write 工具**把通知正文逐字写入一个临时文件,例如 `/tmp/feishu-notify.txt`。正文写你要让用户看到的原话(可中文、可多行)。**正文支持 markdown**(v1.4.1 起)——标题/粗体/列表/代码块等常用语法飞书会渲染,像平时那样写即可,不必刻意退化成纯文本。(渲染范围以飞书自己支持的 markdown 子集为准,不保证完整 CommonMark/GFM;个别冷门语法可能按原文显示,不影响送达。)
2. 运行(每条 Bash 命令内联完整路径;`< 文件` 把正文喂给 stdin):
   ```bash
   python3 "${CLAUDE_SKILL_DIR}/../../bin/notifyctl.py" < /tmp/feishu-notify.txt
   ```

系统会自动在群里以 bot 身份发出 `@群主 <你的正文>`(@ 在第一行,正文另起一段)。**你不需要、也不该自己写 `<at ...>` 标签**——前缀是系统加的;正文里若出现字面 `<at` 会被拒绝(防误触发真实 @,markdown 会把它渲染成真 mention)。

> 为什么用临时文件 + `< 文件` 而不是 heredoc / `echo`:正文经 stdin 逐字传入,不过 shell 解析,`$()`、反引号、引号、换行全部原样进消息,也不会有 heredoc 结束符碰撞。

## 读返回的 JSON(据此如实转述给用户)

输出是一段 JSON,`sent` 是主信号:

- `{"ok":true,"sent":true,"message_id":...,"chat_id":...}` → **已通知群里**(@到群主了)。告诉用户"已在群里 @你了,待你决策",然后按需要停下等回复。
- `{"ok":false,"sent":false,"reason":...}` → **确定没发出去**,如实告诉用户原因,别假装已通知:
  - `not-bound` / `empty-message` → 本 session 没绑群 / 正文是空的(exit 0,属正常前置,不是错误)。
  - `send-failed` / `send-rejected`(可能带 `code`/`retryable`)/ `chat-not-allowed` / `gate-degraded` / `invalid-*` / `config` / `instance-unresolved` / `session-unresolved` → 各自含义见 `reason`/`detail`,转述给用户;`retryable:true` 表示可稍后重试。
- `{"ok":false,"sent":"unknown","reason":...}` → **不确定发没发出去**(超时/网络抖动等)。**别急着重发**——让用户看一眼群里是否已出现该通知,避免重复 @打扰。

## 什么时候用它(判断)

- 需要用户批准/授权才能继续的高风险或不可逆操作(删数据、改配置、外发、花钱…)。
- 需要用户做方向选择、提供密钥/权限/缺失信息,而你无法自行决定。
- 长任务跑到重要里程碑或需要转向,值得主动知会一声。

不该用它:把最终答案再发一遍到群里(绑定 session 的每轮输出**已自动**转发回群,别重复);或用它冒充群桥的正常回复。它只用于**主动推送 blocker/知会**。

## 边界与安全

- 只对**本 session 用三元组(session_id + 进程实例 + active)精确命中的绑定**发;不会误发到别的 session 或旧绑定。
- 发前尊重与普通桥出站同款的门:群 `chat_allowlist`、出站身份门(`outbound_gate`)。门不通=不发、如实返回。
- 正文进群会被群成员看到:**不要把密钥/token/内网凭证写进通知正文**。
- 这是 daemon 唯一发送者之外的**受门控显式直发例外**之一(另一个是 StopFailure API 错误告警 hook,底座同为 `lib/notify.py`;加上 bind 相关命令);别用 `lark-cli` 直接往群里发。
