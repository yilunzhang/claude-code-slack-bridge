# slack-bridge — Slack ↔ 本地 Claude Code session 绑定桥(Claude Code plugin)

把一个正在跑的本地 Claude Code(CC)session 与一个 Slack 会话(频道 / 私有频道 / owner 自己的 DM)**一一绑定**:
会话里 @bot 的消息(DM 无需 @)投递进 session 作为指令;session 每 turn 的最终输出自动转发回会话;owner 的消息
直投,其他成员的消息由 owner 在 Slack 里点按钮审批;`/slack-bridge:notify` 可主动 @owner 推送 blocker;
一轮因 API 错误结束时 StopFailure hook 自动告警。多个 session 可各绑不同会话;bind 可在 session 半途进行;
启动 Claude Code 不需要任何特殊参数。

> **状态**:WP0–WP4 已落地(脚手架 / 传输 / 入站审批媒体 / 出站状态机 / 控制面与文档),**WP5 集成前
> 不保证可运行**;真机验证清单见文末。跨模块语义冻结在 [`docs/contracts.md`](docs/contracts.md)。

## 出处

由 [feishu-bridge](../feishu-bridge) **v1.6.1**(commit `9941306`)复制而来并 `git init` 新历史:绑定 / 队列 /
hook / listener 核心沿用,传输层(飞书 lark-cli → Slack Socket Mode + Web API)重写。设计说明见
`docs/contracts.md`(冻结契约)、`docs/slack-app-manifest.json`(app 清单)、`docs/capability-probe.md`(真机能力探测记录)。

## 架构一图

```
Slack ──Socket Mode WS── bin/slack_consumer.py(daemon 子进程;slack_sdk)
                              │ 每个信封:短事务 INSERT slack_events → 提交后才 ack
                              ▼
  bridge daemon(bin/daemon.py;flock 单例;单线程;bridge.db 唯一真相;唯一 SlackClient)
    入站:drain slack_events → inbound / approval → deliveries
    出站:outbound_jobs 状态机 → chat.postMessage / chat.update / reactions.add;结果不确定只核验,最多自动重发一次
    门:FingerprintGate(每 tick stat tokens.json,变了就重验 auth.test;门控结果绑定凭据版本)
              ▲ hooks:Stop/SessionEnd 只写库;StopFailure 走 notify 直发(同款门控)
              ▼ listener(plugin monitor,跟随本 CC 实例)领取 deliveries → NDJSON 进 session
```

## 安装

1. 作为 plugin 加载(开发/本机):
   ```bash
   claude --plugin-dir /path/to/slack-bridge
   ```
   或加入 marketplace 后 `/plugin install slack-bridge`。**装完/更新后重启 Claude Code**,hooks(`hooks/hooks.json`:
   Stop / SessionEnd / StopFailure)与 listener monitor(`monitors/monitors.json`)才生效;不需要手改 `settings.json`。
2. 唯一第三方依赖(只有 Socket Mode consumer 用):
   ```bash
   python3 -m pip install 'slack_sdk>=3.44,<4'
   ```
   用**运行 daemon 的那个 python3**装(默认 = `sys.executable`);要换解释器,在 `config.json` 里加
   `"consumer_python": "/path/to/python3"`。`bridgectl preflight` 的 `slack_sdk.importable` 会告诉你装对了没有。
3. Python ≥ 3.9;macOS(桌面通知走 `osascript`;其它平台通知静默跳过)。

## 创建 Slack app(一次)

1. [api.slack.com/apps](https://api.slack.com/apps) → **Create New App → From an app manifest** → 选 workspace →
   粘贴 [`docs/slack-app-manifest.json`](docs/slack-app-manifest.json)(已含全部 bot scopes、事件订阅、Interactivity、
   Socket Mode、Messages tab)。
2. **Install to Workspace** → 复制 **Bot User OAuth Token**(`xoxb-…`)。
3. **Basic Information → App-Level Tokens → Generate**,scope `connections:write` → 复制(`xapp-…`)。
   Socket Mode 靠它连接;没有它收不到任何消息。
4. 同页记下 **App ID**(`A…`);`bootstrap` 一般能经 `bots.info` 自动取到,取不到时用 `--app-id` 补。
5. 在每个要绑的频道里 `/invite @slack-bridge`(bot 不在频道 = 收不到、发不出)。DM 不用邀请。
6. owner 的 user id(`U…`):Slack 个人资料 → ⋯ → Copy member ID;或用 `--owner-email`(manifest 已带
   `users:read.email`)。

## 首次配置:bootstrap(在**你自己的终端**跑,token 走 env)

```bash
export SLACK_BOT_TOKEN='xoxb-…' SLACK_APP_TOKEN='xapp-…'
python3 /path/to/slack-bridge/bin/bridgectl.py bootstrap --owner U0XXXXXXX
#   或 --owner-email you@example.com;bots.info 取不到 App ID 时加 --app-id A0XXXXXXX
#   可选 --chat-allowlist C0AAA,C0BBB(只允许绑这些会话;灰度/测试隔离)
#   不想用 env:python3 … bootstrap --owner U… --tokens-stdin < tokens.json(JSON {bot_token, app_token})
```

它做的事:`auth.test` 钉死 `team_id / bot_user_id / bot_id`,`bots.info` 取 `app_id`,写 `config.json`(指纹,0600)
与 `tokens.json`(凭据,0600)。**不发任何消息**;两个文件**存在即拒**(要重来先 unbind 所有绑定并手动删掉它们)。
**token 只从 env 或 stdin 进入**,没有任何命令行选项能传 token;所有输出/日志/状态都不含 token;agent 绝不接触 token
(SKILL.md 会让 agent 把你引到这一步,而不是在对话里要 token)。

接着建议:

```bash
python3 …/bridgectl.py probe --chat-id C0TESTCHAN --write-config   # 真机能力探测(见下)
python3 …/bridgectl.py open-dm                                    # 钉住 owner DM(想绑 DM 时)
python3 …/bridgectl.py preflight                                  # 检查 config/tokens/slack_sdk/hooks
```

## 能力探测:probe(不需要 daemon;会发两条测试消息并删除)

```bash
python3 …/bridgectl.py probe --chat-id C0TESTCHAN --write-config
```

它先独立 `auth.test` 核对身份(不符 → exit 3,不发消息不导入),然后在测试频道发顶层 `markdown_text`+`metadata` 消息 →
`conversations.history(include_all_metadata)` 读回 → 线程内同形消息 → `conversations.replies` 读回 → 两条 `chat.delete`。
`--write-config` 写 `config.markdown_mode`(`markdown_text` 或回退 `text`)与 `daemon_state.verify_capability`(`ok` |
`degraded:<err>`)+ 其**凭据版本**。daemon 只在 `verify_capability_tokens_version == 当前 tokens.json 版本` 时采信 `ok`
—— **换过 token 就要重跑 probe**,否则自动重发关闭(unknown 只核验,三次未见即 `unconfirmed`)。结果记入
`docs/capability-probe.md`。退出码:0 完成 / 2 配置或凭据错 / 3 身份不符 / 4 未完成(API 失败/冷却)。
`doctor --chat-id …` = probe + daemon 健康摘要。

## 绑定:在 Claude Code 里

在要绑定的 session 里执行 `/slack-bridge:bridge bind`。agent 会按 [`skills/bridge/SKILL.md`](skills/bridge/SKILL.md):
`preflight` → `chats`(或 `open-dm`)让你选会话 → `bind --chat-id … --chat-name …` → 等 listener 认领 → 在回复里
放 marker 触发 Stop hook 握手 → daemon 往会话发「✅ 已绑定」。之后:

- 频道里 `@slack-bridge 你的指令`(DM 直接发)→ 投递进 session;bot 会先加 👀 表情作为回执。
- session 每 turn 的最终输出自动转发回会话(超长自动分块;末块带 `🧠 上下文 · 模型 · effort` 页脚)。
- 非 owner 的消息 → 会话里出现审批卡(线程内),owner 点「投递给 session」或「忽略」;带附件的批准会先显示
  「已批准,附件处理中」再「已投递」。owner 可让 agent 把某人加入直投白名单(`bridgectl allow add --chat-id C… --user-id U…`)。
- `/slack-bridge:notify`:agent 主动 @你 推送需要你拍板的事(Block Kit;正文 markdown)。
- `/slack-bridge:bridge unbind` 立即解绑(敏感操作前的逃生门);随时可 rebind。

手动运维命令(任意 cwd):

| 命令 | 作用 |
|---|---|
| `bridgectl status` | daemon / consumer / 门与凭据版本 / 各绑定 / 队列各态 / `slack_events` 隔离行 / 冷却 / `hints[]` |
| `bridgectl ensure-daemon` | 拉起或接管 daemon(挂死精确匹配 pid+start 后 SIGTERM 再拉起) |
| `bridgectl chats` / `open-dm` | 列可绑会话(频道只列 bot 已是成员的;DM 只列 owner 的)/ 钉 owner DM |
| `bridgectl bind/unbind` | 由 skill 调用;人也可直接跑(要在 CC 的子进程树里才能定位实例) |
| `bridgectl probe/doctor --chat-id C…` | 能力探测 / + daemon 健康 |
| `bridgectl allow list\|add\|remove --chat-id C… --user-id U…` | 成员直投白名单(chat+user 双精确匹配) |
| `notifyctl < body.md` | 给本 session 绑定会话发 @owner 通知(见 `skills/notify/SKILL.md`) |

## 数据目录与文件

`~/.claude/data/slack-bridge/`(0700;env `SLACK_BRIDGE_DATA_DIR` 重定向):

| 文件 | 内容 |
|---|---|
| `config.json`(0600) | 指纹 `team_id bot_user_id bot_id app_id owner_user_id` + 可选 `owner_dm_id markdown_mode consumer_python chat_allowlist`。bootstrap 写;运行时只经 `set_persist` 改单键(probe 写 `markdown_mode`,open-dm 写 `owner_dm_id`)。daemon 每 tick 重读 |
| `tokens.json`(0600) | `{"bot_token":"xoxb-…","app_token":"xapp-…"}`。**daemon 的唯一真相**:版本 = `mtime_ns:sha256[:16]`;权限非 0600 → 拒读(fail-closed) |
| `allowlist.json` | 成员直投白名单 |
| `bridge.db` | SQLite(WAL):bindings / slack_events / inbox / pendings / deliveries / outbound_jobs / daemon_state… |
| `bridge.lock` `ensure.lock` `bootstrap.lock` | flock:daemon 单例 / ensure 单飞 / bootstrap 互斥 |
| `daemon.log` `hook_drops.log` | daemon 日志(5MB 轮转)/ hook fail-closed 记录(固定文案,不含正文) |
| `hook_heartbeat.stop` `.session_end` | plugin hooks 已生效的哨兵(preflight/bind 的 advisory 信号) |
| `media/<binding_id>/<channel:ts>/` | 已下载附件 |

其它 env:`SLACK_BRIDGE_SETTINGS_PATH`(只读 settings.json 找其它 Stop hook);`SLACK_BOT_TOKEN`/`SLACK_APP_TOKEN`
**只供 CLI(bootstrap/probe/chats)与测试**,daemon / notify / StopFailure 一律只读文件。

### 换 token(rotation)

用 0600 权限**整文件替换** `tokens.json`(例如 `python3 -c 'import json,os;…'` 或先写临时文件再 `mv`)。daemon 下一 tick
就会看到 mtime 变化 → 重读 → 立即 `auth.test` → 同一事务把 `outbound_gate` / `outbound_gate_tokens_version` 绑定到新版本,
并把 `verify_capability` 置回 `unverified`(重跑 probe 恢复自动重发)。这期间 `notifyctl` / StopFailure 会返回
`credentials-unverified`(几秒)。`app_token` 变了 → daemon SIGTERM consumer,由 ConsumerManager 用新 token 重拉。
换成**别的 app** 的 token → `auth.test` 身份不符 → `outbound_gate=mismatch`、出站关门、daemon 重启会拒启;
换回或重新 bootstrap 即可。

## 已知限制

**Slack 侧(plan「风险」)**
1. `chat.postMessage` 没有幂等键:发送结果不确定(超时/5xx/传输错)时 daemon **只核验**(按 `metadata.job_id` 读回
   history/replies),核验能力经 probe 确证且凭据版本匹配时最多**自动重发一次**,否则进 `unconfirmed` 诚实终态并告警。
   残余风险:极小概率重复,以及重发后与后续块乱序(同组余块会被取消并告警,要你决定是否补发)。
2. `markdown_text` 参数与 `metadata` 读回在你的 workspace 是否可用要靠 probe 真机确证;不可用则回退 `text` 模式 +
   自动重发关闭。
3. 频道 @bot 会双投(`message` + `app_mention`,同 `ts`):按 `message_id` 去重,files/blocks 只在 `message` 上时做快照升级;
   DM 内 bot 自身消息回流靠 `bot_id / user / app_id` 三组比较过滤。真机样本待确认(`docs/capability-probe.md`)。
4. 争锁:consumer 每信封短事务(1.5s 超时;超时不 ack,Slack 会重投);drain 每 tick 限批;反复失败的事件进
   `quarantined`(`status` 高亮,保留 30 天)。
5. `slack_sdk` 缺失/版本不符 → consumer rc 4/致命鉴权 rc 3,daemon 直接最大退避重拉;`preflight` 会提示。
6. 只有 owner 自己的 DM 可绑;别人 DM 给 bot 的消息不会被投递(也不回提示,以免打扰)。
7. bot 不在频道 → 发送 `not_in_channel`/`channel_not_found` 永久失败并告警;`/invite` 后让 agent 补发。
8. 限流:`SlackClient.call` 层统一方法级冷却(完整 `Retry-After`,daemon / notify / probe 共用 `daemon_state`);
   每频道 postMessage 节流 1 条/秒。人为 429 时 notify 也会被冷却(`reason=cooldown`)。
9. 长网络操作:附件下载在子进程里、父进程持绝对 deadline(90s);每 tick 只物化 1 条带下载 / 5 条纯文本;单文件
   100MB 配额;失败按 10 分钟预算退避后终态。
10. `markdown` Block Kit 块(notify / StopFailure 正文)按 Slack 的标准 markdown 子集渲染,不保证完整 CommonMark;
    正文 ≤ 12000 字符。

**继承自 feishu-bridge、仍然适用**
- listener 由 plugin monitor 承载:只有以 `/slack-bridge:bridge` 全名调用时才 arm;monitor 未 arm/启动失败时要按 bind 输出的
  `listener_cmd` 手动起 Monitor(30 分钟到期要重挂)。
- 其它阻断型 Stop hook 共存时,同一 turn 可能触发多次 Stop → 普通 turn 有重复转发组风险(bind turn 有链闩保护);
  `preflight` 会列出 `foreign_stop_hooks`。
- 页脚里的 tokens/model 来自 transcript,可能滞后约一 turn;effort 取当前值。
- 开发时原地改代码但不改 plugin 版本 → bind 前置的 code-identity 检查察觉不到旧 daemon,需自己 `status` 看 `daemon.pid` 后 kill。
- daemon 冷启动窗口的 ensure 竞态只是缩窗 + 可观测(`startup=stopping`),非根治;bind 遇 `in_progress` 重跑即可。
- 桌面通知 `osascript` rc=0 ≠ 用户看见(通知权限/专注模式挂在 osascript 上)。
- daemon 单线程:一次同步网络操作期间不刷心跳,挂死阈值 = 90s 下载 deadline + 60s。
- 个人轻量工具假设:单用户、会话内可信;成员经批准/白名单的消息仍是不可信输入(agent 只当数据),但告警正文含有界的
  error/cwd 路径会进会话。

## 排障

| 现象 | 看什么 / 怎么办 |
|---|---|
| `preflight` `config_present=false` / `tokens_present=false` | 在自己终端跑 bootstrap(见上) |
| `preflight` `tokens_error` 提到 0600 | `chmod 600 ~/.claude/data/slack-bridge/tokens.json` |
| `slack_sdk.importable=false` | `<slack_sdk.python> -m pip install 'slack_sdk>=3.44,<4'` |
| bind 后会话里没有「✅ 已绑定」 | `status`:`daemon.last_loop_age_s` 应 <5s;`consumer.ready` 应 `ready num_connections=N`;`hooks.confirmed=false` 时重启 CC(hooks 未生效握手不会完成);`bindings[].phase` 是否 `confirmed` |
| `status.outbound_gate` = `degraded:auth_error` | token 被撤销 / 网络;看 `daemon.log`;修好后 daemon 按退避自动重探 |
| `status.outbound_gate` = `mismatch` | tokens.json 属于别的 app;换回原 token 或删 config.json/tokens.json 重新 bootstrap(先 unbind) |
| `credentials_verified=false` | tokens.json 刚换,等 daemon 下一 tick;或 daemon 没跑 → `ensure-daemon` |
| `notifyctl` 返回 `credentials-unverified` / `gate-degraded` / `cooldown` | 同上;`cooldown` 到 `cooldown_until` 后重发 |
| 消息进不来 | 频道里有没有 @bot;bot 是否已 `/invite`;`status.slack_events` 有没有 `staged` 堆积或 `quarantined`;`counters.event_dropped_*` 哪个在涨(`foreign_app`=app_id 不对、`chat_not_allowed`=allowlist、`self`=自身回流) |
| 输出发不出去 | `status.outbound_jobs` 里 `failed/unconfirmed`;`hints[]`;`cooldowns`;bot 是否在频道 |
| `verify_capability` 非 `ok` / `auto_resend_enabled=false` | 跑 `probe --chat-id … --write-config`(换过 token 必跑) |
| `quarantined` > 0 | `status.quarantined[].error` + `daemon.log`;修复后这些事件不会自动重放(保留 30 天供排查) |
| 旧版本 daemon 无法自动重启(bind exit 6) | `status` 看 `daemon.pid` → `kill <pid>` → 重跑 bind |
| listener 没认领(`listener_claimed=false`) | 按 bind 输出的 `listener_cmd` 手动起 Monitor;monitor 没 arm 则重启 session 再 bind |

## 真机验证清单(WP5 / 用户联调;需要 token)

0. 用 manifest 建 app 并安装;`pip install 'slack_sdk>=3.44,<4'`。
1. `bootstrap --owner U…`;人为让 `bots.info` 取不到时确认它要求 `--app-id`。
2. `probe --chat-id <测试频道> --write-config`(daemon 未启动也可):`markdown_text_ok` / `metadata_history` /
   `metadata_replies` / `cleanup_ok`;写入了 `markdown_mode`、`verify_capability` 与其凭据版本;结果记入
   `docs/capability-probe.md`。
3. `chats`、`open-dm`。
4. `ensure-daemon` + `status`:gate `ok` 且 `verify_capability=ok` 版本匹配(`credentials_verified=true`,`auto_resend_enabled=true`)。
5. bind → 会话里「✅ 已绑定」。
6. owner 在频道 `@bot hi` → 恰一个 👀 + 恰一条投递;`counters.inbox_dup_message==1`;**保存脱敏样本**到 `tests/fixtures/real/`
   (`message` 与 `app_mention` 两条信封)。
7. owner 不 @ → 不投递。
8. 第二个 session 绑 owner DM;第三个 session 绑同一频道 → `chat_busy`。
9. 成员在已有线程里 `@bot` → 审批卡在同一线程;非 owner 点击无效;Approve / Reject 各一次;带附件的 approve 先显示
   「已批准,附件处理中」再「已投递」;approve 后立刻 unbind → 卡片仍被更新;还 pending 的卡片 → 「已过期」。
10. 文件矩阵(小文件 / 超 100MB 的 `too_large` / 隐藏文件 `hidden_by_limit` / 已删除的 tombstone)→ payload `files[].skipped_reason`;保存样本。
11. 一轮 >12000 字符的输出 → 分块、页脚只在末块、每块 ≤ 12000。
12. `/slack-bridge:notify` 与 StopFailure(触发一次 API 错误):含 `credentials-unverified` 路径 —— 换 tokens.json 后立刻 notify → 拒;
    daemon 重验后 → 通过。
13. 重投:`kill -STOP` consumer → 发一条消息 → 观察 Slack 重投 → `kill -CONT` → 恰一条投递。
14. `SLACK_BRIDGE_SEND_TIMEOUT_S=0.05` 跑一轮 → 出站 `unknown` → 核验命中 → `sent`;`SLACK_BRIDGE_VERIFY_FORCE_ABSENT=1`
    → 重发一次 → `unconfirmed` 告警;三块输出首块 unconfirmed → 余块取消、下一轮仍能发。
15. 断网重连 / 撤销 token / 换 xapp → consumer 被重拉;换 xoxb → gate 重验、notify 版本门。
16. 5 轮连发节流(每频道 1 条/秒);人为 429 → notify 也被冷却(`reason=cooldown`)。
17. `unbind` / rebind。

## 测试

```bash
python3 -m pytest tests/ -q            # 全离线(FakeSlackClient;真实 SQLite)
python3.9 -m pytest tests/ -q          # 3.9 门禁(有则跑)
python3 -m pytest tests/test_contracts.py tests/test_drain.py -q   # 冻结契约守卫
```
`tests/test_sdk_contract.py` 需要真实 `slack_sdk`(`.venv-test`),未安装则 skip。

## 目录

- `.claude-plugin/` plugin 清单;`hooks/` Stop/SessionEnd/StopFailure;`monitors/` listener monitor;`skills/` bridge / notify
- `bin/` `daemon.py` `listener.py` `slack_consumer.py` `download_worker.py` `bridgectl.py` `notifyctl.py`
- `lib/` 核心模块;`schema.sql` 新库 schema(不做迁移);`scripts/capability_probe.py` 真机能力探测
- `docs/` `contracts.md` `slack-app-manifest.json` `capability-probe.md`;`tests/` 离线测试

MIT(见 LICENSE)。
