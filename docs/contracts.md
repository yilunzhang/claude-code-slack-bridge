# slack-bridge 冻结契约(WP0 → WP1–WP4 并行实现的共享语义)

> 本文是 plan(`happy-plotting-kernighan.md`,Codex 六轮评审收敛)中跨模块语义的**可执行化冻结**。
> 守卫:`tests/test_contracts.py`(WP0 能满足的直接绿;归属后续 WP 的以 `xfail(strict=True, reason="WPn")` 存在,
> 该 WP 合入前**必须**转绿并摘掉标记)与 `tests/test_drain.py`(真实 SQLite 事务语义)。
> **改契约 = 改本文 + 改守卫测试 + 通知所有并行 WP**;不得只改实现。
> 名字来源:`lib/constants.py`(WP0 预声明,值可调、名字不改)。时间单位 ms,后缀 `_S` 为秒。

---

## 0. 不变量(继承 feishu-bridge plan v7 §2 + I6)

- **I1** 模型输入通道 = 本绑定 `deliveries` 经 listener;控制行仅固定文案;投递判定零模型参与。v1 不读整线程、无 `reply_hint`。
- **I2** 一切外发由 daemon 经 `outbound_jobs` 单点发出,每 kind 固定守卫(`_guard_ok`);notify / StopFailure / probe 是受同款门控(身份门 + 凭据版本 + 方法级冷却)的显式例外(probe 免版本门)。
- **I3** `binding_id` 唯一不复用;入队/领取在事务内复验绑定 active;`deliveries UNIQUE(binding_id, message_id)`。
- **I5** Stop/SessionEnd hook 与 listener 不联网,只读写 bridge.db,异常 fail-closed。
- **I6** consumer 只在事件**持久化提交后**才 ack(故障模型 = 应用进程崩溃,不承诺断电);postMessage 类结果不确定时**只核验**(精确关联 = metadata `job_id`),核验能力经真机确证后最多自动重发**一次**,否则 `unconfirmed` 诚实终态;幂等类(chat.update / reactions.add)直接重试 ≤3 次;重试资格、cap 与终态**只由 Outbound 判定**,recovery 不复活终态;token 只在 0600 文件或 env,不上 argv、不进模型输出与日志。
- 未绑定 session 的输出绝不外发(含 bind turn);非 owner 消息投递 = 纯机械审批门;owner 与"其余全部"只有两档信任。

---

## 1. 事务边界(R2-M16)

```python
# lib/daemon_core.py  (WP1)
class DaemonCore:
    def __init__(self, conn, cfg, clock, inbound, approval, outbound, recovery, log=None, gate=None): ...
    def drain_staging(self) -> dict:      # {"handed": int, "dropped": int, "retried": int, "quarantined": int}
        """每 tick:SELECT staged ∧ (next_drain_at IS NULL OR next_drain_at<=now) ORDER BY seq LIMIT DRAIN_BATCH;
        **每一行各开一个 db.tx**(drain 是唯一事务拥有者):
          events_api  → inbound.ingest_in_tx(conn, row)
          interactive → approval.process_in_tx(conn, json.loads(row.payload_json))
        返回 ("handed", x) | ("dropped", reason) → 同事务 UPDATE slack_events SET state='consumed', consumed_at=now,
          error=(reason if dropped else NULL)。
        业务抛任何异常 → 事务回滚(行仍 staged)→ 单独小事务:drain_attempts+1,
          next_drain_at = now + min(DRAIN_BACKOFF_MS·2^attempts, DRAIN_BACKOFF_MAX_MS),error=repr 截断;
          attempts ≥ DRAIN_MAX_ATTEMPTS → state='quarantined'(保留 payload+error;计数 drain_quarantined)。
        handed 的 inbox 行**不在 drain 里驱动**(见 §1.2)。"""
    def run_followups(self, budget=constants.FOLLOWUP_BUDGET_PER_TICK) -> dict: ...  # 调 inbound.drive_pending_rows
```

```python
# lib/inbound.py  (WP2)
def ingest_in_tx(conn, row) -> tuple:   # ("handed", inbox_row) | ("dropped", reason:str)
    """假定调用方已 BEGIN;**绝不** BEGIN/COMMIT/ROLLBACK,不调用 db.tx。零网络。row = slack_events 行
    (sqlite3.Row 或等价 mapping,需 payload_json / binding_id / chat_id / event_key)。
    reason ∈ {"foreign_team","foreign_app","event_type","self","subtype","chat_not_allowed",
              "not_mentioned_unbound","cap","dup_message","invalid"}(计数键 event_dropped_<reason>)。
    IntegrityError 分支:同一行 → handed(已存在);event_id 无命中 ∧ message_id 命中 → 双投:
      已存行 message_type=='app_mention' ∧ state ∈ {received,resolving} ∧ 新事件 type=='message' ∧ 带 files/blocks
      → 替换 snapshot_json/sender/thread 等(计数 inbox_snapshot_upgraded)并 handed;否则 dropped("dup_message")。
      已过审批/入队的行**绝不替换**。"""

class Inbound:
    def drive_pending_rows(self, budget=constants.FOLLOWUP_BUDGET_PER_TICK) -> dict:
        """先做零网络本地分流(received→resolving→…;不限量),再按预算做需要网络的物化:
        每 tick ≤ budget[0] 条带下载、≤ budget[1] 条纯文本(纯文本 = materializing 但所有 files 都 skipped)。
        每条之后回主循环。recovery 的重驱走同一预算。"""
```

```python
# lib/approval.py  (WP2)
def process_in_tx(conn, payload) -> tuple:  # ("handed", followup_row|None) | ("dropped", reason)
    """假定已在事务内;不 BEGIN。reason ∈ {"skipped"(slackwire.interactive_fields 为 None / team 不符),
    "dup"(callback_events 已有), "invalid"(校验失败;含 card_ts 非法、非 owner、nonce 不符), "late"(CAS 失败)}。
    dropped 也要 INSERT OR IGNORE callback_events(裸去重)——由本函数在同一事务内做。
    followup_row = 需要物化的 inbox 行(approve 且带 files),否则 None;由 drive_pending_rows 按预算驱动。"""
```

- `db.tx` 仍禁止嵌套;`db.connect_short(db_file, busy_ms)` = consumer 线程用的短超时连接。
- listener / hook 事务规则不变。

---

## 2. 出站状态机(`lib/outbound.py`,WP3)

### 2.1 分块与页脚

```python
util.chunk_text_with_footer(body: str, footer: str, limit=constants.CHUNK_LIMIT) -> list[str]
```
每块(含页脚的末块)≤ `limit`;末块装不下 → 末块再切:前 `limit-len(footer)` 字符留原位,尾部+页脚成新末块;页脚 > limit → 丢页脚,**绝不丢正文**;body 空 → `[]`。hooklib(WP4)必须用它替代 `chunk_text` + 直接拼接。

### 2.2 kind → 传输映射(`op_*` 冻结)

| kind | op_method | op_target | op_thread_ts | op_payload_kind | 类别 |
|---|---|---|---|---|---|
| session_turn | `chat.postMessage` | chat_id | NULL | `cfg.markdown_mode`(`markdown_text`\|`text`) | postMessage |
| lifecycle_notice / inbound_notice / unsupported_notice | `chat.postMessage` | chat_id | NULL(inbound/unsupported:`reply_to` 若有) | `text` | postMessage |
| approval_card | `chat.postMessage` | chat_id | `reply_to`(= inbox.reply_thread_ts) | `blocks` | postMessage |
| decision_notice,`card_message_id` 已知 | `chat.update` | card `channel:ts` | NULL | `blocks` | 幂等 |
| decision_notice,`card_message_id` 未知 | `chat.postMessage` | chat_id | `reply_to` | `text` | postMessage |
| receipt_reaction | `reactions.add` | chat_id | NULL | `reaction` | 幂等 |

- 所有 `chat.postMessage` 带 `unfurl_links=false, unfurl_media=false` 与 `metadata={"event_type": METADATA_EVENT_TYPE, "event_payload": {"job_id": job_id}}`。`job_id` 重发后不变。
- 类别由 **`op_method`** 决定:`op_method ∈ POSTMESSAGE_METHODS` → postMessage;`∈ IDEMPOTENT_METHODS` → 幂等。
- `reactions.add` 的 `already_reacted` = sent(`ALREADY_DONE_ERRORS`)。

### 2.3 `op_for` 纯函数与 `job_view` 投影(R5-m1 / R6-m3)

```python
def op_for(job_view, cfg) -> tuple:   # (method:str, target:str, thread_ts:str|None, payload_kind:str)
```
- **纯函数**:同输入同输出;不读 DB、不读时钟、不读文件;只看 `job_view` 与 `cfg`(`cfg.get("markdown_mode")`)。
- `job_view` = `outbound_jobs LEFT JOIN pendings p ON p.pending_id = outbound_jobs.ref_pending_id` 的投影,至少含:
  `job_id kind chat_id reply_to ref_pending_id ref_message_id body op_method op_target op_thread_ts op_payload_kind had_unknown attempt_count state` + **`card_message_id`**(来自 pendings,可 NULL)。
- 已冻结(`op_method IS NOT NULL`)的 job:`op_for` 原样返回存值(不重算)。
- `tick` 的冷却预检与 `_prepare` 的冻结**用同一投影、同一函数**;`_prepare` 在事务内重读投影后再冻结。

### 2.4 `_prepare`(`pending→sending`,同一事务)检查顺序(R3-P3 / R5-S1 / R3-m1)

1. 重读 job_view;state ∉ {pending, unknown(幂等类到期)} → gone。
2. allowlist / 顺序门(§2.8)/ `_guard_ok`(不满足 → cancelled)—— 与 feishu-bridge 相同。
3. **cap**:`attempt_count ≥ cap(kind, category)` → 幂等类 → `failed`;postMessage 类 → **禁止发送**(仍可核验;若已无核验在途 → 终态按 `had_unknown` 选 failed/unconfirmed)。cap:turn 6 / card 3 / notice 3 / 幂等 3(`TURN_CAP CARD_CAP NOTICE_CAP IDEMPOTENT_CAP`)。
4. **重发资格复验**:postMessage ∧ `had_unknown=1`(这是一次重发)→ 必须 `daemon_state.verify_capability=="ok"` ∧ `verify_capability_tokens_version == client.tokens_version`(**本次实际使用的凭据快照**,即 `_transmit` 将用的同一个 client 对象的版本)。否则不发送,job → `unconfirmed` + 告警。
5. **`op_*` 冻结断言**:`had_unknown=1` 时 `op_for(job_view,cfg)` 必须与存值完全相同(方法/目标/线程/形态);不同 → 不发送、`unconfirmed` + 告警(编程错误也 fail-closed)。
6. 未冻结 → `op_* = op_for(job_view, cfg)` 写入;CAS `state='sending', sending_at=now, attempt_count+1`。
7. 冷却:`tick` 在 `_prepare` **之前**读 `cooldown:<op_method>`;已冷却 → 小事务只写 `next_attempt_at=cooldown_until`,**不进 `_prepare`**。`_prepare` 之后 `SlackClient.call` 内再兜底检查,竞态命中返回 `wait`(§2.5)。

### 2.5 结果分类与转移表

分类(`slackapi.classify_send_error` + Outbound 上下文特判):
`sent` / `failed`(PERMANENT_SEND_ERRORS)/ `ratelimited`(429 或 `ratelimited`;`SlackClient` 已发布冷却)/ `wait`(`cooldown_until` 非空:本地冷却,请求未发出)/ `not_sent`(`CallResult.not_sent` 或 NOT_SENT_ERRORS;含 **markdown_rejected** = `invalid_arguments ∧ op_payload_kind=='markdown_text'`,优先于 PERMANENT)/ `unknown`(其余:超时、写出后的传输错、5xx、AMBIGUOUS、不可解析、未知错误码)。

ac = attempt_count;`had_unknown` 一旦 1 永不清零。

| 类别 | 结果 | → state | 时序/计数 | 备注 |
|---|---|---|---|---|
| 任意 | wait | 发送分支:CAS `sending→pending` 且 `attempt_count-1`;核验分支:保持 `unknown` | 发送:`next=cooldown_until`;核验:`verify_after=cooldown_until` | 不动任何 count;`cooldown_waits+1`。正常路径发送分支不会到此(tick 预检) |
| postMessage | sent | sent | `sent_message_id='channel:ts'` | approval_card 回填 `pendings.card_message_id`(仅 NULL 时) |
| postMessage | failed ∧ had_unknown=0 | failed | – | turn → 告警(`send_failure_alert_body`);card → status 高亮 |
| postMessage | failed ∧ had_unknown=1 | **unconfirmed** | – | 首次可能已送达(R2-M3);告警 `unconfirmed_alert_body` |
| postMessage | not_sent(markdown_rejected)∧ had_unknown=0 | pending | `next=now`;`ConfigSnapshot.set_persist("markdown_mode","text")`;`op_payload_kind` 重置为 NULL 由下一次 `_prepare` 记 `text` | 同一 attempt 内不发第二个请求(R2-M6);`ac-1` 不计 transient |
| postMessage | not_sent(markdown_rejected)∧ had_unknown=1 | **unconfirmed** | – | 不得改形态(R4-m1);告警 |
| 任意 | ratelimited | pending | `next=max(now+RA·1000, cooldown[method])`;`ac-1`;`ratelimit_count+1` | ≥ RATELIMIT_CAP(20) → `had_unknown ? unconfirmed : failed`(+告警) |
| 任意 | not_sent(其它) | pending | `next=now+min(TRANSIENT_BACKOFF_MS·2^tc, TRANSIENT_BACKOFF_MAX_MS)`;`ac-1`;`transient_count+1` | ≥ TRANSIENT_CAP(5) → 同上;冷却不走此行 |
| postMessage | unknown | unknown,`had_unknown=1` | `verify_after=now+VERIFY_SCHEDULE_MS[0]` | 绝不盲重发 |
| 幂等 | sent(含 already_reacted) | sent | – | |
| 幂等 | unknown | unknown | `next=now+IDEMPOTENT_RETRY_DELAY_MS` | 直接重发;`_prepare` 重发前 ac≥3 → failed(R3-P3) |
| 幂等 | failed | failed | – | 不告警 |

### 2.6 核验(`_verify_unknown`,postMessage 类,R5-S2 / R6-m2)

- 触发:`state='unknown' ∧ verify_after<=now`;`op_thread_ts` 非空 → `conversations.replies(channel=op_target, ts=op_thread_ts)`;否则 `conversations.history(channel=op_target)`;参数 `oldest=(sending_at-VERIFY_LOOKBACK_MS)/1000, inclusive=true, include_all_metadata=true, limit=VERIFY_PAGE_LIMIT`,最多 `VERIFY_MAX_PAGES` 页。
- 命中 = `msg.bot_id==cfg.bot_id ∧ msg.metadata.event_type==METADATA_EVENT_TYPE ∧ msg.metadata.event_payload.job_id==job_id`。
- 结果三分 + CAS `WHERE state='unknown' AND verify_round=?`:

| 结果 | → | 计数/时序 |
|---|---|---|
| hit | sent(`sent_message_id`=命中 ts) | `verify_hit+1` |
| absent,`n = verify_absent_count+1 < VERIFY_ABSENT_RESEND_AT(3)` | unknown | `verify_absent_count=n`;`verify_after = now + VERIFY_SCHEDULE_MS[n]`(n=1→+20s,n=2→+60s);**n 在同一 CAS 内算出并分支** |
| absent,`n==3` ∧ `verify_capability=="ok"` ∧ `verify_capability_tokens_version==client.tokens_version` ∧ `resend_count==0` ∧ RESEND_ONCE | pending | 同一 CAS:`next=now, resend_count=1, verify_round+1, verify_absent_count=0, verify_error_count=0, verify_after=NULL` |
| absent,`n==3`,其余情况 | unconfirmed | 告警;`verify_unconfirmed+1` |
| error(任何失败/翻页未完/不可解析/5xx/超时) | unknown | `verify_error_count+1`;`verify_after=now+min(VERIFY_ERROR_BACKOFF_MS·2^ve, VERIFY_ERROR_BACKOFF_MAX_MS)`;不动 absent;`ve ≥ VERIFY_ERROR_CAP(8)` 或 `now - sending_at > VERIFY_DEADLINE_MS(10min)` → unconfirmed + 告警 |
| 429 / wait | unknown | `verify_after=冷却到期`;两个计数都不动;**绝不因冷却回到 pending**(R3-P2) |
| 永久错(channel_not_found / missing_scope 等) | unconfirmed | `verify_capability=degraded:<err>` |

- **`verify_round` 只在"获得重发资格 → pending"的那一次 CAS 里 +1**并把 `verify_absent_count / verify_error_count / verify_after` 归零;`_prepare`、wait、not_sent、ratelimited 都不切轮、不清零(R6-m2)。
- 错误与冷却**不能凑数**:只有查询完整成功且未命中才 `absent+1`。
- 重发后同一 `job_id` 的 metadata 不变 → 新一轮核验命中任一条即视为已送达。

### 2.7 `had_unknown` / `op_*` 改写规则(R3-m1 / R4-m1)

- 只有 `had_unknown=0 ∧ markdown_rejected` 允许改 `op_payload_kind`(markdown_text → text)。
- `had_unknown=1` 的任何重试必须保持 `op_method / op_target / op_thread_ts / op_payload_kind` 完全相同;`_prepare` 断言(§2.4-5)。
- `_transmit` / `_verify_unknown` **只用冻结的 `op_*`**,`had_unknown=1` 后不得因 `cfg.markdown_mode` 变化重选形态。

### 2.8 排序(R2-N1)

- 组内(同 `turn_group`):前块 `sent` 才发后块;前块 `failed/cancelled` → 后块 cancelled(原规则);**前块 `unconfirmed` → 同一事务把本组剩余块 `cancelled`(error `prev-unconfirmed`)**,计数 `group_cancelled_after_unconfirmed`,只发一条告警(`group_cancelled_alert_body`)。
- 跨组(同 binding 的 session_turn):更早组存在 `pending/sending/unknown` → 阻塞;`sent/failed/cancelled/unconfirmed` 放行。
- 同 chat 通知类按 job_seq(不变)。
- postMessage 类另受每频道 `POST_MIN_INTERVAL_MS`。

### 2.9 `startup_scan`(R3-P3)

1. `sending ∧ postMessage` → `unknown, had_unknown=1, verify_after=now`(到 cap 的仍只核验,未见即 unconfirmed)。
2. `sending ∧ 幂等 ∧ ac<IDEMPOTENT_CAP` → `unknown, next=now`;`sending ∧ 幂等 ∧ ac≥cap` → `failed`。
3. `unknown` 无时序(`verify_after` 与 `next_attempt_at` 都 NULL)→ 按类别重臂(幂等到 cap → failed)。
4. `pending ∧ ac≥cap` → 终态(按 `had_unknown` 选 failed/unconfirmed)。
- `recovery._legacy_sending`:`sending_at < now - 2·SEND_TIMEOUT_S·1000` 的 postMessage → `unknown, had_unknown=1, verify_after=now`;幂等 → 按 cap 收口。

### 2.10 终态与 recovery

- 终态集 = `OUTBOUND_TERMINAL_STATES = (sent, failed, cancelled, unconfirmed)`;**recovery 绝不把终态改回非终态**(删 `_rearm_failed_cards`);`_replenish_cards` 只在**缺** card job 时创建。
- `_retention`:`unconfirmed` 正文纳入终态裁剪;`slack_events.consumed` > SLACK_EVENTS_RETENTION_MS 删,`quarantined` > SLACK_EVENTS_QUARANTINE_RETENTION_MS 删;media 孤儿 `.tmp-*` 由 `_retention` 清理。

---

## 3. 传输层(`lib/slackapi.py`,WP0 已实现;WP1 接入)

```python
@dataclass
class CallResult:
    ok: bool = False; data: dict|None = None; error: str|None = None; http_status: int|None = None
    retry_after: int|None = None; timed_out: bool = False; exc: BaseException|None = None
    not_sent: bool = False; cooldown_until: int|None = None

class CooldownStore:                      # 协议
    def get(self, method) -> int|None     # 到期墙钟 ms
    def publish(self, method, until_ms) -> int   # 取 max(旧, 新),返回生效值
class InMemoryCooldownStore(CooldownStore)
class DaemonStateCooldownStore(CooldownStore):   # daemon_state 键 f"cooldown:{method}",单条 UPSERT 取 max
    def __init__(self, conn)

class SlackClient:
    def __init__(self, tokens_path=None, token=None, cooldown_store=None,
                 timeout_s=constants.SEND_TIMEOUT_S, clock=None, base_url=None,
                 tokens_version=None, environ=None)
    tokens_version: str                    # 文件版本 / "env" / 显式
    def reload_tokens(self, version=None) -> str
    def call(self, method, params=None, timeout_s=None) -> CallResult

def classify_send_error(res) -> "sent"|"failed"|"ratelimited"|"wait"|"not_sent"|"unknown"
```
- `call`:POST JSON + `Authorization: Bearer`;**无重试、不跟随 3xx**(3xx → `http_redirect`,unknown);每次 call 前读 `cooldown:<method>` 未到期 → 不发请求返回 `CallResult(cooldown_until=…)`;429 → `publish(method, now+RA·1000)`(缺头默认 1s)+ `retry_after`;200 体 `error=="ratelimited"` 同样发布;`not_sent=True` 仅当 DNS / 拒连 / 网络不可达 / TLS 握手(请求写出前);超时 → `timed_out=True`(不 not_sent);写出后的传输错(RemoteDisconnected / reset / broken pipe)→ unknown。
- daemon(WP1)、notify(WP4)、probe 共用 `DaemonStateCooldownStore(conn)`。测试用 `tests.helpers.FakeSlackClient`(同构:冷却预检 → wait 并记入 `.waits`;429 响应发布冷却;未注册方法 → AssertionError)。

---

## 4. Wire 形状

### 4.1 `slack_events` 行(consumer 写,drain 读)

| 列 | 值 |
|---|---|
| envelope_type | `events_api` \| `interactive`(SocketModeRequest.type;其它类型 ack 后丢弃不落库) |
| event_key | `slackwire.event_key(type, payload)`;None → ack + 丢弃,计数 `staged_invalid` |
| chat_id | `slackwire.chat_of(type, payload)`(可 NULL) |
| binding_id | 同事务 `SELECT binding_id FROM bindings WHERE chat_id=? AND status IN ('starting','active') ORDER BY binding_seq DESC LIMIT 1` 或 NULL |
| payload_json | `util.jdumps(req.payload)` 原样 |
| received_at | 墙钟 ms |
| state / drain_attempts / next_drain_at / error / consumed_at | 见 §1 |

`INSERT … ON CONFLICT(event_key) DO NOTHING`;rowcount 0 → 计数 `staged_dup`,**仍 ack**。锁超时/异常 → **不 ack**(Slack 会重投)。

### 4.2 `slackwire` 提取规则(WP0 已实现,consumer 与 approval 必须调用它、不得自写)

- `event_key("events_api", p)` = `"ev:"+p.event_id`(非空、不含 `:`)。
- `event_key("interactive", p)` = `"act:"+":".join(team.id, channel, card_ts, user.id, actions[0].action_id, actions[0].action_ts)`,要求 `p.type=="block_actions"`、恰一个 action、六段全非空且不含 `:`;**不看 value**。
- `channel = p.channel.id or p.container.channel_id`;`card_ts = p.container.message_ts or p.message.ts`。
- `interactive_fields(p)` 额外要求 `action_id ∈ ACTION_IDS`、value JSON 对象 `{pending_id, nonce, act}`、`act` 与 action_id 一致;→ dict 或 None(None → drain dropped("skipped"))。
- `events_fields(p)` 扁平化;`is_dm(ev)`;`is_self_event(ev, cfg)` 三组比较(bot_id / user↔bot_user_id / app_id)各要求**两边非空**;`is_bot_mentioned(ev, bot_user_id)` = app_mention ∨ rich_text user 元素 ∨ `<@BOT(\|[^>]*)?>`;`render_text` = 去 bot mention → `slack_unescape`(`&lt; &gt; &amp;`)→ strip。

### 4.3 listener payload(deliveries.payload_json;listener 加 `type` 与 `delivery_seq`)

```json
{"type":"slack_message","delivery_seq":12,"message_id":"C0CHAT:1700000000.000100","chat_id":"C0CHAT",
 "ts":"1700000000.000100","thread_ts":null,"sender_user_id":"U0MEMBER","sender_is_owner":false,
 "approved_by":"U0OWNER","message_type":"message","text":"look at this",
 "media_paths":["/…/media/<binding_id>/<channel>:<ts>/f01-F1-a.pdf"],
 "files":[{"id":"F1","name":"a.pdf","mimetype":"application/pdf","size":1234,"local_path":"/…/f01-F1-a.pdf"},
          {"id":"F2","name":"big.zip","mimetype":"application/zip","size":999999999,"skipped_reason":"too_large"}]}
```
`approved_by`:owner 本人 → null;白名单 → `"allowlist"`;审批 → 点击者 user id。`skipped_reason ∈ FILE_SKIP_REASONS`。控制行:`{"type":"farewell","code":…}` / `{"type":"daemon_alert","code":"daemon_down"}`。

### 4.4 Block Kit / metadata

- 审批卡 = `texts.build_approval_card(pending_id, nonce, sender_label, preview)` → JSON `{"text": fallback, "blocks": [section(header plain_text), section(preview plain_text 转义 ≤2900), actions(block_id "sb_actions:<pid>", 两个 button action_id sb_approve/sb_reject, value=JSON {pending_id,nonce,act} ≤2000B)]}`。job.body 存此 JSON;`_transmit` 解出 `blocks` + `text` 传 `chat.postMessage(channel, thread_ts, blocks, text, metadata)`。
- 决策更新 = `chat.update(channel, ts, blocks=texts.decision_update_blocks(outcome, decided_by[, preview]), text=texts.decision_update_text(outcome))`,**无按钮**;后写覆盖前写。
- metadata = `{"event_type":"slack_bridge","event_payload":{"job_id":"<job_id>"}}`。
- 核验读回:`msg.metadata.event_type / msg.metadata.event_payload.job_id`(`include_all_metadata=true`)。

### 4.5 `daemon_state` 键

| 键 | 值 / 写者 |
|---|---|
| `schema_version` | `"1"` |
| `daemon_pid daemon_started_at daemon_proc_start daemon_generation daemon_code_identity startup last_loop_at suspect_until last_error` | 继承 |
| `consumer_socket_ready` | `starting` \| `ready num_connections=N` \| `down`(ConsumerManager on_status) |
| `consumer_socket_last_status` `consumer_socket_restarts` `consumer_socket_last_exit_rc` | ConsumerManager |
| `outbound_gate` | `ok` \| `degraded:<reason>` \| `mismatch`(FingerprintGate) |
| `outbound_gate_tokens_version` | 与 `outbound_gate` **同事务**写;= 通过 auth.test 的凭据版本 |
| `tokens_version_seen` | FingerprintGate 最近看到的文件版本 |
| `verify_capability` | `unverified`(默认,schema 初始化写入)\| `ok` \| `degraded:<err>`(probe / verify 永久错 / gate 失效) |
| `verify_capability_tokens_version` | probe 写;daemon 只在 == 当前凭据版本时采信 `ok` |
| `cooldown:<method>` | 到期墙钟 ms(publish 取 max) |
| 计数 | `staged_dup staged_invalid inbox_dup_message inbox_snapshot_upgraded event_dropped_<reason> dm_notice_suppressed ratelimit_hits cooldown_waits verify_hit verify_absent verify_resent verify_unconfirmed drain_quarantined group_cancelled_after_unconfirmed media_budget_exhausted worker_unexpected_exit` + 继承的 `hook_drop_count event_processing_errors malformed_event_lines` |

---

## 5. 入站 / 交互 / 附件

### 5.1 入站分流(events_api,`ingest_in_tx` + `drive_pending_rows`)
- drop 序:`team_id≠cfg.team_id ∨ api_app_id≠cfg.app_id` → `event.type ∉ {message, app_mention}` → `is_self_event` → `subtype ∉ ACCEPT_SUBTYPES` → chat_allowlist → 预过滤(`binding_id is None ∧ 未提及 ∧ 非 DM`)→ 非 owner 且非终态 inbox ≥ INBOX_NONTERMINAL_CAP。
- INSERT inbox:`event_id, message_id=channel:ts, chat_id, binding_id=row.binding_id, sender_user_id, message_type=event.type, thread_ts, reply_thread_ts=thread_ts or ts, snapshot_json=事件本体, state='received'`。
- `_decide_in_tx`(本地):未提及 ∧ 非 DM → `ignored_not_mentioned`;`binding_id` NULL/终态 → `inbound_notice`(`is_dm ∧ sender≠owner` 时压制,计数 `dm_notice_suppressed`);`starting` → `waiting_binding`;`active` → `_gate_active_in_tx`。
- `_gate_active_in_tx`:`unsupported` 仅当文本空 ∧ 无 files ∧ 有 blocks/attachments;owner/白名单无 files → 入队;有 files → `materializing(reason=owner|allowlist)`;成员 → 配额 → pendings + approval_card(`reply_to=reply_thread_ts`);审批通过带 files → `materializing(reason=approved)`。

### 5.2 附件预算(四条路径统一在 `materializing` 列上,R3-P1)
- `drive_pending_rows` 按预算取 `materializing ∧ (materialize_next_at IS NULL OR ≤ now)`;**首次实际尝试**写 `materialize_started_at`。
- 瞬态失败(`materialize` 返回 None)→ `materialize_attempts+1`,`materialize_next_at = now + min(MEDIA_RETRY_BACKOFF_MS·2^n, MEDIA_RETRY_BACKOFF_MAX_MS)`。
- `now - materialize_started_at > MEDIA_RETRY_DEADLINE_MS` 或 `MediaError` → 终态:owner/allowlist → inbox `failed`(计数 `media_budget_exhausted`,静默);approved → inbox `failed` + `decision_notice(attachment_failed)`。
- 成功 → 事务内复验绑定 active → `_enqueue_in_tx(approved_by 按 reason)`;approved 再入队 `decision_notice(delivered)`。绑定非 active:approved → `undeliverable` + `decision_notice(closed_undelivered)`;其它 → 4.2.4 映射。
- 预算持久化在列上,重启不重置;`waiting_binding` 激活后进入同一路径。

### 5.3 `media.materialize`(WP2)

```python
def materialize(client_tokens, media_root, binding_id, message_id, files, deadline_s=constants.DOWNLOAD_DEADLINE_S,
                worker_path=None, log=None, clock=None) -> tuple:   # (paths: list[str], skipped: list[dict]) | None
```
- 返回 `(paths, skipped)`;瞬态失败 → `None`(走预算);确定性失败 → `raise MediaError`。
- `skipped` 元素 `{"id","name","skipped_reason"}`,reason ∈ `FILE_SKIP_REASONS`(`hidden_by_limit / tombstone / check_file_info / no_url / too_large`)。
- 每个文件在子进程 `bin/download_worker.py` 下载(§6);父进程持**绝对**截止时刻。

### 5.4 交互(`process_in_tx`)校验序
`interactive_fields` 非 None ∧ `team==cfg.team_id` → INSERT callback_events(dup → dropped) → pending 存在 → chat 在 allowlist → `hmac.compare_digest(nonce)` → `user==cfg.owner_user_id` → inbox 存在 → `channel==inbox.chat_id` → `card_message_id` 已回填则须 `== channel:card_ts`;**通过且未回填 → 回填 `card_message_id=channel:card_ts`,并把该 pending 的 card job 若处于 `unknown/pending/sending` CAS 为 `sent`(error `confirmed-by-click`)**;`card_ts` 非法 → dropped("invalid") 不回填 → CAS `pending→approved|rejected`(失败 → "late")→ approve:有 files → inbox `materializing(reason=approved)` + `decision_notice(approved_pending_files)`,否则 `_enqueue_in_tx(approved_by=user, create_receipt=False)` + `decision_notice(delivered)`;reject → inbox `rejected` + `decision_notice(rejected)`。

### 5.5 `decision_notice` 六种 outcome:键与守卫(R3-M6)

| outcome | idempotency_key | expected_state | `_guard_ok` | 入队点 |
|---|---|---|---|---|
| delivered | `dec:<pid>:delivered` | delivered | `pendings.state=='approved' ∧ inbox.state=='enqueued'` | approve 无 files;或物化成功入队后 |
| approved_pending_files | `dec:<pid>:approved_pending_files` | approved_pending_files | `pendings.state=='approved' ∧ inbox.state=='materializing'` | approve 有 files |
| rejected | `dec:<pid>:rejected` | rejected | `pendings.state=='rejected'` | reject |
| expired | `dec:<pid>:expired` | expired | `pendings.state=='expired'` | `_expire_pendings` / `_terminate_in_tx` |
| attachment_failed | `dec:<pid>:attachment_failed` | attachment_failed | `inbox.state=='failed'` | 预算耗尽 / MediaError(approved) |
| closed_undelivered | `dec:<pid>:closed_undelivered` | closed_undelivered | `inbox.state=='undeliverable'` | `_terminate_in_tx` |

守卫失败 → cancelled。传输 = `chat.update` 卡片(幂等,后写覆盖前写;同 chat 通知按 job_seq),卡片身份未知 → `chat.postMessage/text`(§2.2)。迟到/非 owner 点击只记录。

### 5.6 过期 vs 终止:两份不同范围的契约(R4-Q1)
- **审批过期**(`recovery._expire_pendings`,单条审批范围):对每条 TTL 到期且仍 `pending` 的审批:`pendings→expired`、其 inbox(`awaiting_approval`)→ `expired`、其**未发**的 `approval_card` job(pending/unknown)→ cancelled、入队 `decision_notice(expired)`。**不取消绑定的输出、不影响其它审批或正在物化的附件。**
- **绑定终止**(`lifecycle._terminate_in_tx`,绑定范围,调用方已开事务):① 取消该绑定的业务发送(session_turn、仍 pending 审批的 approval_card、receipt_reaction、未发的 `decision_notice(approved_pending_files)`)**但保留已决审批的 `delivered/rejected/attachment_failed` 更新**;② 仍 pending 的审批 → `expired` + `decision_notice(expired)`;③ `approved ∧ inbox.materializing` → inbox `undeliverable` + `decision_notice(closed_undelivered)`;④ deliveries enqueued → dropped;waiting_binding → 4.2.4 映射 + inbound_notice;lifecycle_notice(close)入队。全部新建 job 在取消步骤之后创建。

---

## 6. `bin/download_worker.py` 接口(R3-M10 / R4-Q2 / R6-m1)

- 父进程保存**绝对**截止时刻 `deadline_at = now + DOWNLOAD_DEADLINE_S`;传给 worker 的是 `timeout_s = 剩余秒数`(相对,>0)。
- stdin JSON:`{"url": str, "dest_tmp": str, "token": str, "max_bytes": int, "timeout_s": float}`(token 经 stdin,不上 argv)。
- stdout JSON(单行):`{"ok": bool, "nbytes": int|null, "content_type": str|null, "http_status": int|null, "error": str|null}`。
- worker 内:仅 https;`host ∈ FILE_HOSTS ∨ host.endswith(FILE_HOST_SUFFIXES)`;`Authorization: Bearer <token>`;**拒绝任何 3xx**;拒 `text/html`;边下边计数超 `max_bytes` 即中止;写入只落 `dest_tmp`;启动时 `signal.alarm(int(timeout_s)+WORKER_ALARM_SLACK_S)` → handler `os._exit(124)`;看门狗线程每秒 `os.getppid()==1` → `os._exit(125)`。
- 退出码:`0` 成功 / `2` 参数错 / `3` 永久拒绝(非 https、主机不在白名单、任何 3xx、text/html、超 max_bytes、HTTP 4xx 非 429)/ `4` 瞬态网络错(DNS、拒连、超时、连接中断)/ `5` HTTP 429 或 5xx / `124` 自身到期 / `125` 父亡。
- 父进程:`proc.wait(timeout=剩余)`,到期 SIGTERM → 2s → SIGKILL,视为瞬态。映射:`rc==0 ∧ JSON.ok ∧ os.path.getsize(dest_tmp)==nbytes` → 原子 rename 发布(**只有父进程可以发布**);`rc ∈ {2,3}` → `MediaError`;`rc ∈ {4,5,124,125}` 或被父杀 → None(瞬态,走预算);其它 rc → None 并计数 `worker_unexpected_exit`。孤儿 `.tmp-*` 由 `_retention` 清理。
- `DownloadResult(ok, nbytes, content_type, http_status, error, rc, permanent, path)` 为父进程内部表示。

---

## 7. 凭据契约(R2-M14 / R3-M3 / R5-S1)

- **文件是真相**:daemon 只信 `tokens.json`(`config.load_tokens(allow_env=False)`);env `SLACK_BOT_TOKEN/SLACK_APP_TOKEN` 仅 CLI/测试,版本恒 `"env"`,换 token 需重启。
- 版本 = `f"{mtime_ns}:{sha256(bytes)[:16]}"`;权限非 0600 → `ConfigError`(fail-closed)。
- `FingerprintGate.tick()` 每 tick `stat`(`config.tokens_mtime_ns`);版本变 → `client.reload_tokens(version)` → 立即 `auth.test` 核对 `team_id/user_id==bot_user_id/bot_id` → **同一事务**写 `outbound_gate` + `outbound_gate_tokens_version` + `tokens_version_seen`,并把 `verify_capability` 置回 `unverified`(除非 `verify_capability_tokens_version == 新版本`);身份漂移 → `mismatch` 关门;`degraded:auth_error` 退避重探。
- xapp(app_token)变化 → SIGTERM consumer,由 ConsumerManager 重拉(consumer 自读文件)。
- 直发(notify / StopFailure):读文件版本并要求 `== outbound_gate_tokens_version ∧ outbound_gate=="ok"`,否则 `not-sent: credentials-unverified`;冷却由 `SlackClient.call` 统一处理 → `not-sent: cooldown`。
- probe 例外(它就是验证步骤):自行 `auth.test` 核对身份,不一致 → 退出、不发消息、不导入结果。
- 出站两处复验(§2.4-4、§2.6 n==3):`verify_capability=="ok" ∧ verify_capability_tokens_version == client.tokens_version`(**`_prepare` 校验的快照必须就是 `_transmit` 实际使用的快照** —— 同一个 client 对象,不在两者之间 reload)。
- config 变更经 `ConfigSnapshot.refresh()` 原地更新;所有组件持**同一引用**(`Env.cfg` 亦然)。

---

## 8. 构造签名冻结(`tests/conftest.Env` 依赖;WP1–WP4 不得改动位置参数)

```python
Inbound(conn, cfg, client, clock, media_root, heartbeat=None, log=None)
Outbound(conn, cfg, client, clock, heartbeat=None, log=None)
Approval(conn, cfg, clock, inbound)
Recovery(conn, cfg, client, clock, inbound, prober)
DaemonCore(conn, cfg, clock, inbound, approval, outbound, recovery, log=None, gate=None)
FingerprintGate(conn, cfg, client, clock, notifier=None)          # WP4
ConsumerManager(clock, on_line, on_status, argv_builder, keys=(SOCKET_KEY,))   # WP1
ListenerCore(...) / InstanceFollower(...)                          # 不变
```
`cfg` = `config.ConfigSnapshot`(dict 子类);`client` = `slackapi.SlackClient` 或 `tests.helpers.FakeSlackClient`。
`Env.stage(type, payload)` 与 consumer 同写法;`Env.drain()` → `DaemonCore.drain_staging()`;`Env.click(...)` 生成 block_actions 并 stage。

---

## 9. consumer 协议(`bin/slack_consumer.py`,WP1)

- argv:`[cfg.consumer_python or sys.executable, <root>/bin/slack_consumer.py]`;stdin 保持打开(EOF = 退出);tokens 自读 `tokens.json`(`allow_env=False`);stdout **不承载数据**。
- `WebClient(token=app_token, retry_handlers=[])`;`SocketModeClient(app_token=app_token, web_client=web, auto_reconnect_enabled=True, ping_interval=10)`;`socket_mode_request_listeners.append(on_request)`;`connect()`;退出 `close()`。
- `on_request(client, req)`:每线程 `db.connect_short(CONSUMER_DB_BUSY_MS)`;`key=slackwire.event_key(req.type, req.payload)`(None → ack + 计数 staged_invalid);`chat=slackwire.chat_of(...)`;同事务 SELECT 钉 binding → `INSERT … ON CONFLICT DO NOTHING` → 提交 → `send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))`;锁超时/异常 → **不 ack**。
- stderr:`[socket] ready num_connections=N`(= `CONSUMER_READY_SENTINEL` 前缀)、`[socket] disconnect reason=…`、`[socket] fatal <code>`。退出码见 `CONSUMER_RC_*`;`_mark_exited` 对 rc ∈ `CONSUMER_SKIP_BACKOFF_RCS` 直接最大退避。看门狗:断连 > CONSUMER_DISCONNECT_EXIT_S → exit 0。

---

## 10. 能力探测(`scripts/capability_probe.py`,WP0 已实现)

- 输入:tokens(文件或 env)快照 + 版本;config.json;`--chat-id`;`--write-config`。
- 步骤:`auth.test` 核对身份(不符 → exit 3,不发消息不导入)→ 顶层 `chat.postMessage(markdown_text + metadata)`(`invalid_arguments` → `markdown_text_ok=false`,改 `text` 重发)→ `conversations.history(include_all_metadata)` 读回 → 线程内同形消息 → `conversations.replies(include_all_metadata)` 读回 → 两条 `chat.delete`。
- 输出 JSON:`{identity_ok, markdown_text_ok, metadata_history, metadata_replies, cleanup_ok, tokens_version, markdown_mode, verify_capability, errors}`;**绝不打印 token**。
- `--write-config`:`ConfigSnapshot.set_persist("markdown_mode", …)`;`daemon_state.verify_capability = ok | degraded:<err>` 与 `verify_capability_tokens_version = 快照版本`(bridge.db 不存在则建 schema)。
- 遵守冷却(`DaemonStateCooldownStore`,db 存在时);不受 `outbound_gate` 约束。退出码:0 完成 / 2 配置或凭据错 / 3 身份不符 / 4 探测未完成(API 失败/冷却)。

---

## 11. 守卫清单(`tests/test_contracts.py` 各条归属)

| 守卫 | 归属 |
|---|---|
| schema 表/列/CHECK、constants 名字与值、slackwire 行为、FakeSlackClient 行为、chunk 边界、CallResult 字段、texts 卡片/六 outcome、config/tokens、listener payload type、marker 前缀 | WP0(绿) |
| `DaemonCore.drain_staging` 存在;handed/dropped → consumed;异常 → staged+attempts+backoff;quarantine;`ConsumerManager` 单 key + ready 哨兵 | WP1 |
| `inbound.ingest_in_tx` 存在且不 BEGIN、返回映射;`approval.process_in_tx` 返回映射与 dup/invalid/late;`media.materialize` 返回 `(paths, skipped)`;`lifecycle._terminate_in_tx` closed_undelivered;`recovery._expire_pendings` 单条范围;`bin/download_worker.py` 存在 | WP2 |
| `outbound.op_for` 纯函数与 decision_notice 二选一;`startup_scan` 的 sending→unknown had_unknown=1;`OUTBOUND_TERMINAL_STATES` 不被 recovery 复活 | WP3 |
| `hooklib` 用 `chunk_text_with_footer`;`notify` credentials-unverified;`fingerprint` 同事务写 gate 版本 | WP4 |

---

## 12. 实现偏差记录(WP5 汇总;正文不改,偏差只在此登记)

各工作包报告的、与上文冻结文本或 plan 原文有出入但已被守卫测试接受的实现选择。改动语义前先看这里。

| WP | 偏差 | 现状 / 理由 |
|---|---|---|
| WP0 | `slackwire.event_key("interactive", …)` **不看按钮 value**(只用 team / channel / card_ts / user / action_id / action_ts 六段) | 去重键只由 Slack 自己给的标识组成;value 是我们写进卡片的 JSON,不参与键(§4.2 已按此写) |
| WP0 | 鉴权族错误(`not_authed / invalid_auth / account_inactive / token_revoked / token_expired / two_factor_setup_required / org_login_required`)放在 `NOT_SENT_ERRORS` 而非永久错 | 这些错误在消息创建之前就被拒 = 肯定未发送 → `not_sent`(走 transient 退避 / cap),门由 FingerprintGate 收;不按 PERMANENT 直接 failed |
| WP2 | `inbound.ingest_in_tx(conn, row)` / `approval.process_in_tx(conn, payload)` 只收 `(conn, row/payload)`,cfg / clock / inbound 经**模块级登记**(`register_defaults`,最近构造的 `Inbound` / `Approval` 实例)取得 | drain 只传 `(conn, row)`(§1 签名);daemon 内只有一个实例;未登记时 fail-closed(`process_in_tx` 抛错 → drain 退避/隔离,不静默判 undeliverable) |
| WP2 | `bin/download_worker.py` 看门狗判「父亡」的条件是 `getppid()==1 ∨ getppid()!=启动时的 ppid`(§6 只写了 `==1`) | 超集:macOS 上被 launchd 以外的进程收养也算父亡;仍 `os._exit(125)` |
| WP2 | `media.materialize(..., allow_plain_http_hosts=None)` 多一个仅测试用的旋钮(允许本地 `http://` 主机) | 只为 `tests/test_download_worker.py` 起本地 `http.server`;生产路径不传,worker 仍只认 https + 主机白名单 |
| WP3 | 出站告警(`send_failure_alert_body / unconfirmed_alert_body / group_cancelled_alert_body`)**只对 `session_turn`** 入队;approval_card / 各类 notice 失败不告警 | 卡片失败在 `status` 高亮;通知类固定文案不值得再发一条通知去说通知没发出去(告警自身失败也不再生告警) |
| WP3 | §2.6 error 分支的退避 `min(VERIFY_ERROR_BACKOFF_MS·2^ve, MAX)` 里的 `ve` 用**递增前**的 `verify_error_count`(首次出错退避 5s,而非 10s) | 首次错误不必等两档;cap 判定仍用递增后的值(`ve ≥ VERIFY_ERROR_CAP`) |
| WP3 | §2.6「永久错」拆成两组:`VERIFY_GLOBAL_DEGRADE_ERRORS`(missing_scope / invalid_auth / …)→ 本 job unconfirmed **且** `verify_capability=degraded:<err>`;`VERIFY_CHANNEL_ERRORS`(channel_not_found / not_in_channel / thread_not_found / …)→ 只本 job unconfirmed,**不动**能力;其余错误码 → error 分支 | 频道级错误不代表核验能力坏了,不应让别的频道的 job 失去自动重发资格 |
| WP4 | `FingerprintGate._apply` 在**每次**门判定(ok / degraded / mismatch)都写 `outbound_gate_tokens_version`(§4.5 写的是「= 通过 auth.test 的凭据版本」) | 版本键语义 = 「本次判定所绑定的凭据版本」;notify / StopFailure 的直发门仍要求 `outbound_gate=="ok" ∧ 版本相等`,degraded 时版本相等也不放行,语义不变 |
| WP4 | `outbound_gate` 多一个值 `mismatch`(§4.5 已列)—— 身份漂移与 `degraded:*` 区分 | mismatch 不退避重探(换回 token 或重新 bootstrap 才会变),daemon 启动遇到即拒启(rc 3) |
| WP4 | `ctl.bind_prepare` 对**任何** `D…` chat 都要求 `cfg.owner_dm_id` 已钉住且相等,否则 `foreign_dm` 拒绝(plan 只写「拒绝非 owner_dm_id 的 D…」) | 没跑过 `open-dm` 时 `owner_dm_id` 为空,此时任何 DM 都拒(fail-closed),而不是放行 |
| WP5 | daemon 的 xapp 变化探测只保留 `FingerprintGate.app_token_changed()` 一处(WP1 的 `AppTokenWatch` 已删) | 一个 stat、一个真相;主循环在 `core.loop_iteration()`(内含 gate.tick)之后读一次性信号并 `mgr.restart(SOCKET_KEY, "app_token_changed")` |
| R1-m2 | `util.chunk_text_with_footer`:末块装不下时,§2.1 写的「前 `limit-len(footer)` 字符留原位」只在 `len(footer) ≤ limit/2` 时能保证新末块 ≤ limit;页脚更长时(body=10 / footer=8 / limit=10 曾得 `[2,16]`)改为把**尾部**限制在 `limit-len(footer)` 字符、前段随之变长 | 「每块(含页脚的末块)≤ limit」是主不变量;页脚 ≤ limit/2 时行为与契约文本完全一致,守卫 `tests/test_contracts.py::test_chunk_text_with_footer_invariant_sweep` |
| R1-M7 | 附件落盘名 = `f<idx:02d>-<file_id>-<sanitized_name>`(`media.dest_name_for`),不再是「原名,同名加 `<i>-`」 | 旧去重会撞(`a.txt / 2-a.txt / a.txt` → 第三个与第二个同名 → worker O_EXCL → 永久 MediaError,整条消息含正文不投递);序号 + id 使同一消息内名字确定且两两不同,`seen` 循环再兜底。payload `files[].name` 仍是 Slack 原名,只有 `local_path` 的 basename 变了 |
