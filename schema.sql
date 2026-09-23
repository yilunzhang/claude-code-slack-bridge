-- slack-bridge bridge.db schema — schema_version 1(新库,不做迁移;继承 feishu-bridge plan v7 §3)
-- 原样建库:所有状态列 NOT NULL + CHECK;部分唯一索引承载 1:1 不变量。
-- 注意:foreign_keys 必须每连接 PRAGMA 开启(SQLite 不持久化该设置)。
-- 故障模型(I6):consumer 只在 slack_events 行提交后才 ack(应用进程崩溃安全,不承诺断电);
-- drain 是唯一开事务者;业务 *_in_tx 不 BEGIN。

CREATE TABLE bindings(
  binding_seq INTEGER PRIMARY KEY AUTOINCREMENT,      -- "最新绑定"一律 ORDER BY binding_seq DESC
  binding_id TEXT NOT NULL UNIQUE,
  chat_id TEXT NOT NULL,                              -- Slack channel id(C…/G…)或 owner DM(D…)
  chat_name TEXT,
  session_id TEXT,          -- starting 期为 NULL(skill 侧无 session_id),Stop 确认事务回填
  cc_pid INTEGER NOT NULL,
  cc_start TEXT NOT NULL,
  cwd TEXT,
  status TEXT NOT NULL CHECK(status IN ('starting','active','dead','closed')),
  bind_phase TEXT NOT NULL DEFAULT 'unconfirmed' CHECK(bind_phase IN ('unconfirmed','confirmed')),
  confirmed_at INTEGER,
  listener_pid INTEGER,
  listener_start TEXT,
  listener_epoch INTEGER NOT NULL DEFAULT 0,
  listener_beat_at INTEGER,
  suspect_since INTEGER,
  bound_at INTEGER,
  closed_at INTEGER,
  close_reason TEXT,
  CHECK(status!='active' OR session_id IS NOT NULL)   -- 激活门(真约束)
);
CREATE UNIQUE INDEX b_chat ON bindings(chat_id)    WHERE status IN ('starting','active');
CREATE UNIQUE INDEX b_sess ON bindings(session_id) WHERE status IN ('starting','active') AND session_id IS NOT NULL;
CREATE UNIQUE INDEX b_inst ON bindings(cc_pid, cc_start) WHERE status IN ('starting','active');
-- 实例级 1:1 由 b_inst 在 starting INSERT 时原子保证;session 级由 b_sess 在回填时保证(冲突→close+失败通知)

-- ---------------------------------------------------------------------------
-- slack_events:Socket Mode consumer 的持久化暂存区(consumer 写 → daemon drain 读)。
--   event_key:events_api = 'ev:<event_id>';interactive = 'act:<team>:<channel>:<card_ts>:<user>:<action_id>:<action_ts>'
--     (consumer 与审批共用 lib/slackwire.event_key;任一段缺失 → consumer ack 并丢弃,计数 staged_invalid)。
--   binding_id:**接收时**钉死 = chat_id 当时最新的 starting/active 绑定(ORDER BY binding_seq DESC LIMIT 1)或 NULL,
--     consumer 同事务 SELECT 后写入;重复事件保留首个(INSERT … ON CONFLICT(event_key) DO NOTHING)。
--   state:staged(待 drain)→ consumed(ingest/process 返回 handed 或 dropped;dropped 的 reason 记入 error)
--     / quarantined(drain_attempts ≥ DRAIN_MAX_ATTEMPTS;保留 payload+error,status 高亮)。
--   业务抛异常 → 事务回滚、仍 staged、drain_attempts+1、next_drain_at = now + min(2s·2^n, 60s)。
CREATE TABLE slack_events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  envelope_type TEXT NOT NULL CHECK(envelope_type IN ('events_api','interactive')),
  event_key TEXT NOT NULL UNIQUE,
  chat_id TEXT,
  binding_id TEXT,                                    -- 有意不设外键:NULL/已终态绑定都合法,drain 按 unbound 处理
  payload_json TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'staged' CHECK(state IN ('staged','consumed','quarantined')),
  drain_attempts INTEGER NOT NULL DEFAULT 0,
  next_drain_at INTEGER,
  error TEXT,
  consumed_at INTEGER
);
CREATE INDEX se_drain ON slack_events(state, next_drain_at, seq);

-- ---------------------------------------------------------------------------
CREATE TABLE inbox(
  inbox_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,                      -- Slack event_id(Ev…);app_mention 与 message 双投 = 两个 event_id、同一 message_id
  message_id TEXT NOT NULL UNIQUE,                    -- 'channel:ts'(util.message_id_of)
  chat_id TEXT NOT NULL,
  binding_id TEXT REFERENCES bindings(binding_id),
  -- = slack_events.binding_id(接收时钉死);NULL = 到达时无 starting/active 绑定 → 按 unbound 处理,不查 latest。
  -- 重驱只复验此行,绝不重解析当前绑定。
  sender_user_id TEXT,                                -- event.user
  sender_type TEXT,                                   -- 'user'|'bot'(保留列;Slack 自身回流在 ingest 已过滤)
  message_type TEXT,                                  -- event.type:'message'|'app_mention'
  thread_ts TEXT,                                     -- event.thread_ts(顶层消息为 NULL)
  reply_thread_ts TEXT,                               -- = thread_ts or ts;出站 reply_to / approval_card thread_ts 的来源
  snapshot_json TEXT,                                 -- **插入时即写**:= 事件本体(无 mget);双投升级规则见 inbound.ingest_in_tx
  state TEXT NOT NULL CHECK(state IN ('received','resolving','ignored_not_mentioned','unsupported',
    'waiting_binding','awaiting_approval','materializing','rejected','expired',
    'enqueued','undeliverable','failed','unbound','session_closed')),
  -- materializing(单一状态替代 approved_materializing)= 带附件、待物化;四条路径统一预算在下面四列上:
  materialize_reason TEXT CHECK(materialize_reason IS NULL OR materialize_reason IN ('owner','allowlist','approved')),
  materialize_started_at INTEGER,                     -- **首次实际下载尝试**时写(不是消息到达时);预算 = now - 此值 > MEDIA_RETRY_DEADLINE_MS
  materialize_attempts INTEGER NOT NULL DEFAULT 0,
  materialize_next_at INTEGER,                        -- 瞬态失败退避 now + min(10s·2^n, 5min);drive_pending_rows 只取到期行
  ts INTEGER
);
-- unbound / session_closed = 终态,inbound_notice 的守卫引用;区分依据=钉死绑定的 status+close_reason:
--   无绑定史 / close_reason∈{user_unbind,bind_failed,bind_timeout,bind_superseded} → unbound("此会话未绑定")
--   dead 或 close_reason∈{cc_gone,session_end,listener_gone,listener_never_ready} → session_closed("session 已关闭")
--   (映射的权威来源=lib/constants.py 的 UNBOUND_CLOSE_REASONS / SESSION_CLOSED_REASONS)
-- undeliverable:approved ∧ materializing 期间绑定终止 → 入队 decision_notice(closed_undelivered)。

CREATE TABLE pendings(
  pending_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL UNIQUE REFERENCES inbox(message_id),
  binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
  nonce TEXT NOT NULL,
  card_message_id TEXT,                               -- 'channel:ts' 卡片身份;approval_card sent 回填,或 owner 点击时回填(confirmed-by-click)
  state TEXT NOT NULL CHECK(state IN ('pending','approved','rejected','expired')),
  decided_by TEXT,
  decided_event_id TEXT UNIQUE,                       -- = callback_events.event_id = slack_events.event_key('act:…')
  created_at INTEGER,
  decided_at INTEGER
);

CREATE TABLE deliveries(
  delivery_seq INTEGER PRIMARY KEY AUTOINCREMENT,   -- 领取按此序
  binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
  message_id TEXT NOT NULL REFERENCES inbox(message_id),
  payload_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('enqueued','leased','emitted','dropped')),
  lease_pid INTEGER,
  lease_start TEXT,
  lease_token TEXT,
  lease_epoch INTEGER,
  lease_until INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  enq_at INTEGER,
  emitted_at INTEGER,
  UNIQUE(binding_id, message_id)
);
-- emitted="已写入 Monitor stdout 管道"(诚实语义;到模型=at-least-once,payload 带 message_id 跳重)

CREATE TABLE outbound_jobs(
  job_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL UNIQUE,                        -- 进 metadata.event_payload.job_id(核验的精确关联标识;重发后不变)
  kind TEXT NOT NULL CHECK(kind IN ('session_turn','approval_card','decision_notice',
    'lifecycle_notice','receipt_reaction','unsupported_notice','inbound_notice')),
  binding_id TEXT REFERENCES bindings(binding_id),
  chat_id TEXT NOT NULL,
  reply_to TEXT,                                      -- = inbox.reply_thread_ts(approval_card / decision_notice 的 thread_ts)
  ref_pending_id TEXT,
  ref_delivery_seq INTEGER,
  ref_message_id TEXT,                                -- receipt_reaction:= event.ts(reactions.add timestamp);notice 类:inbox.message_id
  expected_state TEXT,                                -- decision_notice:outcome(六种之一);其余同 feishu-bridge
  turn_group TEXT,
  chunk_index INTEGER,
  body TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  -- 确定性逻辑键,**仅本地唯一**(Slack chat.postMessage 无幂等键;重驱幂等由本列 UNIQUE 承载):
  -- turn:<turn_group>:<chunk> | card:<pending_id> | dec:<pending_id>:<outcome>
  -- lc:<binding_id>:<transition> | rc:<delivery_seq> | un:<message_id> | notice:<message_id>:<code>
  state TEXT NOT NULL CHECK(state IN ('pending','sending','sent','unknown','failed','cancelled','unconfirmed')),
  -- unconfirmed = 诚实终态:postMessage 结果不确定且核验未能确证(见 docs/contracts.md §2)
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sending_at INTEGER,
  next_attempt_at INTEGER,
  sent_message_id TEXT,                               -- 'channel:ts'
  error TEXT,
  created_at INTEGER,
  sent_at INTEGER,
  -- ---- Slack 出站状态机新增(plan「出站状态机」/ R5-S2 / R6-m2)----
  verify_after INTEGER,                               -- unknown 的下次核验时刻(postMessage 类)
  verify_absent_count INTEGER NOT NULL DEFAULT 0,     -- 核验**成功查询**未命中次数(n==3 才获重发资格)
  verify_error_count INTEGER NOT NULL DEFAULT 0,      -- 核验出错次数(≥VERIFY_ERROR_CAP → unconfirmed);与 absent 分开持久化
  verify_round INTEGER NOT NULL DEFAULT 0,            -- **只在**"获重发资格 → pending"的那次 CAS 里 +1,并把两个计数与档位归零
  resend_count INTEGER NOT NULL DEFAULT 0,            -- 已自动重发次数(RESEND_ONCE ⇒ ≤1)
  ratelimit_count INTEGER NOT NULL DEFAULT 0,         -- ≥RATELIMIT_CAP → had_unknown ? unconfirmed : failed
  transient_count INTEGER NOT NULL DEFAULT 0,         -- not_sent 次数;≥TRANSIENT_CAP → 同上(冷却 wait 不计)
  had_unknown INTEGER NOT NULL DEFAULT 0,             -- 一旦 1 永不清零:此后任何 failed → unconfirmed,任何重试 op_* 必须完全相同
  op_method TEXT,                                     -- pending→sending 同事务原子冻结:chat.postMessage | chat.update | reactions.add
  op_target TEXT,                                     -- channel(postMessage/reactions)或 card 'channel:ts'(chat.update)
  op_thread_ts TEXT,                                  -- postMessage thread_ts(NULL = 顶层)
  op_payload_kind TEXT                                -- markdown_text | text | blocks | reaction(只有 had_unknown=0 ∧ markdown_rejected 允许改)
);
CREATE UNIQUE INDEX oj_chunk ON outbound_jobs(turn_group, chunk_index) WHERE turn_group IS NOT NULL;
CREATE INDEX oj_tick ON outbound_jobs(state, next_attempt_at, verify_after, job_seq);

CREATE TABLE callback_events(
  event_id TEXT PRIMARY KEY,                          -- = slack_events.event_key('act:…')
  seen_at INTEGER
);
-- 仅"无效/重复"回调裸去重;有效回调的 event_id 插入必须与 CAS/入队/通知同一事务(drain 持有的事务)

CREATE TABLE pending_bind(
  request_id TEXT PRIMARY KEY,
  chat_id TEXT NOT NULL,
  cwd TEXT,
  cc_pid INTEGER NOT NULL,
  cc_start TEXT NOT NULL,
  nonce TEXT NOT NULL,      -- CSPRNG 固定长度;完整 marker 解析非 substring
  state TEXT NOT NULL CHECK(state IN ('pending','consumed','failed','expired')),
  latch_open INTEGER NOT NULL DEFAULT 0,  -- bind-turn 抑制链闩:消费时置 1,下一个 fresh Stop 置 0
  created_at INTEGER,
  expires_at INTEGER
);
CREATE UNIQUE INDEX pb_inst ON pending_bind(cc_pid, cc_start) WHERE state='pending';
CREATE INDEX pb_lookup ON pending_bind(cc_pid, cc_start);  -- tombstone/latch 热路径
-- 行不删除:终态保留=bind-turn 抑制 tombstone;SessionEnd/超时/强关终态化(并关闩),不留永久占位

CREATE TABLE daemon_state(key TEXT PRIMARY KEY, value TEXT);
-- daemon_state 键(权威清单见 docs/contracts.md §4.5;计数键只增):
--   schema_version
--   存活/就绪(继承):daemon_pid daemon_started_at daemon_proc_start daemon_generation daemon_code_identity
--     startup='<probing|running|degraded|refused|stopping>:<gen>' last_loop_at suspect_until last_error
--   consumer(单一 key 'socket'):consumer_socket_ready('starting'|'ready …'|'down') consumer_socket_last_status
--     consumer_socket_restarts consumer_socket_last_exit_rc
--   凭据/门:outbound_gate('ok'|'degraded:<reason>'|'mismatch') outbound_gate_tokens_version tokens_version_seen
--     verify_capability('unverified'(默认)|'ok'|'degraded:<err>') verify_capability_tokens_version
--     (daemon 只在 verify_capability_tokens_version == 当前凭据版本时采信 ok)
--   冷却:cooldown:<method> = 到期墙钟 ms(publish 取 max;SlackClient.call 前读)
--   计数:staged_dup staged_invalid inbox_dup_message inbox_snapshot_upgraded event_dropped_<reason>
--     dm_notice_suppressed ratelimit_hits cooldown_waits verify_hit verify_absent verify_resent verify_unconfirmed
--     drain_quarantined group_cancelled_after_unconfirmed media_budget_exhausted worker_unexpected_exit
--     hook_drop_count event_processing_errors malformed_event_lines(继承)

INSERT INTO daemon_state(key, value) VALUES('schema_version', '1');
INSERT INTO daemon_state(key, value) VALUES('verify_capability', 'unverified');
