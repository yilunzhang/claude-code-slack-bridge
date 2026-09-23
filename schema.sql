-- feishu-bridge bridge.db schema (plan v7 §3) — schema_version 1
-- 原样建库:所有状态列 NOT NULL + CHECK;部分唯一索引承载 1:1 不变量。
-- 注意:foreign_keys 必须每连接 PRAGMA 开启(SQLite 不持久化该设置)。

CREATE TABLE bindings(
  binding_seq INTEGER PRIMARY KEY AUTOINCREMENT,      -- "最新绑定"一律 ORDER BY binding_seq DESC
  binding_id TEXT NOT NULL UNIQUE,
  chat_id TEXT NOT NULL,
  chat_name TEXT,
  session_id TEXT,          -- starting 期为 NULL(F8:skill 侧无 session_id),Stop 确认事务回填
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

CREATE TABLE inbox(
  inbox_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  message_id TEXT NOT NULL UNIQUE,
  chat_id TEXT NOT NULL,
  binding_id TEXT REFERENCES bindings(binding_id),
  -- 首次落库钉死"该 chat 按 binding_seq 最新的一条绑定(含终态)";NULL=该 chat 从无绑定史。
  -- 重驱只复验此行,绝不重解析当前绑定。
  sender_open_id TEXT,
  sender_type TEXT,
  message_type TEXT,
  snapshot_json TEXT,
  state TEXT NOT NULL CHECK(state IN ('received','resolving','ignored_not_mentioned','unsupported',
    'waiting_binding','awaiting_approval','approved_materializing','rejected','expired',
    'enqueued','undeliverable','failed','unbound','session_closed')),
  ts INTEGER
);
-- unbound / session_closed = 终态,inbound_notice 的守卫引用;区分依据=钉死绑定的 status+close_reason:
--   无绑定史 / close_reason∈{user_unbind,bind_failed,bind_timeout,bind_superseded} → unbound("此群未绑定")
--   dead 或 close_reason∈{cc_gone,session_end,listener_gone,listener_never_ready} → session_closed("session 已关闭")
--   (映射的权威来源=lib/constants.py 的 UNBOUND_CLOSE_REASONS / SESSION_CLOSED_REASONS)

CREATE TABLE pendings(
  pending_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL UNIQUE REFERENCES inbox(message_id),
  binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
  nonce TEXT NOT NULL,
  card_message_id TEXT,
  state TEXT NOT NULL CHECK(state IN ('pending','approved','rejected','expired')),
  decided_by TEXT,
  decided_event_id TEXT UNIQUE,
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
  job_id TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL CHECK(kind IN ('session_turn','approval_card','decision_notice',
    'lifecycle_notice','receipt_reaction','unsupported_notice','inbound_notice')),
  binding_id TEXT REFERENCES bindings(binding_id),
  chat_id TEXT NOT NULL,
  reply_to TEXT,
  ref_pending_id TEXT,
  ref_delivery_seq INTEGER,
  ref_message_id TEXT,
  expected_state TEXT,
  turn_group TEXT,
  chunk_index INTEGER,
  body TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  -- 确定性逻辑键(存本列,承载重驱幂等的 UNIQUE 语义;S4 已证):
  -- turn:<turn_group>:<chunk> | card:<pending_id> | dec:<pending_id>:<终态>
  -- lc:<binding_id>:<transition> | rc:<delivery_seq> | un:<message_id> | notice:<message_id>:<code>
  -- 注意(E4b):传给 lark-cli --idempotency-key 的是**逻辑键经 util.short_key() 哈希后的短键**
  --   (fb:<sha1[:32]>,≤40 字符;飞书 uuid 参数上限 ~50)——本列存可读逻辑键,wire 传短键。
  state TEXT NOT NULL CHECK(state IN ('pending','sending','sent','unknown','failed','cancelled')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sending_at INTEGER,
  next_attempt_at INTEGER,
  sent_message_id TEXT,
  error TEXT,
  created_at INTEGER,
  sent_at INTEGER
);
CREATE UNIQUE INDEX oj_chunk ON outbound_jobs(turn_group, chunk_index) WHERE turn_group IS NOT NULL;

CREATE TABLE callback_events(
  event_id TEXT PRIMARY KEY,
  seen_at INTEGER
);
-- 仅"无效/重复"回调裸去重;有效回调的 event_id 插入必须与 CAS/入队/通知同一事务

CREATE TABLE pending_bind(
  request_id TEXT PRIMARY KEY,
  chat_id TEXT NOT NULL,
  cwd TEXT,
  cc_pid INTEGER NOT NULL,
  cc_start TEXT NOT NULL,
  nonce TEXT NOT NULL,      -- CSPRNG 固定长度;完整 marker 解析非 substring
  state TEXT NOT NULL CHECK(state IN ('pending','consumed','failed','expired')),
  latch_open INTEGER NOT NULL DEFAULT 0,  -- bind-turn 抑制链闩(4.7):消费时置 1,下一个 fresh Stop 置 0
  created_at INTEGER,
  expires_at INTEGER
);
CREATE UNIQUE INDEX pb_inst ON pending_bind(cc_pid, cc_start) WHERE state='pending';
CREATE INDEX pb_lookup ON pending_bind(cc_pid, cc_start);  -- tombstone/latch 热路径
-- 行不删除:终态保留=bind-turn 抑制 tombstone;SessionEnd/超时/强关终态化(并关闩),不留永久占位

CREATE TABLE daemon_state(key TEXT PRIMARY KEY, value TEXT);
-- consumer 就绪/重启计数/最近错误/schema_version/hook 丢弃计数镜像/outbound_gate;
-- 存活与就绪**分义**(r4-1):last_loop_at=心跳(仅表进程活着,多点刷新);
--   daemon_generation + startup='<probing|running|degraded|refused>:<gen>'=就绪态
--   (就绪 = 心跳新鲜 ∧ startup∈{running,degraded} ∧ 同代;daemon_pid/daemon_proc_start=挂死接管精确匹配)

INSERT INTO daemon_state(key, value) VALUES('schema_version', '1');
