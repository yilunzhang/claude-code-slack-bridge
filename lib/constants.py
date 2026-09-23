"""slack-bridge 常量(plan 2026-09-23 R6 收敛版)。时间单位一律 ms,除非后缀 _S。

命名纪律:
- 本文件是 WP1–WP4 并行实现共享的**名字预声明**(plan「并行前提」),值可在各 WP 微调,名字不改。
"""

# ======================================================================
# 出站 chunk / 页脚
# ======================================================================
CHUNK_LIMIT = 12000              # 单条 chat.postMessage 正文上限(字符;Slack 硬限 40000,取保守值)
FOOTER_RESERVE = 256             # util.chunk_text_with_footer 为页脚预留的上限参考(页脚 > 此值仍按实际长度算)

# ======================================================================
# bind / 审批 / listener / daemon 节奏(继承 feishu-bridge plan v7)
# ======================================================================
PENDING_BIND_TTL_MS = 10 * 60 * 1000          # bind 握手 TTL
PENDING_TTL_MS = 6 * 3600 * 1000              # member 审批 pending TTL(单条审批范围,recovery._expire_pendings)

LISTENER_TICK_S = 2.0
HEARTBEAT_FRESH_MS = 6_000       # "新鲜心跳"判定(激活门 / 多余副本判定)
HEARTBEAT_GRACE_MS = 15_000      # 判死:心跳陈旧超此值进入 suspect
SUSPECT_CONFIRM_MS = 15_000      # suspect 持续超此值才判死(两阶段)
DAEMON_GAP_MS = 10_000           # daemon 循环间隔异常 gap 阈值(睡眠恢复检测)
SUSPECT_WINDOW_MS = 30_000       # gap 后的宽限窗:暂停 listener 判死
ACTIVATION_TIMEOUT_MS = 30_000   # confirmed starting 激活超时
LEASE_MS = 30_000                # deliveries 租约

RECOVERY_INTERVAL_MS = 60_000
DEATH_SCAN_INTERVAL_MS = 5_000
CHECKPOINT_INTERVAL_MS = 5 * 60 * 1000

# member 全链限速配额
MAX_UNDECIDED_PER_CHAT = 5
SENDER_COOLDOWN_MS = 30_000
NOTICE_COOLDOWN_MS = 60_000      # 未绑定/已关闭 chat 的提示回复冷却(per chat)
INBOX_NONTERMINAL_CAP = 500      # 非终态 inbox 总量配额(非 owner 消息受限)

# media
MEDIA_MSG_QUOTA_BYTES = 100 * 1024 * 1024   # 单 message 物化配额(全部文件之和)
MEDIA_FILE_MAX_BYTES = MEDIA_MSG_QUOTA_BYTES  # download_worker `max_bytes` 缺省
DOWNLOAD_DEADLINE_S = 90         # 单文件下载**绝对**截止(父进程持有;worker 收到的是剩余秒数)
MEDIA_RETRY_DEADLINE_MS = 10 * 60 * 1000     # materializing 预算:首次实际尝试起 10 分钟
MEDIA_RETRY_BACKOFF_MS = 10_000              # 瞬态失败退避基数(min(10s·2^n, 5min))
MEDIA_RETRY_BACKOFF_MAX_MS = 5 * 60 * 1000
MATERIALIZE_REASONS = ("owner", "allowlist", "approved")

# retention(终态行正文裁剪 / 终态 media TTL)
RETENTION_MS = 7 * 24 * 3600 * 1000
SLACK_EVENTS_RETENTION_MS = 24 * 3600 * 1000            # slack_events.consumed 保留 1 天
SLACK_EVENTS_QUARANTINE_RETENTION_MS = 30 * 24 * 3600 * 1000  # quarantined 保留 30 天(status 高亮)

# ======================================================================
# 传输 / 出站(Slack)
# ======================================================================
SLACK_API_BASE = "https://slack.com/api/"
SEND_TIMEOUT_S = 15              # SlackClient.call 缺省超时(连接+读)
SOCKET_OP_TIMEOUT_S = 10         # consumer 内 Socket Mode / DB 操作超时
CONSUMER_DB_BUSY_MS = 1_500      # consumer 每线程 connect_short(busy_ms);锁超时 → 不 ack
CONSUMER_DISCONNECT_EXIT_S = 120 # consumer 看门狗:断连超此秒数 → exit 0(由 ConsumerManager 重拉)
POST_MIN_INTERVAL_MS = 1_000     # postMessage 类每频道节流

# 结果分类 → 时序
VERIFY_SCHEDULE_MS = (5_000, 20_000, 60_000)  # unknown 后核验档位:首次 +5s;absent n=1 → +20s;n=2 → +60s
VERIFY_LOOKBACK_MS = 60_000      # 核验查询 oldest = sending_at - 60s
VERIFY_PAGE_LIMIT = 100
VERIFY_MAX_PAGES = 3
VERIFY_ERROR_CAP = 8             # 核验出错次数上限 → unconfirmed
VERIFY_ERROR_BACKOFF_MS = 5_000  # 核验出错退避 min(5s·2^ve, 60s)
VERIFY_ERROR_BACKOFF_MAX_MS = 60_000
VERIFY_DEADLINE_MS = 600_000     # 距本轮 sending_at 超 10 分钟仍 unknown → unconfirmed
VERIFY_ABSENT_RESEND_AT = 3      # 第三次**成功查询**未见才获得重发资格
RESEND_ONCE = True               # 能力确证后最多自动重发一次
RATELIMIT_CAP = 20               # ratelimit_count 上限 → had_unknown ? unconfirmed : failed
TRANSIENT_CAP = 5                # transient_count(not_sent)上限 → 同上
TRANSIENT_BACKOFF_MS = 5_000     # not_sent 退避 min(5s·2^tc, 45s)
TRANSIENT_BACKOFF_MAX_MS = 45_000
IDEMPOTENT_RETRY_DELAY_MS = 15_000  # 幂等类 unknown → 直接重发的延迟

# attempt cap(_prepare 事务内检查;postMessage 类到 cap 只核验不发送,幂等类到 cap → failed)
TURN_CAP = 6                     # session_turn
CARD_CAP = 3                     # approval_card
NOTICE_CAP = 3                   # lifecycle/inbound/unsupported/decision_notice(postMessage 形态)
IDEMPOTENT_CAP = 3               # chat.update / reactions.add

# 传输形态分类(op_method 决定类别;decision_notice 二选一由 op_for 决定)
POSTMESSAGE_METHODS = ("chat.postMessage",)
IDEMPOTENT_METHODS = ("chat.update", "reactions.add")
METADATA_EVENT_TYPE = "slack_bridge"          # metadata.event_type;event_payload = {"job_id": …}
RECEIPT_REACTION = "eyes"                     # 👀
PAYLOAD_KINDS = ("markdown_text", "text", "blocks", "reaction")
MARKDOWN_MODE_DEFAULT = "markdown_text"       # cfg.markdown_mode ∈ {"markdown_text","text"};probe 写入
MARKDOWN_REJECTED_ERROR = "markdown_rejected" # not_sent 子类:markdown_text 被 invalid_arguments 明确拒绝

OUTBOUND_TERMINAL_STATES = ("sent", "failed", "cancelled", "unconfirmed")

# Slack 错误字符串分类表(其余一切 → unknown;见 slackapi.classify_send_error)
PERMANENT_SEND_ERRORS = frozenset({
    "channel_not_found", "not_in_channel", "is_archived", "msg_too_long", "no_text",
    "invalid_blocks", "invalid_blocks_format", "invalid_arguments",  # invalid_arguments ∧ markdown_text → markdown_rejected(优先)
    "invalid_metadata_format", "invalid_metadata_schema", "metadata_too_large",
    "no_permission", "missing_scope", "ekm_access_denied", "restricted_action",
    "restricted_action_read_only_channel", "restricted_action_thread_only_channel",
    "restricted_action_non_threadable_channel", "team_access_not_granted", "user_is_bot",
    "cant_update_message", "message_not_found", "edit_window_closed", "thread_not_found",
    "too_many_reactions", "invalid_name", "bad_timestamp", "too_many_attachments",
    "duplicate_channel_not_found", "duplicate_message_not_found",
})
# 请求已抵达 Slack 但结果不确定(服务端内部错)→ unknown(只核验)
AMBIGUOUS_SEND_ERRORS = frozenset({
    "internal_error", "fatal_error", "service_unavailable", "request_timeout",
})
# 请求在消息创建之前就被拒(鉴权族)= 肯定未发送 → not_sent(transient_count;门由 FingerprintGate 收)
NOT_SENT_ERRORS = frozenset({
    "not_authed", "invalid_auth", "account_inactive", "token_revoked", "token_expired",
    "two_factor_setup_required", "org_login_required",
})
# 幂等类的"已达成"错误:reactions.add already_reacted = sent
ALREADY_DONE_ERRORS = frozenset({"already_reacted"})
# 核验(conversations.history / replies)的 ok:false 错误码分流(contracts §2.6「永久错」细分):
# 全局能力错 → 本 job unconfirmed 且 daemon_state.verify_capability = degraded:<err>;
# 频道级错 → 只本 job unconfirmed,**不动** verify_capability;其余任何错误码 → error 分支(退避 / cap)。
VERIFY_GLOBAL_DEGRADE_ERRORS = frozenset({
    "missing_scope", "invalid_auth", "not_authed", "account_inactive", "token_revoked",
    "token_expired", "no_permission", "not_allowed_token_type",
})
VERIFY_CHANNEL_ERRORS = frozenset({
    "channel_not_found", "not_in_channel", "is_archived", "thread_not_found", "message_not_found",
})

# ======================================================================
# 入站(events_api)
# ======================================================================
# message.subtype 允许集(None = 普通消息)。其余(message_changed/message_deleted/bot_message/
# channel_join/…)一律 dropped。
ACCEPT_SUBTYPES = frozenset({None, "file_share", "thread_broadcast"})
EVENT_TYPES_ACCEPTED = ("message", "app_mention")
# 附件下载主机白名单(download_worker:https ∧ (host ∈ FILE_HOSTS ∨ host.endswith(FILE_HOST_SUFFIXES)))
FILE_HOSTS = frozenset({"files.slack.com"})
FILE_HOST_SUFFIXES = (".slack.com",)
FILE_SKIP_REASONS = ("hidden_by_limit", "tombstone", "check_file_info", "no_url", "too_large")

# 白名单直投时 payload 的 approved_by 值。**不留 None** —— None 是 owner 本人的语义。
APPROVED_BY_ALLOWLIST = "allowlist"

# inbox 状态集合(schema.sql CHECK 同源)
INBOX_TERMINAL_STATES = (
    "ignored_not_mentioned", "unsupported", "rejected", "expired", "enqueued",
    "undeliverable", "failed", "unbound", "session_closed",
)
INBOX_NONTERMINAL_STATES = (
    "received", "resolving", "waiting_binding", "awaiting_approval", "materializing",
)

# close_reason → inbound_notice 终态映射
UNBOUND_CLOSE_REASONS = ("user_unbind", "bind_failed", "bind_timeout", "bind_superseded")
SESSION_CLOSED_REASONS = ("cc_gone", "session_end", "listener_gone", "listener_never_ready")

# ======================================================================
# 交互(block_actions)/ 审批
# ======================================================================
ACTION_APPROVE = "sb_approve"
ACTION_REJECT = "sb_reject"
ACTION_IDS = (ACTION_APPROVE, ACTION_REJECT)
CARD_PREVIEW_LIMIT = 2900        # section plain_text ≤ 3000,留余量
CARD_VALUE_MAX_BYTES = 2000      # button.value 上限(Slack 硬限 2000)
DECISION_OUTCOMES = ("delivered", "approved_pending_files", "rejected", "expired",
                     "attachment_failed", "closed_undelivered")

# ======================================================================
# consumer / drain / followup(daemon_core)
# ======================================================================
SOCKET_KEY = "socket"                          # 单一 consumer key(替代 feishu 的两个事件 key)
CONSUMER_READY_SENTINEL = "[socket] ready"     # consumer stderr 就绪哨兵(后跟 num_connections=N)
CONSUMER_RC_OK = 0                             # EOF / SIGTERM
CONSUMER_RC_TOKENS = 2                         # tokens.json 缺失/不可读/无 app_token
CONSUMER_RC_AUTH = 3                           # 致命鉴权(invalid_auth 等)
CONSUMER_RC_NO_SDK = 4                         # 缺 slack_sdk
CONSUMER_RC_OTHER = 5
CONSUMER_SKIP_BACKOFF_RCS = (CONSUMER_RC_AUTH, CONSUMER_RC_NO_SDK)  # _mark_exited 直接最大退避

DRAIN_BATCH = 50                 # 每 tick 取 staged ∧ next_drain_at 到期 的行数上限
DRAIN_MAX_ATTEMPTS = 5           # drain_attempts ≥ 此值 → quarantined
DRAIN_BACKOFF_MS = 2_000         # 业务抛异常退避 min(2s·2^n, 60s)
DRAIN_BACKOFF_MAX_MS = 60_000
FOLLOWUP_BUDGET_PER_TICK = (1, 5)   # (带下载的物化 ≤1 条, 纯文本 ≤5 条)每 tick
FOLLOWUP_BUDGET_DOWNLOAD = FOLLOWUP_BUDGET_PER_TICK[0]
FOLLOWUP_BUDGET_TEXT = FOLLOWUP_BUDGET_PER_TICK[1]

# download_worker 退出码(contracts §6)
WORKER_RC_OK = 0
WORKER_RC_ARGS = 2               # 参数错(永久)
WORKER_RC_PERMANENT = 3          # 非 https / 主机不在白名单 / 任何 3xx / text/html / 超 max_bytes / 4xx 非 429
WORKER_RC_TRANSIENT = 4          # DNS / 拒连 / 超时 / 连接中断
WORKER_RC_HTTP_RETRY = 5         # 429 或 5xx
WORKER_RC_DEADLINE = 124         # worker 自身到期(signal.alarm)
WORKER_RC_ORPHAN = 125           # 父亡(getppid()==1)
WORKER_ALARM_SLACK_S = 2         # alarm = timeout_s + 2

# daemon_state 键名(contracts §4.5)
VERIFY_CAPABILITY_KEY = "verify_capability"                    # unverified | ok | degraded:<err>
VERIFY_CAPABILITY_VERSION_KEY = "verify_capability_tokens_version"
GATE_KEY = "outbound_gate"
GATE_VERSION_KEY = "outbound_gate_tokens_version"
TOKENS_VERSION_SEEN_KEY = "tokens_version_seen"
COOLDOWN_KEY_PREFIX = "cooldown:"
VERIFY_CAP_UNVERIFIED = "unverified"
VERIFY_CAP_OK = "ok"

# ======================================================================
# 其它
# ======================================================================
MARKER_PREFIX = "[slack-bridge-bind:"
LOG_MAX_BYTES = 5 * 1024 * 1024

# busy_timeout(有界;I5)
BUSY_TIMEOUT_DAEMON_MS = 5_000
BUSY_TIMEOUT_HOOK_MS = 3_000
BUSY_TIMEOUT_SESSION_END_MS = 1_500
BUSY_TIMEOUT_OBS_MS = 300
BUSY_TIMEOUT_LISTENER_MS = 3_000

SCHEMA_VERSION = "1"
