"""feishu-bridge 常量(plan v7)。时间单位一律 ms,除非后缀 _S。"""
import re

# 出站 chunk 阈值(S9:20k CJK 单条 OK,阈值取 12000 字符)
CHUNK_LIMIT = 12000

# bind 握手 TTL(plan 4.1.4)
PENDING_BIND_TTL_MS = 10 * 60 * 1000

# member 审批 pending TTL(plan 未定数值;v1 取 6h)
PENDING_TTL_MS = 6 * 3600 * 1000

# listener 心跳节奏与新鲜度
LISTENER_TICK_S = 2.0
HEARTBEAT_FRESH_MS = 6_000       # "新鲜心跳"判定(激活门 / 多余副本判定)
HEARTBEAT_GRACE_MS = 15_000      # 判死:心跳陈旧超此值进入 suspect
SUSPECT_CONFIRM_MS = 15_000      # suspect 持续超此值才判死(两阶段)
DAEMON_GAP_MS = 10_000           # daemon 循环间隔异常 gap 阈值(睡眠恢复检测)
SUSPECT_WINDOW_MS = 30_000       # gap 后的宽限窗:暂停 listener 判死

# confirmed starting 激活超时(plan 4.1.6:超 30s 无新鲜心跳 → listener_never_ready)
ACTIVATION_TIMEOUT_MS = 30_000

# deliveries 租约
LEASE_MS = 30_000

# 入站快照/物化的有限重试(时间上限,由恢复工人驱动)
RESOLVE_DEADLINE_MS = 10 * 60 * 1000
MATERIALIZE_DEADLINE_MS = 10 * 60 * 1000

# member 全链限速配额(plan 4.2.7 / §5)
MAX_UNDECIDED_PER_CHAT = 5
SENDER_COOLDOWN_MS = 30_000
NOTICE_COOLDOWN_MS = 60_000      # 未绑定/已关闭 chat 的提示回复冷却(per chat)
INBOX_NONTERMINAL_CAP = 500      # 非终态 inbox 总量配额(非 owner 消息受限)

# media
MEDIA_MSG_QUOTA_BYTES = 100 * 1024 * 1024   # 单 message 物化配额

# retention(终态行正文裁剪 / 终态 media TTL)
RETENTION_MS = 7 * 24 * 3600 * 1000

# 出站
SEND_TIMEOUT_S = 30
MGET_TIMEOUT_S = 30
DOWNLOAD_TIMEOUT_S = 120
UNKNOWN_RETRY_DELAY_MS = 15_000
MAX_SEND_ATTEMPTS = 2            # 首发 + unknown 同 key 自动重试一次(非 retryable / 非 session_turn)
OUTBOUND_BATCH = 20
# session_turn retryable 硬化(2026-07-18 事故根因修复):飞书后端 503/网络抖动窗可持续
# 数分钟,而旧 15s 窗太薄 → 转发被静默丢弃 + 队头阻塞整群。发送带 idempotency-key(服务端
# 去重)→ 可安全持久重试。仅 session_turn 生效;命名对齐 CARD_REARM_*;退避公式镜像 recovery.py。
TURN_RETRYABLE_MAX_ATTEMPTS = 6       # 含首发;retryable session_turn 尝试上限(~2.4min 总窗)
TURN_RETRY_BACKOFF_MS = 8_000         # 指数退避基数
TURN_RETRY_BACKOFF_MAX_MS = 45_000    # 退避封顶

# daemon 节奏
RECOVERY_INTERVAL_MS = 60_000
DEATH_SCAN_INTERVAL_MS = 5_000
CHECKPOINT_INTERVAL_MS = 5 * 60 * 1000

# bind marker
MARKER_PREFIX = "[feishu-bridge-bind:"

# 日志
LOG_MAX_BYTES = 5 * 1024 * 1024

# busy_timeout(有界;I5)
BUSY_TIMEOUT_DAEMON_MS = 5_000
BUSY_TIMEOUT_HOOK_MS = 3_000
# minor③:SessionEnd 关绑定 best-effort(daemon cc_gone 兜底)→ 更短等锁,锁竞争时快速让路。
BUSY_TIMEOUT_SESSION_END_MS = 1_500
# minor②:可观测计数(hook_drop_count)用极短等锁 —— 拿不到就算了,绝不叠加拖住 CC 退出。
BUSY_TIMEOUT_OBS_MS = 300
BUSY_TIMEOUT_LISTENER_MS = 3_000

SCHEMA_VERSION = "1"

# inbox 终态集合(禁 TTL 删 media 的判定等)
INBOX_TERMINAL_STATES = (
    "ignored_not_mentioned", "unsupported", "rejected", "expired", "enqueued",
    "undeliverable", "failed", "unbound", "session_closed",
)
INBOX_NONTERMINAL_STATES = (
    "received", "resolving", "waiting_binding", "awaiting_approval",
    "approved_materializing",
)

# close_reason → inbound_notice 终态映射(§3/4.2.4/4.8;r7-①:bind_timeout→unbound;
# bind_superseded=同实例 rebind 自愈关闭旧 starting,归"未绑定"类)
UNBOUND_CLOSE_REASONS = ("user_unbind", "bind_failed", "bind_timeout", "bind_superseded")
SESSION_CLOSED_REASONS = ("cc_gone", "session_end", "listener_gone", "listener_never_ready")

# 出站错误分类表(修复项4;集合可维护:权限/成员关系/能力/目标不存在=永久→failed,
# 频控/令牌自刷类=瞬态→unknown(同 key 自动重试仍≤1 次);未知 code→unknown 留人工)
PERMANENT_SEND_CODES = frozenset({
    230002,    # bot/user 不在群(成员关系)
    230013,    # bot 能力未启用
    99991672,  # app 缺权限 scope
    230099,    # 回复目标消息不存在/已撤回
    99992402,  # field validation failed(参数校验;E4 真机实锤,含幂等键超长)
})
TRANSIENT_SEND_CODES = frozenset({
    230020,    # 请求频控
    99991661,  # tenant access token 失效(CLI 自刷新)
    99991663,  # app access token 失效(CLI 自刷新)
})

# session_turn 持久重试的**无显式字段回退**允许集:仅当 lark-cli 未给出 error.retryable 时才用。
# 只放官方明确可重试且值得持久退避的(频控);**故意不含 99991661/99991663 token 类** —— 官方
# 契约标其不可重试(见 notify 契约),放进来会把认证失败当瞬态刷 6 次 + 误导告警(codex MAJOR-2)。
# 有显式 error.retryable 时该字段优先,本集不参与(见 Outbound._is_retryable)。
RETRYABLE_FALLBACK_CODES = frozenset({
    230020,    # 请求频控
})

# approval_card 重臂(修复项3):failed → pending 退避重臂,总尝试上限
CARD_REARM_MAX_ATTEMPTS = 5
CARD_REARM_BACKOFF_MS = 30_000
CARD_REARM_BACKOFF_MAX_MS = 10 * 60 * 1000

# 版本漂移自愈的自检群(v1.5.0)。**硬编码常量,不做成配置项**:个人部署就一个靶子群,
# 加 config key 是过度设计(codex plan r1 CUT)。**绝不回退到工作群** —— 那会在日常群里留下
# "发了又撤回"的痕迹。自检消息留在此群即可(已有纪律:e2e 测试消息无需善后)。
SELFCHECK_CHAT_ID = "oc_ef148370df62f0a61e113731c6c50eb3"   # feishu-bridge e2e 测试群(勿动)

SUPPORTED_MSG_TYPES = ("text", "image", "file", "post")
MEDIA_MSG_TYPES = ("image", "file")
# 白名单直投时 payload 的 approved_by 值。**不留 None** —— None 是 owner 本人的语义,
# 混用会让 agent 把白名单成员的消息当成 owner 本人发的(信任级别不同)。
APPROVED_BY_ALLOWLIST = "allowlist"

# 附件句柄(image_key / file_key)—— 从正文里认出可自取的资源 key。
# 形如 `img_v3_02143_6ea09006-e575-47a2-89e7-4a683ace737g`(真机实测),也有不带版本号的
# `img_…`/`file_…`。**故意放宽**:**漏判会重现原 bug(agent 看不见附件),误判只是多一条
# 取不到的提示** —— 不对称,往宽取。故**不写 `(?:v\d+_)?` 那样的版本号组** —— 它匹配的字符
# 全在后面的宽字符类里 = 死重,留着只会让人误以为它在钉版本号格式(codex impl r2 实证:
# 把 `v\d+` 削成 `v\d` 全套仍绿,因为宽后缀本就吃得下 `v12_`)。
# key 前缀决定 `--type`:`img_*`→image、`file_*`→file(与飞书资源 API 契约一致)。
MEDIA_KEY_RE = re.compile(r"(?:img|file)_[A-Za-z0-9_-]+")
