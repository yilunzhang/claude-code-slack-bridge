"""出站身份门(FingerprintGate)—— 凭据契约的运行时执行者(docs/contracts.md §7 / §4.5)。

- `verify_identity(client, cfg)`:`auth.test` → 比对 `team_id / user_id==bot_user_id / bot_id`;
  缺字段绝不算 ok(fail-closed);确证不符 = `mismatch`;调用失败/缺字段 = `unknown`。
- `FingerprintGate.tick()` 每 tick **stat** tokens 文件(`config.tokens_mtime_ns`,廉价);
  文件版本变 → `client.reload_tokens(version)` → **立即** `auth.test` → **同一事务**写
  `outbound_gate` + `outbound_gate_tokens_version` + `tokens_version_seen`,并把 `verify_capability`
  置回 `unverified`(除非 `verify_capability_tokens_version == 新版本`,即 probe 已对这一版凭据确证过)。
- 身份漂移 → `outbound_gate='mismatch'`(关门;daemon 启动时 `startup()=='mismatch'` 拒启);
  `auth.test` 失败 → `degraded:auth_error` 带退避重探;ok 状态每 REVERIFY_INTERVAL 周期复检。
- xapp(app_token)变化 → `app_token_changed()` 返回 True 一次,daemon 据此 SIGTERM consumer 由
  ConsumerManager 重拉(consumer 自读 tokens.json)。
- 状态**跃迁**时弹本机桌面通知(`notifyos`,fail-open —— 出站门关着时 Slack 那条线正是被堵的)。
- **没有**版本探测 / 自愈:Slack 版没有 lark-cli 那种外部可执行文件,凭据文件就是唯一真相。

daemon 循环序:`gate.tick()` 排在 `outbound.tick()` 之前(漂移 → 本循环零发送)。"""
import hashlib

from . import config as configmod
from . import constants, db, paths

PROBE_BACKOFF_START_MS = 30_000
PROBE_BACKOFF_MAX_MS = 10 * 60 * 1000
REVERIFY_INTERVAL_MS = 10 * 60 * 1000  # ok 状态的周期复检间隔

GATE_KEY = constants.GATE_KEY
GATE_VERSION_KEY = constants.GATE_VERSION_KEY
TOKENS_VERSION_SEEN_KEY = constants.TOKENS_VERSION_SEEN_KEY

GATE_OK = "ok"
GATE_MISMATCH = "mismatch"
REASON_AUTH_ERROR = "auth_error"
REASON_TOKENS_ERROR = "tokens_error"
REASON_IDENTITY_MISMATCH = "identity_mismatch"


def identity_check(auth_data, cfg):
    """auth.test 响应 vs config → 'ok' | 'mismatch' | 'unknown'。
    三个字段(team_id / user_id / bot_id)任一缺失 → unknown(缺字段绝不放行);全在且任一不等 → mismatch。"""
    if not isinstance(auth_data, dict):
        return "unknown"
    team = auth_data.get("team_id")
    user = auth_data.get("user_id")
    bot = auth_data.get("bot_id")
    if not (team and user and bot):
        return "unknown"
    if (team != cfg.get("team_id") or user != cfg.get("bot_user_id")
            or bot != cfg.get("bot_id")):
        return "mismatch"
    return "ok"


def verify_identity(client, cfg):
    """→ 'ok' | 'mismatch' | 'unknown'。网络/冷却/超时/鉴权失败一律 unknown(不是 mismatch:
    只有 Slack 明确返回了另一个身份才算漂移)。"""
    res = client.call("auth.test", {})
    if not res.ok:
        return "unknown"
    return identity_check(res.data, cfg)


def _app_token_digest(tokens):
    app = (tokens or {}).get("app_token")
    if not app:
        return None
    return hashlib.sha256(app.encode("utf-8")).hexdigest()


class FingerprintGate:
    """daemon 内的出站门。构造签名冻结(contracts §8):`FingerprintGate(conn, cfg, client, clock, notifier=None)`。

    公开方法(daemon 调用):
    - `startup()` → 'ok' | 'degraded' | 'mismatch'(mismatch 由 daemon 拒启);
    - `tick()` 每循环一次(在 outbound.tick 之前);
    - `app_token_changed()` → bool,**读一次即清零**:True 表示 tokens.json 里的 app_token 换了,
      daemon 应 SIGTERM consumer 让 ConsumerManager 重拉。
    只读属性:`state`(最近一次判定 'ok'|'degraded'|'mismatch')、`tokens_version`(最近看到的文件版本)。"""

    def __init__(self, conn, cfg, client, clock, notifier=None):
        self.conn = conn
        self.cfg = cfg
        self.client = client
        self.clock = clock
        self._notifier = notifier
        self._tokens_path = str(getattr(client, "tokens_path", None) or paths.tokens_path())
        self._backoff = PROBE_BACKOFF_START_MS
        self._next_probe_at = 0
        self._last_notified_state = None
        self._last_mtime_ns = None
        self._reload_pending = False
        self._app_token_digest = None
        self._app_token_changed_flag = False
        self._seen_version = None
        self.state = None

    # ------------------------------------------------------------------ 属性
    @property
    def tokens_version(self):
        return self._seen_version

    def app_token_changed(self):
        """xapp 变化信号(一次性):True 后立即清零。daemon 每循环调用一次。"""
        flag = self._app_token_changed_flag
        self._app_token_changed_flag = False
        return flag

    # ------------------------------------------------------------------ 通知(旁路)
    def _notify(self, title, message, subtitle=None):
        """fail-open:通知是旁路装饰,任何异常都吞掉 —— 绝不能炸掉门逻辑/daemon 循环。"""
        try:
            fn = self._notifier
            if fn is None:
                from . import notifyos
                fn = notifyos.notify
            fn(title, message, subtitle)
        except Exception:   # noqa: BLE001
            pass

    def _notify_transition(self, state, reason):
        """只在**状态跃迁**时发一条(同状态连续多轮不刷屏)。"""
        key = (state, reason)
        if key == self._last_notified_state:
            return
        self._last_notified_state = key
        if state == "ok":
            self._notify("slack-bridge", "出站已恢复(凭据已通过 auth.test)。", "出站恢复")
        elif state == "mismatch":
            self._notify("slack-bridge",
                         "身份不符:tokens.json 的 bot 与 config.json 指纹不一致,出站已关门。"
                         "换回原 app 的 token,或删 config.json 重新 bootstrap。", "出站停摆")
        else:
            self._notify("slack-bridge",
                         "出站已停摆(%s)。转发/通知都发不出去,需处理。" % (reason,), "出站停摆")

    # ------------------------------------------------------------------ 判定 / 落库
    def _evaluate(self):
        ident = verify_identity(self.client, self.cfg)
        if ident == "ok":
            return "ok", None
        if ident == "mismatch":
            return "mismatch", REASON_IDENTITY_MISMATCH
        return "degraded", REASON_AUTH_ERROR

    def _gate_value(self, state, reason):
        if state == "ok":
            return GATE_OK
        if state == "mismatch":
            return GATE_MISMATCH
        return "degraded:%s" % reason

    def _apply(self, state, reason, version, version_changed):
        """同一事务:outbound_gate + outbound_gate_tokens_version + tokens_version_seen
        (+ 版本变化时 verify_capability 失效,除非 probe 版本 == 新版本)。
        `outbound_gate_tokens_version` = 本次判定所绑定的凭据版本(ok 时即"通过 auth.test 的版本")。"""
        now = self.clock.mono_ms()
        with db.tx(self.conn):
            db.set_state(self.conn, GATE_KEY, self._gate_value(state, reason))
            if version is not None:
                db.set_state(self.conn, GATE_VERSION_KEY, version)
                db.set_state(self.conn, TOKENS_VERSION_SEEN_KEY, version)
            if version_changed:
                probed = db.get_state(self.conn, constants.VERIFY_CAPABILITY_VERSION_KEY)
                if probed != version:
                    db.set_state(self.conn, constants.VERIFY_CAPABILITY_KEY,
                                 constants.VERIFY_CAP_UNVERIFIED)
        self.state = state
        if version is not None:
            self._seen_version = version
        if state == "ok":
            self._backoff = PROBE_BACKOFF_START_MS
            self._next_probe_at = now + REVERIFY_INTERVAL_MS
        else:
            self._next_probe_at = now + self._backoff
            self._backoff = min(self._backoff * 2, PROBE_BACKOFF_MAX_MS)

    # ------------------------------------------------------------------ tokens 文件
    def _read_tokens_file(self):
        """→ (tokens, version) 或抛 ConfigError(缺失 / 权限非 0600 / 畸形)。"""
        return configmod.load_tokens(self._tokens_path, allow_env=False)

    def _check_tokens_file(self):
        """每 tick 的廉价探针。→ ('same', None) | ('changed', new_version) | ('error', detail)。
        mtime 变(或上次读失败待重试)才真正读文件 + `client.reload_tokens`。"""
        mtime = configmod.tokens_mtime_ns(self._tokens_path)
        if mtime == self._last_mtime_ns and not self._reload_pending:
            return "same", None
        self._last_mtime_ns = mtime
        try:
            tokens, ver = self._read_tokens_file()
            new_ver = self.client.reload_tokens(ver)
        except configmod.ConfigError as e:
            self._reload_pending = True
            return "error", str(e)
        self._reload_pending = False
        digest = _app_token_digest(tokens)
        if self._app_token_digest is not None and digest != self._app_token_digest:
            self._app_token_changed_flag = True
        self._app_token_digest = digest
        if new_ver != self._seen_version:
            return "changed", new_ver
        return "same", None

    # ------------------------------------------------------------------ 入口
    def startup(self):
        """只检测(一次 auth.test),绝不发消息。→ 'ok' | 'degraded' | 'mismatch'。
        健康启动不通知(否则每次重启都弹"已恢复");degraded/mismatch 通知。"""
        self._last_mtime_ns = configmod.tokens_mtime_ns(self._tokens_path)
        try:
            tokens, _ver = self._read_tokens_file()
            self._app_token_digest = _app_token_digest(tokens)
        except configmod.ConfigError:
            self._app_token_digest = None
        version = self.client.tokens_version
        state, reason = self._evaluate()
        self._apply(state, reason, version, version_changed=False)
        if state == "ok":
            self._last_notified_state = ("ok", None)   # 记状态但不发声
        else:
            self._notify_transition(state, reason)
        return state

    def tick(self):
        now = self.clock.mono_ms()
        kind, detail = self._check_tokens_file()
        if kind == "error":
            # 文件不可读/权限错:关门(fail-closed),按退避重试读取
            if now >= self._next_probe_at or self.state != "degraded":
                self._apply("degraded", REASON_TOKENS_ERROR, None, version_changed=False)
                self._notify_transition("degraded", REASON_TOKENS_ERROR)
            return
        if kind == "changed":
            # 版本变化:无视退避,立即重验并把结论绑定到新版本
            state, reason = self._evaluate()
            self._apply(state, reason, detail, version_changed=True)
            self._notify_transition(state, reason)
            return
        if now < self._next_probe_at:
            return
        state, reason = self._evaluate()
        self._apply(state, reason, self.client.tokens_version, version_changed=False)
        self._notify_transition(state, reason)
