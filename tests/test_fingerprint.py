"""FingerprintGate(Slack 版,contracts §7 / §4.5):
- verify_identity:auth.test 三字段全比;缺字段 = unknown(fail-closed);确证不符 = mismatch;
- 门:unknown → degraded:auth_error + 退避重探;mismatch → outbound_gate='mismatch'(daemon 拒启);
- tokens.json 版本变化 → reload_tokens → 立即 auth.test → **同一事务**写 gate + gate 版本 + seen,
  verify_capability 置回 unverified(除非 probe 版本 == 新版本);
- app_token 变化 → app_token_changed() 一次性 True;通知只在跃迁;notifier 抛异常不炸门。
全程离线(FakeSlackClient);不依赖 outbound(WP3)。"""
import inspect
import os

import pytest

from tests.conftest import BOT_ID, BOT_USER, TEAM
from tests.helpers import FakeSlackClient, err, not_sent, ok, timeout
from lib import config as configmod
from lib import constants, db as dbmod, fingerprint

AUTH_OK = {"url": "https://t.slack.com/", "team": "t", "user": "slack-bridge",
           "team_id": TEAM, "user_id": BOT_USER, "bot_id": BOT_ID}


class _Notes:
    def __init__(self, raising=False):
        self.calls = []
        self.raising = raising

    def __call__(self, title, message, subtitle=None):
        if self.raising:
            raise RuntimeError("notification backend exploded")
        self.calls.append((title, message, subtitle))
        return True


class _Auth:
    """可变 auth.test 应答:`.data` 可改(漂移)、`.res` 非空则直接返回该 CallResult(失败态)。"""
    def __init__(self, data=None, res=None):
        self.data = dict(AUTH_OK if data is None else data)
        self.res = res
        self.n = 0

    def __call__(self, method, params):
        self.n += 1
        if self.res is not None:
            return self.res
        return ok(self.data)


def _client(clock, auth, version="v1"):
    c = FakeSlackClient(clock=clock, tokens_version=version)
    c.on("auth.test", auth)
    return c


def _gate(env, client, notes=None):
    return fingerprint.FingerprintGate(env.conn, env.cfg, client, env.clock, notifier=notes)


def _rewrite_tokens(tokens_ns, bot="xoxb-new", app="xapp-test-app-token"):
    """换 tokens.json 内容 + 保证 mtime_ns 前进(FingerprintGate 的廉价探针看 mtime)。→ 新版本。"""
    old = os.stat(tokens_ns.path).st_mtime_ns
    v = configmod.save_tokens({"bot_token": bot, "app_token": app}, overwrite=True)
    st = os.stat(tokens_ns.path)
    if st.st_mtime_ns <= old:
        os.utime(tokens_ns.path, ns=(st.st_atime_ns, old + 1_000_000))
        v = configmod.load_tokens(allow_env=False)[1]
    return v


# ---------------------------------------------------------------- 构造签名冻结(contracts §8)
def test_constructor_signature_frozen():
    params = list(inspect.signature(fingerprint.FingerprintGate.__init__).parameters)
    assert params == ["self", "conn", "cfg", "client", "clock", "notifier"]
    for name in ("startup", "tick", "app_token_changed"):
        assert callable(getattr(fingerprint.FingerprintGate, name))
    assert not hasattr(fingerprint, "verify_cli_version") and not hasattr(fingerprint, "probe_cli_version")


# ---------------------------------------------------------------- verify_identity
class TestVerifyIdentity:
    def test_match_ok(self, cfg, clock):
        assert fingerprint.verify_identity(_client(clock, _Auth()), cfg) == "ok"

    @pytest.mark.parametrize("field", ["team_id", "user_id", "bot_id"])
    def test_missing_field_unknown_not_ok(self, cfg, clock, field):
        d = dict(AUTH_OK)
        d.pop(field)
        assert fingerprint.verify_identity(_client(clock, _Auth(d)), cfg) == "unknown"

    @pytest.mark.parametrize("field,value", [("team_id", "T_EVIL"), ("user_id", "U_EVIL"), ("bot_id", "B_EVIL")])
    def test_mismatch(self, cfg, clock, field, value):
        d = dict(AUTH_OK)
        d[field] = value
        assert fingerprint.verify_identity(_client(clock, _Auth(d)), cfg) == "mismatch"

    @pytest.mark.parametrize("res", [err("invalid_auth"), not_sent("dns"), timeout(), err("http_5xx", http_status=503)])
    def test_call_failure_unknown(self, cfg, clock, res):
        assert fingerprint.verify_identity(_client(clock, _Auth(res=res)), cfg) == "unknown"


# ---------------------------------------------------------------- startup
class TestStartup:
    def test_ok_writes_gate_and_versions_no_notification(self, env, tokens):
        notes = _Notes()
        c = _client(env.clock, _Auth(), version=tokens.version)
        g = _gate(env, c, notes)
        assert g.startup() == "ok"
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "ok"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == tokens.version
        assert dbmod.get_state(env.conn, constants.TOKENS_VERSION_SEEN_KEY) == tokens.version
        assert notes.calls == []          # 健康启动不发声
        assert g.app_token_changed() is False

    def test_unknown_degrades_auth_error_and_notifies_once(self, env, tokens):
        notes = _Notes()
        g = _gate(env, _client(env.clock, _Auth(res=not_sent("dns")), tokens.version), notes)
        assert g.startup() == "degraded"
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "degraded:auth_error"
        assert len(notes.calls) == 1 and "停摆" in " ".join(str(x) for x in notes.calls[0])

    def test_mismatch_closes_gate_with_mismatch_value(self, env, tokens):
        notes = _Notes()
        d = dict(AUTH_OK, bot_id="B_EVIL")
        g = _gate(env, _client(env.clock, _Auth(d), tokens.version), notes)
        assert g.startup() == "mismatch"          # daemon 据此拒启
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "mismatch"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == tokens.version
        assert len(notes.calls) == 1 and "身份不符" in notes.calls[0][1]

    def test_notifier_exception_does_not_break_gate(self, env, tokens):
        g = _gate(env, _client(env.clock, _Auth(res=timeout()), tokens.version), _Notes(raising=True))
        assert g.startup() == "degraded"
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "degraded:auth_error"


# ---------------------------------------------------------------- 退避重探 / 周期复检
class TestBackoffAndReverify:
    def test_degraded_reprobes_with_backoff_then_recovers_notifies_once(self, env, tokens):
        notes = _Notes()
        auth = _Auth(res=timeout())
        c = _client(env.clock, auth, tokens.version)
        g = _gate(env, c, notes)
        assert g.startup() == "degraded"
        n = auth.n
        g.tick()                                       # 退避期内不重探
        assert auth.n == n
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()                                       # 仍失败 → 退避翻倍
        assert auth.n == n + 1 and g._backoff == fingerprint.PROBE_BACKOFF_START_MS * 4   # startup 已翻一次
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()                                       # 2×退避未到
        assert auth.n == n + 1
        auth.res = None                                # 网络恢复
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS * 2 + 1)
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "ok"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == tokens.version
        texts = [" ".join(str(x) for x in call) for call in notes.calls]
        assert len(texts) == 2 and "停摆" in texts[0] and "恢复" in texts[1]
        env.clock.tick(fingerprint.REVERIFY_INTERVAL_MS + 1)
        g.tick()                                       # ok → ok 不再发声
        assert len(notes.calls) == 2

    def test_backoff_capped(self, env, tokens):
        auth = _Auth(res=timeout())
        g = _gate(env, _client(env.clock, auth, tokens.version))
        g.startup()
        for _ in range(12):
            env.clock.tick(fingerprint.PROBE_BACKOFF_MAX_MS + 1)
            g.tick()
        assert g._backoff == fingerprint.PROBE_BACKOFF_MAX_MS

    def test_ok_no_probe_before_interval_then_reverifies_and_closes_on_drift(self, env, tokens):
        notes = _Notes()
        auth = _Auth()
        g = _gate(env, _client(env.clock, auth, tokens.version), notes)
        assert g.startup() == "ok"
        n = auth.n
        g.tick()
        assert auth.n == n                             # 10min 内不复检
        auth.data["user_id"] = "U_EVIL"                # 身份漂移
        env.clock.tick(fingerprint.REVERIFY_INTERVAL_MS + 1)
        g.tick()
        assert auth.n == n + 1
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "mismatch"
        assert len(notes.calls) == 1 and "身份不符" in notes.calls[0][1]

    def test_reverify_uses_monotonic_clock(self, env, tokens):
        auth = _Auth()
        g = _gate(env, _client(env.clock, auth, tokens.version))
        g.startup()
        env.clock.rewind_wall(3_600_000)
        auth.data["team_id"] = "T_EVIL"
        env.clock.mono += fingerprint.REVERIFY_INTERVAL_MS + 1
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "mismatch"



# ---------------------------------------------------------------- tokens.json 版本变化(contracts §7)
class _TxSpy:
    """conn 代理:记录 execute 的 SQL 序列(证明"同一事务")。"""
    def __init__(self, conn):
        self._c = conn
        self.sql = []

    def execute(self, sql, *a, **k):
        self.sql.append(sql.strip())
        return self._c.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._c, name)


class TestTokensVersionChange:
    def _ok_gate(self, env, tokens, notes=None, conn=None):
        auth = _Auth()
        c = _client(env.clock, auth, tokens.version)
        g = fingerprint.FingerprintGate(conn or env.conn, env.cfg, c, env.clock, notifier=notes)
        assert g.startup() == "ok"
        return g, c, auth

    def test_version_change_reloads_verifies_immediately_writes_versions_and_resets_capability(self, env, tokens):
        notes = _Notes()
        g, c, auth = self._ok_gate(env, tokens, notes)
        # probe 曾对**旧**版本确证过
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_KEY, "ok")
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_VERSION_KEY, tokens.version)
        n = auth.n
        new_ver = _rewrite_tokens(tokens)
        assert new_ver != tokens.version
        g.tick()                                       # 复检周期未到,但版本变了 → 立即重验
        assert c.reloads == [new_ver] and c.tokens_version == new_ver
        assert auth.n == n + 1
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "ok"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == new_ver
        assert dbmod.get_state(env.conn, constants.TOKENS_VERSION_SEEN_KEY) == new_ver
        assert dbmod.get_state(env.conn, constants.VERIFY_CAPABILITY_KEY) == constants.VERIFY_CAP_UNVERIFIED
        assert g.tokens_version == new_ver
        assert notes.calls == []                       # ok → ok 无跃迁不发声
        g.tick()                                       # 文件没再变 → 不再 reload、不再 auth.test
        assert c.reloads == [new_ver] and auth.n == n + 1

    def test_capability_preserved_when_probe_version_equals_new_version(self, env, tokens):
        g, c, auth = self._ok_gate(env, tokens)
        new_ver = _rewrite_tokens(tokens)
        # probe 已针对**新**版本确证(probe 在 daemon 看到之前先跑)
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_KEY, "ok")
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_VERSION_KEY, new_ver)
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == new_ver
        assert dbmod.get_state(env.conn, constants.VERIFY_CAPABILITY_KEY) == "ok"

    def test_gate_and_versions_and_capability_written_in_one_transaction(self, env, tokens):
        spy = _TxSpy(env.conn)
        g, c, auth = self._ok_gate(env, tokens, conn=spy)
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_KEY, "ok")
        dbmod.set_state(env.conn, constants.VERIFY_CAPABILITY_VERSION_KEY, tokens.version)
        _rewrite_tokens(tokens)
        spy.sql.clear()
        g.tick()
        begins = [i for i, s in enumerate(spy.sql) if s.startswith("BEGIN")]
        commits = [i for i, s in enumerate(spy.sql) if s.startswith("COMMIT")]
        assert len(begins) == 1 and len(commits) == 1
        b, e = begins[0], commits[0]
        inside = "\n".join(spy.sql[b:e])
        assert inside.count("INSERT INTO daemon_state") == 4   # gate + gate 版本 + seen + verify_capability
        assert not any(s.startswith("INSERT INTO daemon_state") for s in spy.sql[:b] + spy.sql[e:])
        assert dbmod.get_state(env.conn, constants.VERIFY_CAPABILITY_KEY) == constants.VERIFY_CAP_UNVERIFIED

    def test_swapped_token_with_foreign_identity_closes_gate_mismatch(self, env, tokens):
        notes = _Notes()
        g, c, auth = self._ok_gate(env, tokens, notes)
        auth.data["bot_id"] = "B_OTHER_APP"            # 新 token 属于别的 app
        new_ver = _rewrite_tokens(tokens)
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "mismatch"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == new_ver
        assert len(notes.calls) == 1 and "身份不符" in notes.calls[0][1]
        # 换回正确 token → 重验放行 + 恢复通知
        auth.data["bot_id"] = BOT_ID
        v3 = _rewrite_tokens(tokens, bot="xoxb-back")
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "ok"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == v3
        assert len(notes.calls) == 2 and "恢复" in notes.calls[1][1]

    def test_unreadable_tokens_file_degrades_then_recovers(self, env, tokens):
        notes = _Notes()
        g, c, auth = self._ok_gate(env, tokens, notes)
        os.chmod(tokens.path, 0o644)                   # 权限错 → load_tokens ConfigError
        st = os.stat(tokens.path)
        os.utime(tokens.path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "degraded:tokens_error"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == tokens.version  # 版本不动
        assert len(notes.calls) == 1
        os.chmod(tokens.path, 0o600)
        env.clock.tick(fingerprint.PROBE_BACKOFF_START_MS + 1)
        g.tick()                                       # 待重试 → 读成功 → 版本变(mtime 变了)→ 重验
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "ok"
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == configmod.load_tokens(allow_env=False)[1]
        assert "恢复" in notes.calls[-1][1]

    def test_app_token_change_signal_once(self, env, tokens):
        g, c, auth = self._ok_gate(env, tokens)
        assert g.app_token_changed() is False
        _rewrite_tokens(tokens, bot="xoxb-new", app="xapp-test-app-token")   # 只换 bot token
        g.tick()
        assert g.app_token_changed() is False
        _rewrite_tokens(tokens, bot="xoxb-new", app="xapp-ROTATED")           # 换 app token
        g.tick()
        assert g.app_token_changed() is True
        assert g.app_token_changed() is False          # 一次性
        assert dbmod.get_state(env.conn, constants.GATE_KEY) == "ok"

    def test_env_tokens_never_consulted(self, env, tokens, monkeypatch):
        """文件是真相:env SLACK_BOT_TOKEN 不影响 gate 看到的版本。"""
        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-env")
        g, c, auth = self._ok_gate(env, tokens)
        g.tick()
        assert dbmod.get_state(env.conn, constants.GATE_VERSION_KEY) == tokens.version
        assert c.reloads == []
