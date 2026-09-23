"""跨模块契约守卫(docs/contracts.md)。WP0 能满足的直接绿;归属后续 WP 的用
`xfail(strict=True, reason="WPn")` 存在 —— 该 WP 合入前必须转绿并摘掉标记(strict:一旦通过即报错提醒)。"""
import inspect
import json
import os
import pathlib
import sqlite3

import pytest

from lib import config as configmod
from lib import constants, db as dbmod, hooklib, listener_core, slackwire, texts, util
from lib.slackapi import CallResult, DaemonStateCooldownStore, InMemoryCooldownStore
from tests.conftest import APP_ID, BOT_ID, BOT_USER, CHAT, DM, MEMBER, OWNER, TEAM
from tests.helpers import (FakeSlackClient, app_mention_event, block_action, envelope, err,
                           message_event, ok, posted, ratelimited)

ROOT = pathlib.Path(__file__).resolve().parents[1]

WP1 = pytest.mark.xfail(strict=True, reason="WP1 传输/daemon_core 未落地")
WP2 = pytest.mark.xfail(strict=True, reason="WP2 入站/审批/媒体/恢复/生命周期 未落地")
WP3 = pytest.mark.xfail(strict=True, reason="WP3 出站状态机 未落地")


# ======================================================================
# WP0:schema / constants
# ======================================================================
def test_schema_tables_columns_checks(conn):
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "slack_events" in tables
    oj = {r[1] for r in conn.execute("PRAGMA table_info(outbound_jobs)")}
    assert {"verify_after", "verify_absent_count", "verify_error_count", "verify_round", "resend_count",
            "ratelimit_count", "transient_count", "had_unknown", "op_method", "op_target",
            "op_thread_ts", "op_payload_kind"} <= oj
    ib = {r[1] for r in conn.execute("PRAGMA table_info(inbox)")}
    assert {"sender_user_id", "thread_ts", "reply_thread_ts", "materialize_reason",
            "materialize_started_at", "materialize_attempts", "materialize_next_at"} <= ib
    conn.execute("INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
                 "VALUES('j','session_turn','C','k','unconfirmed')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO inbox(event_id,message_id,chat_id,state) "
                     "VALUES('e','C:1','C','approved_materializing')")
    assert dbmod.get_state(conn, constants.VERIFY_CAPABILITY_KEY) == constants.VERIFY_CAP_UNVERIFIED


REQUIRED_CONSTANT_NAMES = [
    "SOCKET_KEY", "CONSUMER_READY_SENTINEL", "MARKER_PREFIX", "PERMANENT_SEND_ERRORS",
    "AMBIGUOUS_SEND_ERRORS", "NOT_SENT_ERRORS", "ACCEPT_SUBTYPES", "FILE_HOSTS", "CHUNK_LIMIT",
    "FOOTER_RESERVE", "SEND_TIMEOUT_S", "SOCKET_OP_TIMEOUT_S", "DOWNLOAD_DEADLINE_S",
    "POST_MIN_INTERVAL_MS", "VERIFY_SCHEDULE_MS", "RESEND_ONCE", "RATELIMIT_CAP", "TRANSIENT_CAP",
    "DRAIN_BATCH", "DRAIN_MAX_ATTEMPTS", "FOLLOWUP_BUDGET_PER_TICK", "MEDIA_RETRY_DEADLINE_MS",
    "VERIFY_ERROR_CAP", "VERIFY_DEADLINE_MS", "IDEMPOTENT_CAP", "TURN_CAP", "CARD_CAP", "NOTICE_CAP",
    "DECISION_OUTCOMES", "SLACK_EVENTS_RETENTION_MS", "OUTBOUND_TERMINAL_STATES",
    "POSTMESSAGE_METHODS", "IDEMPOTENT_METHODS", "METADATA_EVENT_TYPE", "ACTION_APPROVE",
    "ACTION_REJECT", "ACTION_IDS", "MATERIALIZE_REASONS", "FILE_SKIP_REASONS",
    "CONSUMER_RC_TOKENS", "CONSUMER_RC_AUTH", "CONSUMER_RC_NO_SDK", "WORKER_RC_DEADLINE",
    "WORKER_RC_ORPHAN", "VERIFY_CAPABILITY_KEY", "VERIFY_CAPABILITY_VERSION_KEY", "GATE_KEY",
    "GATE_VERSION_KEY", "COOLDOWN_KEY_PREFIX", "INBOX_NONTERMINAL_STATES", "INBOX_TERMINAL_STATES",
]


def test_constants_exist_and_values():
    for name in REQUIRED_CONSTANT_NAMES:
        assert hasattr(constants, name), name
    c = constants
    assert c.SOCKET_KEY == "socket" and c.CONSUMER_READY_SENTINEL == "[socket] ready"
    assert c.MARKER_PREFIX == "[slack-bridge-bind:"
    assert c.CHUNK_LIMIT == 12000 and c.FOOTER_RESERVE == 256
    assert c.SEND_TIMEOUT_S == 15 and c.SOCKET_OP_TIMEOUT_S == 10 and c.DOWNLOAD_DEADLINE_S == 90
    assert c.POST_MIN_INTERVAL_MS == 1000 and c.VERIFY_SCHEDULE_MS == (5000, 20000, 60000)
    assert c.RESEND_ONCE is True and c.RATELIMIT_CAP == 20 and c.TRANSIENT_CAP == 5
    assert c.DRAIN_BATCH == 50 and c.DRAIN_MAX_ATTEMPTS == 5
    assert c.FOLLOWUP_BUDGET_PER_TICK == (1, 5) and c.MEDIA_RETRY_DEADLINE_MS == 600_000
    assert c.VERIFY_ERROR_CAP == 8 and c.VERIFY_DEADLINE_MS == 600_000
    assert (c.IDEMPOTENT_CAP, c.TURN_CAP, c.CARD_CAP, c.NOTICE_CAP) == (3, 6, 3, 3)
    assert c.DECISION_OUTCOMES == ("delivered", "approved_pending_files", "rejected", "expired",
                                   "attachment_failed", "closed_undelivered")
    assert c.SLACK_EVENTS_RETENTION_MS == 24 * 3600 * 1000
    assert c.OUTBOUND_TERMINAL_STATES == ("sent", "failed", "cancelled", "unconfirmed")
    assert None in c.ACCEPT_SUBTYPES and "file_share" in c.ACCEPT_SUBTYPES
    assert "message_changed" not in c.ACCEPT_SUBTYPES and "bot_message" not in c.ACCEPT_SUBTYPES
    assert "files.slack.com" in c.FILE_HOSTS
    assert "materializing" in c.INBOX_NONTERMINAL_STATES
    assert "approved_materializing" not in c.INBOX_NONTERMINAL_STATES
    assert c.ACTION_IDS == ("sb_approve", "sb_reject")


def test_legacy_constants_marked_for_wp5():
    src = (ROOT / "lib" / "constants.py").read_text(encoding="utf-8")
    for name in ("MGET_TIMEOUT_S", "PERMANENT_SEND_CODES", "SELFCHECK_CHAT_ID", "MEDIA_KEY_RE",
                 "TURN_RETRYABLE_MAX_ATTEMPTS", "CARD_REARM_MAX_ATTEMPTS"):
        line = [l for l in src.splitlines() if l.startswith(name + " ")][0]
        assert "LEGACY-FEISHU: remove in WP5" in line, name


# ======================================================================
# WP0:util / slackwire / texts / listener / hooklib marker
# ======================================================================
@pytest.mark.parametrize("n,footer,expect_lens", [
    (11999, "F", [12000]),
    (12000, "F", [11999, 2]),
    (12001, "F", [12000, 2]),
    (24000, "F", [12000, 11999, 2]),
    (24000, "FF", [12000, 11998, 4]),
    (5, "", [5]),
    (0, "F", []),
])
def test_chunk_text_with_footer_boundaries(n, footer, expect_lens):
    body = "x" * n
    chunks = util.chunk_text_with_footer(body, footer, 12000)
    assert [len(c) for c in chunks] == expect_lens
    assert all(len(c) <= 12000 for c in chunks)
    joined = "".join(chunks)
    assert joined == body + (footer if body else "")   # 正文完整、页脚恰一次(仅在有正文时)
    if chunks:
        assert chunks[-1].endswith(footer)


def test_chunk_text_with_footer_drops_footer_when_impossible():
    chunks = util.chunk_text_with_footer("x" * 100, "F" * 13000, 12000)
    assert chunks == ["x" * 100]
    chunks = util.chunk_text_with_footer("x" * 24000, "F" * 12000, 12000)
    assert [len(c) for c in chunks] == [12000, 12000, 12000] and chunks[-1] == "F" * 12000


def test_message_id_helpers():
    assert util.message_id_of("C1", "1.2") == "C1:1.2"
    assert util.split_message_id("C1:1.2") == ("C1", "1.2")
    for bad in ("C1", ":1", "C1:", "C1:1:2", None):
        with pytest.raises(ValueError):
            util.split_message_id(bad)
    with pytest.raises(ValueError):
        util.message_id_of("", "1")
    assert util.slack_unescape("&lt;a&gt; &amp; b") == "<a> & b"


def test_slackwire_key_and_fields_contract():
    p = block_action("p", "n", user=OWNER, channel=CHAT, card_ts="1.1", action_ts="2.2")
    assert slackwire.event_key("interactive", p) == "act:%s:%s:1.1:%s:sb_approve:2.2" % (TEAM, CHAT, OWNER)
    assert slackwire.event_key("events_api", envelope(message_event(), event_id="Ev1")) == "ev:Ev1"
    assert slackwire.event_key("events_api", {}) is None
    assert slackwire.interactive_fields(block_action("p", "n", value="x")) is None
    assert slackwire.is_self_event({"user": None}, {"bot_user_id": None}) is False
    assert slackwire.render_text(app_mention_event(text="<@%s> &amp; go" % BOT_USER), BOT_USER) == "& go"


def test_listener_payload_type_is_slack_message():
    src = inspect.getsource(listener_core)
    assert '"slack_message"' in src and "feishu_message" not in src


def test_marker_prefix_from_constants_everywhere():
    assert util.marker_for("abc") == "[slack-bridge-bind:abc]"
    src = inspect.getsource(hooklib)
    assert "constants.MARKER_PREFIX" in src and "feishu-bridge-bind" not in src


def test_texts_card_and_decisions_contract():
    card = json.loads(texts.build_approval_card("p", "n", "U1", "<x>"))
    assert set(card) == {"text", "blocks"}
    assert [b["type"] for b in card["blocks"]] == ["section", "section", "actions"]
    ids = [e["action_id"] for e in card["blocks"][2]["elements"]]
    assert ids == list(constants.ACTION_IDS)
    assert card["blocks"][1]["text"]["text"] == "&lt;x&gt;"
    for o in constants.DECISION_OUTCOMES:
        blocks = texts.decision_update_blocks(o, OWNER)
        assert all(b["type"] != "actions" for b in blocks)
        texts.decision_notice_body(o)


# ======================================================================
# WP0:传输层 fake / CallResult / 冷却存储
# ======================================================================
def test_callresult_fields_contract():
    assert set(CallResult.__dataclass_fields__) == {
        "ok", "data", "error", "http_status", "retry_after", "timed_out", "exc", "not_sent",
        "cooldown_until"}


def test_fake_client_unregistered_raises_and_first_match_wins(clock):
    c = FakeSlackClient(clock=clock)
    with pytest.raises(AssertionError, match="unexpected Slack call"):
        c.call("chat.postMessage", {"channel": CHAT})
    c.on("chat.postMessage", lambda m, p: posted(ts="1.1"))
    c.on("chat.postMessage", lambda m, p: err("channel_not_found"))
    r = c.call("chat.postMessage", {"channel": CHAT})
    assert r.ok and r.data["ts"] == "1.1"
    assert c.calls == [("chat.postMessage", {"channel": CHAT})]
    assert c.calls_for("chat.postMessage") == [{"channel": CHAT}]
    assert c.calls_for("chat.update") == []
    c.on("*", lambda m, p: ok())
    assert c.call("anything").ok


def test_fake_client_cooldown_semantics(conn, clock):
    store = DaemonStateCooldownStore(conn)
    c = FakeSlackClient(cooldown_store=store, clock=clock)
    c.on("chat.postMessage", lambda m, p: ratelimited(retry_after=4))
    r = c.call("chat.postMessage", {})
    assert r.http_status == 429 and store.get("chat.postMessage") == clock.wall_ms() + 4000
    # 冷却期内 → wait,不记入 calls
    r2 = c.call("chat.postMessage", {})
    assert r2.cooldown_until == clock.wall_ms() + 4000 and len(c.calls) == 1 and len(c.waits) == 1
    # 其它方法不受影响;到期后放行
    c.on("chat.update", lambda m, p: ok())
    assert c.call("chat.update", {}).ok
    clock.tick(4000)
    c.on("chat.postMessage", lambda m, p: posted())
    assert c.call("chat.postMessage", {}).http_status == 429  # 首匹配仍是 ratelimited(first match wins)
    assert store.get("chat.postMessage") == clock.wall_ms() + 4000


def test_fake_client_responder_must_return_callresult():
    c = FakeSlackClient()
    c.on("x", lambda m, p: {"ok": True})
    with pytest.raises(AssertionError):
        c.call("x")


def test_cooldown_store_publish_max(conn):
    for store in (InMemoryCooldownStore(), DaemonStateCooldownStore(conn)):
        assert store.publish("m", 10) == 10
        assert store.publish("m", 5) == 10
        assert store.get("m") == 10
        assert store.get("other") is None


# ======================================================================
# WP0:config / tokens / ConfigSnapshot
# ======================================================================
def test_tokens_version_format_and_permissions(tokens, data_dir):
    t, v = configmod.load_tokens()
    assert t == {"bot_token": "xoxb-test-bot-token", "app_token": "xapp-test-app-token"}
    mtime, sha = v.split(":")
    assert mtime.isdigit() and len(sha) == 16 and v == tokens.version
    os.chmod(tokens.path, 0o640)
    with pytest.raises(configmod.ConfigError, match="0600"):
        configmod.load_tokens()
    os.chmod(tokens.path, 0o600)
    v2 = configmod.save_tokens({"bot_token": "xoxb-2"}, overwrite=True)
    assert v2 != v and configmod.load_tokens()[0]["app_token"] is None
    with pytest.raises(configmod.ConfigError):
        configmod.save_tokens({"bot_token": "xoxb-3"})  # 存在即拒
    assert oct(os.stat(tokens.path).st_mode & 0o777) == "0o600"


def test_tokens_env_override_and_daemon_mode(tokens, monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-env")
    t, v = configmod.load_tokens()
    assert t["bot_token"] == "xoxb-env" and v == "env"
    t2, v2 = configmod.load_tokens(allow_env=False)
    assert t2["bot_token"] == "xoxb-test-bot-token" and v2 == tokens.version
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    os.unlink(tokens.path)
    with pytest.raises(configmod.ConfigError):
        configmod.load_tokens()


def test_config_required_keys_and_snapshot_in_place(cfg):
    assert configmod.REQUIRED_KEYS == ("team_id", "bot_user_id", "bot_id", "app_id", "owner_user_id")
    assert isinstance(cfg, configmod.ConfigSnapshot) and isinstance(cfg, dict)
    holder_a, holder_b = cfg, cfg            # 所有组件持同一引用
    data = json.loads(cfg.path.read_text())
    data["markdown_mode"] = "text"
    data["chat_allowlist"] = [CHAT]
    util.atomic_write(cfg.path, json.dumps(data))
    assert cfg.refresh() is True
    assert holder_a["markdown_mode"] == "text" and holder_b.get("chat_allowlist") == [CHAT]
    assert holder_a is holder_b is cfg
    # 缺必填键 → 不动
    util.atomic_write(cfg.path, json.dumps({"team_id": TEAM}))
    assert cfg.refresh() is False and cfg["owner_user_id"] == OWNER
    # 畸形 → 不动
    cfg.path.write_text("{not json")
    assert cfg.refresh() is False and cfg["bot_id"] == BOT_ID


def test_config_snapshot_set_persist_merges_other_writers(cfg):
    # 另一进程(probe)先写了 markdown_mode
    data = json.loads(cfg.path.read_text())
    data["markdown_mode"] = "text"
    util.atomic_write(cfg.path, json.dumps(data))
    cfg.set_persist("chat_allowlist", [DM])
    on_disk = json.loads(cfg.path.read_text())
    assert on_disk["markdown_mode"] == "text" and on_disk["chat_allowlist"] == [DM]
    assert cfg["markdown_mode"] == "text" and cfg["chat_allowlist"] == [DM]
    assert configmod.ConfigSnapshot.load()["chat_allowlist"] == [DM]


# ======================================================================
# WP0:Env.stage 钉死绑定(与 consumer 同写法)
# ======================================================================
def test_env_stage_pins_binding_and_dedupes(env):
    row0 = env.stage("events_api", envelope(message_event(channel=CHAT), event_id="Ev0"))
    assert row0["binding_id"] is None and row0["state"] == "staged" and row0["chat_id"] == CHAT
    bid = env.make_binding(status="active", chat_id=CHAT)
    row1 = env.stage("events_api", envelope(message_event(channel=CHAT), event_id="Ev1"))
    assert row1["binding_id"] == bid and row1["event_key"] == "ev:Ev1"
    again = env.stage("events_api", envelope(message_event(channel=DM), event_id="Ev1"))
    assert again["seq"] == row1["seq"] and again["chat_id"] == CHAT   # 重复保留首个
    env.conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' WHERE binding_id=?", (bid,))
    row2 = env.stage("events_api", envelope(message_event(channel=CHAT), event_id="Ev2"))
    assert row2["binding_id"] is None   # 终态绑定不钉
    assert len(env.slack_events("staged")) == 3


def test_env_click_stages_interactive(env):
    bid = env.make_binding(status="active", chat_id=CHAT)
    row = env.click(pending_id="p1", nonce="n1", act="reject", card_ts="1.1", action_ts="2.2")
    assert row["envelope_type"] == "interactive" and row["binding_id"] == bid
    assert row["event_key"] == "act:%s:%s:1.1:%s:sb_reject:2.2" % (TEAM, CHAT, OWNER)
    with pytest.raises(NotImplementedError):
        env.drain()


# ======================================================================
# WP1:daemon_core.drain_staging / ConsumerManager
# ======================================================================
@WP1
def test_drain_staging_exists_and_marks_consumed_only_on_handed_or_dropped(env):
    from lib import daemon_core
    assert callable(getattr(daemon_core.DaemonCore, "drain_staging"))
    env.make_binding(status="active", chat_id=CHAT)
    env.stage("events_api", envelope(message_event(text="hi", channel=CHAT, user=OWNER), event_id="EvH"))
    env.stage("events_api", envelope(message_event(channel=CHAT), event_id="EvF", team_id="T_OTHER"))
    res = env.drain()
    rows = {r["event_key"]: r for r in env.slack_events()}
    assert rows["ev:EvH"]["state"] == "consumed" and rows["ev:EvH"]["error"] is None
    assert rows["ev:EvF"]["state"] == "consumed" and rows["ev:EvF"]["error"]
    assert res["handed"] == 1 and res["dropped"] == 1


@WP1
def test_drain_exception_rolls_back_keeps_staged_and_backs_off(env, monkeypatch):
    from lib import inbound
    env.make_binding(status="active", chat_id=CHAT)
    env.stage("events_api", envelope(message_event(channel=CHAT, user=OWNER), event_id="EvX"))

    def boom(conn, row):
        conn.execute("INSERT INTO callback_events(event_id,seen_at) VALUES('poison',0)")
        raise RuntimeError("boom")
    monkeypatch.setattr(inbound, "ingest_in_tx", boom)
    env.drain()
    r = env.slack_events()[0]
    assert r["state"] == "staged" and r["drain_attempts"] == 1
    assert r["next_drain_at"] >= env.clock.wall_ms() + constants.DRAIN_BACKOFF_MS
    assert env.conn.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0] == 0  # 回滚
    for _ in range(constants.DRAIN_MAX_ATTEMPTS):
        env.clock.tick(constants.DRAIN_BACKOFF_MAX_MS + 1)
        env.drain()
    r = env.slack_events()[0]
    assert r["state"] == "quarantined" and r["drain_attempts"] >= constants.DRAIN_MAX_ATTEMPTS
    assert int(dbmod.get_state(env.conn, "drain_quarantined", "0")) == 1


@WP1
def test_consumer_manager_single_socket_key_and_ready_sentinel(clock):
    from lib import daemon_core
    statuses = []
    mgr = daemon_core.ConsumerManager(clock, lambda k, l: None, lambda k, s, d: statuses.append((k, s)),
                                      lambda key: ["true"])
    assert tuple(mgr.consumers) == (constants.SOCKET_KEY,)
    c = mgr.consumers[constants.SOCKET_KEY]
    mgr._feed(c, "stderr", (constants.CONSUMER_READY_SENTINEL + " num_connections=1\n").encode())
    assert c.ready and (constants.SOCKET_KEY, "ready") in statuses


@WP1
def test_consumer_script_exists():
    assert (ROOT / "bin" / "slack_consumer.py").exists()


# ======================================================================
# WP2:inbound / approval / media / lifecycle / recovery
# ======================================================================
@WP2
def test_ingest_in_tx_exists_never_opens_transaction_and_returns_mapping(env):
    from lib import inbound
    env.make_binding(status="active", chat_id=CHAT)
    row = env.stage("events_api", envelope(message_event(text="hi", channel=CHAT, user=OWNER), event_id="EvI"))
    with dbmod.tx(env.conn):
        assert env.conn.in_transaction
        res = inbound.ingest_in_tx(env.conn, row)   # 不得 BEGIN(否则 db.tx 报 nested / sqlite 报错)
        assert env.conn.in_transaction
    assert res[0] == "handed" and res[1]["message_id"].startswith(CHAT + ":")
    foreign = env.stage("events_api", envelope(message_event(channel=CHAT), event_id="EvJ", team_id="T_X"))
    with dbmod.tx(env.conn):
        res = inbound.ingest_in_tx(env.conn, foreign)
    assert res[0] == "dropped" and isinstance(res[1], str)


@WP2
def test_process_in_tx_return_mapping(env):
    from lib import approval
    with dbmod.tx(env.conn):
        res = approval.process_in_tx(env.conn, {"type": "block_actions"})
    assert res == ("dropped", "skipped")
    p = block_action("nope", "n", user=OWNER, channel=CHAT)
    with dbmod.tx(env.conn):
        res = approval.process_in_tx(env.conn, p)
    assert res == ("dropped", "invalid")
    with dbmod.tx(env.conn):
        res = approval.process_in_tx(env.conn, p)
    assert res == ("dropped", "dup")


@WP2
def test_media_materialize_signature_returns_paths_and_skipped(env):
    from lib import media
    sig = inspect.signature(media.materialize)
    assert "files" in sig.parameters and "deadline_s" in sig.parameters
    out = media.materialize({"bot_token": "x"}, env.media_root, "b1", "%s:1.1" % CHAT, files=[])
    assert out == ([], [])


@WP2
def test_terminate_in_tx_closed_undelivered(env):
    from lib import lifecycle
    bid = env.make_binding(status="active", chat_id=CHAT)
    mid = "%s:1.1" % CHAT
    env.conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,materialize_reason,ts) "
        "VALUES('Ev1',?,?,?,'materializing','approved',0)", (mid, CHAT, bid))
    env.conn.execute(
        "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,decided_by,created_at) "
        "VALUES('p1',?,?,'n','approved',?,0)", (mid, bid, OWNER))
    with dbmod.tx(env.conn):
        assert lifecycle._terminate_in_tx(env.conn, bid, "user_unbind", env.clock.wall_ms())
    assert env.inbox_row(mid)["state"] == "undeliverable"
    keys = {j["idempotency_key"] for j in env.jobs("decision_notice")}
    assert "dec:p1:closed_undelivered" in keys


@WP2
def test_expire_pendings_single_scope(env):
    bid = env.make_binding(status="active", chat_id=CHAT)
    now = env.clock.wall_ms()
    for i, (state, istate) in enumerate((("pending", "awaiting_approval"), ("approved", "materializing"))):
        mid = "%s:%d.1" % (CHAT, i)
        env.conn.execute("INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) VALUES(?,?,?,?,?,0)",
                         ("Ev%d" % i, mid, CHAT, bid, istate))
        env.conn.execute("INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,created_at) "
                         "VALUES(?,?,?,'n',?,?)", ("p%d" % i, mid, bid, state, now - constants.PENDING_TTL_MS - 1))
    env.conn.execute("INSERT INTO outbound_jobs(job_id,kind,binding_id,chat_id,idempotency_key,state,turn_group,chunk_index) "
                     "VALUES('t','session_turn',?,?,'turn:g:0','pending','g',0)", (bid, CHAT))
    env.conn.execute("INSERT INTO outbound_jobs(job_id,kind,binding_id,chat_id,idempotency_key,state,ref_pending_id) "
                     "VALUES('c0','approval_card',?,?,'card:p0','unknown','p0')", (bid, CHAT))  # 未发的卡片
    env.recovery._expire_pendings(now)
    rows = {p["pending_id"]: p["state"] for p in env.pendings()}
    assert rows == {"p0": "expired", "p1": "approved"}
    assert env.inbox_row("%s:0.1" % CHAT)["state"] == "expired"
    assert env.inbox_row("%s:1.1" % CHAT)["state"] == "materializing"      # B 不受影响
    assert env.conn.execute("SELECT state FROM outbound_jobs WHERE job_id='t'").fetchone()[0] == "pending"  # 输出不受影响
    assert env.conn.execute("SELECT state FROM outbound_jobs WHERE job_id='c0'").fetchone()[0] == "cancelled"  # 未发卡片取消
    assert any(j["idempotency_key"] == "dec:p0:expired" for j in env.jobs("decision_notice"))


@WP2
def test_recovery_never_revives_terminal_jobs():
    from lib.recovery import Recovery
    assert not hasattr(Recovery, "_rearm_failed_cards")


@WP2
def test_download_worker_exists():
    assert (ROOT / "bin" / "download_worker.py").exists()


# ======================================================================
# WP3:outbound
# ======================================================================
@WP3
def test_op_for_is_pure_and_maps_decision_notice(cfg):
    from lib import outbound
    base = {"job_id": "j", "kind": "decision_notice", "chat_id": CHAT, "reply_to": "1.1",
            "ref_pending_id": "p", "ref_message_id": None, "body": "{}", "op_method": None,
            "op_target": None, "op_thread_ts": None, "op_payload_kind": None, "had_unknown": 0,
            "attempt_count": 0, "state": "pending", "card_message_id": None}
    a = outbound.op_for(dict(base), cfg)
    b = outbound.op_for(dict(base), cfg)
    assert a == b == ("chat.postMessage", CHAT, "1.1", "text")
    known = dict(base, card_message_id="%s:9.9" % CHAT)
    assert outbound.op_for(known, cfg) == ("chat.update", "%s:9.9" % CHAT, None, "blocks")
    turn = dict(base, kind="session_turn", reply_to=None)
    assert outbound.op_for(turn, cfg)[3] == cfg["markdown_mode"]
    frozen = dict(turn, op_method="chat.postMessage", op_target=CHAT, op_thread_ts=None,
                  op_payload_kind="text", had_unknown=1)
    assert outbound.op_for(frozen, cfg) == ("chat.postMessage", CHAT, None, "text")  # 冻结原样返回


@WP3
def test_startup_scan_sending_postmessage_becomes_unknown_had_unknown(env):
    bid = env.make_binding(status="active", chat_id=CHAT)
    env.conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,binding_id,chat_id,idempotency_key,state,attempt_count,"
        "sending_at,op_method,op_target,op_payload_kind) VALUES('j','session_turn',?,?,'k','sending',1,0,"
        "'chat.postMessage',?,'text')", (bid, CHAT, CHAT))
    env.outbound.startup_scan()
    j = env.jobs()[0]
    assert j["state"] == "unknown" and j["had_unknown"] == 1 and j["verify_after"] is not None


@WP3
def test_prepare_freezes_op_columns(env):
    bid = env.make_binding(status="active", chat_id=CHAT)
    env.conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,binding_id,chat_id,idempotency_key,state,turn_group,chunk_index,body) "
        "VALUES('j','session_turn',?,?,'turn:g:0','pending','g',0,'hi')", (bid, CHAT))
    job = env.jobs()[0]
    assert env.outbound._prepare(job) == "send"
    j = env.jobs()[0]
    assert j["state"] == "sending" and j["op_method"] == "chat.postMessage"
    assert j["op_target"] == CHAT and j["op_payload_kind"] in ("markdown_text", "text")


# ======================================================================
# WP4:hooklib / notify / fingerprint(已落地,守卫直接绿)
# ======================================================================
def test_hooklib_uses_chunk_text_with_footer():
    src = inspect.getsource(hooklib)
    assert "chunk_text_with_footer" in src


def test_notify_has_credentials_unverified_path():
    from lib import notify
    src = inspect.getsource(notify)
    assert "credentials-unverified" in src and "outbound_gate_tokens_version" in src


def test_fingerprint_writes_gate_tokens_version():
    from lib import fingerprint
    src = inspect.getsource(fingerprint)
    assert "GATE_VERSION_KEY" in src or "outbound_gate_tokens_version" in src
    assert "reload_tokens" in src
