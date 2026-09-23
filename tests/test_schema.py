"""schema.sql 原样建库 + 全部约束真跑(外键/CHECK/部分唯一索引)+ Slack 版新增表/列。"""
import sqlite3

import pytest


def ins_binding(conn, bid, chat="C0X", status="starting", session=None,
                pid=1, start="s1"):
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,session_id,cc_pid,cc_start,status) "
        "VALUES(?,?,?,?,?,?)", (bid, chat, session, pid, start, status))


def ins_inbox(conn, mid="C0X:1.1", eid=None, chat="C0X", state="received", binding=None):
    conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) VALUES(?,?,?,?,?,0)",
        (eid or ("Ev_" + mid), mid, chat, binding, state))


def cols(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def test_schema_builds_and_versioned(conn):
    v = conn.execute("SELECT value FROM daemon_state WHERE key='schema_version'").fetchone()
    assert v[0] == "1"
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"bindings", "inbox", "pendings", "deliveries", "outbound_jobs",
            "callback_events", "pending_bind", "daemon_state", "slack_events"} <= tables


def test_verify_capability_defaults_unverified(conn):
    v = conn.execute("SELECT value FROM daemon_state WHERE key='verify_capability'").fetchone()
    assert v[0] == "unverified"


def test_foreign_keys_enforced(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES('nope','C:1','{}','enqueued')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
            "VALUES('p1','C:1','nope','n','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, binding="no-such-binding")


def test_bindings_status_check(conn):
    with pytest.raises(sqlite3.IntegrityError):
        ins_binding(conn, "b1", status="weird")


def test_active_requires_session_id(conn):
    with pytest.raises(sqlite3.IntegrityError):
        ins_binding(conn, "b1", status="active", session=None)
    ins_binding(conn, "b2", status="active", session="s")  # ok


def test_b_chat_partial_unique(conn):
    ins_binding(conn, "b1", chat="C0A", status="starting")
    with pytest.raises(sqlite3.IntegrityError):
        ins_binding(conn, "b2", chat="C0A", status="starting", pid=2)
    # 终态不占用
    conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' WHERE binding_id='b1'")
    ins_binding(conn, "b3", chat="C0A", status="starting", pid=3)


def test_b_inst_partial_unique(conn):
    ins_binding(conn, "b1", chat="C0A", pid=9, start="t")
    with pytest.raises(sqlite3.IntegrityError):
        ins_binding(conn, "b2", chat="C0B", pid=9, start="t")
    conn.execute("UPDATE bindings SET status='closed' WHERE binding_id='b1'")
    ins_binding(conn, "b3", chat="C0C", pid=9, start="t")


def test_b_sess_partial_unique_allows_null(conn):
    ins_binding(conn, "b1", chat="C0A", session=None, pid=1)
    ins_binding(conn, "b2", chat="C0B", session=None, pid=2)  # 多个 NULL ok
    conn.execute("UPDATE bindings SET session_id='sX' WHERE binding_id='b1'")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE bindings SET session_id='sX' WHERE binding_id='b2'")


# ---------------------------------------------------------------- slack_events
def test_slack_events_columns_and_defaults(conn):
    assert {"seq", "envelope_type", "event_key", "chat_id", "binding_id", "payload_json",
            "received_at", "state", "drain_attempts", "next_drain_at", "error",
            "consumed_at"} <= cols(conn, "slack_events")
    conn.execute(
        "INSERT INTO slack_events(envelope_type,event_key,chat_id,payload_json,received_at) "
        "VALUES('events_api','ev:Ev1','C0X','{}',1)")
    r = conn.execute("SELECT * FROM slack_events").fetchone()
    assert r["state"] == "staged" and r["drain_attempts"] == 0 and r["binding_id"] is None


def test_slack_events_event_key_unique_and_on_conflict_do_nothing(conn):
    sql = ("INSERT INTO slack_events(envelope_type,event_key,chat_id,payload_json,received_at) "
           "VALUES('events_api','ev:Ev1','C0X',?,1)")
    conn.execute(sql, ('{"first":1}',))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(sql, ('{"second":2}',))
    cur = conn.execute(sql + " ON CONFLICT(event_key) DO NOTHING", ('{"second":2}',))
    assert cur.rowcount == 0
    rows = conn.execute("SELECT payload_json FROM slack_events").fetchall()
    assert [r[0] for r in rows] == ['{"first":1}']  # 重复保留首个


def test_slack_events_checks(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO slack_events(envelope_type,event_key,payload_json,received_at) "
            "VALUES('slash_commands','x','{}',1)")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO slack_events(envelope_type,event_key,payload_json,received_at,state) "
            "VALUES('events_api','x','{}',1,'weird')")
    conn.execute(
        "INSERT INTO slack_events(envelope_type,event_key,payload_json,received_at,state) "
        "VALUES('interactive','act:T:C:1.1:U:sb_approve:2.2','{}',1,'quarantined')")


def test_slack_events_binding_id_has_no_fk(conn):
    # 有意:绑定可能已终态/不存在,消费者只钉死一个 id,不建外键
    conn.execute(
        "INSERT INTO slack_events(envelope_type,event_key,binding_id,payload_json,received_at) "
        "VALUES('events_api','ev:Ev1','gone-binding','{}',1)")


# ---------------------------------------------------------------- inbox
def test_inbox_unique_event_and_message(conn):
    ins_inbox(conn, mid="C0X:1.1", eid="Ev1")
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, mid="C0X:1.1", eid="Ev2")   # 双投:同 message_id 不同 event_id → 由 ingest 处理
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, mid="C0X:2.2", eid="Ev1")


def test_inbox_state_check_materializing_replaces_approved_materializing(conn):
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, state="bogus")
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, state="approved_materializing")
    ins_inbox(conn, mid="C0X:3.3", state="materializing")
    ins_inbox(conn, mid="C0X:4.4", state="undeliverable")


def test_inbox_new_columns(conn):
    c = cols(conn, "inbox")
    assert {"sender_user_id", "thread_ts", "reply_thread_ts", "materialize_reason",
            "materialize_started_at", "materialize_attempts", "materialize_next_at",
            "snapshot_json", "message_type"} <= c
    assert "sender_open_id" not in c
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO inbox(event_id,message_id,chat_id,state,materialize_reason) "
            "VALUES('Ev9','C0X:9.9','C0X','materializing','bogus')")
    conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,state,materialize_reason,thread_ts,reply_thread_ts) "
        "VALUES('Ev8','C0X:8.8','C0X','materializing','approved',NULL,'8.8')")
    r = conn.execute("SELECT * FROM inbox WHERE event_id='Ev8'").fetchone()
    assert r["materialize_attempts"] == 0 and r["materialize_started_at"] is None


def test_deliveries_unique_binding_message(conn):
    ins_binding(conn, "b1")
    ins_inbox(conn, mid="C0X:1.1")
    conn.execute(
        "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
        "VALUES('b1','C0X:1.1','{}','enqueued')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES('b1','C0X:1.1','{}','enqueued')")


# ---------------------------------------------------------------- outbound_jobs
def test_oj_chunk_partial_unique(conn):
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state,turn_group,chunk_index) "
        "VALUES('j1','session_turn','C','k1','pending','g1',0)")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state,turn_group,chunk_index) "
            "VALUES('j2','session_turn','C','k2','pending','g1',0)")
    # NULL turn_group 不受限
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j3','decision_notice','C','k3','pending')")
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j4','decision_notice','C','k4','pending')")


def test_outbound_kind_and_state_checks(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
            "VALUES('j1','bogus_kind','C','k1','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
            "VALUES('j1','session_turn','C','k1','bogus')")
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j2','session_turn','C','k2','unconfirmed')")  # 新终态


def test_outbound_new_columns_and_defaults(conn):
    c = cols(conn, "outbound_jobs")
    assert {"verify_after", "verify_absent_count", "verify_error_count", "verify_round",
            "resend_count", "ratelimit_count", "transient_count", "had_unknown",
            "op_method", "op_target", "op_thread_ts", "op_payload_kind"} <= c
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j1','session_turn','C','k1','pending')")
    r = conn.execute("SELECT * FROM outbound_jobs WHERE job_id='j1'").fetchone()
    for k in ("verify_absent_count", "verify_error_count", "verify_round", "resend_count",
              "ratelimit_count", "transient_count", "had_unknown", "attempt_count"):
        assert r[k] == 0, k
    for k in ("verify_after", "op_method", "op_target", "op_thread_ts", "op_payload_kind"):
        assert r[k] is None, k


def test_idempotency_key_unique(conn):
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j1','session_turn','C','K','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
            "VALUES('j2','session_turn','C','K','pending')")


def test_pb_inst_partial_unique(conn):
    conn.execute(
        "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state) "
        "VALUES('r1','C',5,'t','n1','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state) "
            "VALUES('r2','C',5,'t','n2','pending')")
    # 终态 tombstone 不占用
    conn.execute("UPDATE pending_bind SET state='failed' WHERE request_id='r1'")
    conn.execute(
        "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state) "
        "VALUES('r3','C',5,'t','n3','pending')")


def test_pendings_unique_message_and_decided_event(conn):
    ins_binding(conn, "b1")
    ins_inbox(conn, mid="C0X:1.1")
    ins_inbox(conn, mid="C0X:2.2")
    conn.execute(
        "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
        "VALUES('p1','C0X:1.1','b1','n','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
            "VALUES('p2','C0X:1.1','b1','n','pending')")
    conn.execute(
        "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,decided_event_id) "
        "VALUES('p3','C0X:2.2','b1','n','approved','act:T:C:1:U:sb_approve:2')")
    ins_inbox(conn, mid="C0X:3.3")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,decided_event_id) "
            "VALUES('p4','C0X:3.3','b1','n','approved','act:T:C:1:U:sb_approve:2')")


def test_wal_and_fk_pragmas_applied(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
