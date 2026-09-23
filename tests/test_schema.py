"""schema.sql 原样建库 + 全部约束真跑(外键/CHECK/部分唯一索引)。"""
import sqlite3

import pytest


def ins_binding(conn, bid, chat="oc_x", status="starting", session=None,
                pid=1, start="s1"):
    conn.execute(
        "INSERT INTO bindings(binding_id,chat_id,session_id,cc_pid,cc_start,status) "
        "VALUES(?,?,?,?,?,?)", (bid, chat, session, pid, start, status))


def ins_inbox(conn, mid="om_x", eid=None, chat="oc_x", state="received", binding=None):
    conn.execute(
        "INSERT INTO inbox(event_id,message_id,chat_id,binding_id,state,ts) VALUES(?,?,?,?,?,0)",
        (eid or ("ev_" + mid), mid, chat, binding, state))


def test_schema_builds_and_versioned(conn):
    v = conn.execute("SELECT value FROM daemon_state WHERE key='schema_version'").fetchone()
    assert v[0] == "1"
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"bindings", "inbox", "pendings", "deliveries", "outbound_jobs",
            "callback_events", "pending_bind", "daemon_state"} <= tables


def test_foreign_keys_enforced(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES('nope','om_none','{}','enqueued')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
            "VALUES('p1','om_none','nope','n','pending')")
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
    ins_binding(conn, "b1", chat="oc_a", status="starting")
    with pytest.raises(sqlite3.IntegrityError):
        ins_binding(conn, "b2", chat="oc_a", status="starting", pid=2)
    # 终态不占用
    conn.execute("UPDATE bindings SET status='closed', close_reason='user_unbind' WHERE binding_id='b1'")
    ins_binding(conn, "b3", chat="oc_a", status="starting", pid=3)


def test_b_inst_partial_unique(conn):
    ins_binding(conn, "b1", chat="oc_a", pid=9, start="t")
    with pytest.raises(sqlite3.IntegrityError):
        ins_binding(conn, "b2", chat="oc_b", pid=9, start="t")
    conn.execute("UPDATE bindings SET status='closed' WHERE binding_id='b1'")
    ins_binding(conn, "b3", chat="oc_c", pid=9, start="t")


def test_b_sess_partial_unique_allows_null(conn):
    ins_binding(conn, "b1", chat="oc_a", session=None, pid=1)
    ins_binding(conn, "b2", chat="oc_b", session=None, pid=2)  # 多个 NULL ok
    conn.execute("UPDATE bindings SET session_id='sX' WHERE binding_id='b1'")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE bindings SET session_id='sX' WHERE binding_id='b2'")


def test_inbox_unique_event_and_message(conn):
    ins_inbox(conn, mid="om_1", eid="ev_1")
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, mid="om_1", eid="ev_2")
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, mid="om_2", eid="ev_1")


def test_inbox_state_check(conn):
    with pytest.raises(sqlite3.IntegrityError):
        ins_inbox(conn, state="bogus")


def test_deliveries_unique_binding_message(conn):
    ins_binding(conn, "b1")
    ins_inbox(conn, mid="om_1")
    conn.execute(
        "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
        "VALUES('b1','om_1','{}','enqueued')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO deliveries(binding_id,message_id,payload_json,state) "
            "VALUES('b1','om_1','{}','enqueued')")


def test_oj_chunk_partial_unique(conn):
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state,turn_group,chunk_index) "
        "VALUES('j1','session_turn','oc','k1','pending','g1',0)")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state,turn_group,chunk_index) "
            "VALUES('j2','session_turn','oc','k2','pending','g1',0)")
    # NULL turn_group 不受限
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j3','decision_notice','oc','k3','pending')")
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j4','decision_notice','oc','k4','pending')")


def test_outbound_kind_and_state_checks(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
            "VALUES('j1','bogus_kind','oc','k1','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
            "VALUES('j1','session_turn','oc','k1','bogus')")


def test_idempotency_key_unique(conn):
    conn.execute(
        "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
        "VALUES('j1','session_turn','oc','K','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO outbound_jobs(job_id,kind,chat_id,idempotency_key,state) "
            "VALUES('j2','session_turn','oc','K','pending')")


def test_pb_inst_partial_unique(conn):
    conn.execute(
        "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state) "
        "VALUES('r1','oc',5,'t','n1','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state) "
            "VALUES('r2','oc',5,'t','n2','pending')")
    # 终态 tombstone 不占用
    conn.execute("UPDATE pending_bind SET state='failed' WHERE request_id='r1'")
    conn.execute(
        "INSERT INTO pending_bind(request_id,chat_id,cc_pid,cc_start,nonce,state) "
        "VALUES('r3','oc',5,'t','n3','pending')")


def test_pendings_unique_message_and_decided_event(conn):
    ins_binding(conn, "b1")
    ins_inbox(conn, mid="om_1")
    ins_inbox(conn, mid="om_2")
    conn.execute(
        "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
        "VALUES('p1','om_1','b1','n','pending')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state) "
            "VALUES('p2','om_1','b1','n','pending')")
    conn.execute(
        "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,decided_event_id) "
        "VALUES('p3','om_2','b1','n','approved','cb_1')")
    ins_inbox(conn, mid="om_3")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO pendings(pending_id,message_id,binding_id,nonce,state,decided_event_id) "
            "VALUES('p4','om_3','b1','n','approved','cb_1')")


def test_wal_and_fk_pragmas_applied(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
