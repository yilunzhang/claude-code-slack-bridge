"""SQLite 底座:WAL / synchronous=NORMAL / foreign_keys=ON(每连接)/ busy_timeout 有界。
所有状态推进 = 带旧状态 CAS(rowcount 定胜负);写事务显式 BEGIN IMMEDIATE。"""
import contextlib
import sqlite3

from . import constants


class SchemaMismatch(Exception):
    pass


def connect(db_file, busy_timeout_ms=constants.BUSY_TIMEOUT_DAEMON_MS):
    conn = sqlite3.connect(str(db_file), timeout=busy_timeout_ms / 1000.0,
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
    return conn


def check_schema(conn):
    """只读校验 schema_version —— **绝不建库、绝不写**(供 notify 直发等只读入口用)。
    缺 daemon_state 表 / 缺 schema_version 行 / 值不符 → SchemaMismatch(fail-closed)。
    与 init_schema 共用一份比对逻辑(后者建库后调用本函数)。"""
    has = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='daemon_state'").fetchone()
    if not has:
        raise SchemaMismatch("bridge.db 未初始化(缺 daemon_state 表)")
    ver = conn.execute(
        "SELECT value FROM daemon_state WHERE key='schema_version'").fetchone()
    if not ver or ver[0] != constants.SCHEMA_VERSION:
        raise SchemaMismatch(
            f"bridge.db schema_version={ver[0] if ver else None!r}, "
            f"expected {constants.SCHEMA_VERSION!r}")


def init_schema(conn, schema_file):
    """schema.sql 原样建库(仅当空库);随后核对 schema_version。"""
    has = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='daemon_state'").fetchone()
    if not has:
        sql = open(schema_file, "r", encoding="utf-8").read()
        conn.executescript(sql)  # 一次性建库;sqlite 原生处理注释/分号
    check_schema(conn)  # 建库后表必存在;比对逻辑单一来源


@contextlib.contextmanager
def tx(conn):
    """显式写事务。嵌套禁止(设计上单层)。"""
    if conn.in_transaction:
        raise RuntimeError("nested write transaction (design forbids)")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def cas(conn, sql, params=()):
    """带旧状态的条件 UPDATE;返回是否恰好推进一行。"""
    cur = conn.execute(sql, params)
    return cur.rowcount == 1


def get_state(conn, key, default=None):
    row = conn.execute("SELECT value FROM daemon_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_state(conn, key, value):
    conn.execute(
        "INSERT INTO daemon_state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))


def bump_counter(conn, key, delta=1):
    cur = int(get_state(conn, key, "0") or 0)
    set_state(conn, key, cur + delta)
    return cur + delta
