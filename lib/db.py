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


def connect_short(db_file, busy_ms=constants.CONSUMER_DB_BUSY_MS):
    """consumer 线程用的**短超时**连接(contracts §1 / §9):与 `connect` 同一套 PRAGMA
    (WAL / synchronous=NORMAL / foreign_keys=ON / Row),只是 busy_timeout 换成 `busy_ms`
    (缺省 CONSUMER_DB_BUSY_MS=1500)。锁等待超过 busy_ms → sqlite3.OperationalError,consumer 据此**不 ack**
    (Slack 重投),不拖住 sdk 线程池。"""
    return connect(db_file, busy_timeout_ms=int(busy_ms))


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


def get_states(conn, keys):
    """一条 SELECT 读多个 daemon_state 键 → {key: value}(缺行的键不在 dict 里)。
    单条语句 = 同一读快照(WAL 下语句执行期间看到的是一致的快照):调用方要**联合判定**的几个键
    (如 outbound_gate + outbound_gate_tokens_version,R1-M4)必须用它,不能分两次 get_state
    ——两次自动提交读之间 daemon 可能原子写入了新版本的 (gate, version),拼出一个从未存在过的组合。"""
    keys = tuple(keys)
    if not keys:
        return {}
    rows = conn.execute(
        "SELECT key, value FROM daemon_state WHERE key IN (%s)" % ",".join("?" for _ in keys),
        keys).fetchall()
    return {r[0]: r[1] for r in rows}


def set_state(conn, key, value):
    conn.execute(
        "INSERT INTO daemon_state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))


def bump_counter(conn, key, delta=1):
    cur = int(get_state(conn, key, "0") or 0)
    set_state(conn, key, cur + delta)
    return cur + delta
