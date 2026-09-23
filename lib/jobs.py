"""outbound_jobs 入队:确定性 idempotency 键(重驱幂等,DB UNIQUE 保证;plan §3)。
一切飞书外发只经此表(I2)。本模块只写库,不联网。"""
from . import constants, texts, util


# E4b 键模板最坏长度审计(逻辑键存 DB;wire 一律 util.short_key ≤40):
#   turn:<32hex>:<idx>            = 38+len(idx)  → 大 idx 可超 50
#   card:<32hex>                  = 37
#   dec:<32hex>:<outcome≤8>       ≤ 46
#   lc:<32hex>:<transition≤20>    ≤ 56  ← 超限
#   rc:<int>                      小
#   un:<message_id~35>            ≈ 38(message_id 长度不受我们控制)
#   notice:<message_id~35>:<code≤14> ≈ 57  ← 超限
def key_turn(turn_group, chunk_index):
    return f"turn:{turn_group}:{chunk_index}"


def key_card(pending_id):
    return f"card:{pending_id}"


def key_dec(pending_id, outcome):
    return f"dec:{pending_id}:{outcome}"


def key_lc(binding_id, transition):
    return f"lc:{binding_id}:{transition}"


def key_rc(delivery_seq):
    return f"rc:{delivery_seq}"


def key_un(message_id):
    return f"un:{message_id}"


def key_notice(message_id, code):
    return f"notice:{message_id}:{code}"


def create_job(conn, *, kind, chat_id, idempotency_key, now, binding_id=None,
               reply_to=None, ref_pending_id=None, ref_delivery_seq=None,
               ref_message_id=None, expected_state=None, turn_group=None,
               chunk_index=None, body=None):
    """INSERT OR IGNORE(键冲突=已存在,幂等);返回是否新插入。须在调用方事务内或自动提交下均可。"""
    cur = conn.execute(
        "INSERT OR IGNORE INTO outbound_jobs(job_id,kind,binding_id,chat_id,reply_to,"
        "ref_pending_id,ref_delivery_seq,ref_message_id,expected_state,turn_group,"
        "chunk_index,body,idempotency_key,state,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)",
        (util.new_id(), kind, binding_id, chat_id, reply_to, ref_pending_id,
         ref_delivery_seq, ref_message_id, expected_state, turn_group, chunk_index,
         body, idempotency_key, now))
    return cur.rowcount == 1


def recent_inbound_notice_exists(conn, chat_id, now):
    row = conn.execute(
        "SELECT 1 FROM outbound_jobs WHERE kind='inbound_notice' AND chat_id=? "
        "AND created_at>? LIMIT 1",
        (chat_id, now - constants.NOTICE_COOLDOWN_MS)).fetchone()
    return bool(row)


def create_inbound_notice(conn, *, chat_id, message_id, code, binding_id, now):
    """未绑定/已关闭提示;per-chat 冷却限速(4.2.0)。"""
    if recent_inbound_notice_exists(conn, chat_id, now):
        return False
    return create_job(
        conn, kind="inbound_notice", chat_id=chat_id,
        idempotency_key=key_notice(message_id, code), ref_message_id=message_id,
        expected_state=code, binding_id=binding_id,
        body=texts.inbound_notice_body(code), now=now)
