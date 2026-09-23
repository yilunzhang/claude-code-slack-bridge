"""成员直投白名单:`(chat_id, sender_open_id)` 在名单里 → 跳过审批卡,直接投递。

**为什么是独立文件而不是 config.json**:config 是 bootstrap 期钉死的指纹
(profile/app_id/owner/cli_version),且被 FingerprintGate 当作比对基准;白名单则由
**agent 在运行时按 owner 的群内指令改写**,两者生命周期完全不同。混在一起会让"改白名单"
有机会碰坏指纹。

**每次判定都读盘**(不缓存):agent 写完文件后**下一条消息就生效**,不需要重启 daemon。
与 `FingerprintGate._current_cfg()` 同一模式(那里是 doctor 写盘、门读盘)。
代价 = 每条 member 消息一次小文件读,在个人应用的消息量下无所谓。

**文件形状**(缺失/坏 = 空名单,fail-closed 回到审批门):

    {"entries": [{"chat_id": "oc_...", "open_id": "ou_...", "note": "张三"}]}

`note` 纯给人看,判定只认 `chat_id` + `open_id` 两个字段。

**没有 per-bot 维度**:一个 data dir 只有一个 config.json、`app_id`/`bot_open_id` 在
bootstrap 时钉死 → 同一个桥不可能有第二个 bot。多 bot 必然是第二个
`FEISHU_BRIDGE_DATA_DIR`,那时白名单文件天然就是分开的 —— per-bot 隔离免费自带。
"""
import json

from . import paths, util


def load_entries():
    """→ 条目 list。文件不存在/无法解析/形状不对 → `[]`(fail-closed:回到审批门)。"""
    p = paths.allowlist_path()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(raw, dict):
        return []
    entries = raw.get("entries")
    if not isinstance(entries, list):
        return []
    return [e for e in entries if isinstance(e, dict)]


def is_allowed(chat_id, sender_open_id):
    """`(chat_id, sender_open_id)` 是否可直投。

    **两个字段都必须非空且精确相等** —— 少任一条件就成了"整个群放行"或"该用户在所有群
    放行",都不是 owner 授权的范围。空串/None 一律不匹配(否则畸形条目会变成通配)。
    """
    if not chat_id or not sender_open_id:
        return False
    for e in load_entries():
        if e.get("chat_id") == chat_id and e.get("open_id") == sender_open_id:
            return True
    return False


def add_entry(chat_id, open_id, note=None):
    """加一条(已存在则原样返回)。→ `(entries, added)`。"""
    if not chat_id or not open_id:
        raise ValueError("chat_id 与 open_id 均必填")
    entries = load_entries()
    for e in entries:
        if e.get("chat_id") == chat_id and e.get("open_id") == open_id:
            return entries, False
    item = {"chat_id": chat_id, "open_id": open_id}
    if note:
        item["note"] = note
    entries.append(item)
    _save(entries)
    return entries, True


def remove_entry(chat_id, open_id):
    """删一条。→ `(entries, removed)`。"""
    entries = load_entries()
    kept = [e for e in entries
            if not (e.get("chat_id") == chat_id and e.get("open_id") == open_id)]
    if len(kept) == len(entries):
        return entries, False
    _save(kept)
    return kept, True


def _save(entries):
    paths.ensure_data_dir()
    util.atomic_write(paths.allowlist_path(),
                      json.dumps({"entries": entries}, ensure_ascii=False, indent=2))
