#!/usr/bin/env python3
"""Slack 能力探测(stdlib;**独立于 daemon**;contracts §10)。

    python3 scripts/capability_probe.py --chat-id C0123 [--write-config] [--tokens-path P] [--timeout 15]

步骤:
  1. 一次性读取 tokens(文件或 env)快照并计算版本;`auth.test` 核对 team_id / user_id==bot_user_id / bot_id
     与 config.json 一致 —— 不一致 → exit 3、**不发消息、不导入结果**(R4-m2)。
  2. 顶层 `chat.postMessage(markdown_text + metadata)`;`invalid_arguments` → markdown_text_ok=false,改 text 重发。
  3. `conversations.history(include_all_metadata=true)` 读回 → metadata_history。
  4. 线程内同形消息 → `conversations.replies(include_all_metadata=true)` 读回 → metadata_replies。
  5. 两条 `chat.delete`。
  6. 输出 JSON(**绝不打印 token**);`--write-config` → config.markdown_mode、
     daemon_state.verify_capability(ok | degraded:<err>)与 verify_capability_tokens_version(= 快照版本)。
遵守方法级冷却(bridge.db 存在时用 daemon_state 存储);不受 outbound_gate 版本门约束(它就是验证步骤)。
退出码:0 完成 / 2 配置或凭据错 / 3 身份不符 / 4 探测未完成(API 失败 / 冷却)。"""
import argparse
import json
import pathlib
import sys
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import config as configmod  # noqa: E402
from lib import constants, db, paths  # noqa: E402
from lib.slackapi import (DaemonStateCooldownStore, InMemoryCooldownStore,  # noqa: E402
                          SlackClient, classify_send_error)

EXIT_OK, EXIT_CONFIG, EXIT_IDENTITY, EXIT_INCOMPLETE = 0, 2, 3, 4
PROBE_KIND_TOP, PROBE_KIND_THREAD = "top", "thread"


def _metadata(probe_id, kind):
    return {"event_type": constants.METADATA_EVENT_TYPE,
            "event_payload": {"probe_id": probe_id, "kind": kind}}


def identity_check(auth_data, cfg):
    """auth.test 响应 vs config → (ok, mismatches:[{"field","config","actual"}])。缺字段 = 不符(fail-closed)。"""
    pairs = (("team_id", auth_data.get("team_id"), cfg.get("team_id")),
             ("bot_user_id", auth_data.get("user_id"), cfg.get("bot_user_id")),
             ("bot_id", auth_data.get("bot_id"), cfg.get("bot_id")))
    mism = [{"field": f, "config": c, "actual": a} for f, a, c in pairs if not a or not c or a != c]
    return (not mism), mism


def _fail(result, method, res):
    cls = classify_send_error(res)
    detail = res.error or ("http_%s" % res.http_status)
    if cls == "wait":
        detail = "cooldown_until=%s" % res.cooldown_until
    result["errors"].append("%s:%s:%s" % (method, cls, detail))
    return cls


def _find_probe(messages, ts, probe_id):
    """读回列表里找 ts 匹配的消息;返回 (found_message, metadata_ok)。"""
    for m in messages or []:
        if not isinstance(m, dict) or m.get("ts") != ts:
            continue
        md = m.get("metadata") if isinstance(m.get("metadata"), dict) else {}
        payload = md.get("event_payload") if isinstance(md.get("event_payload"), dict) else {}
        ok = (md.get("event_type") == constants.METADATA_EVENT_TYPE
              and payload.get("probe_id") == probe_id)
        return m, ok
    return None, False


def run_probe(client, cfg, chat_id, tokens_version, probe_id=None):
    """→ result dict(见模块 docstring)。`complete` = 步骤 1–4 全部有结论(5 的失败只影响 cleanup_ok)。"""
    probe_id = probe_id or uuid.uuid4().hex[:12]
    result = {
        "identity_ok": False, "markdown_text_ok": None, "metadata_history": None,
        "metadata_replies": None, "cleanup_ok": None, "tokens_version": tokens_version,
        "markdown_mode": None, "verify_capability": None, "chat_id": chat_id,
        "probe_id": probe_id, "complete": False, "errors": [],
    }
    # 1. 身份
    res = client.call("auth.test", {})
    if not res.ok:
        _fail(result, "auth.test", res)
        return result
    ok, mism = identity_check(res.data, cfg)
    result["identity_ok"] = ok
    if not ok:
        result["identity_mismatch"] = mism
        return result

    # 2. 顶层消息(markdown_text → 回退 text)
    text_body = "slack-bridge capability probe %s (auto-deleted)" % probe_id
    md_body = "*slack-bridge* capability probe `%s` _(auto-deleted)_" % probe_id
    base = {"channel": chat_id, "unfurl_links": False, "unfurl_media": False}
    res = client.call("chat.postMessage", dict(base, markdown_text=md_body,
                                               metadata=_metadata(probe_id, PROBE_KIND_TOP)))
    if res.ok:
        result["markdown_text_ok"] = True
    elif res.error == "invalid_arguments":
        result["markdown_text_ok"] = False
        res = client.call("chat.postMessage", dict(base, text=text_body,
                                                   metadata=_metadata(probe_id, PROBE_KIND_TOP)))
        if not res.ok:
            _fail(result, "chat.postMessage", res)
            return result
    else:
        _fail(result, "chat.postMessage", res)
        return result
    top_ts = res.get("ts")
    if not top_ts:
        result["errors"].append("chat.postMessage:unknown:no_ts")
        return result
    payload_kind = "markdown_text" if result["markdown_text_ok"] else "text"
    result["markdown_mode"] = payload_kind
    posted = [top_ts]

    # 3. history 读回
    res = client.call("conversations.history", {
        "channel": chat_id, "oldest": top_ts, "latest": top_ts, "inclusive": True,
        "include_all_metadata": True, "limit": 5})
    if res.ok:
        _, md_ok = _find_probe(res.get("messages"), top_ts, probe_id)
        result["metadata_history"] = md_ok
    else:
        _fail(result, "conversations.history", res)

    # 4. 线程消息 + replies 读回
    params = dict(base, thread_ts=top_ts, metadata=_metadata(probe_id, PROBE_KIND_THREAD))
    params[payload_kind] = md_body if payload_kind == "markdown_text" else text_body
    res = client.call("chat.postMessage", params)
    if res.ok and res.get("ts"):
        reply_ts = res.get("ts")
        posted.append(reply_ts)
        res = client.call("conversations.replies", {
            "channel": chat_id, "ts": top_ts, "include_all_metadata": True, "limit": 20})
        if res.ok:
            _, md_ok = _find_probe(res.get("messages"), reply_ts, probe_id)
            result["metadata_replies"] = md_ok
        else:
            _fail(result, "conversations.replies", res)
    else:
        _fail(result, "chat.postMessage(thread)", res)

    # 5. 清理
    cleanup = True
    for ts in posted:
        res = client.call("chat.delete", {"channel": chat_id, "ts": ts})
        if not res.ok:
            cleanup = False
            _fail(result, "chat.delete", res)
    result["cleanup_ok"] = cleanup

    result["complete"] = (result["metadata_history"] is not None
                          and result["metadata_replies"] is not None)
    if result["complete"]:
        if result["metadata_history"] and result["metadata_replies"]:
            result["verify_capability"] = constants.VERIFY_CAP_OK
        else:
            first = "metadata_history" if not result["metadata_history"] else "metadata_replies"
            result["verify_capability"] = "degraded:%s" % first
    return result


def write_results(result, cfg_snapshot, conn):
    """--write-config:markdown_mode 进 config.json;verify_capability + 其凭据版本进 daemon_state(同事务)。
    只在 identity_ok ∧ complete 时写;markdown_mode 只在有结论时写。"""
    written = {}
    if not result.get("identity_ok"):
        return written
    if result.get("markdown_mode"):
        cfg_snapshot.set_persist("markdown_mode", result["markdown_mode"])
        written["markdown_mode"] = result["markdown_mode"]
    if result.get("complete") and result.get("verify_capability"):
        with db.tx(conn):
            db.set_state(conn, constants.VERIFY_CAPABILITY_KEY, result["verify_capability"])
            db.set_state(conn, constants.VERIFY_CAPABILITY_VERSION_KEY, result["tokens_version"])
        written[constants.VERIFY_CAPABILITY_KEY] = result["verify_capability"]
        written[constants.VERIFY_CAPABILITY_VERSION_KEY] = result["tokens_version"]
    return written


def _open_db(create):
    """bridge.db 存在 → 连接(不建表,只读冷却也够);create=True 时建 schema。不存在且不 create → None。"""
    dbf = paths.db_path()
    if not dbf.exists() and not create:
        return None
    paths.ensure_data_dir()
    conn = db.connect(dbf)
    db.init_schema(conn, paths.schema_path())
    return conn


def main(argv=None, client_factory=None, out=None):
    out = out or sys.stdout
    ap = argparse.ArgumentParser(description="slack-bridge capability probe (stdlib, daemon-independent)")
    ap.add_argument("--chat-id", required=True, help="测试频道/DM id(会发两条消息并删除)")
    ap.add_argument("--write-config", action="store_true",
                    help="写 config.markdown_mode 与 daemon_state.verify_capability(+tokens_version)")
    ap.add_argument("--tokens-path", default=None, help="覆盖 tokens.json 路径(缺省 data_dir/tokens.json;env SLACK_BOT_TOKEN 优先)")
    ap.add_argument("--timeout", type=float, default=constants.SEND_TIMEOUT_S)
    args = ap.parse_args(argv)

    try:
        cfg = configmod.ConfigSnapshot.load()
        tokens, version = configmod.load_tokens(args.tokens_path)
    except configmod.ConfigError as e:
        print(json.dumps({"ok": False, "error": "config", "detail": str(e)}, ensure_ascii=False), file=out)
        return EXIT_CONFIG

    conn = _open_db(create=args.write_config)
    store = DaemonStateCooldownStore(conn) if conn is not None else InMemoryCooldownStore()
    if client_factory is not None:
        client = client_factory(tokens, version, store)
    else:
        client = SlackClient(token=tokens["bot_token"], cooldown_store=store,
                             timeout_s=args.timeout, tokens_version=version)

    result = run_probe(client, cfg, args.chat_id, version)
    if args.write_config and conn is not None:
        result["written"] = write_results(result, cfg, conn)
    if conn is not None:
        conn.close()

    print(json.dumps(result, ensure_ascii=False, indent=2), file=out)
    if not result["identity_ok"]:
        return EXIT_IDENTITY if result.get("identity_mismatch") else EXIT_INCOMPLETE
    if not result["complete"]:
        return EXIT_INCOMPLETE
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
