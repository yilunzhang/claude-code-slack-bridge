"""bridgectl 逻辑层(bin/bridgectl.py 的可测核心)。
I4 例外声明:bootstrap / chats / open-dm / probe(doctor)的读操作与探测是 skill 交互期例外(I2)。
token 只从 env / stdin 进入(绝不上 argv),只落 0600 的 tokens.json,绝不进返回值/日志。"""
import fcntl
import importlib.util
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time

from . import config as configmod
from . import constants, db, lifecycle, paths, procs, texts
from .slackapi import DaemonStateCooldownStore, InMemoryCooldownStore, SlackClient

USER_ID_RE = re.compile(r"[UW][A-Z0-9]{2,32}")
LIST_PAGE_CAP = 50           # conversations.list 最多翻 50 页(200/页)
LIST_PAGE_LIMIT = 200


# ---------------------------------------------------------------- bootstrap(身份配方)
def _validate_bootstrap_tokens(tokens):
    if not isinstance(tokens, dict):
        raise configmod.ConfigError("tokens 必须是 {bot_token, app_token} 对象(env 或 --tokens-stdin)")
    bot = tokens.get("bot_token")
    app = tokens.get("app_token")
    if not isinstance(bot, str) or not bot.strip():
        raise configmod.ConfigError("缺 bot_token(env SLACK_BOT_TOKEN 或 --tokens-stdin)")
    bot = bot.strip()
    if not bot.startswith("xoxb-"):
        raise configmod.ConfigError("bot_token 应为 bot token(xoxb-…);拒绝 user/其它类型 token")
    configmod.check_token_chars(bot, "bootstrap", "bot_token")   # 内部换行/空白:联网前就拒(R1-M5)
    if app is not None:
        if not isinstance(app, str):
            raise configmod.ConfigError("app_token 须为字符串")
        app = app.strip() or None
        if app and not app.startswith("xapp-"):
            raise configmod.ConfigError("app_token 应为 app-level token(xapp-…,scope connections:write)")
        if app:
            configmod.check_token_chars(app, "bootstrap", "app_token")
    return {"bot_token": bot, "app_token": app}


def bootstrap(client_factory, owner=None, owner_email=None, app_id=None, tokens=None,
              chat_allowlist=None, clock=None):
    """身份配方(plan「控制面」):
    - tokens 只来自 env / stdin(调用方负责;本函数绝不看 argv);
    - `auth.test` → team_id / bot_user_id(=user_id)/ bot_id;
    - app_id:`bots.info(bot=bot_id)` 取 `.bot.app_id`;取不到必须显式 `app_id`;两者都有且不同 → 拒绝;
    - owner:`owner`(U…/W…)或 `owner_email`(`users.lookupByEmail`),二选一必给;
    - 写 tokens.json(0600,存在即拒)与 config.json(0600,存在即拒;bootstrap 锁下);
    - **不发任何消息**。→ cfg dict(不含 token)。"""
    tokens = _validate_bootstrap_tokens(tokens)
    if bool(owner) == bool(owner_email):
        raise configmod.ConfigError("--owner U… 与 --owner-email 二选一(且只能给一个)")
    if owner is not None and not USER_ID_RE.fullmatch(str(owner)):
        raise configmod.ConfigError("--owner 须为 Slack user id(U…/W…)")
    # 存在即拒(先查再联网,少一次无谓的 auth.test)
    if configmod.load_config() is not None:
        raise configmod.ConfigError(
            "config.json 已存在(指纹钉死,不隐式变);如确要重来,先 unbind 所有绑定并手动删除 "
            f"{paths.config_path()} 与 {paths.tokens_path()}")
    if os.path.exists(str(paths.tokens_path())):
        raise configmod.ConfigError(
            f"tokens.json 已存在({paths.tokens_path()});如确要换 token,先手动删除它再 bootstrap")

    client = client_factory(tokens)
    res = client.call("auth.test", {})
    if not res.ok or not isinstance(res.data, dict):
        raise configmod.ConfigError("auth.test 失败:%s(检查 bot token 是否有效)" % (res.error,))
    team_id = res.data.get("team_id")
    bot_user_id = res.data.get("user_id")
    bot_id = res.data.get("bot_id")
    if not (team_id and bot_user_id and bot_id):
        raise configmod.ConfigError("auth.test 缺 team_id / user_id / bot_id(须为 bot token)")
    bot_name = res.data.get("user")
    team_name = res.data.get("team")

    # app_id:bots.info(bot=bot_id) 优先;失败/缺失 → 必须 --app-id;两者冲突 → 拒
    probed_app_id = None
    bi = client.call("bots.info", {"bot": bot_id})
    if bi.ok and isinstance(bi.data, dict):
        b = bi.data.get("bot")
        if isinstance(b, dict) and b.get("app_id"):
            probed_app_id = b["app_id"]
            bot_name = b.get("name") or bot_name
    if app_id:
        if probed_app_id and probed_app_id != app_id:
            raise configmod.ConfigError(
                "--app-id(%s)与 bots.info 返回的 app_id(%s)不一致,拒绝写入" % (app_id, probed_app_id))
        final_app_id = app_id
    elif probed_app_id:
        final_app_id = probed_app_id
    else:
        raise configmod.ConfigError(
            "bots.info 未取到 app_id(%s);请从 api.slack.com/apps 复制 App ID 后加 --app-id A…"
            % (bi.error or "no app_id in response",))

    if owner_email:
        lu = client.call("users.lookupByEmail", {"email": owner_email})
        if not lu.ok or not isinstance(lu.data, dict):
            raise configmod.ConfigError(
                "users.lookupByEmail 失败:%s(需 users:read.email scope;或改用 --owner U…)"
                % (lu.error,))
        u = lu.data.get("user")
        owner = u.get("id") if isinstance(u, dict) else None
        if not owner or not USER_ID_RE.fullmatch(str(owner)):
            raise configmod.ConfigError("users.lookupByEmail 未返回合法 user.id")
    if owner == bot_user_id:
        raise configmod.ConfigError("owner 不能是 bot 自己")

    now = clock.wall_ms() if clock is not None else int(time.time() * 1000)
    cfg = {
        "team_id": team_id,
        "bot_user_id": bot_user_id,
        "bot_id": bot_id,
        "app_id": final_app_id,
        "owner_user_id": owner,
        "bot_name": bot_name,
        "team_name": team_name,
        "created_at": now,
    }
    if chat_allowlist:
        cfg["chat_allowlist"] = list(chat_allowlist)
    with configmod.bootstrap_lock():
        if configmod.load_config() is not None:
            raise configmod.ConfigError("config.json 已存在(并发 bootstrap?),拒绝覆盖")
        configmod.save_tokens(tokens)           # 0600;存在即拒
        configmod.save_config(cfg)              # 0600(atomic_write 缺省 mode)
    return cfg


# ---------------------------------------------------------------- hooks(plugin 提供;检测生效)
# plugin 化后 hooks 由 plugin 的 hooks/hooks.json 提供,**不再**手贴进 settings.json。
# 无法在 bind 的同一 turn 内直接读"CC 是否已加载 plugin hooks";用**哨兵心跳**做正向检测:
# Stop/SessionEnd hook 每次运行都写 hook_heartbeat(见 hooklib._touch_hook_heartbeat)。
HOOK_HEARTBEAT_FRESH_MS = 7 * 24 * 3600 * 1000  # 7 天


def _read_heartbeat(event, now_ms, cur_root, cur_ver):
    ev = {"seen": False, "fresh": False, "current": False, "age_s": None,
          "plugin_version": None, "pkg_root": None}
    try:
        data = json.loads(paths.hook_heartbeat_path(event).read_text())
        ts = int(data["ts"])
    except (OSError, ValueError, KeyError, TypeError):
        return ev
    age = now_ms - ts
    ev["seen"] = True
    ev["age_s"] = round(age / 1000, 1)
    ev["fresh"] = 0 <= age <= HOOK_HEARTBEAT_FRESH_MS   # 挡未来时间戳
    ev["plugin_version"] = data.get("plugin_version")
    ev["pkg_root"] = data.get("pkg_root")
    ev["current"] = (ev["pkg_root"] == cur_root and ev["plugin_version"] == cur_ver)
    return ev


def hooks_live_status(now_ms=None):
    """plugin hooks 生效信号(哨兵心跳)。**advisory only** —— 权威证明只有「本次 Stop 握手成功
    + 会话内 ✅ 已绑定」。顶层 `confirmed` = Stop 心跳 seen ∧ fresh ∧ current。"""
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    from . import version as versionmod
    cur_root, cur_ver = versionmod.install_identity()
    stop = _read_heartbeat("stop", now, cur_root, cur_ver)
    se = _read_heartbeat("session_end", now, cur_root, cur_ver)
    return {
        "advisory": True,
        "window_ms": HOOK_HEARTBEAT_FRESH_MS,
        "current_install": {"pkg_root": cur_root, "plugin_version": cur_ver},
        "stop": stop,
        "session_end": se,
        "confirmed": bool(stop["seen"] and stop["fresh"] and stop["current"]),
    }


def foreign_stop_hooks():
    """best-effort:扫 settings.json 里**非本 plugin** 的 Stop hook(阻断型共存告警,信息性)。"""
    try:
        obj = json.loads(paths.settings_json_path().read_text())
    except (OSError, ValueError):
        return []
    out = []
    for entry in (obj.get("hooks") or {}).get("Stop") or []:
        for h in (entry or {}).get("hooks") or []:
            cmd = (h or {}).get("command", "")
            if cmd and "slack-bridge/hooks/stop_hook.py" not in cmd:
                out.append(cmd)
    return out


# ---------------------------------------------------------------- chats / open-dm
def _chat_entry(c, owner):
    """conversations.list 单项 → 条目或 None(过滤:归档、非成员频道、非 owner 的 IM、mpim)。"""
    if not isinstance(c, dict) or not c.get("id"):
        return None
    cid = c["id"]
    if c.get("is_archived"):
        return None
    if c.get("is_im"):
        if owner and c.get("user") == owner:
            return {"chat_id": cid, "name": "owner DM", "type": "owner_dm", "is_member": True}
        return None
    if c.get("is_mpim"):
        return None
    if c.get("is_member") is not True:
        return None
    kind = "private_channel" if (c.get("is_private") or c.get("is_group")) else "public_channel"
    return {"chat_id": cid, "name": c.get("name"), "type": kind, "is_member": True}


def list_chats(client, cfg, page_cap=LIST_PAGE_CAP):
    """列出 bot 可绑的**全部**会话:频道按 `is_member`,IM 只留 `user==owner_user_id`(标 owner_dm)。
    `conversations.list(types=public_channel,private_channel,im, exclude_archived=true, limit=200)`,
    跟随 `response_metadata.next_cursor` 到空(**空页带 cursor 合法**,继续翻);cursor 重复 = 循环 → None;
    超过 page_cap 页 → None;任何调用失败 → None。契约:**要么完整列表,要么 None**,绝不返回残缺前缀。"""
    owner = cfg.get("owner_user_id")
    chats, seen_ids, seen_cursors, cursor = [], set(), set(), None
    for _ in range(page_cap):
        params = {"types": "public_channel,private_channel,im",
                  "exclude_archived": True, "limit": LIST_PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        res = client.call("conversations.list", params)
        if not res.ok or not isinstance(res.data, dict):
            return None
        channels = res.data.get("channels")
        if not isinstance(channels, list):
            return None
        for c in channels:
            e = _chat_entry(c, owner)
            if e is None or e["chat_id"] in seen_ids:
                continue
            seen_ids.add(e["chat_id"])
            if e["type"] == "owner_dm":
                e["is_pinned_owner_dm"] = (cfg.get("owner_dm_id") == e["chat_id"])
            chats.append(e)
        meta = res.data.get("response_metadata")
        nxt = meta.get("next_cursor") if isinstance(meta, dict) else None
        if nxt is None:
            nxt = ""
        if not isinstance(nxt, str):
            return None
        nxt = nxt.strip()
        if not nxt:
            return chats
        if nxt in seen_cursors:
            return None  # cursor 循环
        seen_cursors.add(nxt)
        cursor = nxt
    return None  # 翻页超上限:证明不了取全


def open_owner_dm(client, cfg):
    """`conversations.open(users=owner_user_id)` → 钉 `owner_dm_id`(`cfg.set_persist`)。
    → {"ok": True, "owner_dm_id": D…, "chat_id": D…, "already_pinned": bool} | {"ok": False, "error": …}。
    **幂等**(R1-m4):Slack 对同一 owner 总返回同一个 DM;已钉住同一 id 时不重写 config,只报 already_pinned;
    钉的是别的 id(陈旧)→ 覆盖为最新。只有 owner DM 可绑(拒他人 DM);`chats` 只列出、**不**钉住,
    列表里 `is_pinned_owner_dm=false` 的 owner DM 要先跑本命令再 bind,否则 bind 得 `foreign_dm`。"""
    owner = cfg.get("owner_user_id")
    if not owner:
        return {"ok": False, "error": "config 缺 owner_user_id"}
    res = client.call("conversations.open", {"users": owner})
    if not res.ok or not isinstance(res.data, dict):
        return {"ok": False, "error": "conversations.open 失败:%s" % (res.error,)}
    ch = res.data.get("channel")
    dm = ch.get("id") if isinstance(ch, dict) else None
    if not isinstance(dm, str) or not dm.startswith("D"):
        return {"ok": False, "error": "conversations.open 未返回 D… 会话 id"}
    already = cfg.get("owner_dm_id") == dm
    if not already:
        if hasattr(cfg, "set_persist"):
            cfg.set_persist("owner_dm_id", dm)
        else:
            cfg["owner_dm_id"] = dm
    return {"ok": True, "owner_dm_id": dm, "chat_id": dm, "already_pinned": already}


# ---------------------------------------------------------------- bind / unbind
def bind_prepare(conn, cfg, clock, prober, chat_id, chat_name, cwd, start_pid):
    # 目标 chat 不在 allowlist → 直接报错,零残留
    allow = cfg.get("chat_allowlist")
    if allow and chat_id not in allow:
        raise lifecycle.BindConflict(
            "chat_not_allowed",
            f"该会话不在 config.json 的 chat_allowlist 内({chat_id});改 allowlist 或换会话")
    # DM 只能绑 owner 自己的(bootstrap 后经 open-dm 钉死的 owner_dm_id);其它 D… 一律拒
    if isinstance(chat_id, str) and chat_id.startswith("D"):
        if not cfg.get("owner_dm_id") or chat_id != cfg.get("owner_dm_id"):
            raise lifecycle.BindConflict(
                "foreign_dm",
                f"DM {chat_id} 不是 owner DM(owner_dm_id={cfg.get('owner_dm_id')!r});"
                "只有 owner 自己的 DM 可绑 —— 先 `bridgectl open-dm` 钉住 owner DM 再 bind")
    inst = procs.find_cc_instance(prober, start_pid)
    if inst is None:
        raise lifecycle.BindConflict("no_instance", "无法定位 CC 实例(ppid 链解析失败)")
    pid, lstart = inst
    res = lifecycle.create_binding(conn, chat_id=chat_id, chat_name=chat_name,
                                   cwd=cwd, cc_pid=pid, cc_start=lstart, clock=clock)
    # shlex.join + sys.executable —— plugin 根/python 路径含空格也安全。
    listener_cmd = shlex.join(
        [sys.executable, str(paths.pkg_root() / "bin" / "listener.py"), res["binding_id"]])
    return {
        "binding_id": res["binding_id"],
        "marker": res["marker"],
        "banner": texts.BIND_BANNER,
        "listener_cmd": listener_cmd,
        "chat_id": chat_id,
        "chat_name": chat_name,
        "is_owner_dm": bool(chat_id and chat_id == cfg.get("owner_dm_id")),
        "ttl_minutes": constants.PENDING_BIND_TTL_MS // 60000,
    }


def wait_listener_claim(conn, binding_id, clock, sleep, timeout_ms):
    """等常驻 listener(插件 monitor 的 follower)认领刚建的绑定:行 listener_epoch>=1 即 True,
    每 0.5s 轮询,超 timeout_ms → False(调用方据此回退为手动起有参 listener)。已认领时不 sleep。"""
    deadline = clock.mono_ms() + timeout_ms
    while True:
        row = conn.execute("SELECT listener_epoch FROM bindings WHERE binding_id=?",
                           (binding_id,)).fetchone()
        if row is not None and row["listener_epoch"] >= 1:
            return True
        if clock.mono_ms() >= deadline:
            return False
        sleep(0.5)


def resolve_instance_binding(conn, prober, start_pid):
    inst = procs.find_cc_instance(prober, start_pid)
    if inst is None:
        return None
    pid, lstart = inst
    return conn.execute(
        "SELECT * FROM bindings WHERE cc_pid=? AND cc_start=? "
        "AND status IN ('starting','active')", (pid, lstart)).fetchone()


def unbind(conn, clock, prober, start_pid=None, binding_id=None):
    if binding_id is None:
        row = resolve_instance_binding(conn, prober, start_pid)
        if row is None:
            return {"ok": False, "error": "本 CC 实例没有 starting/active 绑定"}
        binding_id = row["binding_id"]
    won = lifecycle.terminate_binding(conn, binding_id, "user_unbind", clock)
    return {"ok": won, "binding_id": binding_id,
            "note": "已解绑(立即生效)" if won else "绑定已处于终态"}


# ---------------------------------------------------------------- status
STATUS_COUNTERS = (
    "staged_dup", "staged_invalid", "inbox_dup_message", "inbox_snapshot_upgraded",
    "dm_notice_suppressed", "ratelimit_hits", "cooldown_waits", "verify_hit", "verify_absent",
    "verify_resent", "verify_unconfirmed", "drain_quarantined", "group_cancelled_after_unconfirmed",
    "media_budget_exhausted", "worker_unexpected_exit", "hook_drop_count",
    "event_processing_errors", "malformed_event_lines",
)


def tokens_file_status():
    """当前 tokens.json 状态(**绝不含 token**):{present, version|None, app_token_present, error|None}。"""
    st = {"present": os.path.exists(str(paths.tokens_path())), "version": None,
          "app_token_present": None, "error": None}
    if not st["present"]:
        return st
    try:
        tokens, version = configmod.load_tokens(allow_env=False)
        st["version"] = version
        st["app_token_present"] = bool(tokens.get("app_token"))
    except configmod.ConfigError as e:
        st["error"] = str(e)
    return st


def status_report(conn, cfg, clock):
    now = clock.wall_ms()

    def age(ms):
        return None if ms is None else round((now - int(ms)) / 1000, 1)

    daemon = {
        "pid": db.get_state(conn, "daemon_pid"),
        "started_at": db.get_state(conn, "daemon_started_at"),
        "startup": db.get_state(conn, "startup"),
        "generation": db.get_state(conn, "daemon_generation"),
        "code_identity": db.get_state(conn, "daemon_code_identity"),
        "last_loop_age_s": age(db.get_state(conn, "last_loop_at")),
        "suspect_until": db.get_state(conn, "suspect_until"),
        "last_error": db.get_state(conn, "last_error"),
    }
    k = constants.SOCKET_KEY
    consumer = {
        "ready": db.get_state(conn, f"consumer_{k}_ready"),
        "last_status": db.get_state(conn, f"consumer_{k}_last_status"),
        "restarts": db.get_state(conn, f"consumer_{k}_restarts", "0"),
        "last_exit_rc": db.get_state(conn, f"consumer_{k}_last_exit_rc"),
    }
    bindings = []
    for b in conn.execute(
            "SELECT * FROM bindings ORDER BY binding_seq DESC LIMIT 20").fetchall():
        bindings.append({
            "binding_id": b["binding_id"][:8],
            "chat": b["chat_name"] or b["chat_id"],
            "chat_id": b["chat_id"],
            "status": b["status"],
            "phase": b["bind_phase"],
            "close_reason": b["close_reason"],
            "session": (b["session_id"] or "")[:8] or None,
            "listener_epoch": b["listener_epoch"],
            "beat_age_s": age(b["listener_beat_at"]),
            "suspect_since": b["suspect_since"],
        })
    jobs_by_state = {r[0]: r[1] for r in conn.execute(
        "SELECT state, COUNT(*) FROM outbound_jobs GROUP BY state")}
    deliveries_by_state = {r[0]: r[1] for r in conn.execute(
        "SELECT state, COUNT(*) FROM deliveries GROUP BY state")}
    inbox_by_state = {r[0]: r[1] for r in conn.execute(
        "SELECT state, COUNT(*) FROM inbox GROUP BY state")}
    events_by_state = {r[0]: r[1] for r in conn.execute(
        "SELECT state, COUNT(*) FROM slack_events GROUP BY state")}
    quarantined = [{
        "seq": r["seq"], "envelope_type": r["envelope_type"], "chat_id": r["chat_id"],
        "received_at": r["received_at"], "drain_attempts": r["drain_attempts"],
        "error": (r["error"] or "")[:200],
    } for r in conn.execute(
        "SELECT * FROM slack_events WHERE state='quarantined' ORDER BY seq DESC LIMIT 20").fetchall()]
    counters = {}
    for key in STATUS_COUNTERS:
        v = db.get_state(conn, key)
        if v is not None:
            counters[key] = v
    for r in conn.execute("SELECT key, value FROM daemon_state WHERE key LIKE 'event_dropped_%'"):
        counters[r[0]] = r[1]
    cooldowns = {}
    for r in conn.execute("SELECT key, value FROM daemon_state WHERE key LIKE ?",
                          (constants.COOLDOWN_KEY_PREFIX + "%",)):
        try:
            until = int(r[1])
        except (TypeError, ValueError):
            continue
        cooldowns[r[0][len(constants.COOLDOWN_KEY_PREFIX):]] = {
            "until": until, "remaining_s": max(0.0, round((until - now) / 1000, 1)),
            "active": until > now}
    gate = db.get_state(conn, constants.GATE_KEY)
    gate_version = db.get_state(conn, constants.GATE_VERSION_KEY)
    tokens_st = tokens_file_status()
    verify_cap = db.get_state(conn, constants.VERIFY_CAPABILITY_KEY)
    verify_ver = db.get_state(conn, constants.VERIFY_CAPABILITY_VERSION_KEY)
    rep = {
        "fingerprint": {k2: cfg.get(k2) for k2 in
                        ("team_id", "team_name", "bot_user_id", "bot_id", "bot_name", "app_id",
                         "owner_user_id", "owner_dm_id")},
        "schema_version": db.get_state(conn, "schema_version"),
        "chat_allowlist": cfg.get("chat_allowlist") or "全部(未限制)",
        "markdown_mode": cfg.get("markdown_mode") or constants.MARKDOWN_MODE_DEFAULT,
        "outbound_gate": gate,
        "outbound_gate_tokens_version": gate_version,
        "tokens_version_seen": db.get_state(conn, constants.TOKENS_VERSION_SEEN_KEY),
        "tokens_file": tokens_st,
        "credentials_verified": bool(gate == "ok" and gate_version
                                     and gate_version == tokens_st.get("version")),
        "verify_capability": verify_cap,
        "verify_capability_tokens_version": verify_ver,
        "auto_resend_enabled": bool(verify_cap == constants.VERIFY_CAP_OK and verify_ver
                                    and verify_ver == tokens_st.get("version")),
        "cooldowns": cooldowns,
        "daemon": daemon,
        "consumer": consumer,
        "bindings": bindings,
        "outbound_jobs": jobs_by_state,
        "deliveries": deliveries_by_state,
        "inbox": inbox_by_state,
        "slack_events": events_by_state,
        "quarantined": quarantined,
        "counters": counters,
    }
    hints = []
    if gate is None:
        hints.append("outbound_gate 尚未写入(daemon 未启动过?)→ 先 ensure-daemon。")
    elif gate == "mismatch":
        hints.append("身份不符:tokens.json 的 bot 与 config.json 指纹不一致,出站关门、daemon 拒启;"
                     "换回原 app 的 token,或删 config.json/tokens.json 重新 bootstrap。")
    elif gate.startswith("degraded"):
        hints.append("出站停摆(%s):auth.test 失败或 tokens.json 不可读;daemon 带退避重探。"
                     "检查网络 / token 是否被撤销 / 文件权限 0600。" % gate)
    if gate == "ok" and gate_version and tokens_st.get("version") \
            and gate_version != tokens_st["version"]:
        hints.append("tokens.json 已变但 daemon 尚未重验(gate 版本 ≠ 文件版本):"
                     "notify/StopFailure 直发会被 credentials-unverified 拒,等 daemon 下一 tick。")
    if verify_cap != constants.VERIFY_CAP_OK or (verify_ver and verify_ver != tokens_st.get("version")):
        hints.append("verify_capability 非 ok 或版本不匹配 → 自动重发关闭(unknown 只核验,"
                     "三次未见即 unconfirmed);跑 `bridgectl probe --chat-id <测试频道> --write-config` 确证。")
    if quarantined:
        hints.append("有 %d 条 slack_events 被隔离(drain 反复失败);看 error 字段与 daemon.log。"
                     % len(quarantined))
    if hints:
        rep["hints"] = hints
    return rep


# ---------------------------------------------------------------- daemon 拉起
def daemon_lock_held():
    lock = paths.lock_path()
    if not lock.exists():
        return False
    fd = os.open(str(lock), os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


# daemon 挂死恢复:锁被持有但心跳陈旧(>HUNG_THRESHOLD)= 挂死 → 按记录的 (daemon_pid, daemon_proc_start)
# 精确匹配后 SIGTERM → 等退出 → 接管重启;身份不匹配(pid 复用/无记录)绝不杀随机进程 → failed。
# 阈值必须 ≥ 单次**最长同步网络操作** + 余量:daemon 单线程,一次附件物化期间主循环阻塞;
# media.materialize 让整条消息的所有附件共享一个 DOWNLOAD_DEADLINE_S=90s 绝对截止,且文件之间刷心跳
# (R1-M6),所以两次心跳之间最长 = 90s + SIGTERM→SIGKILL 宽限 2s;取 90s + 60s = 150s。
# 守卫:tests/test_media.py::test_hung_threshold_covers_shared_deadline。
HUNG_THRESHOLD_MS = (constants.DOWNLOAD_DEADLINE_S + 60) * 1000
_POLL_STEP_S = 0.3


class _FlockSingleflight:
    def __init__(self, path):
        self.path = str(path)
        self.fd = None

    def try_acquire(self):
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            os.close(self.fd)
            self.fd = None
            return False

    def release(self):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


# ensure() 的返回值语义 —— 只有 READY_RESULTS 才代表"daemon 就绪、bind 可继续";
# in_progress/failed/down 都不就绪。
READY_RESULTS = frozenset({"running", "started", "recovered"})


def is_ready_result(result):
    return result in READY_RESULTS


def state_ready(st, now_ms):
    """就绪 = 心跳新鲜 ∧ startup ∈ {running,degraded} ∧ 同代 generation。
    probing/refused/stopping 不算就绪;generation 对齐防"新代心跳 + 旧代 running"误判。"""
    from .daemon_core import parse_startup, _READY_PHASES
    if not st or st.get("last_loop_at") is None:
        return False
    if (now_ms - int(st["last_loop_at"])) > HUNG_THRESHOLD_MS:
        return False  # 心跳陈旧(回拨=负值,视为新鲜)
    phase, sgen = parse_startup(st.get("startup"))
    if phase not in _READY_PHASES:
        return False
    return sgen == (st.get("daemon_generation") or "")


def daemon_healthy(conn, now_ms=None):
    """listener/调用方探活:锁被持有 ∧ startup 就绪(心跳新鲜+running/degraded+同代)。"""
    if not daemon_lock_held():
        return False
    try:
        st = {
            "last_loop_at": db.get_state(conn, "last_loop_at"),
            "startup": db.get_state(conn, "startup"),
            "daemon_generation": db.get_state(conn, "daemon_generation"),
        }
    except Exception:
        return False
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    return state_ready(st, now)


# 等 probing 结论(record_identity + gate.startup:一次 auth.test ≤ SEND_TIMEOUT_S)的上限,含余量。
STARTUP_PROBE_WAIT_S = 40


class DaemonSupervisor:
    """依赖全注入的 ensure 逻辑(可测):lock_held()/read_state()/spawn()/kill(pid,sig);
    singleflight(可选,对象须有 try_acquire()/release())防重叠接管。
    liveness(心跳)与 readiness(startup+generation)分离——
    ① takeover(SIGTERM+重启)**只依据心跳陈旧**,绝不因'尚未就绪'而杀;
    ② 心跳新鲜的 probing → 等结论(不 kill);refused → 终态失败(不重启);
    ③ 拉起/等待路径以 baseline generation 拒'旧代完整对齐'假成功。"""

    def __init__(self, *, lock_held, read_state, spawn, kill, prober,
                 now_ms, sleep, wait_s=12, singleflight=None, probe_wait_s=None,
                 mono_ms=None):
        self.lock_held = lock_held
        self.read_state = read_state
        self.spawn = spawn
        self.kill = kill
        self.prober = prober
        # now_ms = 墙钟(心跳新鲜度/state_ready:last_loop_at 是 DB 持久化的墙钟时间戳);
        # mono_ms = 单调钟(仅本进程内的等待 deadline —— 墙钟跳变不得让 deadline 提前结束)。
        self.now_ms = now_ms
        self.mono_ms = mono_ms or (lambda: int(time.monotonic() * 1000))
        self.sleep = sleep
        self.wait_s = wait_s
        self.singleflight = singleflight
        self.probe_wait_s = probe_wait_s if probe_wait_s is not None else STARTUP_PROBE_WAIT_S

    def _ready(self, st):
        return state_ready(st, self.now_ms())

    def _heartbeat_fresh(self, st):
        if not st or st.get("last_loop_at") is None:
            return False
        return (self.now_ms() - int(st["last_loop_at"])) <= HUNG_THRESHOLD_MS

    def _gen_of(self, st):
        from .daemon_core import parse_startup
        return (st or {}).get("daemon_generation") or parse_startup((st or {}).get("startup"))[1]

    def _phase_of(self, st):
        from .daemon_core import parse_startup
        return parse_startup((st or {}).get("startup"))[0]

    def ensure(self):
        if self.singleflight is not None:
            if not self.singleflight.try_acquire():
                return self._await_handoff(self._gen_of(self.read_state()))
            try:
                return self._ensure_locked()
            finally:
                self.singleflight.release()
        return self._ensure_locked()

    def _await_handoff(self, baseline_gen):
        """busy 路径:等 owner 的 handoff。① 每轮先 try_acquire —— 拿到 = owner 已结束,走权威 _ensure_locked;
        ② 没拿到 → 只认'非空且 != baseline 的新代且 state_ready'为成功;baseline 代一律不认;③ 短 sleep 重试。
        caller_deadline 覆盖 owner 最坏临界区 = 2*wait_s + probe_wait_s(单调钟)。
        到期但 daemon 活着在忙 → in_progress(可重试);无进展/死 → failed。"""
        budget_s = 2 * self.wait_s + self.probe_wait_s
        deadline = self.mono_ms() + int(budget_s * 1000)
        max_steps = int(budget_s / _POLL_STEP_S) + 2
        steps = 0
        while self.mono_ms() < deadline and steps < max_steps:
            if self.singleflight.try_acquire():
                try:
                    return self._ensure_locked()
                finally:
                    self.singleflight.release()
            st = self.read_state()
            gen = self._gen_of(st)
            if self.lock_held() and gen and gen != baseline_gen and self._ready(st):
                return "running"
            self.sleep(_POLL_STEP_S)
            steps += 1
        st = self.read_state()
        if self.lock_held() and self._heartbeat_fresh(st):
            return "in_progress"
        return "failed"

    def _ensure_locked(self):
        st = self.read_state()
        if self.lock_held() and self._ready(st):
            return "running"
        if self.lock_held():
            if self._heartbeat_fresh(st):
                phase = self._phase_of(st)
                if phase == "refused":
                    return self._await_lock_release_then_failed()
                return self._await_probing_conclusion()
            return self._takeover_and_restart(st)
        return self._spawn_and_wait()

    def _await_lock_release_then_failed(self):
        for _ in range(int(self.wait_s / _POLL_STEP_S) + 1):
            if not self.lock_held():
                break
            self.sleep(_POLL_STEP_S)
        return "failed"

    def _await_probing_conclusion(self):
        for _ in range(int(self.probe_wait_s / _POLL_STEP_S) + 1):
            st = self.read_state()
            if not self.lock_held():
                return "failed"
            if self._ready(st):
                return "running"
            if self._phase_of(st) == "refused":
                return self._await_lock_release_then_failed()
            if not self._heartbeat_fresh(st):
                return "failed"
            self.sleep(_POLL_STEP_S)
        return "failed"

    def _takeover_and_restart(self, st):
        import signal as _signal
        pid = st.get("daemon_pid") if st else None
        pstart = st.get("daemon_proc_start") if st else None
        if not pid or not pstart:
            return "failed"
        if procs.probe_alive(self.prober, int(pid), pstart) != procs.ALIVE:
            return "failed"
        try:
            self.kill(int(pid), _signal.SIGTERM)
        except OSError:
            return "failed"
        for _ in range(int(self.wait_s / _POLL_STEP_S) + 1):
            if not self.lock_held():
                break
            self.sleep(_POLL_STEP_S)
        else:
            return "failed"
        return "recovered" if self._spawn_and_wait() == "started" else "failed"

    def _spawn_and_wait(self):
        baseline = self._gen_of(self.read_state())
        self.spawn()
        steps = int((self.wait_s + self.probe_wait_s) / _POLL_STEP_S) + 1
        for _ in range(steps):
            self.sleep(_POLL_STEP_S)
            st = self.read_state()
            gen = self._gen_of(st)
            if self.lock_held() and self._ready(st) and gen != baseline:
                return "started"
            if self.lock_held() and gen != baseline and self._phase_of(st) == "refused":
                return "failed"
        return "failed"


def _read_daemon_state():
    try:
        conn = db.connect(paths.db_path(), busy_timeout_ms=2000)
        try:
            return {
                "last_loop_at": db.get_state(conn, "last_loop_at"),
                "daemon_pid": db.get_state(conn, "daemon_pid"),
                "daemon_proc_start": db.get_state(conn, "daemon_proc_start"),
                "startup": db.get_state(conn, "startup"),
                "daemon_generation": db.get_state(conn, "daemon_generation"),
                "daemon_code_identity": db.get_state(conn, "daemon_code_identity"),
            }
        finally:
            conn.close()
    except Exception:
        return None


def _spawn_daemon():
    daemon_py = paths.pkg_root() / "bin" / "daemon.py"
    logf = open(paths.daemon_log_path(), "a")
    try:
        subprocess.Popen([sys.executable, str(daemon_py)],
                         stdin=subprocess.DEVNULL, stdout=logf, stderr=logf,
                         start_new_session=True)
    finally:
        logf.close()


def ensure_daemon(wait_s=12, spawn=True):
    if not spawn:
        try:
            conn = db.connect(paths.db_path(), busy_timeout_ms=2000)
            try:
                return "running" if daemon_healthy(conn) else "down"
            finally:
                conn.close()
        except Exception:
            return "down"
    sup = DaemonSupervisor(
        lock_held=daemon_lock_held, read_state=_read_daemon_state,
        spawn=_spawn_daemon, kill=os.kill, prober=procs.SystemProber(),
        now_ms=lambda: int(time.time() * 1000),
        mono_ms=lambda: int(time.monotonic() * 1000),
        sleep=time.sleep, wait_s=wait_s,
        singleflight=_FlockSingleflight(paths.ensure_lock_path()))
    return sup.ensure()


# ---------------------------------------------------------------- bind 前置:code-identity 串行检查
def reconcile_code_identity(*, my_identity, read_state, lock_held, prober, kill,
                            ensure, sleep, wait_s=12):
    """bind 前置**串行**检查:读 daemon_state.daemon_code_identity,与本 CLI code_identity_str() 比。
    **fail-closed 不变式**:reconcile 成功返回(无 `error`)⟹「identity 已匹配」或「本版本新 daemon 已
    (重新)拉起」二者之一 —— 绝不在 identity 不一致时放行用旧代码 bind。"""
    if not lock_held():
        return {"restarted": True, "reason": "no-daemon-respawn", "new": my_identity,
                "state": ensure()}
    st = read_state() or {}
    recorded = st.get("daemon_code_identity")
    if recorded == my_identity:
        return {"restarted": False, "reason": "match", "new": my_identity}
    pid, pstart = st.get("daemon_pid"), st.get("daemon_proc_start")
    can_kill = bool(pid) and bool(pstart) \
        and procs.probe_alive(prober, int(pid), pstart) == procs.ALIVE
    if not can_kill:
        if not lock_held():
            return {"restarted": True, "reason": "respawned-daemon-gone",
                    "old": recorded, "new": my_identity, "state": ensure()}
        return {"restarted": False, "reason": "unverified-cannot-restart",
                "old": recorded, "new": my_identity,
                "error": (f"检测到不同版本/位置的 daemon(旧:{recorded},本:{my_identity})但**无法"
                          f"安全自动重启**(无法定位/确认其进程,不误杀)。请手动停止旧 daemon 后重试:"
                          f"先 `... status` 看 `daemon.pid` → `kill <pid>`(见 README)。")}
    try:
        kill(int(pid), signal.SIGTERM)
    except OSError as e:
        return {"restarted": False, "reason": "kill-failed", "old": recorded, "new": my_identity,
                "error": f"检测到不同版本/位置的 daemon(旧:{recorded}),SIGTERM 失败:{e};请手动停止后重试"}
    for _ in range(int(wait_s / _POLL_STEP_S) + 1):
        if not lock_held():
            break
        sleep(_POLL_STEP_S)
    else:
        return {"restarted": False, "reason": "no-exit", "old": recorded, "new": my_identity,
                "error": "旧 daemon SIGTERM 后未退出(flock 未释放);请手动停止旧 daemon 后重试(见 README)"}
    return {"restarted": True, "reason": "restarted", "old": recorded, "new": my_identity,
            "state": ensure()}


def reconcile_daemon_code_identity(wait_s=12):
    from . import version as versionmod
    return reconcile_code_identity(
        my_identity=versionmod.code_identity_str(),
        read_state=_read_daemon_state, lock_held=daemon_lock_held,
        prober=procs.SystemProber(), kill=os.kill, ensure=ensure_daemon,
        sleep=time.sleep, wait_s=wait_s)


# ---------------------------------------------------------------- probe / doctor(显式诊断例外,I2)
_PROBE_MOD = None


def load_probe_module():
    """scripts/capability_probe.py 不是包:按路径 import 一次并缓存。"""
    global _PROBE_MOD
    if _PROBE_MOD is None:
        p = paths.pkg_root() / "scripts" / "capability_probe.py"
        spec = importlib.util.spec_from_file_location("slack_bridge_capability_probe", str(p))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _PROBE_MOD = mod
    return _PROBE_MOD


def default_probe_client_factory(tokens, version, cooldown_store):
    return SlackClient(token=tokens["bot_token"], cooldown_store=cooldown_store,
                       timeout_s=constants.SEND_TIMEOUT_S, tokens_version=version)


def doctor(chat_id, *, cfg=None, client_factory=None, write_config=False, conn=None):
    """doctor = capability probe(scripts/capability_probe.run_probe,真发两条并删除)+ daemon 健康。
    需要 token(文件或 env),**不需要 daemon 在跑**;不受 outbound_gate 版本门约束,但遵守方法级冷却
    (bridge.db 存在时用 daemon_state 冷却存储)。→ {"ok", "probe", "daemon", "written"?}。
    绝不含 token。"""
    probe = load_probe_module()
    if cfg is None:
        cfg = configmod.ConfigSnapshot.load()
    tokens, version = configmod.load_tokens()
    own_conn = None
    if conn is None:
        dbf = paths.db_path()
        if dbf.exists() or write_config:
            paths.ensure_data_dir()
            own_conn = db.connect(dbf)
            db.init_schema(own_conn, paths.schema_path())
            conn = own_conn
    try:
        store = DaemonStateCooldownStore(conn) if conn is not None else InMemoryCooldownStore()
        factory = client_factory or default_probe_client_factory
        client = factory(tokens, version, store)
        result = probe.run_probe(client, cfg, chat_id, version)
        written = None
        if write_config and conn is not None:
            written = probe.write_results(result, cfg, conn)
        daemon = {"lock_held": daemon_lock_held(), "healthy": False,
                  "outbound_gate": None, "outbound_gate_tokens_version": None,
                  "verify_capability": None, "verify_capability_tokens_version": None,
                  "tokens_version_current": version}
        if conn is not None:
            daemon["healthy"] = daemon_healthy(conn)
            daemon["outbound_gate"] = db.get_state(conn, constants.GATE_KEY)
            daemon["outbound_gate_tokens_version"] = db.get_state(conn, constants.GATE_VERSION_KEY)
            daemon["verify_capability"] = db.get_state(conn, constants.VERIFY_CAPABILITY_KEY)
            daemon["verify_capability_tokens_version"] = db.get_state(
                conn, constants.VERIFY_CAPABILITY_VERSION_KEY)
        ok = bool(result.get("identity_ok") and result.get("complete")
                  and result.get("verify_capability") == constants.VERIFY_CAP_OK
                  and result.get("cleanup_ok"))
        out = {"ok": ok, "probe": result, "daemon": daemon}
        if written is not None:
            out["written"] = written
        if not result.get("identity_ok"):
            out["error"] = "身份不符或 auth.test 失败:未发送、未导入"
        elif not result.get("complete"):
            out["error"] = "探测未完成(API 失败/冷却):见 probe.errors"
        elif not daemon["healthy"]:
            out["note"] = "daemon 未在跑(probe 本身不需要它);bind 前跑 ensure-daemon"
        return out
    finally:
        if own_conn is not None:
            own_conn.close()
