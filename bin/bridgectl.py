#!/usr/bin/env python3
"""bridgectl:slack-bridge 控制 CLI(skill 从 CC 内调用;人也可直接跑)。
子命令:bootstrap / preflight / chats / open-dm / bind / unbind / status / ensure-daemon /
probe / doctor / allow。输出:machine-friendly JSON 到 stdout(SKILL.md 解析);人读信息带在字段里。
**token 只从 env(SLACK_BOT_TOKEN / SLACK_APP_TOKEN)或 `--tokens-stdin` JSON 进入,绝不上 argv、
绝不出现在任何输出里。**"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lib import config as configmod  # noqa: E402
from lib import constants, ctl, db, lifecycle, paths, procs, senderallow, util  # noqa: E402
from lib.clock import SystemClock  # noqa: E402
from lib.slackapi import SlackClient  # noqa: E402


def out(obj, code=0):
    print(json.dumps(obj, ensure_ascii=False, indent=2))
    sys.exit(code)


def open_db():
    paths.ensure_data_dir()
    conn = db.connect(paths.db_path(), busy_timeout_ms=constants.BUSY_TIMEOUT_DAEMON_MS)
    db.init_schema(conn, paths.schema_path())
    return conn


def load_snapshot():
    """ConfigSnapshot(set_persist 可用);缺 config → ConfigError(由 main 兜成 JSON exit 2)。"""
    return configmod.ConfigSnapshot.load()


def cli_client(timeout_s=constants.SEND_TIMEOUT_S):
    """CLI 用 SlackClient:tokens 文件为准(env 覆盖仅供无文件的测试/一次性场景)。"""
    tokens, version = configmod.load_tokens()
    return SlackClient(token=tokens["bot_token"], timeout_s=timeout_s, tokens_version=version)


# ---------------------------------------------------------------- bootstrap
def _read_bootstrap_tokens(args):
    """env 或 stdin JSON;**没有** argv 选项可以传 token。"""
    if args.tokens_stdin:
        try:
            raw = sys.stdin.read()
            obj = json.loads(raw)
        except Exception as e:  # noqa: BLE001
            out({"ok": False, "error": "--tokens-stdin 需要 stdin 提供 JSON {bot_token, app_token}:%s"
                 % type(e).__name__}, 2)
        if not isinstance(obj, dict):
            out({"ok": False, "error": "--tokens-stdin JSON 顶层须为对象"}, 2)
        return {"bot_token": obj.get("bot_token"), "app_token": obj.get("app_token")}
    bot = os.environ.get(configmod.ENV_BOT_TOKEN)
    app = os.environ.get(configmod.ENV_APP_TOKEN)
    if not bot:
        out({"ok": False,
             "error": "缺 token:在**你自己的终端**里 `export SLACK_BOT_TOKEN=xoxb-… SLACK_APP_TOKEN=xapp-…` "
                      "后再跑 bootstrap(或 `--tokens-stdin < tokens.json`);绝不要把 token 贴进对话/argv"}, 2)
    return {"bot_token": bot, "app_token": app or None}


def cmd_bootstrap(args):
    tokens = _read_bootstrap_tokens(args)
    allow = [x.strip() for x in (args.chat_allowlist or "").split(",") if x.strip()] or None
    try:
        cfg = ctl.bootstrap(
            lambda t: SlackClient(token=t["bot_token"], timeout_s=constants.SEND_TIMEOUT_S),
            owner=args.owner, owner_email=args.owner_email, app_id=args.app_id,
            tokens=tokens, chat_allowlist=allow, clock=SystemClock())
    except configmod.ConfigError as e:
        out({"ok": False, "error": util.redact_secrets(str(e))}, 2)
    tok = ctl.tokens_file_status()
    res = {
        "ok": True,
        "config": {k: cfg.get(k) for k in
                   ("team_id", "team_name", "bot_user_id", "bot_id", "bot_name", "app_id",
                    "owner_user_id", "chat_allowlist")},
        "config_path": str(paths.config_path()),
        "tokens": {"path": str(paths.tokens_path()), "version": tok.get("version"),
                   "app_token_present": tok.get("app_token_present")},
        "next_steps": [
            "probe --chat-id <测试频道>(--write-config):真机确证 markdown_text / metadata 读回能力",
            "open-dm:钉住 owner DM(只有 owner DM 可绑)",
            "把 bot /invite 进要绑的频道 → chats 看列表 → bind",
        ],
    }
    if not tok.get("app_token_present"):
        res["warning"] = ("没有 app_token(xapp-…):Socket Mode consumer 无法连接,收不到任何消息。"
                          "重新 bootstrap 时把 SLACK_APP_TOKEN 一起给。")
    out(res)


# ---------------------------------------------------------------- preflight
# consumer 解释器里跑的探针(以**字符串**形式存在:本文件顶层不 import slack_sdk,
# tests/test_consumer.py::test_only_consumer_imports_slack_sdk 守着)。版本来源优先级(R1-m3):
#   importlib.metadata.version("slack_sdk")(dist 元数据)→ slack_sdk.version.__version__ → slack_sdk.__version__。
# slack_sdk 3.44.x **没有**顶层 __version__(在 slack_sdk.version),旧探针 `print(slack_sdk.__version__)`
# 会 AttributeError → 把已安装的 sdk 误报成 importable=false。
SDK_PROBE_CODE = "\n".join([
    "import json, sys",
    "try:",
    "    import slack_sdk",
    "except Exception as e:",
    "    print(json.dumps({'importable': False, 'error': type(e).__name__})); sys.exit(0)",
    "v = None; src = None",
    "for name in ('metadata', 'slack_sdk.version', 'slack_sdk.__version__'):",
    "    try:",
    "        if name == 'metadata':",
    "            from importlib.metadata import version as _mv; v = _mv('slack_sdk')",
    "        elif name == 'slack_sdk.version':",
    "            from slack_sdk.version import __version__ as v",
    "        else:",
    "            v = slack_sdk.__version__",
    "    except Exception:",
    "        v = None",
    "    if v:",
    "        src = name; break",
    "print(json.dumps({'importable': True, 'version': (str(v) if v else None), 'source': src}))",
])


def _slack_sdk_status(python_exe, env=None):
    """consumer 依赖 slack_sdk(>=3.44,<4)。用 consumer 将使用的解释器跑 SDK_PROBE_CODE:
    → {"importable": True|False|None, "version": str|None, "source": str|None, "python": python_exe}。
    importable=None = 探针本身没跑成(解释器不可执行 / 超时 / 输出不可解析),信息性,不当作"未安装"。"""
    try:
        r = subprocess.run([python_exe, "-c", SDK_PROBE_CODE], capture_output=True, text=True,
                           timeout=15, env=env)
        lines = [l for l in (r.stdout or "").splitlines() if l.strip()]
        obj = json.loads(lines[-1]) if (r.returncode == 0 and lines) else None
        if not isinstance(obj, dict) or "importable" not in obj:
            return {"importable": None, "version": None, "source": None, "python": python_exe}
        return {"importable": bool(obj["importable"]), "version": obj.get("version"),
                "source": obj.get("source"), "python": python_exe}
    except Exception:  # noqa: BLE001
        return {"importable": None, "version": None, "source": None, "python": python_exe}


def cmd_preflight(args):
    cfg = configmod.load_config()
    hooks = ctl.hooks_live_status()
    tok = ctl.tokens_file_status()
    ready = bool(cfg) and bool(tok.get("present")) and not tok.get("error")
    python_exe = (cfg or {}).get("consumer_python") or sys.executable
    sdk = _slack_sdk_status(python_exe)
    res = {
        "ok": ready,
        "config_present": bool(cfg),
        "tokens_present": bool(tok.get("present")),
        "tokens_ok": bool(tok.get("present")) and not tok.get("error"),
        "tokens_error": tok.get("error"),
        "app_token_present": tok.get("app_token_present"),
        "owner_dm_id": (cfg or {}).get("owner_dm_id"),
        "slack_sdk": sdk,
        "hooks": hooks,
        "next_steps": [],
    }
    if not cfg or not tok.get("present"):
        res["next_steps"].append(
            "首次配置(每人一次,在**用户自己的终端**里跑,token 走 env):"
            "export SLACK_BOT_TOKEN=xoxb-… SLACK_APP_TOKEN=xapp-… && "
            "python3 \"${CLAUDE_SKILL_DIR}/../../bin/bridgectl.py\" bootstrap --owner U…(或 --owner-email …)")
    elif tok.get("error"):
        res["next_steps"].append("tokens.json 不可用:%s(权限须 0600)" % tok["error"])
    if sdk.get("importable") is False:
        res["next_steps"].append(
            "consumer 需要 slack_sdk:`%s -m pip install 'slack_sdk>=3.44,<4'`" % python_exe)
    if cfg and tok.get("present") and not tok.get("app_token_present"):
        res["next_steps"].append("tokens.json 没有 app_token(xapp-…):收不到消息;重新 bootstrap 补上")
    if not hooks["confirmed"]:
        stop = hooks["stop"]
        if not stop["seen"]:
            why = "尚未检测到 Stop hook 心跳(全新安装 / 尚未完成一轮对话时是正常的)"
        elif not stop["fresh"]:
            why = f"Stop hook 心跳过旧或时间戳异常(age {stop['age_s']}s)"
        elif not stop["current"]:
            why = (f"Stop hook 心跳来自另一 install/版本"
                   f"(心跳 {stop['pkg_root']}@{stop['plugin_version']} ≠ "
                   f"当前 {hooks['current_install']['pkg_root']}@{hooks['current_install']['plugin_version']})")
        else:
            why = "Stop hook 未确认"
        res["next_steps"].append(
            f"{why}。hooks 由 plugin 自带 —— 若刚 /plugin install 或更新了 slack-bridge,请**重启 "
            "Claude Code** 让 hooks 生效;之后随便完成一轮对话即会记录心跳(可再跑 preflight 确认)。"
            "**绝不需要手改 settings.json。**(心跳仅诊断提示;权威证明是握手成功 + 会话内 ✅ 已绑定。)")
    foreign = ctl.foreign_stop_hooks()
    if foreign:
        res["foreign_stop_hooks"] = foreign
        res["warning"] = ("检测到其它 Stop hook(可能是阻断型):同一 turn 可能触发多次 Stop,"
                          "普通 turn 存在重复转发组风险(已声明限制;bind turn 有链闩保护)")
    out(res, 0 if ready else 3)


# ---------------------------------------------------------------- chats / open-dm
def cmd_chats(args):
    cfg = load_snapshot()
    chats = ctl.list_chats(cli_client(), cfg)
    if chats is None:
        out({"ok": False,
             "error": "会话列表未能完整取回(conversations.list 失败 / 分页未取完 / cursor 循环)"}, 2)
    out({"ok": True, "chats": chats, "owner_dm_id": cfg.get("owner_dm_id"),
         "note": "频道只列 bot 已是成员的(先 /invite @bot);DM 只列 owner 自己的;"
                 "owner DM 未出现时跑 open-dm"})


def cmd_open_dm(args):
    cfg = load_snapshot()
    res = ctl.open_owner_dm(cli_client(), cfg)
    out(res, 0 if res.get("ok") else 2)


# ---------------------------------------------------------------- bind / unbind
def cmd_bind(args):
    cfg = load_snapshot()
    hooks = ctl.hooks_live_status()
    state = ctl.ensure_daemon()
    if not ctl.is_ready_result(state):
        if state == "in_progress":
            out({"ok": False, "retryable": True, "daemon": state,
                 "error": "daemon 正在启动(尚未就绪),请稍候重跑 /slack-bridge:bridge bind"}, 5)
        out({"ok": False, "daemon": state, "error": "daemon 拉起失败,看 daemon.log"}, 2)
    reconcile = ctl.reconcile_daemon_code_identity()
    if reconcile.get("error"):
        out({"ok": False, "code_identity": reconcile, "error": reconcile["error"]}, 6)
    if reconcile.get("restarted"):
        state = reconcile.get("state", state)
        if not ctl.is_ready_result(state):
            out({"ok": False, "daemon": state, "code_identity": reconcile,
                 "error": "检测到旧版本 daemon 已重启,但新 daemon 未就绪;请稍候重跑 bind"}, 5)
    conn = open_db()
    clock = SystemClock()
    try:
        res = ctl.bind_prepare(conn, cfg, clock, procs.SystemProber(),
                               chat_id=args.chat_id, chat_name=args.chat_name,
                               cwd=os.getcwd(), start_pid=os.getppid())
    except lifecycle.BindConflict as e:
        out({"ok": False, "error": str(e), "code": e.code}, 4)
    res["ok"] = True
    res["listener_claimed"] = ctl.wait_listener_claim(
        conn, res["binding_id"], clock, time.sleep,
        int(constants.LISTENER_TICK_S * 3 * 1000))
    res["daemon"] = state
    res["code_identity"] = reconcile
    res["hooks"] = hooks
    if not hooks["confirmed"]:
        res["hooks_note"] = ("未确认 plugin hooks 已生效(Stop 心跳缺失/过旧/来自另一 install)。"
                             "绑定确认(握手)依赖 Stop hook —— 若刚安装/更新 plugin,请确保**已重启 "
                             "Claude Code** 让 hooks 生效。这是软提示;权威证明是握手成功 + 会话内「✅ 已绑定」:"
                             "若约 10 分钟内会话里未出现,说明 hooks 未生效,重启 CC 后重跑 bind。")
    res["next"] = ("1) listener_claimed=true 则常驻 listener 已接管、不要手动起 Monitor;"
                   "false = 6s 内未观察到认领 → 手动 Monitor 跑 listener_cmd(须在回复 marker 之前);"
                   "2) 在给用户的回复文本里原样包含 marker 一行(触发 Stop 握手);"
                   "3) 回复里带上 banner 提醒。")
    out(res)


def cmd_unbind(args):
    configmod.require_config()
    conn = open_db()
    res = ctl.unbind(conn, SystemClock(), procs.SystemProber(),
                     start_pid=os.getppid(), binding_id=args.binding_id)
    out(res, 0 if res.get("ok") else 4)


# ---------------------------------------------------------------- status / ensure-daemon
def cmd_status(args):
    cfg = configmod.load_config()
    if not cfg:
        out({"ok": False, "error": "未 bootstrap"}, 2)
    conn = open_db()
    rep = ctl.status_report(conn, cfg, SystemClock())
    rep["daemon_lock_held"] = ctl.daemon_lock_held()
    rep["daemon_healthy"] = ctl.daemon_healthy(conn)
    out(rep)


def cmd_ensure_daemon(args):
    configmod.require_config()
    out({"ok": True, "daemon": ctl.ensure_daemon()})


# ---------------------------------------------------------------- probe / doctor
def cmd_probe(args):
    """= scripts/capability_probe.py main(同进程调用;输出其 JSON,退出码透传:0/2/3/4)。"""
    probe = ctl.load_probe_module()
    argv = ["--chat-id", args.chat_id]
    if args.write_config:
        argv.append("--write-config")
    sys.exit(probe.main(argv))


def cmd_doctor(args):
    cfg = load_snapshot()
    res = ctl.doctor(args.chat_id, cfg=cfg, write_config=args.write_config)
    out(res, 0 if res.get("ok") else 2)


# ---------------------------------------------------------------- allow
def cmd_allow(args):
    """成员直投白名单的读/加/删。写完**下一条消息即生效**(daemon 每次判定读盘)。"""
    configmod.require_config()
    if args.action == "list":
        out({"ok": True, "entries": senderallow.load_entries()})
    # add/remove 两个 id 都必填:少任一个就成了"整个会话放行"或"该用户在所有会话放行"。
    if not args.chat_id or not args.user_id:
        out({"ok": False, "error": "add/remove 必须同时给 --chat-id 与 --user-id"}, 2)
    if args.action == "add":
        entries, added = senderallow.add_entry(args.chat_id, args.user_id, args.note)
        out({"ok": True, "added": added, "entries": entries,
             "note": "已加入" if added else "已存在,未重复添加"})
    entries, removed = senderallow.remove_entry(args.chat_id, args.user_id)
    out({"ok": True, "removed": removed, "entries": entries,
         "note": "已移除" if removed else "名单里没有这一条"})


def main():
    p = argparse.ArgumentParser(prog="bridgectl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("bootstrap", help="首次配置:auth.test 钉指纹,写 config.json + tokens.json(0600)")
    sp.add_argument("--owner", default=None, help="owner Slack user id(U…/W…)")
    sp.add_argument("--owner-email", default=None, help="owner 邮箱(users.lookupByEmail;需 users:read.email)")
    sp.add_argument("--app-id", default=None, help="App ID(A…);bots.info 取不到时必填")
    sp.add_argument("--tokens-stdin", action="store_true",
                    help="从 stdin 读 JSON {bot_token, app_token}(缺省读 env SLACK_BOT_TOKEN/SLACK_APP_TOKEN)")
    sp.add_argument("--chat-allowlist", default=None,
                    help="逗号分隔 chat_id 列表;缺省=不限制(灰度/测试隔离)")
    sp.set_defaults(fn=cmd_bootstrap)
    sp = sub.add_parser("preflight")
    sp.set_defaults(fn=cmd_preflight)
    sp = sub.add_parser("chats")
    sp.set_defaults(fn=cmd_chats)
    sp = sub.add_parser("open-dm", help="conversations.open(owner) 并钉 owner_dm_id")
    sp.set_defaults(fn=cmd_open_dm)
    sp = sub.add_parser("bind")
    sp.add_argument("--chat-id", required=True)
    sp.add_argument("--chat-name", default=None)
    sp.set_defaults(fn=cmd_bind)
    sp = sub.add_parser("unbind")
    sp.add_argument("--binding-id", default=None)
    sp.set_defaults(fn=cmd_unbind)
    sp = sub.add_parser("status")
    sp.set_defaults(fn=cmd_status)
    sp = sub.add_parser("ensure-daemon")
    sp.set_defaults(fn=cmd_ensure_daemon)
    sp = sub.add_parser("probe", help="真机能力探测(发两条并删除;不需要 daemon)")
    sp.add_argument("--chat-id", required=True)
    sp.add_argument("--write-config", action="store_true")
    sp.set_defaults(fn=cmd_probe)
    sp = sub.add_parser("doctor", help="probe + daemon 健康")
    sp.add_argument("--chat-id", required=True)
    sp.add_argument("--write-config", action="store_true")
    sp.set_defaults(fn=cmd_doctor)
    sp = sub.add_parser("allow", help="成员直投白名单(chat+user 双精确匹配)")
    sp.add_argument("action", choices=("list", "add", "remove"))
    sp.add_argument("--chat-id", default=None)
    sp.add_argument("--user-id", dest="user_id", default=None, help="Slack user id(U…)")
    sp.add_argument("--open-id", dest="user_id", help=argparse.SUPPRESS)  # 兼容旧拼写
    sp.add_argument("--note", default=None, help="备注(给人看,不参与判定)")
    sp.set_defaults(fn=cmd_allow)
    args = p.parse_args()
    try:
        args.fn(args)
    except configmod.ConfigError as e:
        out({"ok": False, "error": util.redact_secrets(str(e))}, 2)
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 —— 绝不裸 traceback(异常文本可能含 token;R1-M5)
        out({"ok": False, "error": "internal-error", "type": type(e).__name__}, 2)


if __name__ == "__main__":
    main()
