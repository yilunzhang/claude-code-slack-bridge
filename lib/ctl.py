"""bridgectl 逻辑层(bin/bridgectl.py 的可测核心)。
I4 例外声明:bootstrap/chats 的读操作是 skill 交互期例外。"""
import fcntl
import json
import os
import shlex
import signal
import subprocess
import sys
import time

from . import config as configmod
from . import constants, db, jobs, lifecycle, paths, procs, texts, util
from . import runner as runner_mod


# ---------------------------------------------------------------- bootstrap(S5 身份配方)
def bootstrap(runner, profile, clock, chat_allowlist=None):
    auth = runner.run(["auth", "status"], timeout_s=30)
    auth_obj = runner_mod.parse_envelope(auth.stdout)
    if auth.rc != 0 or not isinstance(auth_obj, dict):
        raise configmod.ConfigError("auth status 失败:先 lark-cli auth login")
    app_id = auth_obj.get("appId")
    owner = ((auth_obj.get("identities") or {}).get("user") or {}).get("openId")
    if not app_id or not owner:
        raise configmod.ConfigError("auth status 缺 appId / identities.user.openId")
    bot = runner.run(["api", "GET", "/open-apis/bot/v3/info", "--as", "bot",
                      "--format", "ndjson"], timeout_s=30)
    bot_open_id, bot_name = None, None
    for line in (bot.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        b = obj.get("bot") if isinstance(obj, dict) else None
        if isinstance(b, dict) and b.get("open_id"):
            bot_open_id = b["open_id"]
            bot_name = b.get("app_name")
            break
    if not bot_open_id:
        raise configmod.ConfigError("bot/v3/info 未取到 .bot.open_id(--format ndjson)")
    from . import fingerprint as fp
    cli_version = fp.probe_cli_version(runner)  # E1:裸 --version(不拖 --profile)
    if not cli_version:
        raise configmod.ConfigError("lark-cli --version 探测失败:cli_version 必填(版本门基准)")
    cfg = {
        "profile": profile,
        "app_id": app_id,
        "bot_open_id": bot_open_id,
        "bot_name": bot_name,
        "owner_open_id": owner,
        "cli_version": cli_version,
        "created_at": clock.wall_ms(),
    }
    if chat_allowlist:
        cfg["chat_allowlist"] = list(chat_allowlist)  # E2:灰度/测试隔离;缺省/空=全部
    with configmod.bootstrap_lock():
        if configmod.load_config() is not None:
            raise configmod.ConfigError(
                "config.json 已存在(指纹钉死,不隐式变);如确要换 profile,先手动删除 "
                f"{paths.config_path()} 并 unbind 所有绑定")
        configmod.save_config(cfg)
    return cfg


# ---------------------------------------------------------------- hooks(plugin 提供;检测生效)
# plugin 化后 hooks 由 plugin 的 hooks/hooks.json 提供,**不再**手贴进 settings.json。
# 无法在 bind 的同一 turn 内直接读"CC 是否已加载 plugin hooks";用**哨兵心跳**做正向检测:
# Stop/SessionEnd hook 每次运行都写 hook_heartbeat(见 hooklib._touch_hook_heartbeat)。
# 见过近期心跳 = plugin hooks 确已在本机某个 CC 会话里跑过 = 已生效(强信号);
# 没见过 = 不确定(全新安装/尚未完成一轮对话是正常的)→ 不硬阻断,提示重启 CC。

# "近期"窗口:一次对话轮结束就会刷新;给足冷启动/低频使用余量。
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
    # fresh 要求 0<=age<=window(MAJOR 2:挡未来时间戳误判 fresh)
    ev["fresh"] = 0 <= age <= HOOK_HEARTBEAT_FRESH_MS
    ev["plugin_version"] = data.get("plugin_version")
    ev["pkg_root"] = data.get("pkg_root")
    # current = 心跳来自**本 install/版本**(MAJOR 2:另一 root/旧版本的心跳不算「我的 hooks 已生效」)
    ev["current"] = (ev["pkg_root"] == cur_root and ev["plugin_version"] == cur_ver)
    return ev


def hooks_live_status(now_ms=None):
    """plugin hooks 生效信号(哨兵心跳)。**advisory only** —— 权威证明只有「本次 Stop 握手成功
    + 群内 ✅ 已绑定」;心跳只是软提示,不做安全判定。不读 settings.json、不要求手装。
    Stop 与 SessionEnd 分开;记录 plugin version/pkg_root 以剔除「另一 install/旧版本/仅 SessionEnd」假阳性。
    顶层 `confirmed` = Stop 心跳 seen ∧ fresh ∧ current(bind/preflight 的主信号)。"""
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
    """best-effort:扫 settings.json 里**非本 plugin** 的 Stop hook(D2 阻断型共存告警,信息性)。
    读不到/无 = 空;绝不影响就绪判定。plugin 化后本 plugin 的 hooks 不在 settings.json。"""
    try:
        obj = json.loads(paths.settings_json_path().read_text())
    except (OSError, ValueError):
        return []
    out = []
    for entry in (obj.get("hooks") or {}).get("Stop") or []:
        for h in (entry or {}).get("hooks") or []:
            cmd = (h or {}).get("command", "")
            if cmd and "feishu-bridge/hooks/stop_hook.py" not in cmd:
                out.append(cmd)
    return out


# ---------------------------------------------------------------- chats
def list_chats(runner):
    """列出 bot 所在的**全部**群:跟着 has_more 翻页,取不全就整体失败(返回 None)。

    `im +chat-list` 不自动翻页(默认 page-size=20)。只取首页会得到一个**和全集长得
    一模一样**的残缺列表——没有报错、条目也都是真的——调用方会把"不在列表里"读成
    "这个群不存在"并去新建一个重复群。所以宁可失败,也不返回取不全的前缀。

    三条契约:
    - 终止只认 `has_more is False`;缺失/非布尔一律当"证明不了取全"→ None。
    - **`page_token` 重复不代表没前进**:该接口从第 2 页起会一直返回同一个 token,
      内容却照常推进,按"重复即失败"判会把能正常取全的路径判死。
    - 推进以**本页有没有带来新 chat_id** 为准(空页/全畸形/全是已见 id 都不算),
      否则会在那个恒定 token 上无限循环。"""
    chats, seen_ids, page_token = [], set(), None
    while True:
        argv = ["im", "+chat-list", "--as", "bot", "--page-size", "100"]
        if page_token:
            argv += ["--page-token", page_token]
        res = runner.run(argv, timeout_s=30)
        env = runner_mod.parse_envelope(res.stdout)
        if res.rc != 0 or not runner_mod.envelope_ok(env):
            return None
        data = runner_mod.data_of(env)
        fresh = 0
        for i in data.get("items") or data.get("chats") or []:
            if not isinstance(i, dict) or not i.get("chat_id"):
                continue
            if i["chat_id"] in seen_ids:
                continue
            seen_ids.add(i["chat_id"])
            chats.append({"chat_id": i["chat_id"], "name": i.get("name")})
            fresh += 1
        has_more = data.get("has_more")
        if has_more is False:
            return chats
        # 缺失/非布尔 has_more:证明不了取全了,不当成功(该接口实测始终返回真布尔)。
        if has_more is not True:
            return None
        # 声称还有更多,却没给 cursor、或本页没带来任何新 id:无法证明有推进。
        if not data.get("page_token") or fresh == 0:
            return None
        page_token = data["page_token"]


# ---------------------------------------------------------------- bind / unbind
def bind_prepare(conn, cfg, clock, prober, chat_id, chat_name, cwd, start_pid):
    # r3-1②(E2 盖全):目标 chat 不在 allowlist → 直接报错,零残留
    allow = cfg.get("chat_allowlist")
    if allow and chat_id not in allow:
        raise lifecycle.BindConflict(
            "chat_not_allowed",
            f"该群不在 config.json 的 chat_allowlist 内({chat_id});改 allowlist 或换群")
    inst = procs.find_cc_instance(prober, start_pid)
    if inst is None:
        raise lifecycle.BindConflict("no_instance", "无法定位 CC 实例(ppid 链解析失败)")
    pid, lstart = inst
    res = lifecycle.create_binding(conn, chat_id=chat_id, chat_name=chat_name,
                                   cwd=cwd, cc_pid=pid, cc_start=lstart, clock=clock)
    # minor①:shlex.join + sys.executable —— plugin 根/python 路径含空格也安全。
    listener_cmd = shlex.join(
        [sys.executable, str(paths.pkg_root() / "bin" / "listener.py"), res["binding_id"]])
    return {
        "binding_id": res["binding_id"],
        "marker": res["marker"],
        "banner": texts.BIND_BANNER,
        "listener_cmd": listener_cmd,
        "chat_id": chat_id,
        "chat_name": chat_name,
        "ttl_minutes": constants.PENDING_BIND_TTL_MS // 60000,
    }


def wait_listener_claim(conn, binding_id, clock, sleep, timeout_ms):
    """等常驻 listener(插件 monitor 的 follower)认领刚建的绑定:行 listener_epoch>=1 即 True,
    每 0.5s 轮询,超 timeout_ms → False(调用方据此回退为手动起有参 listener)。已认领时不 sleep。
    必须发生在 marker 握手之前:确认后 30s 无心跳会 listener_never_ready 关行,之后再起 listener 只会看到终态。"""
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
def status_report(conn, cfg, clock):
    now = clock.wall_ms()

    def age(ms):
        return None if ms is None else round((now - int(ms)) / 1000, 1)

    daemon = {
        "pid": db.get_state(conn, "daemon_pid"),
        "started_at": db.get_state(conn, "daemon_started_at"),
        "last_loop_age_s": age(db.get_state(conn, "last_loop_at")),
        "suspect_until": db.get_state(conn, "suspect_until"),
        "last_error": db.get_state(conn, "last_error"),
    }
    consumers = {}
    for k in ("im.message.receive_v1", "card.action.trigger"):
        consumers[k] = {
            "ready": db.get_state(conn, f"consumer_{k}_ready"),
            "last_status": db.get_state(conn, f"consumer_{k}_last_status"),
            "restarts": db.get_state(conn, f"consumer_{k}_restarts", "0"),
        }
    bindings = []
    for b in conn.execute(
            "SELECT * FROM bindings ORDER BY binding_seq DESC LIMIT 20").fetchall():
        bindings.append({
            "binding_id": b["binding_id"][:8],
            "chat": b["chat_name"] or b["chat_id"],
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
    counters = {}
    for key in ("hook_drop_count", "inbox_conflict_alerts", "inbox_cap_drops",
                "malformed_event_lines", "event_processing_errors", "media_failed",
                "resolve_deadline_failed", "approval_card_given_up"):
        v = db.get_state(conn, key)
        if v is not None:
            counters[key] = v
    gate = db.get_state(conn, "outbound_gate", "ok") or "ok"
    given_up_cards = conn.execute(
        "SELECT COUNT(*) FROM outbound_jobs WHERE kind='approval_card' "
        "AND state='failed' AND error='given-up'").fetchone()[0]
    rep = {
        "fingerprint": {k: cfg.get(k) for k in
                        ("profile", "app_id", "bot_open_id", "bot_name", "owner_open_id",
                         "cli_version")},
        "schema_version": db.get_state(conn, "schema_version"),
        "chat_allowlist": cfg.get("chat_allowlist") or "全部(未限制)",
        "outbound_gate": gate,
        "daemon": daemon,
        "consumers": consumers,
        "bindings": bindings,
        "outbound_jobs": jobs_by_state,
        "deliveries": deliveries_by_state,
        "inbox": inbox_by_state,
        "counters": counters,
        "given_up_approval_cards": given_up_cards,
    }
    if gate == "degraded:version_mismatch":
        rep["gate_hint"] = ("lark-cli 版本与 config.cli_version 不符,出站已停摆。"
                            "**v1.5.0 起 daemon 会自动自检重钉**(用与真实转发同形的 --markdown "
                            "发一条到固定测试群),通过即放行、并弹一条系统通知;"
                            "若这里仍显示停摆,说明自检**未通过**(可能是真回归)——"
                            "看 daemon.log,必要时人工 `bridgectl doctor --chat-id <测试群oc>`。")
    elif gate.startswith("degraded"):
        rep["gate_hint"] = "身份指纹未验证(出站停摆),daemon 带退避重探;检查 VPN/lark-cli 登录。"
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


# 修复项5:daemon 挂死恢复。锁被持有但心跳陈旧(>HUNG_THRESHOLD)= 挂死 →
# 按记录的 (daemon_pid, daemon_proc_start) 精确匹配后 SIGTERM → 等退出 → 接管重启;
# 身份不匹配(pid 复用/无记录)绝不杀随机进程 → failed。
# r7-3:阈值必须 ≥ 单次**最长同步网络操作**(daemon 单线程,一次媒体下载 DOWNLOAD_TIMEOUT_S=120s
# 期间主循环阻塞、不刷心跳)+ 余量;多点心跳只在"处理完一个含网络条目后"刷,盖不住单次下载内部。
# 从 r2 的 300s 收窄到 =DOWNLOAD_TIMEOUT_S+60s(=180s):覆盖 120s 长下载不误判,同时更贴合。
# r2-M1④:接管全程持 singleflight flock,防两个 ensure 重叠 kill/spawn。
HUNG_THRESHOLD_MS = (constants.DOWNLOAD_TIMEOUT_S + 60) * 1000  # =180_000
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


# r7-1:ensure() 的返回值语义 —— 只有 READY_RESULTS 才代表"daemon 就绪、bind 可继续";
# in_progress/failed/down 都不就绪(bind 只在 is_ready_result()==True 时继续,别再"排除字符串 failed")。
READY_RESULTS = frozenset({"running", "started", "recovered"})


def is_ready_result(result):
    return result in READY_RESULTS


def state_ready(st, now_ms):
    """r4-1/r7-2:就绪 = 心跳新鲜 ∧ startup ∈ {running,degraded} ∧ 同代 generation。
    probing/refused/**stopping** 不算就绪(heartbeat 只表活着,startup 才表就绪;
    stopping=正在退出,天然不在 _READY_PHASES);generation 对齐防"新代心跳 + 旧代 running"误判。"""
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


# r5-M2:等 probing 结论(record_identity + gate.startup,最坏探测 auth~20s + version~10s)的上限,
# 覆盖探测最坏 + 余量;心跳新鲜的 probing 期间绝不 takeover。
STARTUP_PROBE_WAIT_S = 40


class DaemonSupervisor:
    """依赖全注入的 ensure 逻辑(可测):lock_held()/read_state()/spawn()/kill(pid,sig);
    singleflight(可选,对象须有 try_acquire()/release())防重叠接管(r2-M1④)。
    r5:liveness(心跳)与 readiness(startup+generation)分离——
    ① takeover(SIGTERM+重启)**只依据心跳陈旧**,绝不因'尚未就绪'而杀;
    ② 心跳新鲜的 probing → 等结论(不 kill);refused → 终态失败(不重启);
    ③ 拉起/等待路径以 baseline generation 拒'旧代完整对齐'假成功(M1)。"""

    def __init__(self, *, lock_held, read_state, spawn, kill, prober,
                 now_ms, sleep, wait_s=12, singleflight=None, probe_wait_s=None,
                 mono_ms=None):
        self.lock_held = lock_held
        self.read_state = read_state
        self.spawn = spawn
        self.kill = kill
        self.prober = prober
        # 注:supervisor **只判 daemon 健康在跑吗**,不掺 code-identity(换层:identity 检测
        # 移到 bind 前置串行检查,见 reconcile_code_identity)——避免在并发 ready 状态机的多个出口
        # 各自校验(security 三律③:别在错误的层堆东西)。
        # codex-final:两个时钟各司其职,别混。
        #   now_ms = **墙钟**(wall):用于心跳新鲜度/state_ready —— last_loop_at 是 DB 持久化的
        #     墙钟时间戳,跨进程比较必须墙钟。
        #   mono_ms = **单调钟**(monotonic):仅用于本进程内的等待 deadline —— 墙钟前跳/回拨
        #     不得让 caller_deadline 提前结束。
        self.now_ms = now_ms
        self.mono_ms = mono_ms or (lambda: int(time.monotonic() * 1000))
        self.sleep = sleep
        self.wait_s = wait_s
        self.singleflight = singleflight
        self.probe_wait_s = probe_wait_s if probe_wait_s is not None else STARTUP_PROBE_WAIT_S

    # ---- 判定原语:严格区分 liveness 与 readiness ----
    def _ready(self, st):
        return state_ready(st, self.now_ms())

    def _heartbeat_fresh(self, st):
        """liveness:仅看心跳,不看 startup(r5-M2:takeover 只依此)。"""
        if not st or st.get("last_loop_at") is None:
            return False
        return (self.now_ms() - int(st["last_loop_at"])) <= HUNG_THRESHOLD_MS

    def _gen_of(self, st):
        from .daemon_core import parse_startup
        return (st or {}).get("daemon_generation") or parse_startup((st or {}).get("startup"))[1]

    def _phase_of(self, st):
        from .daemon_core import parse_startup
        return parse_startup((st or {}).get("startup"))[0]

    # ---- 入口 ----
    def ensure(self):
        # r5-M1④:健康态 fast-path 也必须经 singleflight,不越过正在进行的 ensure。
        if self.singleflight is not None:
            if not self.singleflight.try_acquire():
                # r6:singleflight busy = 另一 owner 正持锁 ensure。busy 调用者唯一正确行为
                # = 等 owner 的 handoff 结果,绝不观察 baseline 代的 DB 状态独立判定。
                return self._await_handoff(self._gen_of(self.read_state()))
            try:
                return self._ensure_locked()
            finally:
                self.singleflight.release()
        return self._ensure_locked()

    def _await_handoff(self, baseline_gen):
        """r6:纯等待-handoff 循环(彻底重写,一次同解 M1/M2 在 busy 路径的两处未适配)。
        为什么不能观察:signal handler 只置退出标志、当前 loop 仍会跑完刷心跳、锁到 shutdown
        才释放——所以 owner 接管旧代时,**baseline 代退出前还能刷新鲜心跳/暂持锁/pid 暂存活**,
        任何对 baseline 代的观察量都不可信。唯一可信信号 = handoff:
        ① 每轮先 try_acquire —— **拿到**=owner 已结束,我成为新 owner,走权威 _ensure_locked;
        ② 没拿到 → 只认'非空且 != baseline 的新代且该新代 state_ready(running/degraded)'为成功
           (= owner 拉起的新 daemon 就绪);baseline 代无论 pid 死活一律**不**算成功(M1 洞);
        ③ 短 sleep 重试。
        r7-2:caller_deadline **覆盖 owner 最坏临界区** = 等旧锁释放(shutdown≈wait_s)
        + spawn 等新代就绪(startup≈wait_s+probe_wait_s) = **2*wait_s + probe_wait_s**
        (旧 wait_s+probe_wait_s 盖不住 owner 走 takeover 时先等旧锁释放那段)。
        到期但 transition 仍有效(有 daemon 活着在忙)→ 结构化 **in_progress**(可重试,
        **非语义 failed**;bind 靠 is_ready_result 判定,不会误当成功);无进展/死 → failed。"""
        budget_s = 2 * self.wait_s + self.probe_wait_s
        # codex-final:deadline 用**真单调钟**(self.mono_ms),不用墙钟(now_ms)——
        # 否则墙钟前跳/回拨会让 caller_deadline 提前结束(纯步数循环旧实现没有这个回归)。
        deadline = self.mono_ms() + int(budget_s * 1000)  # monotonic absolute deadline
        max_steps = int(budget_s / _POLL_STEP_S) + 2      # fail-safe(mono 不推进的测试兜底)
        steps = 0
        while self.mono_ms() < deadline and steps < max_steps:
            if self.singleflight.try_acquire():
                try:
                    return self._ensure_locked()  # owner 已结束 → 权威判定/接管
                finally:
                    self.singleflight.release()
            st = self.read_state()
            gen = self._gen_of(st)
            # 只认 owner 拉起的**新代**就绪;baseline 代(含旧代垂死刷心跳/pid 存活)一律不认
            if self.lock_held() and gen and gen != baseline_gen and self._ready(st):
                return "running"
            self.sleep(_POLL_STEP_S)
            steps += 1
        st = self.read_state()
        if self.lock_held() and self._heartbeat_fresh(st):
            return "in_progress"  # owner 临界区超时但 daemon 还在忙 → 可重试,不是失败
        return "failed"

    def _ensure_locked(self):
        st = self.read_state()
        if self.lock_held() and self._ready(st):
            return "running"  # 稳态健康(singleflight 下无并发重启,可信)
        if self.lock_held():
            if self._heartbeat_fresh(st):
                # r5-M2:心跳新鲜 → 绝不 takeover。按 startup 相位处置:
                phase = self._phase_of(st)
                if phase == "refused":
                    return self._await_lock_release_then_failed()  # 终态,不重启
                return self._await_probing_conclusion()  # probing/未知:等结论,不 kill
            # 心跳陈旧 = 真挂死 → 精确身份匹配 takeover(唯一 kill 路径)
            return self._takeover_and_restart(st)
        return self._spawn_and_wait()

    def _await_lock_release_then_failed(self):
        for _ in range(int(self.wait_s / _POLL_STEP_S) + 1):
            if not self.lock_held():
                break
            self.sleep(_POLL_STEP_S)
        return "failed"

    def _await_probing_conclusion(self):
        """r5-M2:等心跳新鲜的 probing 得出结论;就绪→running;refused/退出→failed;
        超时或中途心跳陈旧→failed(**不 kill**;下次 ensure 才依陈旧心跳接管)。"""
        for _ in range(int(self.probe_wait_s / _POLL_STEP_S) + 1):
            st = self.read_state()
            if not self.lock_held():
                return "failed"  # daemon 退出(如 refused)
            if self._ready(st):
                return "running"
            if self._phase_of(st) == "refused":
                return self._await_lock_release_then_failed()
            if not self._heartbeat_fresh(st):
                return "failed"  # 中途挂死:本次不误杀,交下次 ensure 接管
            self.sleep(_POLL_STEP_S)
        return "failed"

    def _takeover_and_restart(self, st):
        import signal as _signal
        pid = st.get("daemon_pid") if st else None
        pstart = st.get("daemon_proc_start") if st else None
        if not pid or not pstart:
            return "failed"
        if procs.probe_alive(self.prober, int(pid), pstart) != procs.ALIVE:
            return "failed"  # pid 复用/探测不确定:绝不杀
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
        baseline = self._gen_of(self.read_state())  # r5-M1:spawn 前记 baseline generation
        self.spawn()
        # 等待上限覆盖新 daemon 的 record_identity + gate.startup 探测最坏(probe)+ 拉起余量。
        steps = int((self.wait_s + self.probe_wait_s) / _POLL_STEP_S) + 1
        for _ in range(steps):
            self.sleep(_POLL_STEP_S)
            st = self.read_state()
            gen = self._gen_of(st)
            # r5-M1:只承认**新代**(gen != baseline)就绪;旧代完整对齐(gen==baseline)一律不算。
            if self.lock_held() and self._ready(st) and gen != baseline:
                return "started"
            # 新代明确 refused → 失败(不再空等到超时;bind 不会继续)
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
        now_ms=lambda: int(time.time() * 1000),          # 墙钟:心跳/state_ready(DB 时间戳)
        mono_ms=lambda: int(time.monotonic() * 1000),    # 单调钟:等待 deadline(免疫墙钟跳变)
        sleep=time.sleep, wait_s=wait_s,
        singleflight=_FlockSingleflight(paths.ensure_lock_path()))
    return sup.ensure()


# ---------------------------------------------------------------- bind 前置:code-identity 串行检查(MAJOR 3 换层)
def reconcile_code_identity(*, my_identity, read_state, lock_held, prober, kill,
                            ensure, sleep, wait_s=12):
    """bind 前置**串行**检查(不在 supervisor 并发层):读 daemon_state.daemon_code_identity,
    与本 CLI code_identity_str() 比。**fail-closed 不变式**:reconcile 成功返回(无 `error`)⟹
    「identity 已匹配」或「本版本新 daemon 已(重新)拉起」二者之一 —— 绝不在 identity 不一致时放行
    用旧代码 bind。cmd_bind 见 `error` → 不建 pending_bind、exit 非0。
    返回 {restarted, reason, old?, new, state?, error?}。
    两个不同 identity 的 CLI 并发 bind 的严格串行化 = 已知限制🅒(极罕见,不在此层处理);
    read→SIGTERM 间的 probe→kill 微小窗口不可原子消除(同上,不加复杂度)。"""
    if not lock_held():
        # 无 daemon(或刚退出)→ 拉本版本的 → identity 从此为本版本(满足不变式)
        return {"restarted": True, "reason": "no-daemon-respawn", "new": my_identity,
                "state": ensure()}
    st = read_state() or {}
    recorded = st.get("daemon_code_identity")
    if recorded == my_identity:
        return {"restarted": False, "reason": "match", "new": my_identity}
    # 不一致(含 read_state 失败使 recorded=None):尝试**安全**重启旧 daemon
    pid, pstart = st.get("daemon_pid"), st.get("daemon_proc_start")
    # 只在 pid+start 齐全**且**探测确定存活时才杀(缺 start / 已死 / 复用 / probe UNKNOWN 都不杀)
    can_kill = bool(pid) and bool(pstart) \
        and procs.probe_alive(prober, int(pid), pstart) == procs.ALIVE
    if not can_kill:
        # 无法安全定位/终止旧 daemon:
        #   锁已释放(旧 daemon 自行退出)→ 拉本版本新的(满足不变式);
        #   锁仍持有(旧代码 daemon 还在)→ **fail-closed**:返回 error,绝不放行 bind。
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
            "state": ensure()}  # 无 daemon 了 → 拉本版本的新 daemon


def reconcile_daemon_code_identity(wait_s=12):
    """生产装配:reconcile_code_identity 注入真实依赖。"""
    from . import version as versionmod
    return reconcile_code_identity(
        my_identity=versionmod.code_identity_str(),
        read_state=_read_daemon_state, lock_held=daemon_lock_held,
        prober=procs.SystemProber(), kill=os.kill, ensure=ensure_daemon,
        sleep=time.sleep, wait_s=wait_s)


# ---------------------------------------------------------------- doctor(显式诊断例外,I2)
def doctor(runner, chat_id, clock, cfg=None):
    """真发送+撤回自检;独立 opt-in 工具,不在运行时路径。外发 message_id 落返回值。
    修复项1:自检通过 = 当前 CLI 版本契约已验证 → 若与 config.cli_version 不符,重钉版本
    (写盘;daemon 的 FingerprintGate 重探读盘后自动放行)。"""
    key = f"doctor:{util.new_id()}"
    res = runner.run(["im", "+messages-send", "--as", "bot", "--chat-id", chat_id,
                      "--text", f"feishu-bridge doctor 自检 {clock.wall_ms()}(即将撤回)",
                      "--idempotency-key", key], timeout_s=30)
    env = runner_mod.parse_result(res)  # E4a:stderr 信封回退
    if not runner_mod.envelope_ok(env):
        return {"ok": False, "step": "send", "detail": (res.stdout or res.stderr or "")[:400]}
    mid = runner_mod.data_of(env).get("message_id")
    if not mid:
        return {"ok": False, "step": "send", "detail": "no message_id"}
    rc = runner.run(["api", "DELETE", f"/open-apis/im/v1/messages/{mid}", "--as", "bot"],
                    timeout_s=30)
    rc_env = runner_mod.parse_result(rc)
    recalled = runner_mod.envelope_ok(rc_env)
    out = {"ok": recalled, "step": "recall", "message_id": mid, "recalled": recalled}
    if recalled and cfg:
        from . import fingerprint as fp
        actual = fp.probe_cli_version(runner)
        if actual and actual != cfg.get("cli_version"):
            new_cfg = dict(cfg)
            new_cfg["cli_version"] = actual
            configmod.save_config(new_cfg)
            out["repinned_cli_version"] = actual
    return out
