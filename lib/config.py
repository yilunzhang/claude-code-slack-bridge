"""config.json(指纹:team_id/bot_user_id/bot_id/app_id/owner_user_id + 可选项)与
tokens.json(凭据:bot_token/app_token,0600)。

- config:原子写;bootstrap 期 flock;运行时只经 `ConfigSnapshot.set_persist` 改单键
  (probe 写 markdown_mode、agent 改 chat_allowlist 等),daemon 每 tick `refresh()` 原地更新。
- tokens:**文件是 daemon 的唯一真相**;`load_tokens()` 返回 `(tokens, version)`,
  version = f"{mtime_ns}:{sha256(file)[:16]}"(contracts §7);env 覆盖(`SLACK_BOT_TOKEN`/
  `SLACK_APP_TOKEN`)仅供 CLI/测试,version 恒为 "env",daemon 传 `allow_env=False`。
  token 绝不进日志/模型输出/argv。"""
import fcntl
import hashlib
import json
import os
import stat

from . import paths, util

REQUIRED_KEYS = ("team_id", "bot_user_id", "bot_id", "app_id", "owner_user_id")
OPTIONAL_KEYS = ("owner_dm_id", "markdown_mode", "consumer_python", "chat_allowlist")
TOKEN_KEYS = ("bot_token", "app_token")
ENV_BOT_TOKEN = "SLACK_BOT_TOKEN"
ENV_APP_TOKEN = "SLACK_APP_TOKEN"
ENV_TOKENS_VERSION = "env"
TOKENS_VERSION_HASH_LEN = 16


class ConfigError(Exception):
    pass


# ---------------------------------------------------------------- config.json
def load_config(path=None):
    p = path or paths.config_path()
    if not os.path.exists(str(p)):
        return None
    with open(str(p), "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg


def missing_keys(cfg):
    if not isinstance(cfg, dict):
        return list(REQUIRED_KEYS)
    return [k for k in REQUIRED_KEYS if not cfg.get(k)]


def require_config(path=None):
    cfg = load_config(path)
    if cfg is None:
        raise ConfigError("config.json 不存在:先运行 bridgectl bootstrap --owner U…")
    missing = missing_keys(cfg)
    if missing:
        raise ConfigError(f"config.json 缺字段: {missing}")
    return cfg


def save_config(cfg, path=None):
    if path is None:
        paths.ensure_data_dir()
        path = paths.config_path()
    missing = missing_keys(cfg)
    if missing:
        raise ConfigError(f"config 缺字段: {missing}")
    util.atomic_write(path, json.dumps(dict(cfg), ensure_ascii=False, indent=2))


class bootstrap_lock:
    """bootstrap 期互斥(防并发双写指纹);ConfigSnapshot.set_persist 也借它串行化多进程单键写。"""

    def __enter__(self):
        paths.ensure_data_dir()
        self.fd = os.open(str(paths.bootstrap_lock_path()), os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)
        return False


class ConfigSnapshot(dict):
    """daemon 内**唯一共享**的配置对象(dict 子类,所有组件持同一引用)。
    - `refresh()`:重读 config.json 并**原地**更新(clear+update);文件缺失/畸形/缺必填键 → 不动、返回 False。
    - `set_persist(key, value)`:在 bootstrap_lock 下重读文件 → 改单键 → 原子写 → 原地更新
      (多进程写者:probe 与 daemon 互不覆盖对方的键)。
    - `path`:固定为构造时的 config 路径(默认 paths.config_path(),延迟解析以尊重 env 重定向)。"""

    def __init__(self, data=None, path=None):
        super().__init__(data or {})
        self._path = path

    @property
    def path(self):
        return self._path or paths.config_path()

    @classmethod
    def load(cls, path=None):
        cfg = require_config(path)
        return cls(cfg, path=path)

    def refresh(self):
        try:
            cfg = load_config(self.path)
        except (OSError, ValueError):
            return False
        if cfg is None or missing_keys(cfg):
            return False
        if dict(self) == cfg:
            return True
        self.clear()
        self.update(cfg)
        return True

    def set_persist(self, key, value):
        with bootstrap_lock():
            try:
                data = load_config(self.path)
            except (OSError, ValueError):
                data = None
            if data is None:
                data = dict(self)
            data[key] = value
            util.atomic_write(self.path, json.dumps(data, ensure_ascii=False, indent=2))
            self.clear()
            self.update(data)
        return value


# ---------------------------------------------------------------- tokens.json
def tokens_version_of(raw, mtime_ns):
    """version 字符串:mtime_ns + sha256(文件字节) 前 16 位。同内容不同 mtime 也算新版本
    (FingerprintGate 会重验,代价一次 auth.test,换来"任何触碰都被看见")。"""
    return "%d:%s" % (int(mtime_ns), hashlib.sha256(raw).hexdigest()[:TOKENS_VERSION_HASH_LEN])


def tokens_mtime_ns(path=None):
    """廉价探针(仅 mtime):文件不存在 → None。FingerprintGate 用的是 `tokens_stat_signature`。"""
    p = str(path or paths.tokens_path())
    try:
        return os.stat(p).st_mtime_ns
    except OSError:
        return None


def tokens_stat_signature(path=None):
    """每 tick 的廉价探针(R1-M8):`(st_ino, st_mode, st_size, st_mtime_ns, st_ctime_ns)`;文件不存在 → None。
    只看 mtime 会漏掉 `chmod 644`(改 ctime/mode、不改 mtime)—— 权限变坏时 load_tokens 的 0600 检查
    永远不再执行,门也就关不上。任何一项变化 → 调用方走完整 `load_tokens`(fail-closed)。"""
    p = str(path or paths.tokens_path())
    try:
        st = os.stat(p)
    except OSError:
        return None
    return (st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _validate_tokens(tokens, where):
    if not isinstance(tokens, dict):
        raise ConfigError(f"{where}: 顶层必须是 JSON 对象")
    bot = tokens.get("bot_token")
    if not isinstance(bot, str) or not bot:
        raise ConfigError(f"{where}: 缺 bot_token")
    app = tokens.get("app_token")
    if app is not None and not isinstance(app, str):
        raise ConfigError(f"{where}: app_token 须为字符串")
    return {"bot_token": bot, "app_token": app or None}


def load_tokens(path=None, allow_env=True, environ=None):
    """→ (tokens, version)。tokens = {"bot_token": str, "app_token": str|None}。
    - env 覆盖(仅 allow_env=True 且 SLACK_BOT_TOKEN 非空):version="env"。
    - 文件:必须存在、是普通文件、权限 **0600**(group/other 任何位 → ConfigError);
      fd 上 fstat + read 保证 mtime 与内容同源。"""
    env = os.environ if environ is None else environ
    if allow_env and env.get(ENV_BOT_TOKEN):
        tokens = _validate_tokens({"bot_token": env.get(ENV_BOT_TOKEN),
                                   "app_token": env.get(ENV_APP_TOKEN) or None}, "env")
        return tokens, ENV_TOKENS_VERSION
    p = str(path or paths.tokens_path())
    try:
        fd = os.open(p, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        raise ConfigError(f"tokens.json 不存在({p}):先运行 bridgectl bootstrap")
    except OSError as e:
        raise ConfigError(f"tokens.json 不可读: {e}")
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise ConfigError("tokens.json 不是普通文件")
        mode = stat.S_IMODE(st.st_mode)
        if mode & 0o077:
            raise ConfigError("tokens.json 权限必须为 0600(当前 %04o)" % mode)
        with os.fdopen(fd, "rb") as f:
            fd = None
            raw = f.read()
    finally:
        if fd is not None:
            os.close(fd)
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        raise ConfigError(f"tokens.json 不是合法 JSON: {e}")
    tokens = _validate_tokens(obj, "tokens.json")
    return tokens, tokens_version_of(raw, st.st_mtime_ns)


def save_tokens(tokens, path=None, overwrite=False):
    """原子写 0600。缺省 **存在即拒**(bootstrap 语义);overwrite=True 才替换。→ 新 version。"""
    tokens = _validate_tokens(tokens, "save_tokens")
    if path is None:
        paths.ensure_data_dir()
        path = paths.tokens_path()
    p = str(path)
    if os.path.exists(p) and not overwrite:
        raise ConfigError(f"tokens.json 已存在({p});如需替换请显式 overwrite")
    raw = json.dumps({"bot_token": tokens["bot_token"], "app_token": tokens["app_token"]},
                     ensure_ascii=False, indent=2).encode("utf-8")
    util.atomic_write(p, raw, mode=0o600)
    os.chmod(p, 0o600)
    return tokens_version_of(raw, os.stat(p).st_mtime_ns)
