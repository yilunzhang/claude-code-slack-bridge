"""config.json:指纹 profile/app_id/bot_open_id/bot_name/owner_open_id/cli_version。
原子写;bootstrap 期 flock;此后不隐式变(钉死)。"""
import fcntl
import json
import os

from . import paths, util

# cli_version 必填(修复项1):版本门以 config 钉死值为基准
REQUIRED_KEYS = ("profile", "app_id", "bot_open_id", "owner_open_id", "cli_version")


class ConfigError(Exception):
    pass


def load_config():
    p = paths.config_path()
    if not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg


def require_config():
    cfg = load_config()
    if cfg is None:
        raise ConfigError("config.json 不存在:先运行 bridgectl bootstrap --profile <名>")
    missing = [k for k in REQUIRED_KEYS if not cfg.get(k)]
    if missing:
        raise ConfigError(f"config.json 缺字段: {missing}")
    return cfg


def save_config(cfg):
    paths.ensure_data_dir()
    missing = [k for k in REQUIRED_KEYS if not cfg.get(k)]
    if missing:
        raise ConfigError(f"config 缺字段: {missing}")
    util.atomic_write(paths.config_path(), json.dumps(cfg, ensure_ascii=False, indent=2))


class bootstrap_lock:
    """bootstrap 期互斥(防并发双写指纹)。"""

    def __enter__(self):
        paths.ensure_data_dir()
        self.fd = os.open(paths.bootstrap_lock_path(), os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)
        return False
