"""路径解析。测试经 FEISHU_BRIDGE_DATA_DIR / FEISHU_BRIDGE_SETTINGS_PATH 重定向,
绝不在 import 时创建真实目录。"""
import os
import pathlib


def pkg_root():
    """plugin/包 根目录(含 bin/lib/hooks/schema.sql)。lib/paths.py → parents[1] = 包根。
    plugin 化后 = plugin root(仍 parents[1],因 lib/ 仍直接位于包根下,不受迁移影响)。"""
    return pathlib.Path(__file__).resolve().parents[1]


def data_dir():
    env = os.environ.get("FEISHU_BRIDGE_DATA_DIR")
    if env:
        return pathlib.Path(env)
    return pathlib.Path.home() / ".claude" / "data" / "feishu-bridge"


def ensure_data_dir():
    d = data_dir()
    d.mkdir(parents=True, exist_ok=True)
    os.chmod(d, 0o700)
    (d / "media").mkdir(exist_ok=True)
    return d


def db_path():
    return data_dir() / "bridge.db"


def schema_path():
    return pkg_root() / "schema.sql"


def config_path():
    return data_dir() / "config.json"


def allowlist_path():
    """成员直投白名单。**独立于 config.json**:config 是 bootstrap 期钉死的指纹
    (profile/app_id/owner/cli_version),白名单则由 agent 在运行时按 owner 指令改写。"""
    return data_dir() / "allowlist.json"


def lock_path():
    return data_dir() / "bridge.lock"


def bootstrap_lock_path():
    return data_dir() / "bootstrap.lock"


def ensure_lock_path():
    return data_dir() / "ensure.lock"  # ensure-daemon 接管 singleflight(r2-M1④)


def daemon_log_path():
    return data_dir() / "daemon.log"


def hook_drops_path():
    return data_dir() / "hook_drops.log"


def hook_heartbeat_path(event):
    # plugin 化:Stop/SessionEnd hook 每次运行(先于任何 db/网络)写各自哨兵 =「plugin hooks 已生效」
    # 的正向信号(替代读 settings.json 判断手装 hooks)。**Stop 与 SessionEnd 分开**(MAJOR 2):
    # 握手依赖 Stop,故 bind 只认 Stop 心跳;SessionEnd-only 不该压掉警告。
    assert event in ("stop", "session_end")
    return data_dir() / f"hook_heartbeat.{event}"


def media_root():
    return data_dir() / "media"


def settings_json_path():
    env = os.environ.get("FEISHU_BRIDGE_SETTINGS_PATH")
    if env:
        return pathlib.Path(env)
    return pathlib.Path.home() / ".claude" / "settings.json"
