# slack-bridge — Slack ↔ 本地 Claude Code session 绑定桥(Claude Code plugin)

**状态:WP0 scaffold(脚手架)。** 尚不可用于真机;传输层(Socket Mode consumer / SlackClient 出站)、
入站/审批/媒体/恢复、出站状态机与控制面分别在 WP1–WP4 落地,WP5 集成后才是可运行版本。

## 出处

本仓库由 [feishu-bridge](../feishu-bridge) **v1.6.1**(commit `9941306`)复制而来并 `git init` 新历史:
绑定/队列/hook/listener 核心沿用,传输层(飞书 lark-cli → Slack Socket Mode + Web API)重写。
WP0 保留了全部旧 Feishu 模块(`lib/runner.py`、`lib/selfcheck.py`、旧测试)以维持可导入,
标注 `LEGACY-FEISHU` 的名字在 WP5 删除。

## 冻结契约

跨工作包(WP1–WP4 并行)共享的语义全部冻结在 **[`docs/contracts.md`](docs/contracts.md)**,
由 `tests/test_contracts.py`(xfail-strict 守卫,各 WP 合入前转绿)与 `tests/test_drain.py` 守卫。
另见 `docs/slack-app-manifest.json`(Slack app 清单)与 `docs/capability-probe.md`(真机能力探测记录)。

## 目录

- `.claude-plugin/` plugin 清单;`hooks/` Stop/SessionEnd/StopFailure;`monitors/` listener monitor
- `bin/` daemon / listener / bridgectl / notifyctl(WP1 起新增 `slack_consumer.py` `download_worker.py`)
- `lib/` 核心模块;`schema.sql` 新库 schema(不做迁移)
- `scripts/capability_probe.py` 真机能力探测(stdlib;不需要 daemon 在跑)
- `tests/` 离线测试(`python3 -m pytest tests/`)

## 数据目录

`~/.claude/data/slack-bridge/`(0700;env `SLACK_BRIDGE_DATA_DIR` 重定向):`bridge.db` `config.json`
`tokens.json`(0600)`allowlist.json` `bridge.lock` `daemon.log` `hook_drops.log` `hook_heartbeat.*` `media/`。
