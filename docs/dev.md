# 开发与测试约定(slack-bridge)

## 跑测试

```bash
python3 -m pytest tests/ -q                      # 全量离线:FakeSlackClient + 真实 SQLite,零网络,~25s
python3.9 -m pytest tests/ -q                    # 3.9 门禁(README 承诺 Python ≥ 3.9;本机有 3.9 就必须跑)
python3 -m pytest tests/ --collect-only -q       # 收集检查(import 面 / 语法)
python3.9 -m compileall -q lib bin hooks scripts tests
python3 -m pytest tests/test_contracts.py tests/test_drain.py -q   # 冻结契约守卫(见下)
python3 -m pytest tests/test_integration_flow.py -q               # 全链路集成(bind → 投递 → 审批 → unbind)
```

- 离线铁律:`tests.helpers.FakeSlackClient` **未登记的方法一律 AssertionError**,不会静默放过一次意料之外的外呼;
  每个测试只 `.on()` 它预期的方法。冷却存储与真 daemon 同一份(`DaemonStateCooldownStore(conn)`)。
- 数据目录:`tests/conftest.py` 把 `SLACK_BRIDGE_DATA_DIR` / `SLACK_BRIDGE_SETTINGS_PATH` 指到 tmp,并清掉
  `SLACK_BOT_TOKEN` / `SLACK_APP_TOKEN`;测试**绝不**碰真实 `~/.claude`。
- 时钟:`FakeClock`(wall / mono 独立可拨),节流、退避、TTL 全靠拨钟,不 sleep。
- 一站式接线:`Env`(`conn cfg clock client prober` + `stage / drain / click` = consumer 写法 / daemon drain /
  owner 点卡片)。daemon 节奏用 `env.core.loop_iteration()`,listener 用 `ListenerCore.step()`。

## `.venv-test`:真实 slack_sdk 的运行层契约

仓库里**只有** `bin/slack_consumer.py` 导入 `slack_sdk`(`tests/test_consumer.py::test_only_consumer_imports_slack_sdk`
守着)。日常全量跑用 `tests/fakes/slack_sdk/`(脚本化假 sdk,子进程经 PYTHONPATH 抢占);
`tests/test_sdk_contract.py` 用**真** `SocketModeClient` 类(monkeypatch 底层连接)验证构造签名、listener 调用形状、
ack 帧与 `close()`,未安装时 5 条 skip。合入前必须在 `.venv-test` 里跑通:

```bash
python3 -m venv .venv-test && .venv-test/bin/pip install 'slack_sdk>=3.44,<4' pytest
.venv-test/bin/python -m pytest tests/test_sdk_contract.py -q
```

`.venv-test/` 已在 `.gitignore`。sdk 版本区间与 `README` / consumer 一致(`>=3.44,<4`)。

## 分支 / worktree / 不用 stash

- 并行工作包各自在 **git worktree**(`../slack-bridge-wp<N>`)里做,主仓 `main` 只做合并与串行的集成工作;
  不要在别人的 worktree 里改文件。
- **绝不 `git stash`**:多个 worktree 共享一个 stash 栈,stash/pop 会把改动带错分支。要暂存就提交一个 WIP commit
  或开新 worktree。
- 不 push、不 rebase 已合并的历史;提交信息用中文说明「改了什么 / 为什么」,以
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` 结尾。
- 原地改代码不换 plugin 版本时,旧 daemon 的 code-identity 检查察觉不到,要自己 `bridgectl status` 看 `daemon.pid` 后 kill。

## 契约是怎么被守住的

- `docs/contracts.md` 是跨模块语义的**冻结**文本(事务边界、出站状态机、wire 形状、凭据、构造签名)。
- 守卫测试:`tests/test_contracts.py`(每条契约一个断言;当初归属未落地 WP 的以 `xfail(strict=True)` 存在,
  该 WP 合入时转绿并摘标记,现已全绿)与 `tests/test_drain.py`(真实 SQLite 上的事务语义:提交 / 回滚 / 崩溃恢复)。
- **改契约 = 改 `docs/contracts.md` + 改守卫测试 + 改实现**,三者同一提交;只改实现让守卫变红是不允许的。
- 实现上有意偏离契约文本的地方,记在 `docs/contracts.md` 末尾「实现偏差记录」,而不是悄悄改。
- `lib/constants.py` 的**名字**是各模块共享的预声明(值可调、名字不改);`tests/conftest.Env` 依赖
  contracts §8 冻结的构造签名,改签名先改契约。

## 目录速查

`lib/` 核心;`bin/` daemon / listener / consumer / download_worker / bridgectl / notifyctl;`hooks/` Stop / SessionEnd /
StopFailure;`skills/` bridge / notify;`scripts/capability_probe.py` 真机能力探测;`tests/fakes/slack_sdk/` 假 sdk。
