"""run_stop_hook 页脚集成(footer-plan v3 §测试 test_hooklib_footer 节,逐条覆盖)。

正路径:active binding + payload(transcript fixture + effort)→ 最大 chunk_index 的
session_turn job body 以页脚结尾,其余块不含。
fail-open 铁律(最高优先级):footer_for 抛异常 / 返回 truthy 非 str / 延迟导入失败 /
transcript 不可读 / usage 异常 → chunks 不变、原样转发、reason=='enqueued'。
双解释器门禁:本文件须在 3.12 与真 3.9 各全绿。
"""
import json
import sys

import pytest

from tests.conftest import CHAT, CC_PID
from lib import constants, hooklib

# 独立于 test_stop_hook 的 ppid 链(避免任何交叉);hook(9101)→shell(9100)→claude(CC_PID)
HOOK_PID = 9101
SHELL_PID = 9100

SEP12 = "─" * 12
DOT = "·"
EXPECTED_FOOTER = "\n" + SEP12 + "\n🧠 71K " + DOT + " Opus 4.8 " + DOT + " max"


@pytest.fixture
def hook_env(env):
    env.prober.set(HOOK_PID, SHELL_PID, "Tue Jul 15 10:00:00 2026", "python3")
    env.prober.set(SHELL_PID, CC_PID, "Tue Jul 15 09:59:00 2026", "zsh")
    return env


def _transcript(tmp_path, tokens=71207, model="claude-opus-4-8", name="t.jsonl"):
    rec = {"type": "assistant", "message": {
        "role": "assistant", "model": model,
        "usage": {"input_tokens": tokens, "cache_read_input_tokens": 0,
                  "cache_creation_input_tokens": 0, "output_tokens": 5}}}
    p = tmp_path / name
    p.write_text(json.dumps(rec, ensure_ascii=False), encoding="utf-8")
    return str(p)


def _stop(env, msg, transcript_path=None, effort=None, session_id="sess-1"):
    payload = {"session_id": session_id, "last_assistant_message": msg,
               "stop_hook_active": False, "cwd": "/tmp/x"}
    if transcript_path is not None:
        payload["transcript_path"] = transcript_path
    if effort is not None:
        payload["effort"] = effort
    return hooklib.run_stop_hook(payload, conn=env.conn, prober=env.prober,
                                 clock=env.clock, start_pid=HOOK_PID)


def _turn_jobs(env):
    return env.conn.execute(
        "SELECT * FROM outbound_jobs WHERE kind='session_turn' "
        "ORDER BY chunk_index").fetchall()


# ---------------------------------------------------------------- 正路径:页脚落最大 index 块
def test_footer_appended_to_last_chunk(hook_env, tmp_path):
    """plan:active binding + payload(transcript fixture)→ 单块 body 以页脚结尾。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _stop(env, "本轮 session 输出正文", transcript_path=tp, effort={"level": "max"})
    assert r["reason"] == "enqueued"
    jobs = _turn_jobs(env)
    assert len(jobs) == 1
    assert jobs[0]["body"] == "本轮 session 输出正文" + EXPECTED_FOOTER


def test_footer_only_on_max_index_chunk(hook_env, tmp_path, monkeypatch):
    """plan:多块 msg → footer 仅最大 index 块;其余块不含。"""
    env = hook_env
    monkeypatch.setattr(constants, "CHUNK_LIMIT", 10)
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _stop(env, "a" * 25, transcript_path=tp, effort={"level": "max"})
    assert r["chunks"] == 3
    jobs = _turn_jobs(env)
    assert [j["chunk_index"] for j in jobs] == [0, 1, 2]
    assert jobs[0]["body"] == "a" * 10
    assert jobs[1]["body"] == "a" * 10
    assert jobs[2]["body"] == "a" * 5 + EXPECTED_FOOTER
    assert "🧠" not in jobs[0]["body"] and "🧠" not in jobs[1]["body"]


@pytest.mark.parametrize("n,nchunks", [(10, 1), (11, 2)])
def test_footer_at_chunk_boundary(hook_env, tmp_path, monkeypatch, n, nchunks):
    """plan:msg 长度恰 CHUNK_LIMIT 与 CHUNK_LIMIT+1 → 页脚仅一次、在最大 index。"""
    env = hook_env
    monkeypatch.setattr(constants, "CHUNK_LIMIT", 10)
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _stop(env, "b" * n, transcript_path=tp, effort={"level": "max"})
    assert r["chunks"] == nchunks
    jobs = _turn_jobs(env)
    all_body = "".join(j["body"] for j in jobs)
    assert all_body.count("🧠") == 1                 # 页脚仅一次
    assert jobs[-1]["body"].endswith(EXPECTED_FOOTER)
    for j in jobs[:-1]:
        assert "🧠" not in j["body"]


# ---------------------------------------------------------------- fail-open:transcript 不可读
def test_unreadable_transcript_no_footer_still_enqueued(hook_env, tmp_path):
    """plan:transcript 不可读(坏路径)→ body==原文(无 footer)、reason=='enqueued'。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    bad = str(tmp_path / "does-not-exist.jsonl")
    r = _stop(env, "正文", transcript_path=bad, effort={"level": "max"})
    assert r["reason"] == "enqueued"
    jobs = _turn_jobs(env)
    assert jobs[0]["body"] == "正文"
    assert "🧠" not in jobs[0]["body"]


# ---------------------------------------------------------------- fail-open:footer_for 抛异常(BLOCKER2 计算面)
def test_footer_for_raises_still_enqueued(hook_env, tmp_path, monkeypatch):
    """plan:ctxmeter.footer_for monkeypatch 抛异常 → reason=='enqueued'、body 无 footer。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    import lib.ctxmeter as ctxmeter

    def _boom(payload):
        raise RuntimeError("footer boom")

    monkeypatch.setattr(ctxmeter, "footer_for", _boom)
    r = _stop(env, "正文", transcript_path=_transcript(tmp_path), effort={"level": "max"})
    assert r["reason"] == "enqueued"
    assert _turn_jobs(env)[0]["body"] == "正文"


# ---------------------------------------------------------------- fail-open:footer_for 返回非 str(BLOCKER2 拼接面)
def test_footer_for_returns_nonstring_still_enqueued(hook_env, tmp_path, monkeypatch):
    """plan:footer_for 返回 truthy 非字符串(int)→ chunks[-1]+footer 抛 TypeError →
    本地 try 兜住 → reason=='enqueued'、body 无 footer。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    import lib.ctxmeter as ctxmeter

    monkeypatch.setattr(ctxmeter, "footer_for", lambda payload: 12345)
    r = _stop(env, "正文", transcript_path=_transcript(tmp_path), effort={"level": "max"})
    assert r["reason"] == "enqueued"
    assert _turn_jobs(env)[0]["body"] == "正文"


# ---------------------------------------------------------------- fail-open:延迟导入失败(BLOCKER1)
def test_lazy_import_failure_still_enqueued(hook_env, tmp_path, monkeypatch):
    """plan:强制延迟导入失败(sys.modules['lib.ctxmeter']=None + delattr(lib,'ctxmeter'))
    → from . import ctxmeter 抛 ImportError → 本地 try 兜住 → reason=='enqueued'。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    import lib
    import lib.ctxmeter  # noqa: F401  确保属性存在,monkeypatch 可对称恢复
    monkeypatch.setitem(sys.modules, "lib.ctxmeter", None)
    monkeypatch.delattr(lib, "ctxmeter", raising=False)
    r = _stop(env, "正文", transcript_path=_transcript(tmp_path), effort={"level": "max"})
    assert r["reason"] == "enqueued"
    assert _turn_jobs(env)[0]["body"] == "正文"


# ---------------------------------------------------------------- fail-open:usage 异常
@pytest.mark.parametrize("bad_line", [
    '{"type":"assistant","message":{"role":"assistant","model":"claude-opus-4-8",'
    '"usage":"not-a-dict"}}',
    '{"type":"assistant","message":{"role":"assistant","model":"claude-opus-4-8",'
    '"usage":{"input_tokens":"x","cache_read_input_tokens":"y","cache_creation_input_tokens":"z"}}}',
])
def test_usage_malformed_no_footer_still_enqueued(hook_env, tmp_path, bad_line):
    """plan:合法 JSON 但 usage 异常(usage 为 str / tokens 非数值)→ 无 footer,turn 仍 enqueued。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    p = tmp_path / "bad.jsonl"
    p.write_text(bad_line, encoding="utf-8")
    r = _stop(env, "正文", transcript_path=str(p), effort={"level": "max"})
    assert r["reason"] == "enqueued"
    assert _turn_jobs(env)[0]["body"] == "正文"
    assert "🧠" not in _turn_jobs(env)[0]["body"]


# ---------------------------------------------------------------- BLOCKER:损坏元数据不得让整条 turn 丢失
#   走真实 stop_hook_entry 链路(带 fail-closed 包装 + 真实 db.tx INSERT),证明 fail-open 真闭合:
#   页脚经 ctxmeter._sanitize 按构造安全 → 入库/argv/版式都不炸 → turn 照常 enqueued。
#   三种损坏(均来自损坏的官方元数据,非对抗):
#     surrogate \ud800(JSON 合法、Python 孤立 surrogate)→ 不修则 SQLite INSERT 抛 UnicodeEncodeError → 整条 turn 丢。
#     NUL \x00 → 不修则能入库但 daemon Popen argv 抛 ValueError。
#     换行 \n → 不修则页脚多出一行 = 版式污染。
POISONS = [
    pytest.param("\ud800", id="surrogate-d800"),
    pytest.param("\udc00", id="surrogate-dc00"),    # codex r2 MINOR3:surrogate 中段
    pytest.param("\udfff", id="surrogate-dfff"),    # codex r2 MINOR3:surrogate 终点
    pytest.param("o\x00x", id="nul"),
    pytest.param("o\ny", id="newline"),
    pytest.param("o\x85y", id="nel-0085"),          # codex r2 MINOR1:U+0085 NEL
    pytest.param("o\u2028y", id="ls-2028"),         # codex r2 MINOR1:U+2028 LS
    pytest.param("o\u2029y", id="ps-2029"),         # codex r2 MINOR1:U+2029 PS
]


def _entry(env, payload):
    # 经 stop_hook_entry(fail-closed 包装):若入库阶段抛,会被吞成 reason=='exception'、turn 丢
    return hooklib.stop_hook_entry(payload, conn=env.conn, prober=env.prober,
                                   clock=env.clock, start_pid=HOOK_PID)


def _assert_body_safe(body):
    body.encode("utf-8")                # 不抛 = 无孤立 surrogate(SQLite/入库安全)
    assert "\x00" not in body            # 无 NUL(argv 安全)
    # 无任何(含 Unicode 语义 NEL/LS/PS)换行注入:正文"正文"本身无换行 →
    # 干净 footer 只产 3 个 splitlines 行(正文 + 分隔线 + 页脚);splitlines 覆盖 \n\r\x85
    assert len(body.splitlines()) == 3
    for ch in ("\x85", "\u2028", "\u2029"):     # NEL / U+2028 LS / U+2029 PS 显式不在
        assert ch not in body


@pytest.mark.parametrize("poison", POISONS)
def test_poison_model_sanitized_and_enqueued(hook_env, tmp_path, poison):
    """BLOCKER:transcript model 含 surrogate/NUL/换行 → turn 仍 enqueued、body 干净、job 落库。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    rec = {"type": "assistant", "message": {
        "role": "assistant", "model": poison,
        "usage": {"input_tokens": 71207, "cache_read_input_tokens": 0,
                  "cache_creation_input_tokens": 0}}}
    p = tmp_path / "poison.jsonl"
    p.write_text(json.dumps(rec), encoding="utf-8")     # ensure_ascii → poison 变安全转义写盘
    r = _entry(env, {"session_id": "sess-1", "last_assistant_message": "正文",
                     "stop_hook_active": False, "transcript_path": str(p),
                     "effort": {"level": "max"}})
    assert r["reason"] == "enqueued"                     # 不丢(BLOCKER 核心)
    jobs = _turn_jobs(env)
    assert len(jobs) >= 1                                 # job 真落库
    _assert_body_safe(jobs[-1]["body"])


@pytest.mark.parametrize("poison", POISONS)
def test_poison_effort_sanitized_and_enqueued(hook_env, tmp_path, poison):
    """BLOCKER:payload effort.level 含 surrogate/NUL/换行 → 同样保证 turn enqueued、body 干净。"""
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)                            # 干净 model=claude-opus-4-8,71207
    r = _entry(env, {"session_id": "sess-1", "last_assistant_message": "正文",
                     "stop_hook_active": False, "transcript_path": tp,
                     "effort": {"level": poison}})
    assert r["reason"] == "enqueued"
    jobs = _turn_jobs(env)
    assert len(jobs) >= 1
    _assert_body_safe(jobs[-1]["body"])


# ---------------------------------------------------------------- import smoke(两 hook 入口 + ctxmeter)
def test_import_smoke():
    """plan:hooklib + 两 hook 入口 import smoke。"""
    from lib import hooklib as hl, ctxmeter as cm
    assert callable(hl.stop_hook_entry)
    assert callable(hl.session_end_entry)
    assert callable(hl.run_stop_hook)
    for name in ("footer_for", "format_footer", "read_turn_meter",
                 "extract_effort", "pretty_model", "SEP"):
        assert hasattr(cm, name), name
