"""run_stop_hook 页脚集成(contracts §2.1:`util.chunk_text_with_footer(body, footer, CHUNK_LIMIT)`)。

正路径:active binding + payload(transcript fixture + effort)→ 页脚只在最大 chunk_index 块末尾,
**每块(含页脚块)≤ CHUNK_LIMIT**,正文一个字符都不丢;页脚装不下时末块再切一刀。
边界:11999 / 12000 / 12001 / 24000(真实 CHUNK_LIMIT)与 hooklib 实际入库的 body 逐块相等。
fail-open 铁律:footer_for 抛异常 / 返回 truthy 非 str / 延迟导入失败 / transcript 不可读 / usage 异常
→ 等价纯分块、原样转发、reason=='enqueued'。双解释器门禁:本文件须在 3.12 与真 3.9 各全绿。"""
import inspect
import json
import sys

import pytest

from tests.conftest import CC_PID
from lib import constants, hooklib, util

# 独立于 test_stop_hook 的 ppid 链;hook(9101)→shell(9100)→claude(CC_PID)
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


def _bodies(env):
    return [j["body"] for j in _turn_jobs(env)]


# ---------------------------------------------------------------- 契约守卫:hooklib 用 chunk_text_with_footer
def test_hooklib_uses_chunk_text_with_footer_not_manual_concat():
    src = inspect.getsource(hooklib.run_stop_hook)
    assert "chunk_text_with_footer" in src
    assert "chunks[-1] + footer" not in src and "chunks[-1] = chunks[-1]" not in src
    assert "constants.CHUNK_LIMIT" in src   # 每次读 constants(可被覆盖),不用 def 时绑定的默认值


# ---------------------------------------------------------------- 正路径:页脚落最大 index 块
def test_footer_appended_to_last_chunk(hook_env, tmp_path):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _stop(env, "本轮 session 输出正文", transcript_path=tp, effort={"level": "max"})
    assert r["reason"] == "enqueued" and r["chunks"] == 1
    assert _bodies(env) == ["本轮 session 输出正文" + EXPECTED_FOOTER]


def test_footer_only_on_max_index_chunk_each_chunk_within_limit(hook_env, tmp_path, monkeypatch):
    """limit=60、页脚 36 字符:'a'*100 → 平切 [60,40];末块 40+36>60 → 末块再切:
    前 60-36=24 留原位,尾 16 + 页脚成新末块(52)。三块都 ≤ 60,页脚只在末块。"""
    env = hook_env
    monkeypatch.setattr(constants, "CHUNK_LIMIT", 60)
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _stop(env, "a" * 100, transcript_path=tp, effort={"level": "max"})
    assert r["chunks"] == 3
    jobs = _turn_jobs(env)
    assert [j["chunk_index"] for j in jobs] == [0, 1, 2]
    assert jobs[0]["body"] == "a" * 60
    assert jobs[1]["body"] == "a" * 24
    assert jobs[2]["body"] == "a" * 16 + EXPECTED_FOOTER
    assert all(len(j["body"]) <= 60 for j in jobs)
    assert "🧠" not in jobs[0]["body"] and "🧠" not in jobs[1]["body"]
    assert "".join(_bodies(env)) == "a" * 100 + EXPECTED_FOOTER   # 正文不丢、页脚恰一次


@pytest.mark.parametrize("n", [11999, 12000, 12001, 24000])
def test_footer_at_real_chunk_limit_boundaries(hook_env, tmp_path, n):
    """真实 CHUNK_LIMIT=12000 的四个边界:入库 body 逐块 == util.chunk_text_with_footer 的结果;
    每块 ≤ 12000;拼回 == 正文 + 页脚;页脚只出现一次且在末块。"""
    env = hook_env
    assert constants.CHUNK_LIMIT == 12000
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    body = "x" * n
    r = _stop(env, body, transcript_path=tp, effort={"level": "max"})
    assert r["reason"] == "enqueued"
    expect = util.chunk_text_with_footer(body, EXPECTED_FOOTER, 12000)
    got = _bodies(env)
    assert got == expect and r["chunks"] == len(expect)
    assert all(len(c) <= 12000 for c in got)
    assert "".join(got) == body + EXPECTED_FOOTER
    assert got[-1].endswith(EXPECTED_FOOTER) and sum(c.count("🧠") for c in got) == 1
    # 具体形状(页脚 36 字符):n=11999 → [11964, 71];12000 → [11964, 72];12001 → [12000, 37];24000 → [12000, 11964, 72]
    lens = [len(c) for c in got]
    f = len(EXPECTED_FOOTER)
    if n <= 12000:
        assert lens == [12000 - f, n - (12000 - f) + f]
    elif n == 12001:
        assert lens == [12000, 1 + f]
    else:
        assert lens == [12000, 12000 - f, 12000 - (12000 - f) + f]


def test_footer_longer_than_limit_dropped_body_intact(hook_env, tmp_path, monkeypatch):
    """contracts §2.1:页脚 > limit → 丢页脚,**绝不丢正文**(limit=10、页脚 36)。"""
    env = hook_env
    monkeypatch.setattr(constants, "CHUNK_LIMIT", 10)
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _stop(env, "b" * 25, transcript_path=tp, effort={"level": "max"})
    assert r["chunks"] == 3
    assert _bodies(env) == ["b" * 10, "b" * 10, "b" * 5]


# ---------------------------------------------------------------- fail-open:transcript 不可读
def test_unreadable_transcript_no_footer_still_enqueued(hook_env, tmp_path):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    bad = str(tmp_path / "does-not-exist.jsonl")
    r = _stop(env, "正文", transcript_path=bad, effort={"level": "max"})
    assert r["reason"] == "enqueued"
    assert _bodies(env) == ["正文"]


# ---------------------------------------------------------------- fail-open:footer_for 抛异常
def test_footer_for_raises_still_enqueued(hook_env, tmp_path, monkeypatch):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    import lib.ctxmeter as ctxmeter

    def _boom(payload):
        raise RuntimeError("footer boom")

    monkeypatch.setattr(ctxmeter, "footer_for", _boom)
    r = _stop(env, "正文", transcript_path=_transcript(tmp_path), effort={"level": "max"})
    assert r["reason"] == "enqueued" and _bodies(env) == ["正文"]


# ---------------------------------------------------------------- fail-open:footer_for 返回非 str
def test_footer_for_returns_nonstring_still_enqueued(hook_env, tmp_path, monkeypatch):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    import lib.ctxmeter as ctxmeter

    monkeypatch.setattr(ctxmeter, "footer_for", lambda payload: 12345)
    r = _stop(env, "正文", transcript_path=_transcript(tmp_path), effort={"level": "max"})
    assert r["reason"] == "enqueued" and _bodies(env) == ["正文"]


# ---------------------------------------------------------------- fail-open:延迟导入失败
def test_lazy_import_failure_still_enqueued(hook_env, tmp_path, monkeypatch):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    import lib
    import lib.ctxmeter  # noqa: F401
    monkeypatch.setitem(sys.modules, "lib.ctxmeter", None)
    monkeypatch.delattr(lib, "ctxmeter", raising=False)
    r = _stop(env, "正文", transcript_path=_transcript(tmp_path), effort={"level": "max"})
    assert r["reason"] == "enqueued" and _bodies(env) == ["正文"]


# ---------------------------------------------------------------- fail-open:usage 异常
@pytest.mark.parametrize("bad_line", [
    '{"type":"assistant","message":{"role":"assistant","model":"claude-opus-4-8",'
    '"usage":"not-a-dict"}}',
    '{"type":"assistant","message":{"role":"assistant","model":"claude-opus-4-8",'
    '"usage":{"input_tokens":"x","cache_read_input_tokens":"y","cache_creation_input_tokens":"z"}}}',
])
def test_usage_malformed_no_footer_still_enqueued(hook_env, tmp_path, bad_line):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    p = tmp_path / "bad.jsonl"
    p.write_text(bad_line, encoding="utf-8")
    r = _stop(env, "正文", transcript_path=str(p), effort={"level": "max"})
    assert r["reason"] == "enqueued" and _bodies(env) == ["正文"]


# ---------------------------------------------------------------- 损坏元数据不得让整条 turn 丢失
POISONS = [
    pytest.param("\ud800", id="surrogate-d800"),
    pytest.param("\udc00", id="surrogate-dc00"),
    pytest.param("\udfff", id="surrogate-dfff"),
    pytest.param("o\x00x", id="nul"),
    pytest.param("o\ny", id="newline"),
    pytest.param("o\x85y", id="nel-0085"),
    pytest.param("o y", id="ls-2028"),
    pytest.param("o y", id="ps-2029"),
]


def _entry(env, payload):
    return hooklib.stop_hook_entry(payload, conn=env.conn, prober=env.prober,
                                   clock=env.clock, start_pid=HOOK_PID)


def _assert_body_safe(body):
    body.encode("utf-8")
    assert "\x00" not in body
    assert len(body.splitlines()) == 3
    for ch in ("\x85", " ", " "):
        assert ch not in body


@pytest.mark.parametrize("poison", POISONS)
def test_poison_model_sanitized_and_enqueued(hook_env, tmp_path, poison):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    rec = {"type": "assistant", "message": {
        "role": "assistant", "model": poison,
        "usage": {"input_tokens": 71207, "cache_read_input_tokens": 0,
                  "cache_creation_input_tokens": 0}}}
    p = tmp_path / "poison.jsonl"
    p.write_text(json.dumps(rec), encoding="utf-8")
    r = _entry(env, {"session_id": "sess-1", "last_assistant_message": "正文",
                     "stop_hook_active": False, "transcript_path": str(p),
                     "effort": {"level": "max"}})
    assert r["reason"] == "enqueued"
    jobs = _turn_jobs(env)
    assert len(jobs) >= 1
    _assert_body_safe(jobs[-1]["body"])


@pytest.mark.parametrize("poison", POISONS)
def test_poison_effort_sanitized_and_enqueued(hook_env, tmp_path, poison):
    env = hook_env
    env.make_binding(status="active", session_id="sess-1")
    tp = _transcript(tmp_path)
    r = _entry(env, {"session_id": "sess-1", "last_assistant_message": "正文",
                     "stop_hook_active": False, "transcript_path": tp,
                     "effort": {"level": poison}})
    assert r["reason"] == "enqueued"
    jobs = _turn_jobs(env)
    assert len(jobs) >= 1
    _assert_body_safe(jobs[-1]["body"])


# ---------------------------------------------------------------- import smoke
def test_import_smoke():
    from lib import hooklib as hl, ctxmeter as cm
    assert callable(hl.stop_hook_entry)
    assert callable(hl.session_end_entry)
    assert callable(hl.run_stop_hook)
    for name in ("footer_for", "format_footer", "read_turn_meter",
                 "extract_effort", "pretty_model", "SEP"):
        assert hasattr(cm, name), name
