"""ctxmeter 单元测试(footer-plan v3 §测试节,逐条覆盖)。

被测面(纯函数 + 只读 transcript 尾部):
  pretty_model / extract_effort / _read_tail(256KiB 有界)/ read_turn_meter(逆序、
  synthetic 跳过、tok<=0 继续、EOF 半写回退、U+2028 不误切、float 归一、nan/inf 守卫)/
  format_footer(rounding、上限、段级 cap)/ footer_for(total 绝不抛)。
双解释器门禁:本文件须在 3.12 与真 3.9 各全绿(3.9 惰性注解)。
"""
import json

import pytest

from lib import ctxmeter

# footer-plan v3 权威格式:分隔线=12×U+2500,段间 " · "(U+00B7),🧠=U+1F9E0
SEP12 = "─" * 12
DOT = "·"
EXACT_FOOTER = "\n" + SEP12 + "\n🧠 71K " + DOT + " Opus 4.8 " + DOT + " max"


# ---------------------------------------------------------------- fixtures / helpers
def _usage(inp=0, cread=0, ccreate=0, output=5):
    return {"input_tokens": inp, "cache_read_input_tokens": cread,
            "cache_creation_input_tokens": ccreate, "output_tokens": output}


def _rec(model="claude-opus-4-8", usage=None, role="assistant"):
    m = {"role": role}
    if model is not None:
        m["model"] = model
    if usage is not None:
        m["usage"] = usage
    return {"type": "assistant", "message": m}


def _line(rec):
    # ensure_ascii=False → U+2028 等留字面(测 split("\n") 不误切);单行无内嵌 \n
    return json.dumps(rec, ensure_ascii=False)


def _write(tmp_path, *lines, name="transcript.jsonl"):
    p = tmp_path / name
    p.write_text("\n".join(lines), encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------- 正常单 turn(精确串)
def test_normal_single_turn_footer_exact(tmp_path):
    """plan:正常单 turn → footer 精确 == '\\n────────────\\n🧠 71K · Opus 4.8 · max'。"""
    tp = _write(tmp_path, _line(_rec(usage=_usage(inp=71207))))
    footer = ctxmeter.footer_for({"transcript_path": tp, "effort": {"level": "max"}})
    assert footer == EXACT_FOOTER


# ---------------------------------------------------------------- read_turn_meter 逆序语义
def test_multiple_usage_picks_last(tmp_path):
    """plan:多条 usage → 选最后一条(逆序命中最近的 assistant 记录)。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=10000))),
                _line(_rec(usage=_usage(inp=71207))))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207


def test_synthetic_skipped_uses_prior_real(tmp_path):
    """plan:最新 synthetic + 前一条真实 → 用前一条真实的 tokens+model,不用 synthetic。"""
    tp = _write(tmp_path,
                _line(_rec(model="claude-opus-4-8", usage=_usage(inp=71207))),
                _line(_rec(model="<synthetic>", usage=_usage(inp=5))))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207 and meter["model"] == "claude-opus-4-8"


def test_nonpositive_tokens_omits_footer(tmp_path):
    """codex r2 MAJOR(语义反转,原 test_nonpositive_tokens_continues_to_prior):
    最新真实 assistant 记录一旦命中即权威;其 tok<=0(不可用)→ return None(省略页脚),
    **绝不回退到更旧记录**(否则用任意滞后的旧 tokens/model 拼当前 effort = 可信外观的错值)。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),          # 更旧有效(不许取)
                _line(_rec(usage=_usage(inp=0, cread=0, ccreate=0))))  # 最新真实 tok=0
    assert ctxmeter.read_turn_meter(tp) is None
    assert ctxmeter.footer_for({"transcript_path": tp, "effort": "max"}) == ""


# ---------------------------------------------------------------- extract_effort(仅 payload)
@pytest.mark.parametrize("payload,expected", [
    ({"effort": "max"}, "max"),
    ({"effort": {"level": "max"}}, "max"),
    ({"effort": {"level": 123}}, None),          # level 非 str
    ({"effort": {"foo": "bar"}}, None),          # 无 level,不猜其它 key
    ({}, None),                                   # 缺失
    ({"effort": "  high  "}, "high"),            # strip
    ({"effort": ""}, None),                       # 空串
    ({"effort": {"level": "   "}}, None),        # 全空白 level
    ({"effort": 123}, None),                      # effort 是 int
    ("not-a-dict", None),                         # payload 非 dict
    (None, None),
])
def test_extract_effort_variants(payload, expected):
    assert ctxmeter.extract_effort(payload) == expected


def test_effort_only_from_payload_not_transcript(tmp_path):
    """plan:effort 只来自 payload、不回退 transcript。
    transcript 顶层 effort 存在、payload effort 畸形 → footer 不显示 effort 段。"""
    rec = _rec(usage=_usage(inp=71207))
    rec["effort"] = {"level": "high"}            # transcript 记录里带 effort → 必须被忽略
    tp = _write(tmp_path, _line(rec))
    footer = ctxmeter.footer_for({"transcript_path": tp, "effort": {"foo": "bar"}})
    assert footer == "\n" + SEP12 + "\n🧠 71K " + DOT + " Opus 4.8"
    assert "high" not in footer


# ---------------------------------------------------------------- 空 / 缺 usage → None → ""
def test_no_usage_returns_empty_footer(tmp_path):
    """plan:全程无 usage → read_turn_meter None → footer_for ''。"""
    tp = _write(tmp_path,
                _line({"type": "user", "message": {"role": "user", "content": "hi"}}),
                _line(_rec(model="claude-opus-4-8", usage=None)))
    assert ctxmeter.read_turn_meter(tp) is None
    assert ctxmeter.footer_for({"transcript_path": tp, "effort": "max"}) == ""


def test_footer_for_missing_transcript_paths(tmp_path):
    """plan:transcript_path None / 不存在 / 空文件 → footer_for ''(不抛)。"""
    assert ctxmeter.footer_for({"transcript_path": None, "effort": "max"}) == ""
    assert ctxmeter.footer_for(
        {"transcript_path": str(tmp_path / "nope.jsonl"), "effort": "max"}) == ""
    empty = tmp_path / "empty.jsonl"
    empty.write_text("", encoding="utf-8")
    assert ctxmeter.footer_for({"transcript_path": str(empty), "effort": "max"}) == ""
    assert ctxmeter.footer_for({}) == ""          # 连 transcript_path 键都没有


# ---------------------------------------------------------------- 坏行 / EOF 半写 / U+2028
def test_truncated_leading_fragment_does_not_block_valid_record(tmp_path):
    """尾缓冲区开头的截断行(非法 JSON,如尾读切进一行中间)不妨碍找到有效记录。
    (逆序先命中末尾合法行即返回;"坏行后继续"分支由 test_eof_halfwritten_falls_back_to_prior 覆盖。
    改名:原名 test_truncated_first_line_skipped 名不副实——它并不真的走到"跳过坏行继续"分支。)"""
    tp = _write(tmp_path,
                "{ this is a truncated fragment, not valid json",
                _line(_rec(usage=_usage(inp=71207))))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207


def test_eof_halfwritten_falls_back_to_prior(tmp_path):
    """plan(r3-m3)+codex r2 MAJOR:末尾未闭合 JSON = json.loads 失败 → continue(半写不是"真实
    turn",与 MAJOR 保留的两个 continue 之一一致);前一条完整真实记录 = 第一条权威 → 返回它。
    (证明"json 失败即跳过"仍成立,不受 MAJOR 的"真实记录不可用即 None"影响。)"""
    half = ('{"type":"assistant","message":{"role":"assistant",'
            '"model":"claude-opus-4-8","usage":{"input_tokens":5')   # 未闭合
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),
                half)
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207               # 用前一条完整真实记录,非半写行的 5


def test_cache_creation_field_contributes(tmp_path):
    """codex r2 MINOR3(反假绿):三 token 字段都必须计入总和。
    30000+40000+1207=71207;删任一字段累加都会改变结果(现有用例全把 cache_creation 设 0,漏测)。"""
    tp = _write(tmp_path, _line(_rec(usage={
        "input_tokens": 30000, "cache_read_input_tokens": 40000,
        "cache_creation_input_tokens": 1207, "output_tokens": 5})))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207


# ---------------------------------------------------------------- MAJOR:assistant turn 权威屏障(codex r3)
def test_message_null_authoritative_omits_footer(tmp_path):
    """codex r3 MAJOR:最新记录外层 type==assistant 但 message==null(损坏)= 权威 assistant turn;
    不可用 → return None,**绝不回退**更旧有效记录(否则旧 tokens/model 拼当前 effort = 可信错值)。
    现状 bug:靠 `not isinstance(m,dict): continue` 会跳过它回退 71K。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),                  # 更旧有效(诱饵,不许取)
                _line({"type": "assistant", "message": None}))         # 最新 = assistant turn、message 损坏
    assert ctxmeter.read_turn_meter(tp) is None


def test_role_corrupt_authoritative_omits_footer(tmp_path):
    """codex r3 MAJOR:最新 type==assistant 但 message.role 损坏(=user)= 权威 turn,role 不符 →
    None,不回退更旧记录。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),                  # 诱饵
                _line({"type": "assistant", "message": {
                    "role": "user", "model": "claude-opus-4-8",
                    "usage": {"input_tokens": 5, "cache_read_input_tokens": 0,
                              "cache_creation_input_tokens": 0}}}))
    assert ctxmeter.read_turn_meter(tp) is None


def test_usage_nondict_authoritative_omits_footer(tmp_path):
    """codex r3 MINOR1(反假绿):最新 assistant turn 的 usage 非 dict = 权威但不可用 → None;
    有更旧有效记录作诱饵才能真咬住"不回退"(此前无诱饵,该分支改 continue 仍全绿)。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),                  # 诱饵
                _line({"type": "assistant", "message": {
                    "role": "assistant", "model": "claude-opus-4-8",
                    "usage": "not-a-dict"}}))
    assert ctxmeter.read_turn_meter(tp) is None


def test_usage_missing_authoritative_omits_footer(tmp_path):
    """codex r3 MINOR1:最新 assistant turn 缺 usage 键 = 权威但不可用 → None,不回退诱饵。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),                  # 诱饵
                _line(_rec(model="claude-opus-4-8", usage=None)))      # 最新缺 usage
    assert ctxmeter.read_turn_meter(tp) is None


def test_missing_type_record_not_recognized_omits_footer(tmp_path):
    """codex r5(换框架,反转自原 test_assistant_turn_recognized_via_role_when_type_missing):
    删掉 role 回退、纯 type=='assistant' 识别 assistant turn。**故意的 fail-safe 降级**——
    缺外层 type 的记录不被识别为 assistant turn → 若无其它有效记录 → None(无页脚)。
    footer 是 best-effort 装饰,缺 type 的假想 CC 版本下省略页脚可接受;换取消灭 role 回退引入的
    "显式 type=user/null 被损坏 role 劫持"错值边界(codex r4/r5 连续两轮同一分支的洞)。"""
    tp = _write(tmp_path,
                _line({"message": {"role": "assistant", "model": "claude-opus-4-8",
                                   "usage": {"input_tokens": 71207,
                                             "cache_read_input_tokens": 0,
                                             "cache_creation_input_tokens": 0}}}))  # 无 type 字段
    assert ctxmeter.read_turn_meter(tp) is None
    assert ctxmeter.footer_for({"transcript_path": tp, "effort": "max"}) == ""


def test_explicit_null_type_not_hijacked_by_role(tmp_path):
    """codex r5 MAJOR(锁):外层 type==null(**显式 null,非缺失**)即便 message.role 损坏成 'assistant',
    也不得被劫持成权威 —— 纯 type 下 o.get('type')==None != 'assistant' → 跳过该记录 →
    采用更旧有效记录(71207,非 bogus 5)。旧"type 优先但把 null 当缺失、回退 role"逻辑会误劫持返回 5。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),                      # 旧有效(应采用)
                _line({"type": None, "message": {
                    "role": "assistant", "model": "bogus",
                    "usage": {"input_tokens": 5, "cache_read_input_tokens": 0,
                              "cache_creation_input_tokens": 0}}}))         # 最新:type=null、role 劫持
    meter = ctxmeter.read_turn_meter(tp)
    assert meter is not None
    assert meter["tokens"] == 71207 and meter["model"] == "claude-opus-4-8"


def test_explicit_user_type_not_hijacked_by_role(tmp_path):
    """codex r4 MAJOR(锁):外层 type=='user'(显式非 assistant)即便 message.role 被损坏成 'assistant',
    也不得被劫持成权威 assistant turn。**type 优先**:type 字段存在时只认 type、role 不覆盖 → 跳过该 user
    记录 → 采用更旧有效记录(71207,而非被劫持的 bogus 5)。上轮过宽的 OR 会误返回 {5, bogus}。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),                      # 更旧有效(应被采用)
                _line({"type": "user", "message": {
                    "role": "assistant", "model": "bogus",
                    "usage": {"input_tokens": 5, "cache_read_input_tokens": 0,
                              "cache_creation_input_tokens": 0}}}))         # 最新:显式 user、role 被劫持
    meter = ctxmeter.read_turn_meter(tp)
    assert meter is not None
    assert meter["tokens"] == 71207 and meter["model"] == "claude-opus-4-8"


def test_u2028_in_content_still_one_line(tmp_path):
    """plan:U+2028 位于 assistant content 的合法 JSONL 行 → 仍作一行、页脚正常
    (split('\\n') 不误切;若用 splitlines 会把该行劈成非法 JSON)。"""
    rec = _rec(usage=_usage(inp=71207))
    rec["message"]["content"] = "line\u2028sep"
    tp = _write(tmp_path, _line(rec))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter is not None and meter["tokens"] == 71207


# ---------------------------------------------------------------- _read_tail 256KiB 有界
def test_read_tail_bounded_256k(tmp_path, monkeypatch):
    """plan:>256KB 垃圾前缀 + 尾部合法行 → 命中;插桩断言实际 read ≤ _TAIL 字节(read(_TAIL))。"""
    valid = _line(_rec(usage=_usage(inp=71207)))
    p = tmp_path / "big.jsonl"
    p.write_bytes(b"x" * (400 * 1024) + b"\n" + valid.encode("utf-8"))

    reads = []
    real_open = open

    class _Spy:
        def __init__(self, f):
            self._f = f

        def __enter__(self):
            self._f.__enter__()
            return self

        def __exit__(self, *a):
            return self._f.__exit__(*a)

        def seek(self, *a):
            return self._f.seek(*a)

        def tell(self):
            return self._f.tell()

        def read(self, n=-1):
            reads.append(n)
            return self._f.read(n)

        def close(self):
            return self._f.close()

    def _spy_open(path, mode="r", *a, **k):
        return _Spy(real_open(path, mode, *a, **k))

    monkeypatch.setattr("builtins.open", _spy_open)
    lines = ctxmeter._read_tail(str(p))
    monkeypatch.undo()                            # 尽早恢复,避免影响后续断言/框架

    assert reads == [ctxmeter._TAIL]              # 恰一次 read(_TAIL),读取有界
    assert isinstance(lines, list)                # _read_tail 返回逐行严格解码后的行列表(codex MINOR2)
    assert sum(len(l.encode("utf-8")) for l in lines) <= ctxmeter._TAIL
    # 且尾读确实命中末尾合法行
    assert ctxmeter.read_turn_meter(str(p))["tokens"] == 71207


def test_read_tail_drops_undecodable_line_strict(tmp_path):
    """codex r3 MINOR2(反假绿,直测 _read_tail):含非法 UTF-8 字节的整行经逐行严格解码被丢弃、
    不在返回列表;合法行保留。此断言(坏行不在列表 + 无 U+FFFD)才能区分严格 decode 与 errors='replace'
    ——replace 会把坏字节换 U+FFFD 保留整行;旧 test 把坏字节放数字里,replace 后 JSON 仍失败 →
    strict/replace 结果相同 = 测不出区别(假绿)。"""
    p = tmp_path / "corrupt.jsonl"
    good = '{"ok":1}'
    # 坏行:非法 UTF-8 起始字节 \xff\xfe(strict 抛 UnicodeDecodeError → 整行丢;replace 变 "��" 保留)
    p.write_bytes(good.encode("utf-8") + b"\n" + b'{"bad":"\xff\xfe corrupt"}')
    lines = ctxmeter._read_tail(str(p))
    assert good in lines                               # 合法行保留
    assert all("bad" not in l for l in lines)          # 坏行整行丢弃、不在列表(replace 会保留 → 咬住)
    assert all("\ufffd" not in l for l in lines)       # 绝无 U+FFFD 替换字符残留


# ---------------------------------------------------------------- rounding
@pytest.mark.parametrize("tok,ktext", [
    (71207, "71K"),
    (462176, "462K"),
    (300, "1K"),                                   # round→0 但 max(1,..) 兜底,无 "0K"
    (1, "1K"),
])
def test_rounding(tok, ktext):
    footer = ctxmeter.format_footer({"tokens": tok, "model": None})
    assert footer == "\n" + SEP12 + "\n🧠 " + ktext


# ---------------------------------------------------------------- float / nan / inf 硬化
def test_float_token_fields_normalized(tmp_path):
    """plan(r3-m1):float token 字段(1.0 等)→ tok 归一为 int、页脚正常(不被静默吞)。"""
    tp = _write(tmp_path, _line(_rec(usage={
        "input_tokens": 1.0, "cache_read_input_tokens": 71206.0,
        "cache_creation_input_tokens": 0, "output_tokens": 5})))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207 and isinstance(meter["tokens"], int)
    footer = ctxmeter.format_footer(meter, effort="max")
    assert footer == EXACT_FOOTER


def test_nan_inf_token_fields_skipped(tmp_path):
    """plan(r3-m1)+codex r2 MAJOR:字段级 isfinite 守卫仍跳过 nan/inf(不抛,免 int() 崩)。
    但末条(最新真实记录)唯一有效字段被跳空 → tok=0 → 该记录不可用 → return None,
    **不回退更旧记录**(与 MAJOR 语义一致)。"""
    tp = _write(tmp_path,
                _line(_rec(usage=_usage(inp=71207))),          # 更旧有效(不许取)
                ('{"type":"assistant","message":{"role":"assistant",'
                 '"model":"claude-opus-4-8","usage":{"input_tokens":NaN,'
                 '"cache_read_input_tokens":Infinity,"cache_creation_input_tokens":0}}}'))
    assert ctxmeter.read_turn_meter(tp) is None


def test_bool_token_field_not_counted(tmp_path):
    """codex MINOR1:bool 是 int 子类,不能当 token 计数(input_tokens:true 原会 +1)。
    input_tokens=true 应被排除 → 只算 cache_read 的 71207(而非 71208)。"""
    tp = _write(tmp_path, _line(_rec(usage={
        "input_tokens": True, "cache_read_input_tokens": 71207,
        "cache_creation_input_tokens": 0, "output_tokens": 5})))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207


def test_negative_token_field_not_counted(tmp_path):
    """codex MINOR1:负数字段不能计入(会造出可信外观的错值)。
    input_tokens=71207 + cache_read=-70000 → 负数排除 → tokens=71207(而非 1207→🧠1K)。"""
    tp = _write(tmp_path, _line(_rec(usage={
        "input_tokens": 71207, "cache_read_input_tokens": -70000,
        "cache_creation_input_tokens": 0, "output_tokens": 5})))
    meter = ctxmeter.read_turn_meter(tp)
    assert meter["tokens"] == 71207


# ---------------------------------------------------------------- 超限 token
def test_over_limit_tokens_empty_footer():
    """plan(r3-m2):tok > _MAX_TOKENS(100M)→ footer ''(异常值省略,K 不出天量位)。"""
    assert ctxmeter.format_footer(
        {"tokens": 10 ** 308, "model": "claude-opus-4-8"}, effort="max") == ""
    assert ctxmeter.format_footer({"tokens": ctxmeter._MAX_TOKENS + 1, "model": None}) == ""
    # 边界:恰 _MAX_TOKENS 仍出页脚
    edge = ctxmeter.format_footer({"tokens": ctxmeter._MAX_TOKENS, "model": None})
    assert edge.startswith("\n" + SEP12 + "\n🧠 ")


# ---------------------------------------------------------------- pretty_model
def test_pretty_model_none_and_synthetic():
    assert ctxmeter.pretty_model(None) is None
    assert ctxmeter.pretty_model("") is None
    assert ctxmeter.pretty_model("<synthetic>") is None
    assert ctxmeter.pretty_model(123) is None


def test_unknown_model_strips_prefix_and_caps():
    """plan:命中 _NAMES → 映射;haiku 前缀 → 'Haiku 4.5';未知 → 去 claude- 前缀、≤32 截断。"""
    assert ctxmeter.pretty_model("claude-opus-4-8") == "Opus 4.8"
    assert ctxmeter.pretty_model("claude-sonnet-5") == "Sonnet 5"
    assert ctxmeter.pretty_model("claude-fable-5") == "Fable 5"
    assert ctxmeter.pretty_model("claude-haiku-4-5-20260101") == "Haiku 4.5"
    assert ctxmeter.pretty_model("claude-experimental-x") == "experimental-x"
    assert ctxmeter.pretty_model("gpt-9") == "gpt-9"       # 无 claude- 前缀:原样
    long = "claude-" + "z" * 60
    assert ctxmeter.pretty_model(long) == "z" * 32
    assert len(ctxmeter.pretty_model(long)) == 32


def test_pathological_long_model_effort_capped(tmp_path):
    """plan:病态超长 model/effort → 段级 cap 截断(≤32/≤16),footer 仍小、不阻断。"""
    tp = _write(tmp_path, _line(_rec(model="claude-" + "m" * 100, usage=_usage(inp=71207))))
    footer = ctxmeter.footer_for({"transcript_path": tp, "effort": "e" * 100})
    assert footer
    body = footer.split("\n")[-1]                  # "🧠 71K · <model> · <effort>"
    segs = body.split(" " + DOT + " ")
    assert segs[0] == "🧠 71K"
    assert segs[1] == "m" * 32 and len(segs[1]) == 32
    assert segs[2] == "e" * 16 and len(segs[2]) == 16
    assert len(footer) < 120


# ---------------------------------------------------------------- _sanitize / footer 安全构造(codex BLOCKER)
def test_sanitize_strips_dangerous_chars():
    """codex BLOCKER:_sanitize 剔除破坏入库(SQLite UTF-8)/argv(NUL)/版式(换行)的字符:
    控制符 ord<0x20(含 \\n\\r\\t\\0)、DEL 0x7F、孤立 surrogate D800–DFFF;正常字符保留。"""
    assert ctxmeter._sanitize("a\nb\r\tc") == "abc"          # 换行/回车/制表
    assert ctxmeter._sanitize("x\x00y") == "xy"               # NUL
    assert ctxmeter._sanitize("p\x7fq") == "pq"               # DEL
    assert ctxmeter._sanitize("m\ud800n") == "mn"             # 孤立 surrogate 起点
    assert ctxmeter._sanitize("m\udc00n") == "mn"             # 中段(codex r2 MINOR3)
    assert ctxmeter._sanitize("m\udfffn") == "mn"             # 终点(codex r2 MINOR3)
    assert ctxmeter._sanitize("\x01\x1f") == ""               # 其它 C0 控制符
    # codex r2 MINOR1:Unicode 语义换行 NEL/LS/PS(会被 splitlines 拆行)也必须剔除
    assert ctxmeter._sanitize("a\x85b") == "ab"               # U+0085 NEL
    assert ctxmeter._sanitize("a\u2028b") == "ab"             # U+2028 LS
    assert ctxmeter._sanitize("a\u2029b") == "ab"             # U+2029 PS
    assert ctxmeter._sanitize("Opus 4.8") == "Opus 4.8"       # 正常保留(空格/点/数字)
    # 不变量:sanitize 后 UTF-8 可编码 且 splitlines 不再拆行(单行)
    dirty = "a\ud800\x00\nb\x7f\x85c\u2028\u2029"
    clean = ctxmeter._sanitize(dirty)
    clean.encode("utf-8")
    assert len(clean.splitlines()) <= 1


def test_format_footer_sanitizes_model_and_effort():
    """codex BLOCKER + r2 MINOR1:脏 model/effort(surrogate/NUL/\\n 及 Unicode 换行 NEL/LS/PS)
    → 输出干净、UTF-8 可编码、无 NUL、无任何(含 Unicode 语义)换行注入。"""
    f = ctxmeter.format_footer(
        {"tokens": 71207, "model": "opus\ud800\x00\n\u2028"}, effort="ma\nx\x00\u2029\x85")
    f.encode("utf-8")                                          # 不抛 = 无孤立 surrogate
    assert "\x00" not in f
    for ch in ("\ud800", "\x85", "\u2028", "\u2029"):
        assert ch not in f
    assert len(f.splitlines()) == 3                            # 空行+分隔线+页脚,无注入行
    assert f == "\n" + SEP12 + "\n🧠 71K " + DOT + " opus " + DOT + " max"


def test_format_footer_drops_segment_emptied_by_sanitize():
    """codex BLOCKER:某段 sanitize 后为空 → 丢该段(不留空段/多余分隔符)。"""
    # model 全是危险字符 → 清空 → 丢 model 段(只剩 🧠 + effort)
    f = ctxmeter.format_footer({"tokens": 71207, "model": "\ud800\x00\n"}, effort="max")
    assert f == "\n" + SEP12 + "\n🧠 71K " + DOT + " max"
    # effort 全危险 → 丢 effort 段
    f2 = ctxmeter.format_footer({"tokens": 71207, "model": "claude-opus-4-8"}, effort="\x00\n")
    assert f2 == "\n" + SEP12 + "\n🧠 71K " + DOT + " Opus 4.8"


# ---------------------------------------------------------------- format_footer 兜底 / footer_for total
def test_format_footer_falsy_and_bad_tokens():
    """plan:meter 假 / tokens 非正 int → ''。"""
    assert ctxmeter.format_footer(None) == ""
    assert ctxmeter.format_footer({}) == ""
    assert ctxmeter.format_footer({"tokens": 0, "model": None}) == ""
    assert ctxmeter.format_footer({"tokens": -5, "model": None}) == ""
    assert ctxmeter.format_footer({"tokens": "71207", "model": None}) == ""   # 非 int


def test_footer_for_total_on_garbage():
    """plan:footer_for total,绝不抛(payload 非 dict / 缺键均 '')。"""
    assert ctxmeter.footer_for(None) == ""
    assert ctxmeter.footer_for("nope") == ""
    assert ctxmeter.footer_for(123) == ""
