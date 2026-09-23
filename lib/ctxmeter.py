"""每轮转发页脚的上下文计量(footer-plan v3 §改动面1)。

职责:从 Stop payload + CC transcript 尾部,尽力算出 "🧠 <K> · <模型> · <effort>" 页脚串。
纯函数,唯一 IO = 只读 transcript 尾部(≤256KiB)。footer_for 是 hooklib 唯一入口、total(绝不抛)。

关键假设(与 footer-plan v3 一致,变更时回看此处):
  个人轻量工具、单用户、群内可信人员;页脚 = best-effort 装饰,取不到/任何异常 → 省略、原样转发。
  tokens/model 来自 transcript(官方异步写、可能滞后约一 turn = 已知限制);effort 只走 payload(当前值)。

3.9 兼容:首行 from __future__ import annotations(惰性注解 → 3.9 导入不 eval union);无运行期 X|Y。
"""
from __future__ import annotations

import json
import math

SEP = "─" * 12            # 分隔线 = 12×U+2500(字面横线,非 markdown hr)
_TAIL = 262144                 # 256KiB 尾读上限(实测最大单条 assistant JSONL<130KB)
_MODEL_CAP = 32                # model 段最大字符
_EFFORT_CAP = 16               # effort 段最大字符
_MAX_TOKENS = 100_000_000      # token 合理性上限(真实 window≤1M,100M=100x 余量);超则视为异常、省略页脚

# 已知 model id → 展示名(精确匹配);haiku 走前缀匹配(见 pretty_model),未知走去前缀+截断
_NAMES = {
    "claude-opus-4-8": "Opus 4.8",
    "claude-sonnet-5": "Sonnet 5",
    "claude-fable-5": "Fable 5",
}


def pretty_model(mid):
    """model id → 展示名;非 str/空/<synthetic> → None。"""
    if not isinstance(mid, str) or not mid or mid == "<synthetic>":
        return None
    if mid in _NAMES:
        return _NAMES[mid]
    if mid.startswith("claude-haiku-4-5"):
        return "Haiku 4.5"
    if mid.startswith("claude-"):
        mid = mid[len("claude-"):]
    return mid[:_MODEL_CAP]


def extract_effort(payload):
    """effort 只认 payload 域(不回退 transcript);只认 str 或 {'level': str}。
    不猜其它 key、不兜底取"第一个字符串值"(避免把未来新增字段误当 effort)。取不到 → None。"""
    if not isinstance(payload, dict):
        return None
    e = payload.get("effort")
    if isinstance(e, str):
        return e.strip() or None
    if isinstance(e, dict):
        lvl = e.get("level")
        if isinstance(lvl, str):
            return lvl.strip() or None
        return None
    return None


def _read_tail(path):
    """读文件末尾 ≤_TAIL 原始字节(严格 read(_TAIL);期间异步追加的数据留到下一轮 = 符合滞后策略),
    按 b'\\n' 切、逐行**严格** decode('utf-8'):解码失败的行(损坏字节 / 尾部截断的多字节序列)整行丢弃。
    绝不用 errors='ignore'——那会把非法字节删掉、可能把损坏记录"修复"成可信假值(如 9\\xff9999→99999),
    违反"取不到就省略"(codex r2 MINOR2)。返回成功解码的行列表(顺序保留)。
    注:U+2028/2029/0085 在 UTF-8 里是多字节(如 U+2028=\\xe2\\x80\\xa8),不含 0x0A → 不被 b'\\n' 切,
    仍作一行进 json.loads;其值里若有这些字符,由 _sanitize 在页脚层剔除,两处一致。"""
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(0, size - _TAIL))
        raw = f.read(_TAIL)
    lines = []
    for bline in raw.split(b"\n"):
        try:
            lines.append(bline.decode("utf-8"))   # 严格:损坏字节 → UnicodeDecodeError → 丢该行
        except UnicodeDecodeError:
            continue
    return lines


def read_turn_meter(transcript_path):
    """逆序扫 transcript 尾部,定位**最新一条 assistant turn 记录**并返回其 {tokens, model}(不含 effort)。
    权威屏障(codex r3 MAJOR + r5 换框架)——先判定"这条记录是不是 assistant turn"(而非等 message.role 检查):
      is_assistant_turn = **纯外层 type=='assistant'**(不做 message.role 回退)。
    判定成立即**权威**(除 synthetic 外),不可用就 return None、**绝不回退到更旧记录**——否则会把
    某条损坏的 assistant turn(如 message==null)跳过、用任意滞后的旧 tokens/model 拼当前 effort =
    可信外观的错值。具体:
      - synthetic 标记记录(带 usage!)非真实 turn → continue(跳到真实 turn);
      - message 损坏(非 dict,如 null)/ role 损坏(≠assistant)/ usage 非 dict / tok<=0 → return None;
      - usage 是 dict 且 tok>0 → 返回 {tokens, model}。
    非 assistant turn(type≠assistant:user/system/tool/缺type/type=null)与 json 失败行(半写/垃圾)→
    continue(透明跳过找下一条)。这样滞后被限死在"最近一个 assistant turn"(≈已接受的 ~1 turn)。遍历尽 → None。
    **假设**:footer 以外层 type=='assistant' 标识 assistant turn(已核实 CC 2.1.x 所有 assistant 记录都有
    type=='assistant')。**故意不做 role 回退**——role 回退曾引入"显式 type=user/null 被损坏 role 劫持"的
    错值边界(codex r4/r5);缺 type 的假想版本下 footer 省略 = 可接受的 fail-safe(best-effort 装饰,不为
    推测性兼容再冒错值风险)。IO 异常向上冒泡(由 footer_for 的 total 包装兜住)。"""
    if not transcript_path:
        return None
    # _read_tail 已逐行严格解码(丢损坏字节行);逆序找最近记录
    for line in reversed(_read_tail(transcript_path)):
        line = line.strip()
        if not line:
            continue
        try:
            o = json.loads(line)
        except Exception:
            continue                      # 半写/截断/垃圾行 → 非记录,继续逆序
        if not isinstance(o, dict):
            continue
        m = o.get("message")
        # assistant turn 判定 = **纯外层 type=='assistant'**(codex r5:删掉 role 回退换框架)。
        # 已核实 CC 2.1.x 所有 assistant 记录都带 type=='assistant';**故意不做 message.role 回退**——
        # role 回退曾连续两轮引入"显式非 assistant 记录被损坏 role 劫持"的错值边界(codex r4:type=='user'
        # 被 OR 劫持;r5:type==null 被"null 当缺失、回退 role"劫持)。纯 type 一次消掉两类洞。
        # 缺 type 的假想 CC 版本下页脚省略 = 可接受的 fail-safe(footer 是 best-effort 装饰,不值得为推测性
        # 兼容再冒错值风险;YAGNI)。
        if o.get("type") != "assistant":
            continue                      # 非 assistant turn(user/system/tool/缺type/type=null)→ 透明跳过
        # —— 到此 = 最新 assistant turn 记录 = 权威;除 synthetic 外不可用即 None,绝不回退更旧 ——
        if isinstance(m, dict) and m.get("model") == "<synthetic>":
            continue                      # synthetic 标记(带 usage!)非真实 turn → 跳到下面真实 turn
        if not isinstance(m, dict):
            return None                   # 外层 assistant 但 message 损坏(如 null)→ 省略,不回退(MAJOR)
        if m.get("role") != "assistant":
            return None                   # role 损坏 → 省略,不回退(MAJOR)
        usage = m.get("usage")
        if not isinstance(usage, dict):
            return None                   # usage 不可用 → 省略,不回退
        tok = 0
        for key in ("input_tokens", "cache_read_input_tokens",
                    "cache_creation_input_tokens"):
            v = usage.get(key)
            # 逐字段硬化:仅"非 bool 的有限非负 int/float"才累加。
            #   not bool:True 是 int 子类,input_tokens:true 原会 +1 造 🧠1K(codex MINOR1);
            #   isfinite:挡 nan/inf 免 int() 抛错;v>=0:挡负数造可信外观的错值(codex MINOR1);
            #   str/None:isinstance 已挡。
            if (isinstance(v, (int, float)) and not isinstance(v, bool)
                    and math.isfinite(v) and v >= 0):
                tok += v
        tok = int(tok)                    # 归一为 int(某字段 1.0 也不让总和变 float 被下游静默吞)
        if tok <= 0:
            return None                   # 权威记录 tok<=0(不可用)→ 省略页脚,不回退(MAJOR)
        return {"tokens": tok, "model": m.get("model")}
    return None


_LINEISH = frozenset((0x85, 0x2028, 0x2029))   # NEL / LS / PS:被 str.splitlines() 当换行拆行


def _sanitize(s):
    """剔除会破坏下游的字符,让页脚段按构造安全(codex BLOCKER + r2 MINOR1;fail-open 真闭合的关键):
      控制符 ord<0x20(含 \\n\\r\\t\\0)、DEL 0x7F、**Unicode 语义换行 NEL/LS/PS(0x85/0x2028/0x2029)**、
      孤立 surrogate D800–DFFF。
    结果不变量:永远 UTF-8 可编码(SQLite 入库安全)、无 NUL(daemon Popen argv 安全)、
    splitlines() 不再拆行(不注入额外行 = 版式安全;仅剔 \\n\\r 不够——U+2028 等也会被 splitlines 拆)。
    model/effort 来自可能损坏的官方元数据 → 必须过此关。
    (注:页脚只有 model/effort 两段来自外部数据;'🧠 <K>' 段由 int 构造、天然安全。)"""
    if not isinstance(s, str):
        return ""
    return "".join(
        c for c in s
        if not (ord(c) < 0x20 or ord(c) == 0x7F
                or ord(c) in _LINEISH or 0xD800 <= ord(c) <= 0xDFFF))


def format_footer(meter, effort=None):
    """meter({tokens, model}) + effort(str|None)→ 页脚串;取不到/异常值 → ''。
    footer 尺寸按构造有界(K≤9 位 + SEP12 + model≤32 + effort≤16 ≈ ≤80 字符)→ 无需最终 cap。
    model/effort 段过 _sanitize(某段清空则丢该段)→ 页脚永不破坏入库/argv/版式(fail-open 真闭合)。"""
    if not meter:
        return ""
    tok = meter.get("tokens")
    if not isinstance(tok, int) or tok <= 0 or tok > _MAX_TOKENS:
        return ""                         # 缺失/非正/非 int/超限(异常值)→ 省略页脚
    k = max(1, round(tok / 1000))         # 只显 K;max(1,..) → 无 "0K"
    segs = ["🧠 {0}K".format(k)]
    pm = pretty_model(meter.get("model"))
    if pm:
        pm = _sanitize(pm)                # pretty_model 之后再净化;清空则丢该段
        if pm:
            segs.append(pm)
    if isinstance(effort, str):           # effort 仅 payload 来源,不回退 transcript
        eff = _sanitize(effort)[:_EFFORT_CAP]   # 先净化再截长;清空则丢该段
        if eff:
            segs.append(eff)
    return "\n" + SEP + "\n" + " · ".join(segs)   # 段间 " · "(U+00B7)


def footer_for(payload):
    """hooklib 唯一入口:total,绝不抛。取不到/任何异常 → ''(fail-open)。"""
    try:
        tp = payload.get("transcript_path")
        eff = extract_effort(payload)
        return format_footer(read_turn_meter(tp), effort=eff)
    except Exception:
        return ""
