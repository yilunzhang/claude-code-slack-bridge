"""进程探测:ps ppid 链(F8)+ (pid,lstart) 存活判定。探测 UNKNOWN 一律按存活处理(4.4)。"""
import subprocess

ALIVE, DEAD, UNKNOWN = "alive", "dead", "unknown"


class SystemProber:
    """get(pid) → (ppid:int, lstart:str, comm:str) | None(进程不存在);ps 本身失败 → raise。
    lstart 规整为单空格 join(ps 对个位日会双空格)。"""

    def get(self, pid):
        res = subprocess.run(
            ["ps", "-o", "ppid=,lstart=,comm=", "-p", str(int(pid))],
            capture_output=True, text=True, timeout=5)
        out = res.stdout.strip()
        if res.returncode != 0 or not out:
            return None
        toks = out.split()
        if len(toks) < 7:
            raise RuntimeError(f"unparseable ps output: {out!r}")
        ppid = int(toks[0])
        lstart = " ".join(toks[1:6])
        comm = " ".join(toks[6:])
        return (ppid, lstart, comm)


def self_identity(prober, pid):
    """(pid, lstart) 键;失败 → None。"""
    try:
        info = prober.get(pid)
    except Exception:
        return None
    if info is None:
        return None
    return (pid, info[1])


def probe_alive(prober, pid, lstart):
    """确定为死才返回 DEAD;探测异常 → UNKNOWN(按存活处理)。"""
    if pid is None:
        return UNKNOWN
    try:
        info = prober.get(pid)
    except Exception:
        return UNKNOWN
    if info is None:
        return DEAD
    return ALIVE if info[1] == lstart else DEAD  # lstart 不同 = pid 复用 = 原进程确定已死


def find_cc_instance(prober, start_pid):
    """自 start_pid 沿 ppid 链上溯,comm=='claude' 即 CC 实例;键=(pid, lstart)。失败 → None。"""
    pid = start_pid
    for _ in range(15):
        try:
            info = prober.get(pid)
        except Exception:
            return None
        if info is None:
            return None
        ppid, lstart, comm = info
        if comm.rsplit("/", 1)[-1] == "claude":
            return (pid, lstart)
        if not ppid or ppid <= 1 or ppid == pid:
            return None
        pid = ppid
    return None
