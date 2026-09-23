"""lark-cli 子进程底座(I4):argv 无 shell、显式 --profile、超时+进程组 SIGTERM、
并发排空 stdout/stderr(communicate 双管道排空,POSIX 下为 selector 单线程实现)。REST 契约:以解析出预期字段为准,缺=UNKNOWN。"""
import json
import os
import signal
import subprocess

from . import constants


class RunResult:
    __slots__ = ("rc", "stdout", "stderr", "timed_out", "exc")

    def __init__(self, rc=0, stdout="", stderr="", timed_out=False, exc=None):
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.exc = exc


class LarkRunner:
    """一次性 REST 调用。event consume 常驻消费不走这里(见 daemon_core.ConsumerManager)。
    E1(真机实锤):顶层元命令 `--version` 不吃全局 --profile(拖尾 → unknown command rc=2)
    → 用 no_profile=True 走裸 argv;其余 im/api/auth/event 子命令均接受全局 --profile(已审计)。"""

    def __init__(self, profile, lark_bin="lark-cli"):
        if not profile:
            raise ValueError("profile required (指纹钉死)")
        self.profile = profile
        self.lark_bin = lark_bin

    def build_argv(self, args, no_profile=False):
        argv = [self.lark_bin] + [str(a) for a in args]
        if not no_profile:
            argv += ["--profile", self.profile]
        return argv

    def run(self, args, timeout_s=constants.SEND_TIMEOUT_S, cwd=None, no_profile=False):
        argv = self.build_argv(args, no_profile=no_profile)
        try:
            p = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, cwd=cwd, start_new_session=True)
        except OSError as e:
            return RunResult(rc=-1, exc=e)
        try:
            out, err = p.communicate(timeout=timeout_s)
            return RunResult(rc=p.returncode, stdout=out or "", stderr=err or "")
        except subprocess.TimeoutExpired:
            _kill_group(p, sig=signal.SIGTERM)
            try:
                out, err = p.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                # REST 一次性调用无服务端订阅,末路 SIGKILL 防僵死(consume 进程绝不 -9)
                _kill_group(p, sig=signal.SIGKILL)
                try:
                    out, err = p.communicate(timeout=5)
                except Exception:
                    out, err = "", ""
            return RunResult(rc=p.returncode if p.returncode is not None else -1,
                             stdout=out or "", stderr=err or "", timed_out=True)


def _kill_group(p, sig):
    try:
        os.killpg(os.getpgid(p.pid), sig)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            p.send_signal(sig)
        except Exception:
            pass


def parse_envelope(stdout):
    """解析 lark-cli JSON 信封;容忍 `_notice` 附加键与前后噪声行。失败 → None。"""
    if not stdout:
        return None
    s = stdout.strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except ValueError:
        pass
    for line in s.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def parse_result(res):
    """E4a(真机实锤):lark-cli 出错时错误信封可能打在 **stderr**(stdout 空)。
    解析顺序:stdout → 空/不可解析时回退 stderr。"""
    env = parse_envelope(res.stdout)
    if env is None:
        env = parse_envelope(res.stderr)
    return env


def envelope_error_code(env):
    """错误码位置双形状:stdout 信封=顶层 `code`;stderr 错误信封=嵌套 `.error.code`。"""
    if not isinstance(env, dict):
        return None
    if env.get("code") is not None:
        return env.get("code")
    err = env.get("error")
    if isinstance(err, dict):
        return err.get("code")
    return None


def envelope_error_type(env):
    """错误类型位置双形状(与 envelope_error_code 同构):顶层 `type` 或嵌套 `.error.type`。
    notify 直发用它区分「本地 pre-API 失败(validation/config/authentication/authorization)=确定未发」
    与「network/api/unknown=发送结果不确定」。无=None。"""
    if not isinstance(env, dict):
        return None
    if env.get("type") is not None:
        return env.get("type")
    err = env.get("error")
    if isinstance(err, dict):
        return err.get("type")
    return None


def envelope_error_retryable(env):
    """`.error.retryable`(嵌套)或顶层 `retryable`;缺=None(未知)。lark-cli 官方稳定布尔字段,
    false 常省略。出站 session_turn 用它区分「可持久重试(network/5xx/频控)」与「不宜重试」。
    与 lib.notify._env_retryable 同构。"""
    if not isinstance(env, dict):
        return None
    err = env.get("error")
    if isinstance(err, dict) and "retryable" in err:
        return err.get("retryable")
    if "retryable" in env:
        return env.get("retryable")
    return None


def envelope_error_msg(env):
    if not isinstance(env, dict):
        return ""
    if env.get("msg"):
        return str(env.get("msg"))
    err = env.get("error")
    if isinstance(err, dict) and err.get("message"):
        return str(err.get("message"))
    return ""


def envelope_ok(env):
    return bool(env) and env.get("ok") is True


def data_of(env):
    d = (env or {}).get("data")
    return d if isinstance(d, dict) else {}
