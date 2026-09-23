"""本机桌面通知(macOS 通知中心)—— **不经飞书**的旁路告警。

**为什么需要它**:出站身份门降级时,飞书出站(含 notify skill / StopFailure 告警)**全部被堵**
—— 报警器与被报警物共用一条线。2026-07-30 真实事故:lark-cli 被升级导致门降级,出站静默停摆,
owner 只能靠"怎么没转发了"察觉。故告警必须走一条**不依赖飞书**的通道。

**已实测(2026-07-30)**:`osascript display notification` 在 **detached、无 TTY 的后台进程**
里 rc=0 可弹 —— daemon 正是此形态。

**🔑 rc=0 ≠ 用户看见**:通知归属于 `osascript` 这个 app,权限/专注模式挂在它身上;被关时
rc=0 照返回而屏幕上什么都没有。**故本模块的返回值只表示"已投递给系统",绝不可当作"已送达
用户"**;验收必须由人肉眼确认通知中心。

**fail-open**:任何异常/超时都吞掉并返回 False —— 通知是旁路装饰,绝不能影响门逻辑或炸掉
daemon 循环。
"""
import subprocess

# osascript 本身很轻;给个小超时防它卡住 daemon 循环(门逻辑不等它)
_TIMEOUT_S = 10


def notify(title, message, subtitle=None):
    """弹一条系统通知 → True=已投递给系统(**非**已送达用户);False=没投出去。

    文案经 **`run argv` 传参**而非拼进 AppleScript 源:版本号等字段来自外部可执行文件的输出,
    含引号/反斜杠时拼串会让通知**静默失败**(codex plan r1 Low)。
    """
    script = (
        'on run argv\n'
        '  set t to item 1 of argv\n'
        '  set m to item 2 of argv\n'
        '  set s to item 3 of argv\n'
        '  if s is "" then\n'
        '    display notification m with title t\n'
        '  else\n'
        '    display notification m with title t subtitle s\n'
        '  end if\n'
        'end run'
    )
    try:
        res = subprocess.run(
            ["osascript", "-", str(title), str(message), str(subtitle or "")],
            input=script, capture_output=True, text=True, timeout=_TIMEOUT_S)
        return res.returncode == 0
    except Exception:      # noqa: BLE001 —— 含 FileNotFoundError(非 macOS)/超时;一律 fail-open
        return False
