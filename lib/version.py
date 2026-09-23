"""plugin 版本 / 代码身份(plugin 化)。
- plugin_version:读 .claude-plugin/plugin.json 的 version。
- code_identity = pkg_root(resolve 后绝对路径) + plugin_version。
  用途:①心跳记录哪个 install/版本跑的 hook(MAJOR 2 防假阳性);
        ②daemon 启动时发布,CLI **bind 前置串行检查**比对,检测「更新/迁移后复用跑旧代码的旧 daemon」(MAJOR 3)。
  **删掉 git 段(coderev):** git best-effort 探测失败得空串会被当"确定不匹配"误杀健康 daemon;
  `git -C <pkg_root>` 会向父目录找仓库(marketplace cache 若在某 git 目录内,父仓库 HEAD 变就无谓重启);
  --short 不稳定。pkg_root|version 足够抓 plugin 更新主场景(换根/换 version)。dev 同位置改代码不换
  version → 检测不到,dev 自己手动重启(README 说明)。
"""
import json

from . import paths


def plugin_version():
    try:
        pj = json.loads((paths.pkg_root() / ".claude-plugin" / "plugin.json").read_text())
        v = pj.get("version")
        return str(v) if v else "unknown"
    except Exception:
        return "unknown"


def install_identity():
    """心跳用:pkg_root(resolve 后)+ plugin_version。(str, str)。
    注:paths.pkg_root() 已 resolve(消符号链接);macOS 大小写别名极端情形若担心可再 casefold,
    个人轻量应用场景不额外归一(文档标注即可)。"""
    return (str(paths.pkg_root()), plugin_version())


def code_identity_str():
    """daemon 版本检测用:pkg_root|plugin_version(无 git 段)。"""
    root, ver = install_identity()
    return f"{root}|{ver}"
