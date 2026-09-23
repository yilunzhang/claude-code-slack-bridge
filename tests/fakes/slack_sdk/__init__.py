"""脚本化假 `slack_sdk`(离线;仅 bin/slack_consumer.py 的子进程测试用)。

把 `tests/fakes` 放到 sys.path **最前**(子进程用 PYTHONPATH)即以 `slack_sdk` 名字被导入,
形状对齐真 sdk 3.x 的 builtin SocketModeClient / WebClient / SocketModeRequest / SocketModeResponse /
errors.SlackApiError(consumer 只用到这些)。

脚本:env `FAKE_SLACK_SCRIPT` = JSON 列表,元素为**帧**(dict,原样 JSON 化后走 `_on_message` →
enqueue → process → 监听器,和真 sdk 同路径)或**控制项** `{"__control": …}`:
  {"__control":"connect_error","error":"invalid_auth","status":200}  connect() 时 apps.connections.open 报错
  {"__control":"connect_exception","kind":"network"}                  connect() 时抛 SlackClientError(网络)
  {"__control":"sleep","seconds":0.2}
  {"__control":"close","code":1006,"reason":"x","reconnect":false,"reconnect_after":0.0}
      关闭底层连接(is_connected → False,触发 on_close_listeners);reconnect=true 时稍后重连并补发 hello
  {"__control":"done"}   等队列与线程池排空后写 {"__event":"script_done"}(测试据此关 stdin)
  {"__control":"sdk_log","level":"warning","logger":"slack_sdk.socket_mode.builtin.client","text":"… {app_token} …"}
      模拟 sdk 自身 logger 打日志({app_token} 替换为真实 app token;R2-M5 遮蔽 handler 的靶子)
日志:env `FAKE_SLACK_ACK_LOG` 追加 JSON 行:每次 `send_message` 的原文(ack = {"envelope_id": …})
以及生命周期事件 {"__event": ctor|connect|reconnect|close|script_done, …}。**绝不记录 token 本身**。"""
from .errors import SlackApiError, SlackClientError, SlackRequestError  # noqa: F401
from .web import WebClient  # noqa: F401

__version__ = "0.0-fake"
