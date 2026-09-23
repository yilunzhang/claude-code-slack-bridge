"""时间:持久值一律墙钟 ms;进程内间隔判定用单调钟(时钟回拨防护)。"""
import time


class SystemClock:
    def wall_ms(self):
        return int(time.time() * 1000)

    def mono_ms(self):
        return int(time.monotonic() * 1000)
