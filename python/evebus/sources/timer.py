"""
TimerSource — 定时事件源

定期发射事件，用于心跳、定时任务等。

用法:

    from evebus.sources import TimerSource

    timer = TimerSource(
        name="heartbeat",
        topic="system.heartbeat",
        interval_ms=5000,  # 5秒
        payload={"status": "ok"}
    )
    engine.add_source(timer)
"""

import asyncio
from .base import EventSource


class TimerSource(EventSource):
    """定时事件源"""

    def __init__(
        self,
        name: str = "timer",
        topic: str = "timer.tick",
        interval_ms: int = 1000,
        payload: dict = None,
    ):
        super().__init__(name)
        self.topic = topic
        self.interval_ms = interval_ms
        self.payload = payload or {}
        self._count = 0

    async def start(self):
        """启动定时器"""
        self._running = True
        self._count = 0
        while self._running:
            await asyncio.sleep(self.interval_ms / 1000.0)
            if not self._running:
                break
            self._count += 1
            await self.emit(self.topic, {
                **self.payload,
                "tick": self._count,
                "interval_ms": self.interval_ms,
            })

    def info(self) -> dict:
        base = super().info()
        base.update({
            "topic": self.topic,
            "interval_ms": self.interval_ms,
            "tick_count": self._count,
        })
        return base