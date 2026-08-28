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
import logging
from .base import EventSource

logger = logging.getLogger("evebus.sources")


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
        # #4: interval_ms 必须为正，避免 busy-loop
        if interval_ms <= 0:
            raise ValueError(f"interval_ms must be positive, got {interval_ms}")
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
            try:
                await self.emit(self.topic, {
                    **self.payload,
                    "tick": self._count,
                    "interval_ms": self.interval_ms,
                })
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # #5: emit 失败记录日志继续循环，不静默终止定时器
                logger.error("TimerSource '%s' emit failed on tick %d: %s",
                             self.name, self._count, e)

    def info(self) -> dict:
        base = super().info()
        base.update({
            "topic": self.topic,
            "interval_ms": self.interval_ms,
            "tick_count": self._count,
        })
        return base