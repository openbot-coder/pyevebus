"""
EventSource 基类 — 事件产生端

事件源负责产生事件并发射到 EventEngine。

用法:

    from evebus.sources import EventSource

    class MySource(EventSource):
        def __init__(self, name="my_source"):
            super().__init__(name)

        async def start(self):
            self._running = True
            while self._running:
                await self.emit("my.topic", {"data": "..."})
                await asyncio.sleep(1)

        async def stop(self):
            self._running = False
"""

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..engine import EventEngine


class EventSource:
    """事件源基类 — 实现 start/stop 产生事件"""

    def __init__(self, name: str):
        self.name = name
        self._engine: "EventEngine" = None  # type: ignore
        self._running = False
        self._task: asyncio.Task = None  # type: ignore

    @property
    def engine(self) -> "EventEngine":
        if self._engine is None:
            raise RuntimeError(f"Source '{self.name}' not attached to any engine")
        return self._engine

    @property
    def running(self) -> bool:
        return self._running

    def _attach(self, engine: "EventEngine"):
        """内部：挂载到引擎"""
        self._engine = engine

    def _detach(self):
        """内部：从引擎卸载"""
        self._engine = None
        self._running = False

    async def start(self):
        """启动事件源（子类实现）"""
        raise NotImplementedError(
            f"Source '{self.name}' must implement start()"
        )

    async def stop(self):
        """停止事件源"""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None  # type: ignore

    async def run(self):
        """后台运行 — 启动 start() 并跟踪任务"""
        self._running = True
        self._task = asyncio.create_task(self.start())

    async def emit(self, topic: str, payload: Any = None):
        """发射事件到引擎"""
        if self._engine is None:
            raise RuntimeError(f"Source '{self.name}' not attached")
        await self._engine.emit(topic, payload, source=self.name)

    def info(self) -> dict:
        """源信息"""
        return {
            "name": self.name,
            "type": type(self).__name__,
            "running": self._running,
        }