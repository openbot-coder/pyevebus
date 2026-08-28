"""
EventExecutor 基类 — 事件处理端

执行器负责接收匹配的事件并执行处理逻辑。

用法:

    from evebus.executors import EventExecutor

    class MyExecutor(EventExecutor):
        def __init__(self):
            super().__init__("my_executor")
            self.patterns = ["data.*"]

        async def execute(self, topic: str, payload: dict):
            print(f"处理: {topic}")
"""

import logging
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from ..engine import EventEngine

logger = logging.getLogger("evebus.executors")


class EventExecutor:
    """执行器基类 — 接收匹配的事件并处理"""

    def __init__(self, name: str, patterns: List[str] = None):
        self.name = name
        self.patterns = patterns or ["*"]  # 订阅的 patterns
        self._engine: "EventEngine" = None  # type: ignore
        self._attached = False
        self._executed_count = 0
        self._error_count = 0

    @property
    def engine(self) -> "EventEngine":
        if self._engine is None:
            raise RuntimeError(f"Executor '{self.name}' not attached")
        return self._engine

    def _attach(self, engine: "EventEngine"):
        """内部：挂载到引擎并注册 handler"""
        self._engine = engine
        self._attached = True
        # 注册到 engine 的 executor registry
        for pattern in self.patterns:
            engine._executor_handlers.setdefault(pattern, []).append(self)
            engine._router.subscribe(pattern, f"executor:{self.name}:{id(self)}")

    def _detach(self):
        """内部：从引擎卸载（#19: 同时从 router 卸载订阅）"""
        if self._engine:
            for pattern in self.patterns:
                if pattern in self._engine._executor_handlers:
                    self._engine._executor_handlers[pattern] = [
                        e for e in self._engine._executor_handlers[pattern]
                        if e is not self
                    ]
                # 从 router 卸载该 executor 的订阅
                try:
                    self._engine._router.unsubscribe(
                        pattern, f"executor:{self.name}:{id(self)}")
                except Exception:
                    pass
        self._engine = None
        self._attached = False

    async def execute(self, topic: str, payload: dict):
        """
        处理事件（子类实现）

        Args:
            topic: 事件 topic
            payload: 事件 payload
        """
        raise NotImplementedError(
            f"Executor '{self.name}' must implement execute()"
        )

    async def _safe_execute(self, topic: str, payload: dict):
        """安全执行 — 捕获异常，记录错误"""
        try:
            self._executed_count += 1
            await self.execute(topic, payload)
        except Exception as e:
            self._error_count += 1
            logger.error("Executor '%s' failed on %s: %s", self.name, topic, e)
            # #20: 发射错误事件（若引擎存在）
            if self._engine:
                try:
                    await self._engine.emit("error", {
                        "executor": self.name,
                        "topic": topic,
                        "error": str(e),
                    }, source=f"executor:{self.name}")
                except Exception as emit_err:
                    # 错误事件自身失败不递归，仅记录
                    logger.error(
                        "Executor '%s' error-event emit failed: %s",
                        self.name, emit_err,
                    )

    def info(self) -> dict:
        return {
            "name": self.name,
            "type": type(self).__name__,
            "patterns": self.patterns,
            "attached": self._attached,
            "executed_count": self._executed_count,
            "error_count": self._error_count,
        }