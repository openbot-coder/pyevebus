"""
Plugin 基类 — 插件系统

插件通过 on_attach/on_detach 生命周期挂载到 EventEngine。

用法:

    from evebus import EventEngine, Plugin

    class MyPlugin(Plugin):
        def __init__(self):
            super().__init__("my_plugin")

        def on_attach(self):
            @self.on("data.quotes.*")
            async def on_quote(topic, event):
                print(f"Plugin: {topic} = {event}")

        def on_detach(self):
            print("Plugin detached")

    engine = EventEngine()
    engine.add_plugin(MyPlugin())
"""

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .engine import EventEngine


class Plugin:
    """插件基类"""

    def __init__(self, name: str):
        self.name = name
        self._engine: "EventEngine" = None  # type: ignore

    @property
    def engine(self) -> "EventEngine":
        if self._engine is None:
            raise RuntimeError(f"Plugin '{self.name}' not attached to any engine")
        return self._engine

    def _attach(self, engine: "EventEngine"):
        """内部：挂载到引擎"""
        self._engine = engine

    def _detach(self):
        """内部：从引擎卸载"""
        self._engine = None

    def on_attach(self):
        """插件加载时调用 — 在这里注册 handler/hook"""
        pass

    def on_detach(self):
        """插件卸载时调用 — 在这里清理资源"""
        pass

    def on(self, pattern: str, handler: Callable = None):
        """快捷方法 — 委托给 engine.on"""
        return self.engine.on(pattern, handler)

    def once(self, pattern: str, handler: Callable = None):
        """快捷方法 — 委托给 engine.once"""
        return self.engine.once(pattern, handler)

    def off(self, pattern: str, handler: Callable = None):
        """快捷方法 — 委托给 engine.off"""
        return self.engine.off(pattern, handler)

    async def emit(self, topic: str, event: Any = None):
        """快捷方法 — 委托给 engine.emit"""
        return await self.engine.emit(topic, event)