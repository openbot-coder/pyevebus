"""
EventEngine — 异步事件引擎核心

参考 pyee.AsyncIOEventEmitter 设计，扩展：
- 通配符匹配（Rust 实现）
- Hook 系统（中间件）
- 插件系统（扩展）
- EventSource / EventExecutor 实时管理
"""

import asyncio
import functools
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

try:
    from ._ffi import PyRouter
except ImportError:
    PyRouter = None  # type: ignore

from .hooks import HookStage, HookResult, HookContext
from .sources.base import EventSource
from .executors.base import EventExecutor

if TYPE_CHECKING:
    from .plugin import Plugin


class EventEngine:
    """
    异步事件引擎

    核心 API（参考 pyee）:
      - on(pattern, handler)      注册 handler（支持装饰器）
      - once(pattern, handler)    一次性监听
      - off(pattern, handler)     移除 handler
      - emit(topic, event)        发射事件（async）
      - wait_for_complete()       等待所有 pending 协程
      - cancel()                  取消所有 pending

    实时管理:
      - add_source(source)        添加事件源
      - remove_source(name)       移除事件源
      - add_executor(executor)    添加执行器
      - remove_executor(name)     移除执行器
      - add_plugin(plugin)        添加插件
      - remove_plugin(name)       移除插件

    扩展:
      - add_hook(stage, hook)     注册 hook
    """

    def __init__(self):
        # Rust 路由器
        self._router: Any = PyRouter() if PyRouter is not None else _PurePythonRouter()

        # handler 存储（pattern → handler 列表）
        self._handlers: Dict[str, List[Callable]] = {}

        # 执行器存储（pattern → executor 列表）
        self._executor_handlers: Dict[str, List[EventExecutor]] = {}

        # hooks
        self._hooks: Dict[str, List[Callable]] = {}

        # 特殊事件
        self._new_listener_callbacks: List[Callable] = []

        # 源管理
        self._sources: Dict[str, EventSource] = {}

        # 执行器管理
        self._executors: Dict[str, EventExecutor] = {}

        # 插件管理
        self._plugins: Dict[str, "Plugin"] = {}

        # pending tasks
        self._waiting: Set[asyncio.Task] = set()

    # ══════════════════════════════════════════════
    #  Handler 注册（pyee 风格）
    # ══════════════════════════════════════════════

    def on(self, pattern: str, handler: Optional[Callable] = None):
        """注册 handler — 支持装饰器 + 直接调用"""
        if handler is None:
            return self._on_decorator(pattern)
        self._add_handler(pattern, handler)
        return handler

    def once(self, pattern: str, handler: Optional[Callable] = None):
        """一次性监听"""
        if handler is None:
            return self._once_decorator(pattern)
        wrapper = self._wrap_once(pattern, handler)
        self._add_handler(pattern, wrapper, is_once=True)
        return handler

    def off(self, pattern: str, handler: Optional[Callable] = None):
        """移除 handler"""
        if handler is None:
            self._handlers.pop(pattern, None)
            return
        self._remove_handler(pattern, handler)

    def listeners(self, pattern: str) -> List[Callable]:
        return list(self._handlers.get(pattern, []))

    @property
    def event_names(self) -> List[str]:
        return list(self._handlers.keys())

    # ══════════════════════════════════════════════
    #  发射事件
    # ══════════════════════════════════════════════

    async def emit(self, topic: str, event: Any = None, source: str = "") -> bool:
        """
        发射事件 — 异步分发到 handler 和 executor

        Returns:
            True 如果有 handler/executor 被调用
        """
        # 1. pre_emit hooks
        ctx = HookContext(topic=topic, payload=event, source=source)
        result = await self._run_hooks(HookStage.PRE_EMIT, ctx)
        if result == HookResult.INTERCEPTED:
            return False
        topic, event = ctx.topic, ctx.payload

        # 2. 匹配 patterns
        patterns = self._router.match_patterns(topic)

        # 3. 分发到 handler
        handled = False
        for pattern in patterns:
            # 3a. 函数 handler
            for handler in self._handlers.get(pattern, []):
                handled = True
                try:
                    result = handler(topic, event)
                    if asyncio.iscoroutine(result):
                        task = asyncio.ensure_future(result)
                        self._waiting.add(task)
                        task.add_done_callback(self._on_task_done)
                except Exception as exc:
                    await self.emit("error", {
                        "error": str(exc),
                        "handler": handler.__name__,
                        "topic": topic,
                    })

            # 3b. 执行器
            for executor in self._executor_handlers.get(pattern, []):
                handled = True
                task = asyncio.ensure_future(executor._safe_execute(topic, event))
                self._waiting.add(task)
                task.add_done_callback(self._on_task_done)

        # 4. post_emit hooks
        await self._run_hooks(HookStage.POST_EMIT, ctx)

        return handled

    # ══════════════════════════════════════════════
    #  异步管理
    # ══════════════════════════════════════════════

    async def wait_for_complete(self):
        """等待所有 pending 协程完成"""
        if self._waiting:
            await asyncio.gather(*self._waiting, return_exceptions=True)

    def cancel(self):
        """取消所有 pending"""
        for task in list(self._waiting):
            if not task.done():
                task.cancel()
        self._waiting.clear()

    @property
    def complete(self) -> bool:
        return not self._waiting

    # ══════════════════════════════════════════════
    #  Source 实时管理
    # ══════════════════════════════════════════════

    async def add_source(self, source: EventSource) -> dict:
        """添加事件源（实时）"""
        if source.name in self._sources:
            return {"ok": False, "error": f"Source '{source.name}' already exists"}
        source._attach(self)
        self._sources[source.name] = source
        # 启动 source
        await source.run()
        return {"ok": True, "source": source.info()}

    async def remove_source(self, name: str) -> dict:
        """移除事件源（实时）"""
        if name not in self._sources:
            return {"ok": False, "error": f"Source '{name}' not found"}
        source = self._sources.pop(name)
        await source.stop()
        source._detach()
        return {"ok": True, "source": source.info()}

    async def start_source(self, name: str) -> dict:
        """启动事件源"""
        if name not in self._sources:
            return {"ok": False, "error": f"Source '{name}' not found"}
        source = self._sources[name]
        if source.running:
            return {"ok": True, "message": "already running"}
        await source.run()
        return {"ok": True, "source": source.info()}

    async def stop_source(self, name: str) -> dict:
        """停止事件源"""
        if name not in self._sources:
            return {"ok": False, "error": f"Source '{name}' not found"}
        source = self._sources[name]
        await source.stop()
        return {"ok": True, "source": source.info()}

    def list_sources(self) -> List[dict]:
        """列出所有事件源"""
        return [s.info() for s in self._sources.values()]

    def get_source(self, name: str) -> Optional[EventSource]:
        return self._sources.get(name)

    # ══════════════════════════════════════════════
    #  Executor 实时管理
    # ══════════════════════════════════════════════

    async def add_executor(self, executor: EventExecutor) -> dict:
        """添加执行器（实时）"""
        if executor.name in self._executors:
            return {"ok": False, "error": f"Executor '{executor.name}' already exists"}
        executor._attach(self)
        self._executors[executor.name] = executor
        # 如果有 start 方法，调用它（如 ScriptExecutor）
        if hasattr(executor, "start") and callable(executor.start):
            try:
                await executor.start()
            except Exception as e:
                self._executors.pop(executor.name, None)
                executor._detach()
                return {"ok": False, "error": str(e)}
        return {"ok": True, "executor": executor.info()}

    async def remove_executor(self, name: str) -> dict:
        """移除执行器（实时）"""
        if name not in self._executors:
            return {"ok": False, "error": f"Executor '{name}' not found"}
        executor = self._executors.pop(name)
        if hasattr(executor, "stop") and callable(executor.stop):
            await executor.stop()
        executor._detach()
        return {"ok": True, "executor": executor.info()}

    async def reload_executor(self, name: str) -> dict:
        """重新加载脚本执行器"""
        if name not in self._executors:
            return {"ok": False, "error": f"Executor '{name}' not found"}
        executor = self._executors[name]
        if hasattr(executor, "reload"):
            await executor.reload()
            return {"ok": True, "executor": executor.info()}
        return {"ok": False, "error": "Executor does not support reload"}

    def list_executors(self) -> List[dict]:
        return [e.info() for e in self._executors.values()]

    def get_executor(self, name: str) -> Optional[EventExecutor]:
        return self._executors.get(name)

    # ══════════════════════════════════════════════
    #  Plugin 实时管理
    # ══════════════════════════════════════════════

    async def add_plugin(self, plugin: "Plugin") -> dict:
        """添加插件（实时）"""
        if plugin.name in self._plugins:
            return {"ok": False, "error": f"Plugin '{plugin.name}' already exists"}
        plugin._attach(self)
        self._plugins[plugin.name] = plugin
        plugin.on_attach()
        return {"ok": True, "plugin": plugin.name}

    async def remove_plugin(self, name: str) -> dict:
        """移除插件（实时）"""
        if name not in self._plugins:
            return {"ok": False, "error": f"Plugin '{name}' not found"}
        plugin = self._plugins.pop(name)
        plugin.on_detach()
        plugin._detach()
        return {"ok": True, "plugin": name}

    def list_plugins(self) -> List[str]:
        return list(self._plugins.keys())

    # ══════════════════════════════════════════════
    #  Hook 系统
    # ══════════════════════════════════════════════

    def add_hook(self, stage: HookStage, hook: Callable):
        self._hooks.setdefault(stage.value, []).append(hook)

    def remove_hook(self, stage: HookStage, hook: Callable):
        hooks = self._hooks.get(stage.value, [])
        self._hooks[stage.value] = [h for h in hooks if h != hook]

    def on_new_listener(self, callback: Callable):
        self._new_listener_callbacks.append(callback)

    # ══════════════════════════════════════════════
    #  统计
    # ══════════════════════════════════════════════

    def stats(self) -> dict:
        return {
            "sources": {
                "count": len(self._sources),
                "running": sum(1 for s in self._sources.values() if s.running),
                "names": list(self._sources.keys()),
            },
            "executors": {
                "count": len(self._executors),
                "names": list(self._executors.keys()),
                "total_executed": sum(e._executed_count for e in self._executors.values()),
                "total_errors": sum(e._error_count for e in self._executors.values()),
            },
            "plugins": {
                "count": len(self._plugins),
                "names": list(self._plugins.keys()),
            },
            "handlers": {
                "count": len(self._handlers),
                "patterns": list(self._handlers.keys()),
            },
            "pending_tasks": len(self._waiting),
            "hooks": {
                stage: len(hooks)
                for stage, hooks in self._hooks.items()
            },
        }

    # ══════════════════════════════════════════════
    #  内部方法
    # ══════════════════════════════════════════════

    def _add_handler(self, pattern, handler, is_once=False, original_id=None):
        self._handlers.setdefault(pattern, []).append(handler)
        self._router.subscribe(pattern, str(id(handler)))
        for cb in self._new_listener_callbacks:
            try:
                cb(pattern, handler)
            except Exception:
                pass

    def _remove_handler(self, pattern, handler):
        if pattern in self._handlers:
            self._handlers[pattern] = [
                h for h in self._handlers[pattern]
                if h != handler and getattr(h, "_original", None) != handler
            ]
            if not self._handlers[pattern]:
                del self._handlers[pattern]

    def _on_task_done(self, task: asyncio.Task):
        self._waiting.discard(task)
        exc = task.exception()
        if exc:
            loop = asyncio.get_event_loop()
            loop.call_soon(
                lambda: asyncio.ensure_future(self.emit("error", {"error": str(exc)}))
            )

    async def _run_hooks(self, stage: HookStage, ctx: HookContext) -> HookResult:
        for hook_fn in self._hooks.get(stage.value, []):
            try:
                result = hook_fn(ctx)
                if asyncio.iscoroutine(result):
                    result = await result
                if result is not None and result != HookResult.CONTINUE:
                    return result
            except Exception:
                pass
        return HookResult.CONTINUE

    def _wrap_once(self, pattern, handler):
        engine = self
        fired = False

        def wrapper(topic, event):
            nonlocal fired
            if fired:
                return None
            fired = True
            engine._remove_handler(pattern, wrapper)
            return handler(topic, event)

        wrapper._original = handler
        return wrapper

    def _on_decorator(self, pattern):
        def decorator(fn):
            self._add_handler(pattern, fn)
            return fn
        return decorator

    def _once_decorator(self, pattern):
        def decorator(fn):
            wrapper = self._wrap_once(pattern, fn)
            self._add_handler(pattern, wrapper, is_once=True)
            return fn
        return decorator


class _PurePythonRouter:
    """纯 Python 路由器（fallback）"""

    def __init__(self):
        self._patterns: List[str] = []

    def subscribe(self, pattern: str, handler_id: str):
        if pattern not in self._patterns:
            self._patterns.append(pattern)

    def unsubscribe(self, pattern: str, handler_id: str):
        if pattern in self._patterns:
            self._patterns.remove(pattern)

    def match_patterns(self, topic: str) -> List[str]:
        return [p for p in self._patterns if _is_match(topic, p)]

    def has_match(self, topic: str) -> bool:
        return any(_is_match(topic, p) for p in self._patterns)


def _is_match(topic: str, pattern: str) -> bool:
    t, p = topic, pattern
    t_len, p_len = len(t), len(p)
    prev = [False] * (p_len + 1)
    curr = [False] * (p_len + 1)
    prev[0] = True
    for j in range(1, p_len + 1):
        if p[j - 1] == '*':
            prev[j] = prev[j - 1]
    for i in range(1, t_len + 1):
        curr[0] = False
        for j in range(1, p_len + 1):
            if p[j - 1] == '*':
                curr[j] = curr[j - 1] or prev[j]
            elif p[j - 1] == '?':
                curr[j] = prev[j - 1]
            else:
                curr[j] = prev[j - 1] and t[i - 1] == p[j - 1]
        prev, curr = curr, prev
    return prev[p_len]