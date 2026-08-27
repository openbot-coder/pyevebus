"""
evebus — 异步事件引擎

Rust 核心 + Python API，参考 pyee 风格。

用法:

    from evebus import EventEngine

    engine = EventEngine()

    @engine.on("data.quotes.*.ETHUSDT")
    async def on_quote(topic, event):
        print(f"ETH: {event}")

    await engine.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})
"""

from .engine import EventEngine
from .hooks import HookStage, HookResult, HookContext
from .plugin import Plugin
from .sources import EventSource, TimerSource, WebSocketSource, WebhookSource
from .executors import EventExecutor, ScriptExecutor
from .cli import cli as cli_main

__version__ = "0.2.0"

__all__ = [
    "EventEngine",
    "HookStage",
    "HookResult",
    "HookContext",
    "Plugin",
    "EventSource",
    "TimerSource",
    "WebSocketSource",
    "WebhookSource",
    "EventExecutor",
    "ScriptExecutor",
]