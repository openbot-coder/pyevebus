"""
示例 1: 基础用法 — pyee 风格

运行: python examples/01_basic.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from evebus import EventEngine


async def main():
    engine = EventEngine()

    # ── 装饰器方式 ──
    @engine.on("data.quotes.*.ETHUSDT")
    async def on_eth_quote(topic, event):
        print(f"  [ETH handler] {topic} → {event}")

    # ── 直接调用方式 ──
    async def on_btc_quote(topic, event):
        print(f"  [BTC handler] {topic} → {event}")

    engine.on("data.quotes.*.BTCUSDT", on_btc_quote)

    # ── 通配符 ──
    @engine.on("data.quotes.*")
    async def on_all_quotes(topic, event):
        print(f"  [ALL quotes] {topic}")

    # ── once 一次性 ──
    @engine.once("system.start")
    async def on_start(topic, event):
        print(f"  [ONCE] 系统启动: {event}")

    # ── 发射事件 ──
    print("== 发射 system.start ==")
    await engine.emit("system.start", {"ts": 1})
    print("== 再发射 system.start（once 应已移除）==")
    await engine.emit("system.start", {"ts": 2})
    assert not engine.listeners("system.start"), "once handler 应该已被移除"

    print("== 发射行情 ==")
    await engine.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})
    await engine.emit("data.quotes.OKX.BTCUSDT", {"price": 60000})

    # ── 等待所有协程完成 ──
    await engine.wait_for_complete()

    print(f"\n所有协程完成: {engine.complete}")
    print(f"已注册 events: {engine.event_names}")


if __name__ == "__main__":
    asyncio.run(main())