"""
示例 3: 插件系统

运行: python examples/03_plugins.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from evebus import EventEngine, Plugin, HookStage


# ═══════════════════════════════════════
#  示例插件: WebSocket 事件源
# ═══════════════════════════════════════

class MockWebSocketPlugin(Plugin):
    """模拟 WebSocket 数据源"""

    def __init__(self):
        super().__init__("mock_ws")

    def on_attach(self):
        print(f"  [ws] 插件已加载")

    def on_detach(self):
        print(f"  [ws] 插件已卸载")

    async def start(self):
        """模拟产生数据"""
        for i in range(3):
            await self.emit("data.ws.ticker", {
                "symbol": "ETHUSDT",
                "price": 3000 + i * 10,
                "ts": i,
            })
            await asyncio.sleep(0.01)

        await self.emit("data.ws.trade", {
            "symbol": "BTCUSDT",
            "side": "buy",
            "qty": 0.1,
        })


# ═══════════════════════════════════════
#  示例插件: 策略执行器
# ═══════════════════════════════════════

class MomentumPlugin(Plugin):
    """动量策略插件"""

    def __init__(self):
        super().__init__("momentum")
        self.trades = []

    def on_attach(self):
        self.on("data.ws.ticker", self.on_ticker)
        self.on("data.ws.trade", self.on_trade)
        print(f"  [momentum] 插件已加载")

    async def on_ticker(self, topic, event):
        if event.get("price", 0) > 3010:
            print(f"  [momentum] 买入信号: {event}")
            self.trades.append(event)

    async def on_trade(self, topic, event):
        print(f"  [momentum] 收到成交: {event}")


# ═══════════════════════════════════════
#  主程序
# ═══════════════════════════════════════

async def main():
    engine = EventEngine()

    # 添加插件
    ws_plugin = MockWebSocketPlugin()
    momentum_plugin = MomentumPlugin()

    await engine.add_plugin(ws_plugin)
    await engine.add_plugin(momentum_plugin)

    # 启动模拟数据
    await ws_plugin.start()

    # 等待完成
    await engine.wait_for_complete()

    # 结果
    print(f"\n插件列表: {list(engine._plugins.keys())}")
    print(f"策略交易数: {len(momentum_plugin.trades)}")

    # 卸载插件
    await engine.remove_plugin("mock_ws")
    await engine.remove_plugin("momentum")
    print(f"卸载后插件列表: {engine.list_plugins()}")


if __name__ == "__main__":
    asyncio.run(main())