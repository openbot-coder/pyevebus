"""
05_rpc_subscribe.py — RPC 流式订阅示例

启动服务后运行（另一个终端）:
    evebus serve --port 8080

然后运行本示例:
    python examples/05_rpc_subscribe.py

再开一个终端发射事件:
    evebusctl emit "data.quotes.BINANCE.ETHUSDT" -d '{"price": 3000}'
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from evebus.rpc import RPCClient


async def main():
    client = RPCClient("http://localhost:8080")

    print("🔔 订阅 data.*.ETHUSDT ...")
    print("   另开终端: evebusctl emit \"data.quotes.BINANCE.ETHUSDT\" -d '{\"price\": 3000}'")
    print()

    async for event in client.subscribe("data.*.ETHUSDT"):
        print(f"📥 {event['topic']}: {event['event']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  已停止")
