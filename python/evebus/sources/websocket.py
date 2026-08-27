"""
WebSocketSource — WebSocket 事件源

连接 WebSocket 服务端，将消息转换为事件。

用法:

    from evebus.sources import WebSocketSource

    ws = WebSocketSource(
        name="binance_ws",
        url="wss://stream.binance.com:9943/ws/ethusdt@ticker",
        topic_prefix="data.ws.binance",
        parse_json=True,
    )
    engine.add_source(ws)
"""

import asyncio
import json
from .base import EventSource


class WebSocketSource(EventSource):
    """WebSocket 事件源"""

    def __init__(
        self,
        name: str = "websocket",
        url: str = "",
        topic_prefix: str = "ws",
        parse_json: bool = True,
        reconnect_interval_ms: int = 5000,
        max_reconnect: int = 10,
    ):
        super().__init__(name)
        self.url = url
        self.topic_prefix = topic_prefix
        self.parse_json = parse_json
        self.reconnect_interval_ms = reconnect_interval_ms
        self.max_reconnect = max_reconnect
        self._reconnect_count = 0

    async def start(self):
        """启动 WebSocket 连接（自动重连）"""
        self._running = True
        self._reconnect_count = 0

        try:
            import websockets
        except ImportError:
            raise ImportError(
                "需要安装 websockets: pip install websockets"
            )

        while self._running and self._reconnect_count < self.max_reconnect:
            try:
                async with websockets.connect(self.url) as ws:
                    self._reconnect_count = 0  # 连接成功重置计数
                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)
            except Exception as e:
                if not self._running:
                    break
                self._reconnect_count += 1
                print(
                    f"[{self.name}] 连接断开: {e}, "
                    f"重连 {self._reconnect_count}/{self.max_reconnect}"
                )
                await asyncio.sleep(self.reconnect_interval_ms / 1000.0)

    async def _handle_message(self, raw: str):
        """处理 WebSocket 消息"""
        if self.parse_json:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"raw": raw}
        else:
            payload = {"raw": raw}

        topic = f"{self.topic_prefix}.{self.name}"
        await self.emit(topic, payload)

    def info(self) -> dict:
        base = super().info()
        base.update({
            "url": self.url,
            "topic_prefix": self.topic_prefix,
            "reconnect_count": self._reconnect_count,
        })
        return base