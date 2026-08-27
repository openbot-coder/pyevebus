"""
WebhookSource — HTTP Webhook 事件源

监听 HTTP POST 请求，将请求体转换为事件。

用法:

    from evebus.sources import WebhookSource

    webhook = WebhookSource(
        name="webhook",
        path="/events/ingest",
        topic_prefix="webhook",
    )
    engine.add_source(webhook)
    # 外部 POST /events/ingest → engine.emit("webhook.webhook", body)
"""

from .base import EventSource


class WebhookSource(EventSource):
    """HTTP Webhook 事件源（事件通过 HTTP POST 注入引擎）"""

    def __init__(
        self,
        name: str = "webhook",
        path: str = "/events/ingest",
        topic_prefix: str = "webhook",
    ):
        super().__init__(name)
        self.path = path
        self.topic_prefix = topic_prefix
        self._received_count = 0

    async def start(self):
        """WebhookSource 不自行启动，由 HTTP 服务端转发调用"""
        self._running = True

    async def ingest(self, body: dict, path_params: dict = None):
        """外部调用：接收 webhook 数据并发射事件"""
        self._received_count += 1
        topic = f"{self.topic_prefix}.{self.name}"
        if path_params:
            topic += "." + ".".join(str(v) for v in path_params.values())
        await self.emit(topic, body)

    def info(self) -> dict:
        base = super().info()
        base.update({
            "path": self.path,
            "topic_prefix": self.topic_prefix,
            "received_count": self._received_count,
        })
        return base