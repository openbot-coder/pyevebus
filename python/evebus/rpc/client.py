"""
RPCClient — pyevebus 远程调用客户端 SDK

通过标准 HTTP/SSE 与 pyevebus 服务交互：
- emit()       发射事件（单向 RPC）
- subscribe()  流式订阅事件（SSE 推送）

用法:
    from evebus.rpc import RPCClient

    client = RPCClient("http://localhost:8080")

    # 发射事件
    await client.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})

    # 流式订阅
    async for event in client.subscribe("data.*.ETHUSDT"):
        print(event)
"""

import asyncio
import json
from typing import Any, AsyncIterator, Dict

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

DEFAULT_URL = "http://localhost:8080"


class RPCError(Exception):
    """RPC 调用失败"""


class RPCClient:
    """pyevebus 远程客户端 — emit + subscribe"""

    def __init__(self, base_url: str = DEFAULT_URL):
        if httpx is None:
            raise ImportError("需要安装 httpx: pip install httpx")
        self.base_url = base_url.rstrip("/")

    # ══════════════════════════════════════
    #  发射事件（单向 RPC）
    # ══════════════════════════════════════

    async def emit(
        self,
        topic: str,
        payload: Any = None,
        source: str = "",
    ) -> Dict[str, Any]:
        """发射事件到远程引擎"""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/events/emit",
                json={"topic": topic, "payload": payload, "source": source},
            )
            resp.raise_for_status()
            return resp.json()

    # ══════════════════════════════════════
    #  流式订阅（SSE）
    # ══════════════════════════════════════

    async def subscribe(
        self,
        pattern: str = "*",
        auto_reconnect: bool = False,
        max_reconnects: int = 5,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        流式订阅事件（SSE）

        每个事件为 dict: {"topic": str, "event": Any, "timestamp": int}

        Args:
            pattern: 通配符 pattern，如 "data.*.ETHUSDT"
            auto_reconnect: 断线自动重连
            max_reconnects: 自动重连次数上限（auto_reconnect=True 时生效）
        """
        params = {"pattern": pattern}
        reconnect_count = 0

        while True:
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream(
                        "GET",
                        f"{self.base_url}/api/v1/events/subscribe",
                        params=params,
                    ) as resp:
                        resp.raise_for_status()
                        async for line in resp.aiter_lines():
                            if line.startswith("data: "):
                                # #2: JSONDecodeError 不终止订阅，跳过坏帧
                                try:
                                    yield json.loads(line[6:])
                                except json.JSONDecodeError:
                                    continue
                        # 流正常结束（服务端关闭）
                        if not auto_reconnect:
                            return
                        # #3: 正常结束也算一次重连周期，避免 max_reconnects 失效
                        reconnect_count += 1
                        if reconnect_count > max_reconnects:
                            raise RPCError(f"重连次数超限 ({max_reconnects})")
                        await asyncio.sleep(min(2 ** reconnect_count, 30))
            except (httpx.HTTPError, ConnectionError) as e:
                if not auto_reconnect:
                    raise RPCError(f"订阅失败: {e}") from e
                # #1: 此处 auto_reconnect 恒为 True（#3 已处理正常结束路径）
                reconnect_count += 1
                if reconnect_count > max_reconnects:
                    raise RPCError(f"重连次数超限 ({max_reconnects})")
                await asyncio.sleep(min(2 ** reconnect_count, 30))  # 指数退避

    # ══════════════════════════════════════
    #  查询 / 管理（便捷封装）
    # ══════════════════════════════════════

    async def health(self) -> Dict[str, Any]:
        """健康检查"""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.base_url}/api/v1/health")
            resp.raise_for_status()
            return resp.json()

    async def stats(self) -> Dict[str, Any]:
        """引擎统计"""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.base_url}/api/v1/stats")
            resp.raise_for_status()
            return resp.json()

    async def list_sources(self) -> list:
        """列出事件源"""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.base_url}/api/v1/sources")
            resp.raise_for_status()
            return resp.json().get("sources", [])

    async def list_executors(self) -> list:
        """列出执行器"""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.base_url}/api/v1/executors")
            resp.raise_for_status()
            return resp.json().get("executors", [])


__all__ = ["RPCClient", "RPCError", "DEFAULT_URL"]
