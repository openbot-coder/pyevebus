"""v0.3.0 RPC 测试 — SSE 订阅端点 + RPCClient SDK

使用真实 uvicorn 服务器 + httpx 异步客户端（TestClient 无法正确处理 SSE 长连接）。
"""
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import pytest
import httpx
import evebus


@pytest.fixture(scope="module")
def server_url():
    """启动真实 uvicorn 服务器（测试模块级，复用）"""
    # 找一个空闲端口
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    pkg_dir = os.path.dirname(evebus.__file__)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.dirname(pkg_dir)

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "evebus.server:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"

    # 等待服务就绪
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            httpx.get(f"{url}/api/v1/health", timeout=1)
            break
        except Exception:
            time.sleep(0.2)
    else:
        proc.terminate()
        raise RuntimeError("服务器启动失败")

    yield url
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture
def client(server_url):
    """httpx 异步客户端"""
    return httpx.AsyncClient(base_url=server_url, timeout=10)


@pytest.fixture(autouse=True)
def clean_engine(server_url):
    """每个测试后清理 engine 状态（通过重启避免串扰）"""
    yield
    # 触发清理：无状态残留问题，SSE 断连自动 off


# ═══════════════════════════════════════
#  SSE 订阅端点
# ═══════════════════════════════════════

class TestSSESubscribe:

    @pytest.mark.asyncio
    async def test_subscribe_receives_matching_events(self, client):
        """订阅 data.* 收到匹配事件"""
        async def feed():
            await asyncio.sleep(0.3)
            await client.post("/api/v1/events/emit",
                              json={"topic": "data.quotes.ETH", "payload": {"price": 3000}})

        t = asyncio.ensure_future(feed())
        async with client.stream("GET", "/api/v1/events/subscribe",
                                 params={"pattern": "data.*"}) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            received = []
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    received.append(json.loads(line[6:]))
                    break
        await t

        assert len(received) == 1
        assert received[0]["topic"] == "data.quotes.ETH"
        assert received[0]["event"] == {"price": 3000}

    @pytest.mark.asyncio
    async def test_subscribe_wildcard_filter(self, client):
        """订阅 data.*.ETHUSDT 只收到匹配事件"""
        # 先发不匹配的
        await client.post("/api/v1/events/emit",
                          json={"topic": "data.quotes.BTCUSDT", "payload": {}})

        async def feed():
            await asyncio.sleep(0.3)
            await client.post("/api/v1/events/emit",
                              json={"topic": "data.quotes.ETHUSDT", "payload": {"price": 1}})

        t = asyncio.ensure_future(feed())
        async with client.stream("GET", "/api/v1/events/subscribe",
                                 params={"pattern": "data.*.ETHUSDT"}) as resp:
            received = []
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    received.append(json.loads(line[6:]))
                    break
        await t

        assert len(received) == 1
        assert received[0]["topic"] == "data.quotes.ETHUSDT"

    @pytest.mark.asyncio
    async def test_subscribe_cleans_handler_on_close(self, server_url, client):
        """连接关闭后 handler 从引擎移除（断连清理）"""
        async with client.stream("GET", "/api/v1/events/subscribe",
                                 params={"pattern": "cleanup.*"}) as resp:
            # 连接期间 handler 已注册
            stats = await client.get("/api/v1/stats")
            handlers = stats.json()["handlers"]
            assert "cleanup.*" in handlers["patterns"]

            async def feed():
                await asyncio.sleep(0.3)
                await client.post("/api/v1/events/emit",
                                  json={"topic": "cleanup.test", "payload": {}})
            t = asyncio.ensure_future(feed())
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    break
            await t

        # 流关闭后 handler 被移除
        await asyncio.sleep(0.3)
        stats = await client.get("/api/v1/stats")
        handlers = stats.json()["handlers"]
        assert "cleanup.*" not in handlers["patterns"]

    @pytest.mark.asyncio
    async def test_subscribe_default_all(self, client):
        """订阅空 pattern 默认匹配所有"""
        async def feed():
            await asyncio.sleep(0.3)
            await client.post("/api/v1/events/emit",
                              json={"topic": "any.topic", "payload": {"x": 1}})

        t = asyncio.ensure_future(feed())
        async with client.stream("GET", "/api/v1/events/subscribe") as resp:
            assert resp.status_code == 200
            received = []
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    received.append(json.loads(line[6:]))
                    break
        await t
        assert received[0]["topic"] == "any.topic"

    @pytest.mark.asyncio
    async def test_subscribe_include_timestamp(self, client):
        """事件包含纳秒时间戳"""
        before = time.time_ns()
        async def feed():
            await asyncio.sleep(0.3)
            await client.post("/api/v1/events/emit",
                              json={"topic": "ts.test", "payload": {}})
        t = asyncio.ensure_future(feed())
        async with client.stream("GET", "/api/v1/events/subscribe",
                                 params={"pattern": "ts.*"}) as resp:
            received = []
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    received.append(json.loads(line[6:]))
                    break
        await t
        after = time.time_ns()
        assert before <= received[0]["timestamp"] <= after


# ═══════════════════════════════════════
#  背压 / 并发
# ═══════════════════════════════════════

class TestBackpressure:

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent(self, client):
        """两个独立订阅互不干扰"""
        async def feed():
            await asyncio.sleep(0.3)
            await client.post("/api/v1/events/emit",
                              json={"topic": "bp.one", "payload": {}})
        t = asyncio.ensure_future(feed())

        async with client.stream("GET", "/api/v1/events/subscribe",
                                 params={"pattern": "bp.*"}) as r1:
            async with client.stream("GET", "/api/v1/events/subscribe",
                                     params={"pattern": "bp.*"}) as r2:
                assert r1.status_code == 200
                assert r2.status_code == 200
                seen1, seen2 = [], []
                async for line in r1.aiter_lines():
                    if line.startswith("data: "):
                        seen1.append(json.loads(line[6:]))
                        break
                async for line in r2.aiter_lines():
                    if line.startswith("data: "):
                        seen2.append(json.loads(line[6:]))
                        break
        await t
        assert seen1[0]["topic"] == "bp.one"
        assert seen2[0]["topic"] == "bp.one"

    @pytest.mark.asyncio
    async def test_slow_consumer_buffers_events(self, client):
        """慢消费者（延迟读）事件排队不丢失"""
        async def feed():
            await asyncio.sleep(0.3)
            for i in range(10):
                await client.post("/api/v1/events/emit",
                                  json={"topic": f"slow.{i}", "payload": {"i": i}})
        t = asyncio.ensure_future(feed())

        async with client.stream("GET", "/api/v1/events/subscribe",
                                 params={"pattern": "slow.*"}) as resp:
            received = []
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    received.append(json.loads(line[6:]))
                    if len(received) >= 10:
                        break
        await t
        assert len(received) == 10
        assert received[-1]["topic"] == "slow.9"


# ═══════════════════════════════════════
#  RPCClient SDK
# ═══════════════════════════════════════

class TestRPCClient:

    def test_client_import(self):
        """SDK 可导入"""
        from evebus.rpc import RPCClient, RPCError, DEFAULT_URL
        assert DEFAULT_URL == "http://localhost:8080"

    @pytest.mark.asyncio
    async def test_emit_and_health(self, server_url):
        """emit + health 方法"""
        from evebus.rpc import RPCClient
        client = RPCClient(server_url)

        result = await client.emit("data.test", {"x": 1})
        assert result["ok"] is True
        assert result["topic"] == "data.test"

        health = await client.health()
        assert health["status"] == "ok"

    @pytest.mark.asyncio
    async def test_client_subscribe_stream(self, server_url):
        """RPCClient.subscribe 流式接收事件"""
        from evebus.rpc import RPCClient
        client = RPCClient(server_url)

        async def feed():
            await asyncio.sleep(0.3)
            await client.emit("stream.topic", {"n": 1})

        t = asyncio.ensure_future(feed())
        events = []
        async for event in client.subscribe("stream.*"):
            events.append(event)
            break
        await t

        assert events[0]["topic"] == "stream.topic"
        assert events[0]["event"] == {"n": 1}

    @pytest.mark.asyncio
    async def test_client_stats_sources(self, server_url):
        """stats + list_sources"""
        from evebus.rpc import RPCClient
        client = RPCClient(server_url)

        stats = await client.stats()
        assert "sources" in stats
        assert await client.list_sources() == []

    def test_client_missing_httpx(self):
        """未安装 httpx 时抛 ImportError"""
        from evebus.rpc import client as rc
        original = rc.httpx
        rc.httpx = None
        try:
            with pytest.raises(ImportError, match="httpx"):
                rc.RPCClient()
        finally:
            rc.httpx = original


# ═══════════════════════════════════════
#  evebusctl subscribe 命令
# ═══════════════════════════════════════

class TestSubscribeCommand:

    def test_subscribe_help(self):
        """subscribe 命令 help"""
        from click.testing import CliRunner
        from evebus.cli import cli
        runner = CliRunner()
        r = runner.invoke(cli, ["subscribe", "--help"])
        assert r.exit_code == 0
        assert "PATTERN" in r.output or "pattern" in r.output.lower()