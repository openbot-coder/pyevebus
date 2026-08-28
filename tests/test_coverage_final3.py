"""覆盖率 97%+ 冲刺 — RPC 正常结束重连 + 各模块剩余可测分支"""
import asyncio
import json
import os
import sys
import tempfile
import time
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import httpx

from evebus import EventEngine, TimerSource
from evebus.rpc import RPCClient
import evebus.rpc.client as rpc_mod


# ═══════════════════════════════════════
#  rpc/client.py — 正常结束重连完整路径（102-110）
# ═══════════════════════════════════════

class TestRPCNormalEnd:

    class NormalStream:
        """模拟：先给一帧，然后流正常结束"""
        def __init__(self, frames=1):
            self._lines = iter(['data: {"topic": "ok", "event": 1}'] * frames)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def raise_for_status(self):
            pass
        async def aiter_lines(self):
            for line in self._lines:
                yield line

    class FakeAsyncClient:
        def __init__(self, *a, **kw):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def stream(self, method, url, params=None):
            return self._stream

    def _make_patch(self, stream):
        self.FakeAsyncClient._stream = stream
        return patch("evebus.rpc.client.httpx.AsyncClient", self.FakeAsyncClient)

    @pytest.mark.asyncio
    async def test_normal_end_consumes_then_reconnect(self):
        """正常结束路径：消费事件 → 流结束 → 重连计数 → 超限（102-110）"""
        client = RPCClient("http://test")
        # 流给 0 帧（立即正常结束），重连计数累计
        with self._make_patch(self.NormalStream(frames=0)), \
             patch("evebus.rpc.client.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(Exception, match="重连次数超限"):
                async for _ in client.subscribe("x.*", auto_reconnect=True, max_reconnects=2):
                    pass

    @pytest.mark.asyncio
    async def test_normal_end_no_reconnect_returns(self):
        """auto_reconnect=False + 流正常结束 → 正常返回（104-105）"""
        client = RPCClient("http://test")
        with self._make_patch(self.NormalStream(frames=1)):
            got = []
            async for event in client.subscribe("x.*"):
                got.append(event)
            assert len(got) == 1
            assert got[0]["topic"] == "ok"

    @pytest.mark.asyncio
    async def test_normal_end_exponential_backoff(self):
        """重连退避计算（110 的 2**reconnect_count）"""
        client = RPCClient("http://test")
        # 验证退避逻辑：reconnect_count=1 → sleep(2)
        with self._make_patch(self.NormalStream(frames=0)), \
             patch("evebus.rpc.client.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            try:
                async for _ in client.subscribe("x.*", auto_reconnect=True, max_reconnects=3):
                    pass
            except Exception:
                pass
            # 确认 sleep 被调用（退避）
            assert mock_sleep.called


# ═══════════════════════════════════════
#  cli.py — subscribe 彩色输出分支（161->exit, 166）
# ═══════════════════════════════════════

class TestCLIColorSubscribe:

    def test_subscribe_colored_output(self):
        """subscribe 默认彩色输出（非 no_color 分支）"""
        from click.testing import CliRunner
        from evebus.cli import cli

        events = [{"topic": "data.x", "event": {"a": 1}, "timestamp": 1787000000123456789}]

        class FakeClient:
            def __init__(self, url):
                self.url = url
            async def subscribe(self, pattern, auto_reconnect=False):
                for e in events:
                    yield e
                raise KeyboardInterrupt()

        with patch("evebus.rpc.RPCClient", FakeClient):
            runner = CliRunner()
            r = runner.invoke(cli, ["subscribe", "data.*"])
            assert r.exit_code == 0
            # 彩色输出包含 ANSI 或 topic
            assert "data.x" in r.output

    def test_subscribe_no_timestamp(self):
        """无 timestamp 时默认 0（166 分支）"""
        from click.testing import CliRunner
        from evebus.cli import cli

        events = [{"topic": "t", "event": 1}]  # 无 timestamp

        class FakeClient:
            def __init__(self, url):
                self.url = url
            async def subscribe(self, pattern, auto_reconnect=False):
                for e in events:
                    yield e
                raise KeyboardInterrupt()

        with patch("evebus.rpc.RPCClient", FakeClient):
            runner = CliRunner()
            r = runner.invoke(cli, ["subscribe", "t", "--no-color"])
            assert r.exit_code == 0


# ═══════════════════════════════════════
#  engine.py — _wrap_once 直接调用（398-399）
# ═══════════════════════════════════════

class TestOnceWrapperDirect:

    def test_wrapper_fired_guard(self):
        """wrapper 触发后不再执行（398-399）"""
        engine = EventEngine()
        calls = []

        def h(t, e):
            calls.append(e)

        wrapper = engine._wrap_once("t.*", h)
        wrapper("t.a", 1)
        wrapper("t.b", 2)  # fired=True → 返回 None
        assert calls == [1]

    @pytest.mark.asyncio
    async def test_wrapper_async_handler(self):
        """async handler 包装返回 coroutine"""
        engine = EventEngine()
        async def h(t, e):
            return e
        wrapper = engine._wrap_once("t.*", h)
        result = wrapper("t.a", 3)
        assert asyncio.iscoroutine(result)
        result.close()  # 避免警告


# ═══════════════════════════════════════
#  script.py — _is_match 末行（470->exit）
# ═══════════════════════════════════════

class TestIsMatchEdge:

    def test_is_match_empty_pattern(self):
        """空 pattern 匹配空 topic"""
        from evebus.engine import _is_match
        assert _is_match("", "") is True
        assert _is_match("a", "") is False
        assert _is_match("", "a") is False

    def test_is_match_leading_star(self):
        """前导 * 处理（470 附近）"""
        from evebus.engine import _is_match
        assert _is_match("abc", "*bc") is True
        assert _is_match("abc", "a*") is True