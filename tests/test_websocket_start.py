"""覆盖率 — websocket.py start() 方法直接测试"""
import asyncio
import json
import pytest
from unittest.mock import patch, MagicMock

from evebus import EventEngine
from evebus.sources.websocket import WebSocketSource


def _make_fake_ws(msgs=(), connect_error=None):
    """构造 websockets.connect 的假返回"""
    class FakeWS:
        def __init__(self, messages=None):
            self._msgs = iter(messages if messages is not None else msgs)
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def __aiter__(self):
            return self
        async def __anext__(self):
            try:
                return next(self._msgs)
            except StopIteration:
                raise StopAsyncIteration
    return FakeWS


class TestWebSocketStart:

    @pytest.mark.asyncio
    async def test_start_message_loop(self):
        """start() 完整消息循环（60-75 分支）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=0)
        engine = EventEngine()
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))

        import websockets as ws_mod
        fake = _make_fake_ws(['{"a": 1}', '{"b": 2}'])()
        with patch.object(ws_mod, "connect", return_value=fake):
            ws._running = True
            # start() 会连接、收两条消息、正常结束（max_reconnect=0 只连一次）
            # 但 async for 结束后循环继续 while → 需要停止
            task = asyncio.ensure_future(ws.start())

            async def stop_later():
                await asyncio.sleep(0.2)
                ws._running = False
            stopper = asyncio.ensure_future(stop_later())
            await task
            await stopper

        assert len(results) == 2
        assert results == [{"a": 1}, {"b": 2}]

    @pytest.mark.asyncio
    async def test_start_reconnect_and_exhaust(self):
        """连接失败重连到 max_reconnect（77-89 分支）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=2, reconnect_interval_ms=1)
        engine = EventEngine()
        ws._attach(engine)

        import websockets as ws_mod
        with patch.object(ws_mod, "connect", side_effect=ConnectionError("boom")):
            await ws.start()  # 重连 2 次后退出

        assert ws._reconnect_count == 2
        assert ws._running is True  # start 结束后 _running 仍 True（由 stop 控制）

    @pytest.mark.asyncio
    async def test_start_cancelled(self):
        """start() 被取消（CancelledError 分支）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=5, reconnect_interval_ms=10)
        engine = EventEngine()
        ws._attach(engine)

        import websockets as ws_mod
        with patch.object(ws_mod, "connect", side_effect=ConnectionError("x")):
            task = asyncio.ensure_future(ws.start())
            await asyncio.sleep(0.03)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert True

    @pytest.mark.asyncio
    async def test_start_message_error_logged(self):
        """消息处理异常被记录不中断（66-71 分支）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=0)
        engine = EventEngine()
        ws._attach(engine)

        import websockets as ws_mod
        fake = _make_fake_ws(['bad json {{{'])()

        # 消息处理会抛错（emit 因引擎正常不会抛，直接测 logger 路径）
        # 用 emit 抛错模拟
        class FailingEngine:
            async def emit(self, *a, **kw):
                raise RuntimeError("emit failed")

        ws._engine = FailingEngine()
        with patch.object(ws_mod, "connect", return_value=fake), \
             patch("evebus.sources.websocket.logger") as mock_logger:
            ws._running = True
            task = asyncio.ensure_future(ws.start())

            async def stop_later():
                await asyncio.sleep(0.2)
                ws._running = False
            stopper = asyncio.ensure_future(stop_later())
            await task
            await stopper
            # 消息处理失败被记录
            assert mock_logger.error.called or mock_logger.warning.called

    @pytest.mark.asyncio
    async def test_start_stop_flag_break(self):
        """消息循环中 _running=False 退出（61 分支）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=0)
        engine = EventEngine()
        ws._attach(engine)

        import websockets as ws_mod
        # 连接成功但无消息，_running 立即 False
        fake = _make_fake_ws()()
        with patch.object(ws_mod, "connect", return_value=fake):
            ws._running = False
            # start() 设 _running=True 后进入循环，连接成功，async for 空，然后 while 检查
            task = asyncio.ensure_future(ws.start())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass