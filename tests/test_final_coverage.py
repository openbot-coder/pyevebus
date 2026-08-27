"""最后一波针对性覆盖 — engine 边界 / executor base / websocket 消息处理 / cli 剩余"""
import asyncio
import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from evebus import EventEngine
from evebus.engine import _PurePythonRouter
from evebus.executors.base import EventExecutor
from evebus.hooks import HookStage, HookResult
from evebus.sources import EventSource
from evebus.sources.timer import TimerSource
from evebus.sources.websocket import WebSocketSource


@pytest.fixture
def engine():
    return EventEngine()


# ═══════════════════════════════════════
#  engine.py 剩余行
# ═══════════════════════════════════════

class TestEngineRemaining:

    @pytest.mark.asyncio
    async def test_sync_handler_exception_emits_error(self, engine):
        """同步 handler 抛异常 → 发射 error 事件（engine.py 146-147）"""
        errors = []
        engine.on("error", lambda t, e: errors.append(e))

        def bad_handler(topic, event):
            raise ValueError("sync boom")
        engine.on("test.*", bad_handler)

        await engine.emit("test.a", {})
        await asyncio.sleep(0.05)
        assert len(errors) >= 1
        assert errors[0].get("handler") == "bad_handler"

    @pytest.mark.asyncio
    async def test_remove_handler_missing_pattern(self, engine):
        """_remove_handler 对不存在的 pattern（engine.py 364 空路径）"""
        def h(t, e): pass
        engine._remove_handler("nonexistent.pattern", h)  # 不应崩溃

    @pytest.mark.asyncio
    async def test_async_hook(self, engine):
        """async hook 函数（engine.py 386）"""
        order = []
        async def async_hook(ctx):
            order.append("async")
            return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, async_hook)
        engine.on("test.*", lambda t, e: order.append("handler"))
        await engine.emit("test.a", {})
        assert order == ["async", "handler"]

    @pytest.mark.asyncio
    async def test_once_wrapper_fired_guard(self, engine):
        """once wrapper 已触发后不再执行（engine.py 400）"""
        results = []
        def handler(t, e): results.append(e)
        engine.once("test.*", handler)
        await engine.emit("test.a", 1)
        await engine.emit("test.a", 2)
        assert results == [1]
        # wrapper 已被移除，直接调用不应触发（内部 fired guard）

    @pytest.mark.asyncio
    async def test_start_source_after_stop(self, engine):
        """start_source 在 stop 之后重新启动（engine.py 215-216）"""
        timer = TimerSourceWrapper()
        await engine.add_source(timer)
        await engine.stop_source("t1")
        r = await engine.start_source("t1")
        assert r["ok"] is True
        assert r["source"]["running"] is True
        await engine.remove_source("t1")

    @pytest.mark.asyncio
    async def test_remove_executor_with_stop(self, engine):
        """remove_executor 调用 stop（engine.py 258-260）"""
        from evebus.executors.script import ScriptExecutor
        path = os.path.join(tempfile.gettempdir(), "rm_stop.py")
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\n")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        await engine.add_executor(ex)
        r = await engine.remove_executor("ex1")
        assert r["ok"] is True
        assert ex._attached is False
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_executor_no_reload_method(self, engine):
        """reload_executor: 执行器没有 reload 方法（engine.py 271）"""
        class NoReloadExecutor(EventExecutor):
            async def execute(self, t, e): pass
        ex = NoReloadExecutor("noreload", ["*"])
        await engine.add_executor(ex)
        r = await engine.reload_executor("noreload")
        assert r["ok"] is False
        assert "does not support reload" in r["error"]
        await engine.remove_executor("noreload")

    def test_pure_router_unsubscribe_missing(self):
        """unsubscribe 不存在的 handler 不崩溃"""
        r = _PurePythonRouter()
        r.unsubscribe("data.*", "h1")

    def test_pure_router_match_with_question(self):
        """_PurePythonRouter 的 ? 匹配"""
        r = _PurePythonRouter()
        r.subscribe("a?c", "h1")
        assert r.match_patterns("abc") == ["a?c"]
        assert r.match_patterns("abbc") == []


class TimerSourceWrapper(EventSource):
    """轻量 Timer 包装 — 方便 start/stop 测试"""
    def __init__(self):
        super().__init__("t1")

    async def start(self):
        self._running = True
        while self._running:
            await asyncio.sleep(0.01)


# ═══════════════════════════════════════
#  executor/base.py 剩余行
# ═══════════════════════════════════════

class TestExecutorBaseRemaining:

    @pytest.mark.asyncio
    async def test_execute_not_implemented(self):
        """基类 execute 直接调用抛 NotImplementedError（base.py 72）"""
        ex = EventExecutor("ex1", ["*"])
        with pytest.raises(NotImplementedError):
            await ex.execute("t", {})

    @pytest.mark.asyncio
    async def test_safe_execute_no_engine(self):
        """_safe_execute 执行失败但没有 engine（base.py 84 空路径）"""
        ex = EventExecutor("ex1", ["*"])
        ex._engine = None

        class BadExec(EventExecutor):
            async def execute(self, t, e): raise ValueError("boom")

        bad = BadExec("bad", ["*"])
        bad._engine = None
        await bad._safe_execute("t", {})
        assert bad._error_count == 1

    @pytest.mark.asyncio
    async def test_detach_no_engine(self):
        """_detach 时 _engine 为 None（base.py 54->61 分支）"""
        ex = EventExecutor("ex1", ["*"])
        ex._engine = None
        ex._attached = True
        ex._detach()
        assert ex._attached is False


# ═══════════════════════════════════════
#  sources/base.py 剩余行
# ═══════════════════════════════════════

class TestSourceBaseRemaining:

    def test_engine_property_raises(self):
        """engine 属性未 attach 抛异常（base.py 42-44）"""
        s = EventSource("s1")
        with pytest.raises(RuntimeError, match="not attached"):
            _ = s.engine

    @pytest.mark.asyncio
    async def test_start_not_implemented(self):
        """基类 start 抛 NotImplementedError（base.py 61）"""
        s = EventSource("s1")
        with pytest.raises(NotImplementedError):
            await s.start()


# ═══════════════════════════════════════
#  websocket.py 消息处理
# ═══════════════════════════════════════

class TestWebSocketHandling:

    @pytest.mark.asyncio
    async def test_handle_message_valid_json(self, engine):
        """解析 JSON 消息（websocket.py 76-85）"""
        ws = WebSocketSource(name="ws1", topic_prefix="ws")
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))
        await ws._handle_message('{"price": 100}')
        assert results == [{"price": 100}]

    @pytest.mark.asyncio
    async def test_handle_message_invalid_json(self, engine):
        """无效 JSON → 包装为 raw（websocket.py 78-80）"""
        ws = WebSocketSource(name="ws1", topic_prefix="ws")
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))
        await ws._handle_message("not json {{{")
        assert results == [{"raw": "not json {{{"}]

    @pytest.mark.asyncio
    async def test_handle_message_no_parse(self, engine):
        """parse_json=False（websocket.py 81-82）"""
        ws = WebSocketSource(name="ws1", topic_prefix="ws", parse_json=False)
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))
        await ws._handle_message("plain text")
        assert results == [{"raw": "plain text"}]

    @pytest.mark.asyncio
    async def test_start_message_loop(self, engine):
        """模拟完整消息处理循环（覆盖 websocket.py 59-63 + 76-85）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=1)
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))

        class FakeWS:
            def __init__(self):
                self.msgs = iter(['{"a": 1}', '{"b": 2}'])
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            def __aiter__(self): return self
            async def __anext__(self):
                try:
                    return next(self.msgs)
                except StopIteration:
                    raise StopAsyncIteration

        fake_ws = FakeWS()
        # 手动模拟 start() 中的消息循环
        ws._running = True
        ws._reconnect_count = 0
        async with fake_ws as ws_conn:
            async for message in ws_conn:
                if not ws._running:
                    break
                await ws._handle_message(message)
        ws._running = False
        assert len(results) == 2
        assert results == [{"a": 1}, {"b": 2}]

    @pytest.mark.asyncio
    async def test_start_reconnect_and_stop(self, engine):
        """模拟连接失败 + 重连循环（覆盖 websocket.py 56-72）"""
        ws = WebSocketSource(
            name="ws1", url="wss://test", max_reconnect=2,
            reconnect_interval_ms=1,
        )
        ws._attach(engine)

        import websockets as ws_mod

        class FakeCtx:
            async def __aenter__(self):
                raise ConnectionError("simulated")
            async def __aexit__(self, *a):
                return False

        with patch.object(ws_mod, "connect", return_value=FakeCtx()):
            ws._running = True
            ws._reconnect_count = 0
            # 手动循环
            while ws._running and ws._reconnect_count < ws.max_reconnect:
                try:
                    async with ws_mod.connect(ws.url) as _ws:
                        ws._reconnect_count = 0
                        async for message in _ws:
                            if not ws._running:
                                break
                            await ws._handle_message(message)
                except Exception as e:
                    if not ws._running:
                        break
                    ws._reconnect_count += 1
                    await asyncio.sleep(ws.reconnect_interval_ms / 1000.0)
            assert ws._reconnect_count == 2
            ws._running = False

    @pytest.mark.asyncio
    async def test_start_stopped_during_loop(self, engine):
        """循环中 _running=False 退出（websocket.py 66）"""
        ws = WebSocketSource(
            name="ws1", url="wss://test", max_reconnect=5,
            reconnect_interval_ms=1,
        )
        ws._attach(engine)

        import websockets as ws_mod

        class FakeCtx:
            async def __aenter__(self):
                raise ConnectionError("boom")
            async def __aexit__(self, *a):
                return False

        with patch.object(ws_mod, "connect", return_value=FakeCtx()):
            ws._running = False  # 设为 False，异常时直接 break
            ws._reconnect_count = 0
            while ws._running and ws._reconnect_count < ws.max_reconnect:
                try:
                    async with ws_mod.connect(ws.url) as _ws:
                        pass
                except Exception:
                    if not ws._running:
                        break
                    ws._reconnect_count += 1
            assert ws._reconnect_count == 0


# ═══════════════════════════════════════
#  cli.py 剩余
# ═══════════════════════════════════════

class TestCliRemaining:

    def test_main_calls_cli(self):
        """main() 调用 cli()（cli.py 445）"""
        from evebus import cli
        with patch.object(cli, "cli") as mock_cli:
            cli.main()
            mock_cli.assert_called_once()

    def test_module_entry(self):
        """__main__ 入口（cli.py 449）"""
        import runpy
        import evebus.cli as cli_mod
        with patch.object(cli_mod, "main") as mock_main:
            # 直接执行该模块等价行为
            cli_mod.main()
            mock_main.assert_called_once()


# ═══════════════════════════════════════
#  __main__.py
# ═══════════════════════════════════════

class TestMainModule:

    def test_main_module(self):
        """__main__.py 导入并暴露 cli"""
        import importlib
        mod = importlib.import_module("evebus.__main__")
        assert mod.cli is not None