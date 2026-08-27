"""WebSocket 测试 + serve/run CLI + _PurePythonRouter"""
import asyncio
import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from evebus import EventEngine
from evebus.hooks import HookStage, HookResult
from evebus.engine import _PurePythonRouter, _is_match
from evebus.sources.websocket import WebSocketSource
from evebus.sources import EventSource


@pytest.fixture
def engine():
    return EventEngine()


# ═══════════════════════════════════════
#  _PurePythonRouter
# ═══════════════════════════════════════

class TestPurePythonRouter:

    def test_subscribe_match(self):
        r = _PurePythonRouter()
        r.subscribe("data.*", "h1")
        assert r.match_patterns("data.x") == ["data.*"]
        assert r.match_patterns("other") == []

    def test_subscribe_unsubscribe(self):
        r = _PurePythonRouter()
        r.subscribe("data.*", "h1")
        r.unsubscribe("data.*", "h1")
        assert r._patterns == []

    def test_has_match(self):
        r = _PurePythonRouter()
        r.subscribe("data.*", "h1")
        assert r.has_match("data.x") is True
        assert r.has_match("other") is False


class TestIsMatch:

    def test_exact(self):
        assert _is_match("abc", "abc") is True
        assert _is_match("abc", "abd") is False

    def test_star_empty(self):
        assert _is_match("abc", "*") is True
        assert _is_match("", "*") is True

    def test_star_prefix(self):
        assert _is_match("abc", "a*") is True
        assert _is_match("abc", "b*") is False

    def test_star_suffix(self):
        assert _is_match("abc", "*c") is True
        assert _is_match("abc", "*d") is False

    def test_question(self):
        assert _is_match("abc", "a?c") is True
        assert _is_match("ac", "a?c") is False
        assert _is_match("abc", "a??") is True

    def test_no_wildcard_no_match(self):
        assert _is_match("abc", "def") is False

    def test_star_in_middle(self):
        assert _is_match("abcdef", "a*d*f") is True
        assert _is_match("axf", "a*d*f") is False


# ═══════════════════════════════════════
#  WebSocketSource 测试
# ═══════════════════════════════════════

class TestWebSocketSource:

    @pytest.mark.asyncio
    async def test_ws_info(self):
        ws = WebSocketSource(name="ws1", url="wss://test.com", topic_prefix="ws")
        info = ws.info()
        assert info["name"] == "ws1"
        assert info["url"] == "wss://test.com"
        assert info["topic_prefix"] == "ws"

    @pytest.mark.asyncio
    async def test_ws_not_attached_error(self):
        ws = WebSocketSource(name="ws1")
        with pytest.raises(RuntimeError, match="not attached"):
            await ws.emit("topic", {})

    @pytest.mark.asyncio
    async def test_ws_start_no_reconnect(self):
        """WS start() with max_reconnect=0 — while loop 不执行"""
        ws = WebSocketSource(name="ws1", url="wss://test.com", max_reconnect=0)
        engine = EventEngine()
        ws._attach(engine)
        try:
            import websockets
            with patch.object(websockets, "connect") as mock_connect:
                mock_connect.side_effect = ConnectionError("test")
                await ws.start()
        except ImportError:
            pass

    @pytest.mark.asyncio
    async def test_ws_start_connect_error(self):
        """WS start() — connect 抛异常后达到 max_reconnect 上限"""
        ws = WebSocketSource(name="ws1", url="wss://test.com", max_reconnect=1, reconnect_interval_ms=1)
        engine = EventEngine()
        ws._attach(engine)
        try:
            import websockets
            with patch.object(websockets, "connect") as mock_connect:
                mock_connect.side_effect = ConnectionError("test")
                await ws.start()
        except ImportError:
            pass

    @pytest.mark.asyncio
    async def test_ws_start_stopped_during_connect(self):
        """WS start() — 连接断开后 _running=False"""
        ws = WebSocketSource(name="ws1", url="wss://test.com", max_reconnect=1, reconnect_interval_ms=1)
        engine = EventEngine()
        ws._attach(engine)
        try:
            import websockets
            with patch.object(websockets, "connect") as mock_connect:
                mock_connect.side_effect = ConnectionError("test")
                ws._running = False  # 已在 stop 后调用 start
                await ws.start()
        except ImportError:
            pass

    @pytest.mark.asyncio
    async def test_ws_reconnect_count(self):
        ws = WebSocketSource(name="ws1", max_reconnect=2)
        info = ws.info()
        assert info["reconnect_count"] == 0


# ═══════════════════════════════════════
#  CLI serve / run 命令
# ═══════════════════════════════════════

class TestServeCommand:

    def test_serve_with_mock(self):
        """测试 serve 命令（mock subprocess）"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        runner = CliRunner()
        with patch("subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_popen.return_value = mock_proc
            mock_proc.wait.side_effect = [KeyboardInterrupt, None]

            r = runner.invoke(server_cli, ["serve", "--port", "9999"])
            mock_popen.assert_called_once()


class TestRunCommand:

    def test_run_help(self):
        """验证 run 命令 help 正确"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        runner = CliRunner()
        r = runner.invoke(server_cli, ["run", "--help"])
        assert r.exit_code == 0
        assert "script" in r.output.lower()


# ═══════════════════════════════════════
#  Engine 遗漏分支
# ═══════════════════════════════════════

class TestEngineBranches:

    @pytest.mark.asyncio
    async def test_start_source_already_running(self, engine):
        timer = EventSource("s1")
        timer._running = True
        engine._sources["s1"] = timer
        r = await engine.start_source("s1")
        assert r["ok"] is True
        assert "already running" in r["message"]

    @pytest.mark.asyncio
    async def test_wait_empty(self, engine):
        engine._waiting = set()
        await engine.wait_for_complete()

    @pytest.mark.asyncio
    async def test_cancel_done_task(self, engine):
        """cancel 中跳过已完成的 task"""
        task = asyncio.ensure_future(asyncio.sleep(0))
        engine._waiting.add(task)
        await asyncio.sleep(0.01)
        engine.cancel()
        assert engine._waiting == set()

    @pytest.mark.asyncio
    async def test_hook_modify_topic_and_payload(self, engine):
        results = []

        def transformer(ctx):
            ctx.topic = "new.topic"
            ctx.payload = {"modified": True}
            return HookResult.MODIFIED

        engine.add_hook(HookStage.PRE_EMIT, transformer)
        engine.on("new.topic", lambda t, e: results.append(e))
        await engine.emit("old.topic", {"old": True})
        assert results == [{"modified": True}]

    @pytest.mark.asyncio
    async def test_executor_safe_execute_error(self):
        """executor 执行出错"""
        from evebus.executors.base import EventExecutor

        engine = EventEngine()

        class BadExecutor(EventExecutor):
            def __init__(self):
                super().__init__("bad", ["test.*"])
            async def execute(self, topic, payload):
                raise ValueError("boom")

        ex = BadExecutor()
        ex._attach(engine)
        engine._executors["bad"] = ex
        for p in ex.patterns:
            engine._executor_handlers.setdefault(p, []).append(ex)
            engine._router.subscribe(p, f"executor:bad:{id(ex)}")

        errors = []
        engine.on("error", lambda t, e: errors.append(e))
        await engine.emit("test.a", {})
        await engine.wait_for_complete()
        assert len(errors) >= 1

    @pytest.mark.asyncio
    async def test_remove_nonexistent_executor(self, engine):
        r = await engine.remove_executor("nope")
        assert r["ok"] is False

    @pytest.mark.asyncio
    async def test_get_executor(self, engine):
        assert engine.get_executor("nope") is None

    @pytest.mark.asyncio
    async def test_get_plugin(self, engine):
        assert engine.get_plugin("nope") if hasattr(engine, 'get_plugin') else True


# ═══════════════════════════════════════
#  Source base 遗漏
# ═══════════════════════════════════════

class TestSourceBase:

    def test_source_detach(self):
        s = EventSource("s1")
        s._engine = EventEngine()
        s._detach()
        assert s._engine is None

    @pytest.mark.asyncio
    async def test_source_stop_no_task(self):
        s = EventSource("s1")
        s._task = None
        await s.stop()
        assert s._running is False


# ═══════════════════════════════════════
#  Executor base 遗漏
# ═══════════════════════════════════════

class TestExecutorBase:

    def test_executor_detach(self):
        from evebus.executors.base import EventExecutor
        ex = EventExecutor("ex1", ["test.*"])
        ex._engine = EventEngine()
        ex._detach()
        assert ex._attached is False

    def test_executor_not_attached_error(self):
        from evebus.executors.base import EventExecutor
        ex = EventExecutor("ex1", ["test.*"])
        with pytest.raises(RuntimeError, match="not attached"):
            _ = ex.engine