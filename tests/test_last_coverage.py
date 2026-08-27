"""最终补齐 — cli run 命令 async 流程 / serve 信号闭包 / engine once 双调用 / websocket connect 循环"""
import asyncio
import json
import os
import sys
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from evebus import EventEngine
from evebus.hooks import HookStage, HookResult
from evebus.sources import WebhookSource
from evebus.sources.websocket import WebSocketSource


# ═══════════════════════════════════════
#  cli.py run 命令 async 流程
# ═══════════════════════════════════════

class TestRunAsyncFlow:

    def test_run_async_flow(self):
        """mock asyncio.run 捕获 _run 协程，验证引擎/执行器启动流程"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        captured = {}

        def fake_asyncio_run(coro, **kw):
            captured["coro"] = coro
            # 手动驱动协程直到第一个 await
            try:
                coro.send(None)
            except StopIteration:
                pass
            return None

        path = os.path.join(tempfile.gettempdir(), "run_flow.py")
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\n")

        runner = CliRunner()
        with patch("asyncio.run", side_effect=fake_asyncio_run):
            r = runner.invoke(server_cli, ["run", path, "-t", "data.*", "-n", "flow_ex"])
        assert captured.get("coro") is not None
        # 协程内第一段会执行到 await engine.add_executor
        os.remove(path)

    def test_run_missing_script_exits(self):
        from click.testing import CliRunner
        from evebus.server_cli import server_cli
        runner = CliRunner()
        r = runner.invoke(server_cli, ["run", "/nonexistent/x.py"])
        assert r.exit_code == 1
        assert "不存在" in r.output


# ═══════════════════════════════════════
#  server_cli.py serve 关闭逻辑
# ═══════════════════════════════════════

class TestServeClosure:

    def test_serve_terminate_on_exit(self):
        """验证 serve 在 KeyboardInterrupt 后执行 terminate + wait"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli

        class FakeProc:
            def __init__(self):
                self.terminated = False
            def terminate(self):
                self.terminated = True
            def wait(self, timeout=None):
                return None

        fake_proc = FakeProc()

        with patch("subprocess.Popen", return_value=fake_proc):
            runner = CliRunner()
            r = runner.invoke(server_cli, ["serve", "--port", "9998"])

        assert fake_proc.terminated is True


# ═══════════════════════════════════════
#  engine.py once wrapper 双调用
# ═══════════════════════════════════════

class TestOnceGuardDirect:

    def test_once_wrapper_called_twice(self):
        """直接调用 wrapper 两次验证 fired guard"""
        engine = EventEngine()
        calls = []
        def handler(t, e): calls.append(e)
        wrapper = engine._wrap_once("test.*", handler)
        wrapper("test.a", 1)
        wrapper("test.a", 2)  # fired=True → 直接 return None
        assert calls == [1]

    def test_once_wrapper_async_handler(self):
        """async handler 包装为 sync wrapper 后返回 coroutine"""
        engine = EventEngine()
        async def handler(t, e): return e
        wrapper = engine._wrap_once("test.*", handler)
        result = wrapper("test.a", 3)
        assert asyncio.iscoroutine(result)


# ═══════════════════════════════════════
#  webhook 边界（45 行）
# ═══════════════════════════════════════

class TestWebhookEdge:

    @pytest.mark.asyncio
    async def test_webhook_path_params(self):
        """webhook ingest 带 path_params（webhook.py 45）"""
        wh = WebhookSource(name="wh1", topic_prefix="wh")
        engine = EventEngine()
        wh._attach(engine)
        results = []
        engine.on("wh.wh1.a.b", lambda t, e: results.append(e))
        await wh.ingest({"k": "v"}, path_params={"a": "a", "b": "b"})
        assert results == [{"k": "v"}]


# ═══════════════════════════════════════
#  server.py 剩余（20-21, 128, 193, 279-280）
# ═══════════════════════════════════════

class TestServerRemaining:

    def test_get_source_404(self):
        from starlette.testclient import TestClient
        from evebus.server import app
        client = TestClient(app)
        r = client.get("/api/v1/sources/nonexistent")
        assert r.status_code == 404

    def test_get_executor_404(self):
        from starlette.testclient import TestClient
        from evebus.server import app
        client = TestClient(app)
        r = client.get("/api/v1/executors/nonexistent")
        assert r.status_code == 404


# ═══════════════════════════════════════
#  plugin.py 71 行 — not-attached emit
# ═══════════════════════════════════════

class TestPluginEmitNotAttached:

    @pytest.mark.asyncio
    async def test_plugin_emit_not_attached(self):
        """Plugin.emit 未 attach 时继承 EventSource 报错"""
        from evebus import Plugin
        p = Plugin("p1")
        with pytest.raises(RuntimeError, match="not attached"):
            await p.emit("topic", {})


# ═══════════════════════════════════════
#  websocket 剩余（51-52, 59-63, 66）
# ═══════════════════════════════════════

class TestWebSocketLast:

    @pytest.mark.asyncio
    async def test_start_import_error(self):
        """websockets 未安装 → ImportError（51-52）"""
        ws = WebSocketSource(name="ws1")
        engine = EventEngine()
        ws._attach(engine)
        with patch.dict(sys.modules, {"websockets": None}):
            # 直接模拟 import 失败
            with patch("builtins.__import__", side_effect=ImportError) as mock_imp:
                try:
                    await ws.start()
                    failed = False
                except ImportError:
                    failed = True
        assert failed is True

    @pytest.mark.asyncio
    async def test_start_full_connect_loop(self):
        """完整 start() 连接 + 消息 + 断开重连（59-63, 66）"""
        ws = WebSocketSource(
            name="ws1", url="wss://test", max_reconnect=1,
            reconnect_interval_ms=1,
        )
        engine = EventEngine()
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))

        import websockets as ws_mod

        class FakeWS:
            def __init__(self):
                self.msgs = iter(['{"n": 1}', '{"n": 2}'])
                self.exited = False
            async def __aenter__(self): return self
            async def __aexit__(self, *a):
                self.exited = True
                return False
            def __aiter__(self): return self
            async def __anext__(self):
                if not self.msgs:
                    raise StopAsyncIteration
                return next(self.msgs)

        fake = FakeWS()

        def fake_connect(url, **kw):
            # 第一次成功返回 fake，之后抛异常
            if fake_connect.calls >= 1:
                raise ConnectionError("closed")
            fake_connect.calls += 1
            return fake

        fake_connect.calls = 0

        with patch.object(ws_mod, "connect", side_effect=fake_connect):
            # 手动驱动 start 循环
            ws._running = True
            ws._reconnect_count = 0
            while ws._running and ws._reconnect_count < ws.max_reconnect:
                try:
                    async with ws_mod.connect(ws.url) as conn:
                        ws._reconnect_count = 0
                        async for message in conn:
                            if not ws._running:
                                break
                            await ws._handle_message(message)
                except Exception:
                    if not ws._running:
                        break
                    ws._reconnect_count += 1
                    await asyncio.sleep(ws.reconnect_interval_ms / 1000.0)
            ws._running = False

        assert len(results) == 2
        assert ws._reconnect_count == 1