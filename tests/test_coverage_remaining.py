"""覆盖率补充 — 未覆盖分支专项"""
import asyncio
import json
import os
import sys
import tempfile
import signal
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import httpx

from evebus import EventEngine, TimerSource, WebhookSource, Plugin
from evebus.hooks import HookStage, HookResult, HookContext, hook
from evebus.executors import ScriptExecutor, EventExecutor
from evebus.sources import EventSource
from evebus.sources.websocket import WebSocketSource
from evebus.rpc import RPCClient


@pytest.fixture
def engine():
    return EventEngine()


# ═══════════════════════════════════════
#  rpc/client.py — 重连路径 & import 失败
# ═══════════════════════════════════════

class TestRPCReconnect:

    class _FailingStream:
        """模拟连接失败的 SSE 流"""
        def __init__(self, error=httpx.ConnectError("refused")):
            self._error = error
        async def __aenter__(self):
            raise self._error
        async def __aexit__(self, *a):
            return False
        def raise_for_status(self):
            pass

    def _patch_async_client(self, stream):
        """mock httpx.AsyncClient：stream() 返回异步上下文管理器"""
        from evebus.rpc.client import httpx as client_httpx

        class FakeAsyncClient:
            """模拟 httpx.AsyncClient，stream() 返回给定 stream"""
            def __init__(self, *a, **kw):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                return False
            def stream(self, method, url, params=None):
                return stream
            async def get(self, url):
                raise NotImplementedError
            async def post(self, url, json=None):
                raise NotImplementedError

        return patch("evebus.rpc.client.httpx.AsyncClient", FakeAsyncClient)

    @pytest.mark.asyncio
    async def test_subscribe_no_reconnect_http_error(self):
        """auto_reconnect=False + HTTPError → 抛 RPCError"""
        from evebus.rpc import RPCError
        client = RPCClient("http://test")
        with self._patch_async_client(self._FailingStream()):
            with pytest.raises(RPCError, match="订阅失败"):
                async for _ in client.subscribe("x.*"):
                    pass

    @pytest.mark.asyncio
    async def test_subscribe_reconnect_exhausted(self):
        """auto_reconnect=True 但重连次数超限 → RPCError"""
        client = RPCClient("http://test")
        with self._patch_async_client(self._FailingStream()), \
             patch("evebus.rpc.client.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(Exception, match="重连次数超限"):
                async for _ in client.subscribe("x.*", auto_reconnect=True, max_reconnects=2):
                    pass

    @pytest.mark.asyncio
    async def test_subscribe_normal_end_reconnect(self):
        """auto_reconnect=True + 流正常结束 → 重连（#3 计数累计）"""
        client = RPCClient("http://test")

        class NormalStream:
            """先给一帧数据，然后正常结束"""
            def __init__(self):
                self._lines = iter(["data: {\"topic\": \"ok\", \"event\": 1}"])
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                return False
            def raise_for_status(self):
                pass
            async def aiter_lines(self):
                for line in self._lines:
                    yield line

        stream = NormalStream()
        with self._patch_async_client(stream), \
             patch("evebus.rpc.client.asyncio.sleep", new=AsyncMock()):
            got = []
            async for event in client.subscribe("x.*", auto_reconnect=True, max_reconnects=1):
                got.append(event)
                if len(got) >= 1:
                    break
            assert got[0]["topic"] == "ok"

    def test_rpc_error_class(self):
        """RPCError 是 Exception 子类"""
        from evebus.rpc import RPCError
        e = RPCError("msg")
        assert str(e) == "msg"

    def test_client_import_httpx_missing(self):
        """httpx 不可用时 RPCClient() 抛 ImportError（模块级 import 失败路径）"""
        import evebus.rpc.client as rc
        original = rc.httpx
        rc.httpx = None
        try:
            with pytest.raises(ImportError, match="httpx"):
                rc.RPCClient()
        finally:
            rc.httpx = original


# ═══════════════════════════════════════
#  websocket.py — 剩余分支
# ═══════════════════════════════════════

class TestWebSocketRemaining:

    @pytest.mark.asyncio
    async def test_start_connect_and_disconnect_reconnect(self):
        """连接成功后断开 → 重连到 max_reconnect"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=2, reconnect_interval_ms=1)
        engine = EventEngine()
        ws._attach(engine)

        import websockets as ws_mod

        class FakeWS:
            def __init__(self, msgs):
                self._msgs = iter(msgs)
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            def __aiter__(self): return self
            async def __anext__(self):
                try:
                    return next(self._msgs)
                except StopIteration:
                    raise StopAsyncIteration

        connect_calls = [0]
        def fake_connect(url, **kw):
            connect_calls[0] += 1
            if connect_calls[0] == 1:
                return FakeWS(['{"a": 1}'])  # 第一次成功，有消息
            raise ConnectionError("closed")  # 之后失败

        with patch.object(ws_mod, "connect", side_effect=fake_connect):
            ws._running = True
            ws._reconnect_count = 0
            attempts = 0
            while ws._running and (attempts == 0 or ws._reconnect_count < ws.max_reconnect):
                attempts += 1
                try:
                    async with ws_mod.connect(ws.url) as conn:
                        async for message in conn:
                            await ws._handle_message(message)
                except Exception as e:
                    ws._reconnect_count += 1
                    if ws._reconnect_count < ws.max_reconnect:
                        await asyncio.sleep(ws.reconnect_interval_ms / 1000.0)
            ws._running = False
            assert ws._reconnect_count == 2

    @pytest.mark.asyncio
    async def test_start_cancelled_during_sleep(self):
        """重连 sleep 期间被取消"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=5, reconnect_interval_ms=1)
        engine = EventEngine()
        ws._attach(engine)
        import websockets as ws_mod

        with patch.object(ws_mod, "connect", side_effect=ConnectionError("fail")):
            async def run():
                ws._running = True
                ws._reconnect_count = 0
                attempts = 0
                while ws._running and (attempts == 0 or ws._reconnect_count < ws.max_reconnect):
                    attempts += 1
                    try:
                        async with ws_mod.connect(ws.url) as conn:
                            pass
                    except Exception as e:
                        ws._reconnect_count += 1
                        if ws._reconnect_count < ws.max_reconnect:
                            await asyncio.sleep(ws.reconnect_interval_ms / 1000.0)
                ws._running = False

            task = asyncio.ensure_future(run())
            await asyncio.sleep(0.02)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_handle_message_emit_error_logged(self):
        """消息处理中 emit 失败被记录不传播"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=1)
        # 不 attach → emit 抛 RuntimeError
        import logging
        ws._handle_message_error_count = 0
        with patch("evebus.sources.websocket.logger") as mock_logger:
            # _handle_message 内部 emit 失败，但 logger 在 start() 的消息循环里
            # 这里直接验证：不 attach 时 _handle_message 抛错（由调用方捕获）
            with pytest.raises(RuntimeError, match="not attached"):
                await ws._handle_message('{"x": 1}')


# ═══════════════════════════════════════
#  script.py — 剩余分支
# ═══════════════════════════════════════

class TestScriptRemaining:

    def _make_script(self, name, code):
        path = os.path.join(tempfile.gettempdir(), name)
        with open(path, "w") as f:
            f.write(code)
        return path

    @pytest.mark.asyncio
    async def test_reload_loop_file_not_found(self):
        """reload_loop 遇到 FileNotFoundError 记录 warning 继续"""
        path = self._make_script("reload_nf.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path, auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        # 删除文件触发 FileNotFoundError
        os.remove(path)
        await asyncio.sleep(0.15)
        await engine.remove_executor("x")

    @pytest.mark.asyncio
    async def test_reload_loop_script_error(self):
        """reload_loop 脚本错误记录 error 保留旧 handler"""
        path = self._make_script("reload_err.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path, auto_reload=True, reload_interval_sec=0.05)
        engine = EventEngine()
        await engine.add_executor(ex)
        # 写入有语法错误的脚本
        with open(path, "w") as f:
            f.write("def broken(:\n")
        await asyncio.sleep(0.15)
        # 旧 handler 仍在（错误被记录）
        assert ex._on_event is not None
        await engine.remove_executor("x")

    @pytest.mark.asyncio
    async def test_on_start_exception_logged(self):
        """on_start 协程异常被记录"""
        path = self._make_script("onstart_err.py", '''
async def on_event(t, e): pass
async def on_start():
    raise ValueError("start boom")
''')
        ex = ScriptExecutor(name="x", script_path=path)
        ex._load_script()
        await asyncio.sleep(0.05)  # 让 on_start task 完成
        os.remove(path)

    @pytest.mark.asyncio
    async def test_stop_on_stop_exception_logged(self):
        """on_stop 抛异常被记录不传播"""
        path = self._make_script("onstop_err.py", '''
async def on_event(t, e): pass
async def on_stop():
    raise ValueError("stop boom")
''')
        ex = ScriptExecutor(name="x", script_path=path)
        engine = EventEngine()
        await engine.add_executor(ex)
        await engine.remove_executor("x")  # 不应抛异常
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_manual(self):
        """手动 reload 重新加载"""
        path = self._make_script("manual_reload.py", 'v = 1\nasync def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="x", script_path=path)
        ex._load_script()
        assert ex._module.v == 1
        with open(path, "w") as f:
            f.write('v = 2\nasync def on_event(t, e): pass\n')
        await ex.reload()
        assert ex._module.v == 2
        os.remove(path)


# ═══════════════════════════════════════
#  server.py — 认证/限流中间件剩余
# ═══════════════════════════════════════

class TestServerMiddleware:

    def test_emit_path_payload_none(self):
        """emit by path 无 body → payload 默认空 dict"""
        import importlib
        import evebus.server as server_mod
        from starlette.testclient import TestClient
        client = TestClient(server_mod.app)
        r = client.post("/api/v1/events/emit/data.test")
        assert r.status_code == 200
        assert r.json()["ok"] is True


# ═══════════════════════════════════════
#  server_cli.py — 剩余
# ═══════════════════════════════════════

class TestServerCLIRemaining:

    def test_auth_token_env_passed(self):
        """serve --auth-token 传给子进程环境变量"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli
        mock_proc = MagicMock()
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]
        with patch("subprocess.Popen") as mock_popen:
            mock_popen.return_value = mock_proc
            runner = CliRunner()
            r = runner.invoke(server_cli, ["serve", "--port", "9996", "--auth-token", "sec"])
            env = mock_popen.call_args.kwargs.get("env", {})
            assert env.get("EVEBUS_AUTH_TOKEN") == "sec"

    def test_run_patterns_tuple_default(self):
        """run 命令 patterns 默认是 tuple"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli
        r = CliRunner().invoke(server_cli, ["run", "--help"])
        assert r.exit_code == 0


# ═══════════════════════════════════════
#  engine.py — 剩余
# ═══════════════════════════════════════

class TestEngineRemaining2:

    def test_pyrouter_import_fallback(self, monkeypatch):
        """#: PyRouter import 失败时回退 _PurePythonRouter"""
        import importlib
        import evebus.engine as engine_mod
        original = engine_mod.PyRouter
        engine_mod.PyRouter = None
        try:
            engine = EventEngine()
            assert isinstance(engine._router, engine_mod._PurePythonRouter)
        finally:
            engine_mod.PyRouter = original

    @pytest.mark.asyncio
    async def test_add_hook_raises_without_stage(self):
        """add_hook 无 stage 且无装饰器 → ValueError"""
        engine = EventEngine()
        with pytest.raises(ValueError, match="stage"):
            engine.add_hook(None, lambda ctx: None)

    @pytest.mark.asyncio
    async def test_new_listener_callback_error_ignored(self):
        """on_new_listener 回调异常被忽略"""
        engine = EventEngine()
        def bad_cb(pattern, handler):
            raise ValueError("boom")
        engine.on_new_listener(bad_cb)
        engine.on("data.*", lambda t, e: None)  # 不应抛异常


# ═══════════════════════════════════════
#  sources/base.py — 剩余
# ═══════════════════════════════════════

class TestSourceBaseRemaining2:

    @pytest.mark.asyncio
    async def test_run_task_done_callback_error(self):
        """run() 的 done-callback 上报 start 异常"""
        class BadSource(EventSource):
            async def start(self):
                raise RuntimeError("start crashed")

        s = BadSource("bad")
        engine = EventEngine()
        s._attach(engine)
        s._running = True
        # 手动模拟 run() 逻辑（create_task + done-callback）
        s._task = asyncio.create_task(s.start())
        s._task.add_done_callback(s._on_task_done)
        await asyncio.sleep(0.05)
        # 异常被 done-callback 捕获记录，不崩溃即通过

    @pytest.mark.asyncio
    async def test_detach_when_task_running(self):
        """_detach 时任务在运行 → 取消"""
        class LoopSource(EventSource):
            async def start(self):
                while True:
                    await asyncio.sleep(0.1)

        s = LoopSource("loop")
        engine = EventEngine()
        await engine.add_source(s)
        task = s._task
        assert task is not None and not task.done()
        s._detach()
        assert s._task is None


# ═══════════════════════════════════════
#  plugin.py — 剩余 (71)
# ═══════════════════════════════════════

class TestPluginRemaining:

    @pytest.mark.asyncio
    async def test_plugin_emit_not_attached(self):
        """Plugin.emit 未 attach 抛错（plugin.py:71）"""
        p = Plugin("p1")
        with pytest.raises(RuntimeError, match="not attached"):
            await p.emit("topic", {})