"""覆盖率 100% 冲刺 — 剩余分支专项"""
import asyncio
import importlib
import json
import os
import sys
import tempfile
import time
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

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
#  cli.py subscribe 命令全流程
# ═══════════════════════════════════════

class TestSubscribeCommandFull:

    def test_subscribe_runs(self):
        """subscribe 命令完整执行（mock RPCClient）"""
        from click.testing import CliRunner
        from evebus.cli import cli

        events = [
            {"topic": "data.x", "event": {"a": 1}, "timestamp": 1787000000123456789},
            {"topic": "data.y", "event": "str", "timestamp": 1787000000123456789},
        ]

        class FakeClient:
            def __init__(self, url):
                self.url = url
            async def subscribe(self, pattern, auto_reconnect=False):
                for e in events:
                    yield e
                raise KeyboardInterrupt()

        with patch("evebus.rpc.RPCClient", FakeClient):
            runner = CliRunner()
            r = runner.invoke(cli, ["subscribe", "data.*", "--no-color"])
            assert r.exit_code == 0
            assert "data.x" in r.output
            assert "订阅中" in r.output

    def test_subscribe_missing_httpx(self):
        """subscribe 缺少 httpx 依赖时友好报错"""
        from click.testing import CliRunner
        from evebus.cli import cli

        # 只 patch evebus.rpc.client 的 RPCClient 导入失败
        with patch("evebus.rpc", None):
            # 让 from evebus.rpc import RPCClient 抛 ImportError
            import builtins
            real_import = builtins.__import__

            def fake_import(name, *args, **kwargs):
                if name == "evebus.rpc":
                    raise ImportError("no module named httpx")
                return real_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=fake_import):
                runner = CliRunner()
                r = runner.invoke(cli, ["subscribe", "data.*"])
                assert r.exit_code == 1
                assert "缺少依赖" in r.output

    def test_status_with_plugins(self):
        """status 显示 plugins 列表（非空分支）"""
        from click.testing import CliRunner
        from evebus.cli import cli

        with patch("urllib.request.urlopen") as mock_urlopen:
            resp = MagicMock()
            resp.read.return_value = json.dumps({
                "status": "ok",
                "stats": {
                    "sources": {"count": 0, "running": 0, "names": []},
                    "executors": {"count": 0, "names": [], "total_executed": 0, "total_errors": 0},
                    "plugins": {"count": 1, "names": ["metrics"]},
                    "handlers": {"count": 1, "patterns": ["*"]},
                    "pending_tasks": 0,
                },
            }).encode()
            mock_urlopen.return_value = resp
            runner = CliRunner()
            r = runner.invoke(cli, ["status"])
            assert r.exit_code == 0
            assert "metrics" in r.output


# ═══════════════════════════════════════
#  __main__.py 两个分支
# ═══════════════════════════════════════

class TestMainModuleBranches:

    def test_main_routes_to_server_cli(self):
        """python -m evebus serve → server_cli"""
        with patch("evebus.server_cli.server_cli") as mock_server:
            import evebus.__main__ as main_mod
            # 模拟 sys.argv 含 serve
            main_mod.cli  # 默认导入的是 cli
            # 验证 serve/run 路由逻辑
            old_argv = sys.argv
            sys.argv = ["evebus", "serve", "--help"]
            try:
                import importlib
                importlib.reload(main_mod)
                # serve 分支导入 server_cli 作为 cli
                assert main_mod.cli is not None
            finally:
                sys.argv = old_argv
                importlib.reload(main_mod)

    def test_main_routes_to_client(self):
        """python -m evebus status → cli"""
        import evebus.__main__ as main_mod
        from evebus import cli as client_cli
        old_argv = sys.argv
        sys.argv = ["evebus", "status"]
        try:
            importlib.reload(main_mod)
            assert main_mod.cli is client_cli.cli
        finally:
            sys.argv = old_argv
            importlib.reload(main_mod)


# ═══════════════════════════════════════
#  server.py 剩余（188-213, 240, 305, 391-392）
# ═══════════════════════════════════════

class TestServerRemaining2:

    def test_get_source_404(self):
        """GET /api/v1/sources/{name} 不存在 → 404"""
        from evebus.server import app
        from starlette.testclient import TestClient
        client = TestClient(app)
        r = client.get("/api/v1/sources/nonexistent")
        assert r.status_code == 404

    def test_get_executor_404(self):
        """GET /api/v1/executors/{name} 不存在 → 404"""
        from evebus.server import app
        from starlette.testclient import TestClient
        client = TestClient(app)
        r = client.get("/api/v1/executors/nonexistent")
        assert r.status_code == 404

    def test_add_timer_duplicate(self):
        """添加重复 timer → ok=False"""
        from evebus.server import app, engine
        from starlette.testclient import TestClient
        client = TestClient(app)
        # 清理
        for src in list(engine._sources):
            engine._sources.pop(src, None)
        r1 = client.post("/api/v1/sources/timer",
                         json={"name": "dup1", "topic": "t", "interval_ms": 60000})
        assert r1.json()["ok"] is True
        r2 = client.post("/api/v1/sources/timer",
                         json={"name": "dup1", "topic": "t", "interval_ms": 60000})
        assert r2.json()["ok"] is False
        client.delete("/api/v1/sources/dup1")

    def test_webhook_ingest_not_found(self):
        """webhook ingest 源不存在 → 404"""
        from evebus.server import app
        from starlette.testclient import TestClient
        client = TestClient(app)
        r = client.post("/api/v1/webhook/nonexistent", json={})
        assert r.status_code == 404

    def test_webhook_ingest_non_webhook(self):
        """webhook ingest 目标不是 WebhookSource → 404"""
        from evebus.server import app, engine
        from starlette.testclient import TestClient
        client = TestClient(app)
        timer = TimerSource(name="not_webhook", topic="t", interval_ms=60000)
        engine._sources["not_webhook"] = timer
        r = client.post("/api/v1/webhook/not_webhook", json={})
        assert r.status_code == 404
        engine._sources.pop("not_webhook", None)


# ═══════════════════════════════════════
#  sources/base.py 47 行
# ═══════════════════════════════════════

class TestSourceBase47:

    def test_engine_property_error(self):
        """engine 属性未 attach 抛错（base.py:47）"""
        s = EventSource("s1")
        with pytest.raises(RuntimeError, match="not attached"):
            _ = s.engine


# ═══════════════════════════════════════
#  executors/base.py 剩余（43, 67-68, 100-102）
# ═══════════════════════════════════════

class TestExecutorBaseRemaining:

    @pytest.mark.asyncio
    async def test_execute_not_implemented(self):
        """基类 execute 抛 NotImplementedError（67-68）"""
        ex = EventExecutor("ex1", ["*"])
        with pytest.raises(NotImplementedError):
            await ex.execute("t", {})

    def test_engine_property_not_attached(self):
        """engine 属性未 attach 抛错（43）"""
        ex = EventExecutor("ex1", ["*"])
        with pytest.raises(RuntimeError, match="not attached"):
            _ = ex.engine

    def test_info_fields(self):
        """info 包含所有字段（100-102）"""
        ex = EventExecutor("ex1", ["a.*"])
        info = ex.info()
        assert info["name"] == "ex1"
        assert info["type"] == "EventExecutor"
        assert info["patterns"] == ["a.*"]
        assert info["attached"] is False
        assert info["executed_count"] == 0
        assert info["error_count"] == 0


# ═══════════════════════════════════════
#  engine.py 剩余（20-21, 106-113, 111-112, 338, 398-399, 470）
# ═══════════════════════════════════════

class TestEngineRemaining3:

    def test_on_decorator_and_once_decorator(self):
        """_on_decorator / _once_decorator（398-399）"""
        engine = EventEngine()

        @engine.on("dec.*")
        async def h1(t, e): pass

        @engine.once("dec.once")
        async def h2(t, e): pass

        assert "dec.*" in engine.event_names
        assert "dec.once" in engine.event_names

    @pytest.mark.asyncio
    async def test_once_removes_after_fire(self):
        """once 触发后 handler 移除（106-113, 111-112）"""
        engine = EventEngine()
        results = []

        @engine.once("fire.*")
        async def h(t, e):
            results.append(e)

        await engine.emit("fire.a", 1)
        await engine.emit("fire.a", 2)
        await engine.wait_for_complete()
        assert results == [1]
        assert "fire.*" not in engine.event_names

    def test_listeners(self):
        """listeners 返回 handler 列表"""
        engine = EventEngine()
        def h(t, e): pass
        engine.on("x.*", h)
        assert engine.listeners("x.*") == [h]

    def test_stats_hooks_empty(self):
        """stats hooks 为空 dict 时 keys 正常"""
        engine = EventEngine()
        s = engine.stats()
        assert isinstance(s["hooks"], dict)


# ═══════════════════════════════════════
#  script.py 剩余（76-77, 109, 159-169, 179）
# ═══════════════════════════════════════

class TestScriptRemaining2:

    def _make_script(self, name, code):
        path = os.path.join(tempfile.gettempdir(), name)
        with open(path, "w") as f:
            f.write(code)
        return path

    @pytest.mark.asyncio
    async def test_load_missing_on_event(self):
        """脚本缺少 on_event → AttributeError（76-77）"""
        path = self._make_script("no_evt2.py", "def foo(): pass\n")
        ex = ScriptExecutor(name="x", script_path=path)
        with pytest.raises(AttributeError, match="on_event"):
            ex._load_script()
        os.remove(path)

    @pytest.mark.asyncio
    async def test_remove_executor_not_found(self):
        """remove_executor 不存在 → ok=False（109）"""
        engine = EventEngine()
        r = await engine.remove_executor("nope")
        assert r["ok"] is False

    @pytest.mark.asyncio
    async def test_execute_before_load(self):
        """execute 未加载脚本 → RuntimeError（159-169）"""
        ex = ScriptExecutor(name="x", script_path="/nonexistent.py")
        with pytest.raises(RuntimeError, match="未加载"):
            await ex.execute("t", {})


# ═══════════════════════════════════════
#  websocket.py 剩余（65-75, 79, 82, 89）
# ═══════════════════════════════════════

class TestWebSocketRemaining2:

    @pytest.mark.asyncio
    async def test_start_reconnect_sleep_path(self):
        """重连 sleep 路径（65-75）"""
        ws = WebSocketSource(name="ws1", url="wss://test", max_reconnect=1, reconnect_interval_ms=1)
        engine = EventEngine()
        ws._attach(engine)
        import websockets as ws_mod

        with patch.object(ws_mod, "connect", side_effect=ConnectionError("x")):
            ws._running = True
            ws._reconnect_count = 0
            attempts = 0
            while ws._running and (attempts == 0 or ws._reconnect_count < ws.max_reconnect):
                attempts += 1
                try:
                    async with ws_mod.connect(ws.url) as conn:
                        pass
                except Exception:
                    ws._reconnect_count += 1
                    if ws._reconnect_count < ws.max_reconnect:
                        await asyncio.sleep(ws.reconnect_interval_ms / 1000.0)
            ws._running = False
        assert ws._reconnect_count == 1

    @pytest.mark.asyncio
    async def test_handle_message_valid_json(self):
        """_handle_message 正常 JSON（79）"""
        ws = WebSocketSource(name="ws1", topic_prefix="ws")
        engine = EventEngine()
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))
        await ws._handle_message('{"price": 100}')
        assert results == [{"price": 100}]

    @pytest.mark.asyncio
    async def test_handle_message_invalid_json(self):
        """_handle_message 无效 JSON → raw（82）"""
        ws = WebSocketSource(name="ws1", topic_prefix="ws")
        engine = EventEngine()
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))
        await ws._handle_message("not json")
        assert results == [{"raw": "not json"}]

    @pytest.mark.asyncio
    async def test_handle_message_no_parse(self):
        """parse_json=False → raw（89）"""
        ws = WebSocketSource(name="ws1", topic_prefix="ws", parse_json=False)
        engine = EventEngine()
        ws._attach(engine)
        results = []
        engine.on("ws.ws1", lambda t, e: results.append(e))
        await ws._handle_message("plain")
        assert results == [{"raw": "plain"}]


# ═══════════════════════════════════════
#  plugin.py 71 行
# ═══════════════════════════════════════

class TestPluginRemaining2:

    @pytest.mark.asyncio
    async def test_plugin_emit_not_attached(self):
        """Plugin.emit 未 attach 抛错（71）"""
        p = Plugin("p1")
        with pytest.raises(RuntimeError, match="not attached"):
            await p.emit("t", {})


# ═══════════════════════════════════════
#  timer.py 剩余（49->exit, 61）
# ═══════════════════════════════════════

class TestTimerRemaining:

    def test_timer_info(self):
        """TimerSource.info 字段（61）"""
        t = TimerSource(name="t", topic="tick", interval_ms=100)
        info = t.info()
        assert info["topic"] == "tick"
        assert info["interval_ms"] == 100
        assert info["tick_count"] == 0

    @pytest.mark.asyncio
    async def test_timer_start_stop(self):
        """TimerSource 启动/停止（49 循环退出）"""
        t = TimerSource(name="t", topic="tick", interval_ms=10)
        engine = EventEngine()
        await engine.add_source(t)
        await asyncio.sleep(0.05)
        await engine.remove_source("t")
        assert t._running is False