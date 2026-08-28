"""覆盖率 100% 最终冲刺 — 剩余分支"""
import asyncio
import json
import os
import sys
import tempfile
import time
import pytest
from unittest.mock import patch, MagicMock

import httpx
from evebus import EventEngine, TimerSource
from evebus.rpc import RPCClient
import evebus.rpc.client as rpc_client_mod


# ═══════════════════════════════════════
#  rpc/client.py — list_executors & list_sources 失败路径
# ═══════════════════════════════════════

class TestRPCList:

    @pytest.mark.asyncio
    async def test_list_executors(self):
        """list_executors 正常返回（147-150）"""
        transport = httpx.ASGITransport(app=__import__("evebus.server", fromlist=["app"]).app)
        original = rpc_client_mod.httpx.AsyncClient

        class LocalClient(original):
            def __init__(self, *a, **kw):
                kw["transport"] = transport
                kw["base_url"] = "http://test"
                super().__init__(*a, **kw)

        rpc_client_mod.httpx.AsyncClient = LocalClient
        try:
            client = RPCClient("http://test")
            assert await client.list_executors() == []
            assert await client.list_sources() == []
        finally:
            rpc_client_mod.httpx.AsyncClient = original

    @pytest.mark.asyncio
    async def test_emit_success(self):
        """emit 正常返回（post 路径覆盖）"""
        transport = httpx.ASGITransport(app=__import__("evebus.server", fromlist=["app"]).app)
        original = rpc_client_mod.httpx.AsyncClient

        class LocalClient(original):
            def __init__(self, *a, **kw):
                kw["transport"] = transport
                kw["base_url"] = "http://test"
                super().__init__(*a, **kw)

        rpc_client_mod.httpx.AsyncClient = LocalClient
        try:
            client = RPCClient("http://test")
            result = await client.emit("data.test", {"x": 1})
            assert result["ok"] is True
        finally:
            rpc_client_mod.httpx.AsyncClient = original

    def test_all_exports(self):
        """__all__ 导出完整"""
        from evebus.rpc import RPCClient, RPCError, DEFAULT_URL
        assert RPCClient is not None
        assert RPCError is not None
        assert DEFAULT_URL == "http://localhost:8080"


# ═══════════════════════════════════════
#  cli.py — API 失败 sys.exit(1) 分支
# ═══════════════════════════════════════

class TestCLIExitBranches:

    @pytest.mark.parametrize("cmd", [
        ["sources", "list"],
        ["sources", "add-timer", "t1"],
        ["sources", "add-webhook", "w1"],
        ["sources", "start", "t1"],
        ["sources", "stop", "t1"],
        ["sources", "remove", "t1"],
        ["executors", "list"],
        ["executors", "reload", "e1"],
        ["executors", "remove", "e1"],
        ["plugins", "list"],
        ["plugins", "remove", "p1"],
    ])
    def test_api_failure_exits_nonzero(self, cmd):
        """API 失败时各命令非零退出"""
        from click.testing import CliRunner
        from evebus.cli import cli
        runner = CliRunner()
        with patch("evebus.cli._api_get", return_value=False), \
             patch("evebus.cli._api_post", return_value=False), \
             patch("evebus.cli._api_delete", return_value=False):
            r = runner.invoke(cli, cmd)
            assert r.exit_code == 1

    def test_executors_add_missing_script(self):
        """executors add 脚本不存在 → exit 1"""
        from click.testing import CliRunner
        from evebus.cli import cli
        runner = CliRunner()
        r = runner.invoke(cli, ["executors", "add", "e1", "-s", "/nonexistent"])
        assert r.exit_code == 1

    def test_executors_add_api_failure(self):
        """executors add API 失败 → exit 1"""
        from click.testing import CliRunner
        from evebus.cli import cli
        path = os.path.join(tempfile.gettempdir(), "cli_ex.py")
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\n")
        runner = CliRunner()
        with patch("evebus.cli._api_post", return_value=False):
            r = runner.invoke(cli, ["executors", "add", "e1", "-s", path])
            assert r.exit_code == 1
        os.remove(path)


# ═══════════════════════════════════════
#  engine.py — 剩余（20-21, 106-113, 111-112, 338, 398-399, 470）
# ═══════════════════════════════════════

class TestEngineFinal:

    def test_pyrouter_import_fallback(self, monkeypatch):
        """PyRouter import 失败回退 _PurePythonRouter（20-21）"""
        import evebus.engine as engine_mod
        original = engine_mod.PyRouter
        engine_mod.PyRouter = None
        try:
            engine = EventEngine()
            from evebus.engine import _PurePythonRouter
            assert isinstance(engine._router, _PurePythonRouter)
        finally:
            engine_mod.PyRouter = original

    @pytest.mark.asyncio
    async def test_emit_intercepted_by_hook(self):
        """hook INTERCEPTED 阻止分发（106-113）"""
        from evebus.hooks import HookStage, HookResult
        engine = EventEngine()
        results = []

        @engine.on("test.*")
        async def h(t, e):
            results.append(e)

        engine.add_hook(HookStage.PRE_EMIT, lambda ctx: HookResult.INTERCEPTED)
        handled = await engine.emit("test.a", 1)
        assert handled is False
        assert results == []

    @pytest.mark.asyncio
    async def test_emit_no_match(self):
        """无匹配 handler → handled False（111-112）"""
        engine = EventEngine()
        handled = await engine.emit("nobody.listens", {})
        assert handled is False

    def test_add_hook_raises_without_stage(self):
        """add_hook 无 stage 抛 ValueError（338）"""
        engine = EventEngine()
        with pytest.raises(ValueError, match="stage"):
            engine.add_hook(None, lambda ctx: None)


# ═══════════════════════════════════════
#  executors/base.py — 剩余（43, 67-68, 100-102）
# ═══════════════════════════════════════

class TestExecutorBaseFinal:

    def test_engine_property_error(self):
        """engine 属性未 attach 抛错（43）"""
        from evebus.executors import EventExecutor
        ex = EventExecutor("e1", ["*"])
        with pytest.raises(RuntimeError, match="not attached"):
            _ = ex.engine

    @pytest.mark.asyncio
    async def test_execute_not_implemented(self):
        """execute 未实现抛错（67-68）"""
        from evebus.executors import EventExecutor
        ex = EventExecutor("e1", ["*"])
        with pytest.raises(NotImplementedError):
            await ex.execute("t", {})

    def test_info_complete(self):
        """info 完整字段（100-102）"""
        from evebus.executors import EventExecutor
        ex = EventExecutor("e1", ["a.*"])
        info = ex.info()
        assert info["name"] == "e1"
        assert info["patterns"] == ["a.*"]
        assert info["attached"] is False
        assert info["executed_count"] == 0
        assert info["error_count"] == 0


# ═══════════════════════════════════════
#  script.py — 剩余（77-78, 110, 164, 167, 180）
# ═══════════════════════════════════════

class TestScriptFinal:

    def _make(self, name, code):
        path = os.path.join(tempfile.gettempdir(), name)
        with open(path, "w") as f:
            f.write(code)
        return path

    @pytest.mark.asyncio
    async def test_load_missing_on_event(self):
        """脚本无 on_event → AttributeError（77-78）"""
        from evebus.executors import ScriptExecutor
        path = self._make("no_evt3.py", "def foo(): pass\n")
        ex = ScriptExecutor(name="x", script_path=path)
        with pytest.raises(AttributeError, match="on_event"):
            ex._load_script()
        os.remove(path)

    @pytest.mark.asyncio
    async def test_add_duplicate_executor(self):
        """重复添加 executor → ok=False（110）"""
        from evebus.executors import ScriptExecutor
        path = self._make("dup_ex.py", 'async def on_event(t, e): pass\n')
        ex = ScriptExecutor(name="dup", script_path=path)
        engine = EventEngine()
        await engine.add_executor(ex)
        r = await engine.add_executor(ScriptExecutor(name="dup", script_path=path))
        assert r["ok"] is False
        await engine.remove_executor("dup")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_execute_before_load(self):
        """execute 未加载 → RuntimeError（180）"""
        from evebus.executors import ScriptExecutor
        ex = ScriptExecutor(name="x", script_path="/nonexistent.py")
        with pytest.raises(RuntimeError, match="未加载"):
            await ex.execute("t", {})


# ═══════════════════════════════════════
#  server_cli.py — 剩余（20,23,82,95-96,151,156-157,167）
# ═══════════════════════════════════════

class TestServerCLIFinal:

    def test_serve_auth_token_passed(self):
        """serve --auth-token 传给子进程（95-96）"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli
        mock_proc = MagicMock()
        mock_proc.wait.side_effect = [KeyboardInterrupt, None]
        with patch("subprocess.Popen") as mock_popen, patch("signal.signal"):
            mock_popen.return_value = mock_proc
            runner = CliRunner()
            r = runner.invoke(server_cli, ["serve", "--port", "9995", "--auth-token", "sec"])
            env = mock_popen.call_args.kwargs.get("env", {})
            assert env.get("EVEBUS_AUTH_TOKEN") == "sec"

    def test_serve_run_help(self):
        """run --help（167）"""
        from click.testing import CliRunner
        from evebus.server_cli import server_cli
        r = CliRunner().invoke(server_cli, ["run", "--help"])
        assert r.exit_code == 0


# ═══════════════════════════════════════
#  plugin.py — 71
# ═══════════════════════════════════════

class TestPluginFinal:

    @pytest.mark.asyncio
    async def test_plugin_emit_not_attached(self):
        """Plugin.emit 未 attach 抛错（71）"""
        from evebus import Plugin
        p = Plugin("p1")
        with pytest.raises(RuntimeError, match="not attached"):
            await p.emit("t", {})


# ═══════════════════════════════════════
#  sources/base.py — 47
# ═══════════════════════════════════════

class TestSourceBaseFinal:

    def test_engine_property_error(self):
        """engine 属性未 attach 抛错（47）"""
        from evebus.sources import EventSource
        s = EventSource("s1")
        with pytest.raises(RuntimeError, match="not attached"):
            _ = s.engine


# ═══════════════════════════════════════
#  timer.py — 49->exit, 61
# ═══════════════════════════════════════

class TestTimerFinal:

    def test_timer_info(self):
        """TimerSource.info（61）"""
        t = TimerSource(name="t", topic="tick", interval_ms=50)
        info = t.info()
        assert info["topic"] == "tick"
        assert info["interval_ms"] == 50
        assert info["tick_count"] == 0


# ═══════════════════════════════════════
#  __main__.py — 16
# ═══════════════════════════════════════

class TestMainFinal:

    def test_main_module(self):
        """__main__.py 可导入（16）"""
        import importlib
        mod = importlib.import_module("evebus.__main__")
        assert mod.cli is not None