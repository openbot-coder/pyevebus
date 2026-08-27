"""补充测试 — 脚本执行器 reload/stop、Hook 装饰器、Source 生命周期、Plugin 边界"""
import asyncio
import os
import tempfile
import pytest
from evebus import EventEngine, Plugin
from evebus.executors import ScriptExecutor
from evebus.sources import TimerSource, EventSource
from evebus.hooks import HookStage, HookResult, HookContext, hook
from evebus.plugin import Plugin as PluginBase


@pytest.fixture
def engine():
    return EventEngine()


# ═══════════════════════════════════════
#  ScriptExecutor 深度测试
# ═══════════════════════════════════════

class TestScriptExecutorDeep:

    def _make_script(self, name, code):
        path = os.path.join(tempfile.gettempdir(), name)
        with open(path, "w") as f:
            f.write(code)
        return path

    @pytest.mark.asyncio
    async def test_reload_script(self, engine):
        path = self._make_script("reload.py", "async def on_event(t, e): pass\n")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["test.*"])
        await engine.add_executor(ex)
        # 修改脚本
        with open(path, "w") as f:
            f.write("async def on_event(t, e): pass\nasync def on_start(): pass\n")
        await engine.reload_executor("ex1")
        assert ex._module is not None
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_stop_executor(self, engine):
        path = self._make_script("stop.py", "async def on_event(t, e): pass\n")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["test.*"])
        await engine.add_executor(ex)
        await engine.remove_executor("ex1")
        assert ex._attached is False
        os.remove(path)

    @pytest.mark.asyncio
    async def test_script_with_on_stop(self, engine):
        path = self._make_script("withstop.py", """
stopped = False
async def on_event(t, e): pass
def on_stop():
    global stopped
    stopped = True
""")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        await engine.add_executor(ex)
        await engine.remove_executor("ex1")
        # on_stop 在模块级别被调用
        assert ex._module.stopped is True
        os.remove(path)

    @pytest.mark.asyncio
    async def test_script_async_on_start(self, engine):
        path = self._make_script("asyncstart.py", """
started = False
async def on_event(t, e): pass
async def on_start():
    global started
    started = True
""")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        await engine.add_executor(ex)
        await asyncio.sleep(0.05)  # 给 async on_start 执行时间
        assert ex._module.started is True
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_script_async_on_stop(self, engine):
        path = self._make_script("asyncstop.py", """
stopped = False
async def on_event(t, e): pass
async def on_stop():
    global stopped
    stopped = True
""")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        await engine.add_executor(ex)
        await engine.remove_executor("ex1")
        await asyncio.sleep(0.05)
        assert ex._module.stopped is True
        os.remove(path)

    @pytest.mark.asyncio
    async def test_script_missing_on_event(self, engine):
        path = self._make_script("noevent.py", "def not_on_event(): pass\n")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["*"])
        r = await engine.add_executor(ex)
        assert r["ok"] is False
        assert "on_event" in r["error"]
        os.remove(path)

    @pytest.mark.asyncio
    async def test_reload_nonexistent(self, engine):
        r = await engine.reload_executor("nonexistent")
        assert r["ok"] is False

    def test_executor_info_fields(self, engine):
        path = self._make_script("info.py", "async def on_event(t, e): pass\n")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["a.*", "b.*"])
        info = ex.info()
        assert info["name"] == "ex1"
        assert info["type"] == "ScriptExecutor"
        assert info["patterns"] == ["a.*", "b.*"]
        assert info["attached"] is False
        assert info["script_path"] == os.path.abspath(path)
        os.remove(path)


# ═══════════════════════════════════════
#  Hook 装饰器
# ═══════════════════════════════════════

class TestHookDecorator:

    def test_hook_decorator(self):
        @hook(HookStage.PRE_EMIT)
        def my_hook(ctx):
            pass
        assert my_hook._hook_stage == HookStage.PRE_EMIT
        assert my_hook._hook_name == "my_hook"


# ═══════════════════════════════════════
#  Source 生命周期
# ═══════════════════════════════════════

class TestSourceLifecycle:

    @pytest.mark.asyncio
    async def test_source_not_attached_error(self):
        s = EventSource("s1")
        with pytest.raises(RuntimeError, match="not attached"):
            await s.emit("topic", {})

    def test_source_info_base(self):
        s = EventSource("s1")
        info = s.info()
        assert info["name"] == "s1"
        assert info["running"] is False

    @pytest.mark.asyncio
    async def test_source_run_and_stop(self, engine):
        timer = TimerSource(name="t1", topic="tick", interval_ms=50)
        await engine.add_source(timer)
        assert timer.running is True
        await engine.stop_source("t1")
        assert timer.running is False

    @pytest.mark.asyncio
    async def test_start_already_running(self, engine):
        timer = TimerSource(name="t1", topic="tick", interval_ms=50)
        await engine.add_source(timer)
        r = await engine.start_source("t1")
        assert r["ok"] is True
        assert "already running" in r.get("message", "")
        await engine.remove_source("t1")

    @pytest.mark.asyncio
    async def test_start_nonexistent(self, engine):
        r = await engine.start_source("nope")
        assert r["ok"] is False

    @pytest.mark.asyncio
    async def test_stop_nonexistent(self, engine):
        r = await engine.stop_source("nope")
        assert r["ok"] is False


# ═══════════════════════════════════════
#  Plugin 深度测试
# ═══════════════════════════════════════

class TestPluginDeep:

    @pytest.mark.asyncio
    async def test_plugin_remove_from_dict(self, engine):
        class P(Plugin):
            def __init__(self):
                super().__init__("p1")
        await engine.add_plugin(P())
        await engine.remove_plugin("p1")
        assert "p1" not in engine._plugins

    @pytest.mark.asyncio
    async def test_plugin_detach(self, engine):
        class P(Plugin):
            def __init__(self):
                super().__init__("p1")
        p = P()
        await engine.add_plugin(p)
        assert p._engine is not None
        await engine.remove_plugin("p1")
        assert p._engine is None

    @pytest.mark.asyncio
    async def test_plugin_on(self, engine):
        results = []
        class P(Plugin):
            def __init__(self):
                super().__init__("p1")
            def on_attach(self):
                self.on("data.*", lambda t, e: results.append(e))
        await engine.add_plugin(P())
        await engine.emit("data.x", 42)
        assert results == [42]

    @pytest.mark.asyncio
    async def test_plugin_once(self, engine):
        results = []
        class P(Plugin):
            def __init__(self):
                super().__init__("p1")
            def on_attach(self):
                self.once("data.*", lambda t, e: results.append(e))
        await engine.add_plugin(P())
        await engine.emit("data.x", 1)
        await engine.emit("data.x", 2)
        assert results == [1]


# ═══════════════════════════════════════
#  Engine 边界
# ═══════════════════════════════════════

class TestEngineEdge:

    @pytest.mark.asyncio
    async def test_emit_with_executor(self, engine):
        """测试 handler + executor 同时匹配"""
        handler_results = []

        @engine.on("test.*")
        async def handler(topic, event):
            handler_results.append(event)

        class DummyExecutor:
            def __init__(self):
                self.name = "dummy"
                self.patterns = ["test.*"]
                self._engine = None
                self._attached = False
                self._executed_count = 0
                self._error_count = 0
            def _attach(self, e): self._engine = e; self._attached = True
            def _detach(self): self._engine = None
            async def execute(self, t, e): self._executed_count += 1
            async def _safe_execute(self, t, e): await self.execute(t, e)
            def info(self): return {}

        ex = DummyExecutor()
        ex._attach(engine)
        engine._executor_handlers.setdefault("test.*", []).append(ex)

        await engine.emit("test.a", 1)
        await engine.wait_for_complete()
        assert handler_results == [1]
        assert ex._executed_count == 1

    @pytest.mark.asyncio
    async def test_on_new_listener(self, engine):
        registered = []
        engine.on_new_listener(lambda pattern, handler: registered.append(pattern))
        engine.on("data.*", lambda t, e: None)
        assert registered == ["data.*"]

    @pytest.mark.asyncio
    async def test_on_new_listener_exception(self, engine):
        def bad_callback(pattern, handler):
            raise ValueError("boom")
        engine.on_new_listener(bad_callback)
        # 不应该崩溃
        engine.on("data.*", lambda t, e: None)

    @pytest.mark.asyncio
    async def test_hook_stage_error(self, engine):
        results = []
        def error_hook(ctx):
            return HookResult.CONTINUE
        engine.add_hook(HookStage.ON_ERROR, error_hook)
        s = engine.stats()
        assert "on_error" in s.get("hooks", {})

    @pytest.mark.asyncio
    async def test_wait_for_complete_empty(self, engine):
        await engine.wait_for_complete()

    def test_cancel_empty(self, engine):
        engine.cancel()

    @pytest.mark.asyncio
    async def test_multiple_patterns_same_handler(self, engine):
        results = []
        def handler(topic, event):
            results.append(topic)
        engine.on("a.*", handler)
        engine.on("b.*", handler)
        await engine.emit("a.x", 1)
        await engine.emit("b.y", 2)
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_executor_detach_on_remove(self, engine):
        ex = ScriptExecutor(
            name="ex1",
            script_path=os.path.join(tempfile.gettempdir(), "_test.py"),
            patterns=["test.*"],
        )
        os.makedirs(os.path.dirname(ex.script_path), exist_ok=True)
        with open(ex.script_path, "w") as f:
            f.write("async def on_event(t, e): pass\n")
        await engine.add_executor(ex)
        assert ex._attached is True
        await engine.remove_executor("ex1")
        assert ex._attached is False
        os.remove(ex.script_path)