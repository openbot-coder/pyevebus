"""EveBus 完整测试套件"""
import asyncio
import os
import tempfile
import pytest
from evebus import (
    EventEngine,
    HookStage,
    HookResult,
    HookContext,
    Plugin,
    TimerSource,
    WebhookSource,
    ScriptExecutor,
)


@pytest.fixture
def engine():
    return EventEngine()


# ═══════════════════════════════════════
#  通配符匹配
# ═══════════════════════════════════════

class TestWildcardMatching:

    @pytest.mark.asyncio
    async def test_exact_match(self, engine):
        results = []
        engine.on("data.quotes.BINANCE.ETHUSDT", lambda t, e: results.append(t))
        await engine.emit("data.quotes.BINANCE.ETHUSDT", {})
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_star_wildcard(self, engine):
        results = []
        engine.on("data.*", lambda t, e: results.append(t))
        await engine.emit("data.quotes.BINANCE.ETHUSDT", {})
        await engine.emit("data.trades.OKX.BTCUSDT", {})
        await engine.emit("other.topic", {})
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_question_wildcard(self, engine):
        results = []
        engine.on("a?c", lambda t, e: results.append("q"))
        await engine.emit("abc", {})
        await engine.emit("abbc", {})
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_multi_star(self, engine):
        results = []
        engine.on("*", lambda t, e: results.append("star"))
        engine.on("data.*.ETHUSDT", lambda t, e: results.append("eth"))
        await engine.emit("data.quotes.BINANCE.ETHUSDT", {})
        assert results == ["star", "eth"]


# ═══════════════════════════════════════
#  on / once / off
# ═══════════════════════════════════════

class TestRegistration:

    @pytest.mark.asyncio
    async def test_on_decorator(self, engine):
        results = []
        @engine.on("test.*")
        async def handler(topic, event):
            results.append(event)
        await engine.emit("test.a", 1)
        await engine.wait_for_complete()
        assert results == [1]

    @pytest.mark.asyncio
    async def test_on_call(self, engine):
        results = []
        def handler(topic, event):
            results.append(event)
        engine.on("test.*", handler)
        await engine.emit("test.a", 1)
        assert results == [1]

    @pytest.mark.asyncio
    async def test_once_decorator(self, engine):
        results = []
        @engine.once("start")
        async def handler(topic, event):
            results.append(event)
        await engine.emit("start", 1)
        await engine.emit("start", 2)
        await engine.wait_for_complete()
        assert results == [1]

    @pytest.mark.asyncio
    async def test_once_call(self, engine):
        results = []
        def handler(topic, event):
            results.append(event)
        engine.once("start", handler)
        await engine.emit("start", 1)
        await engine.emit("start", 2)
        assert results == [1]

    @pytest.mark.asyncio
    async def test_off_specific(self, engine):
        results = []
        def h1(topic, event): results.append("h1")
        def h2(topic, event): results.append("h2")
        engine.on("test.*", h1)
        engine.on("test.*", h2)
        engine.off("test.*", h1)
        await engine.emit("test.a", {})
        assert results == ["h2"]

    @pytest.mark.asyncio
    async def test_off_all(self, engine):
        engine.on("test.*", lambda t, e: None)
        engine.on("test.*", lambda t, e: None)
        engine.off("test.*")
        assert engine.listeners("test.*") == []

    @pytest.mark.asyncio
    async def test_listeners(self, engine):
        def h1(t, e): pass
        def h2(t, e): pass
        engine.on("test.*", h1)
        engine.on("test.*", h2)
        assert len(engine.listeners("test.*")) == 2

    @pytest.mark.asyncio
    async def test_event_names(self, engine):
        engine.on("data.*", lambda t, e: None)
        engine.on("test.*", lambda t, e: None)
        assert set(engine.event_names) == {"data.*", "test.*"}


# ═══════════════════════════════════════
#  Hook 系统
# ═══════════════════════════════════════

class TestHooks:

    @pytest.mark.asyncio
    async def test_pre_emit_intercept(self, engine):
        results = []
        engine.add_hook(HookStage.PRE_EMIT, lambda ctx: HookResult.INTERCEPTED)
        engine.on("test.*", lambda t, e: results.append(e))
        await engine.emit("test.a", 1)
        assert results == []

    @pytest.mark.asyncio
    async def test_pre_emit_modify(self, engine):
        results = []
        def enricher(ctx):
            ctx.payload["x"] = 1
            return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, enricher)
        engine.on("test.*", lambda t, e: results.append(e))
        await engine.emit("test.a", {})
        assert results[0]["x"] == 1

    @pytest.mark.asyncio
    async def test_pre_emit_topic_modify(self, engine):
        results = []
        def rewriter(ctx):
            ctx.topic = "rewritten.topic"
            return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, rewriter)
        engine.on("rewritten.topic", lambda t, e: results.append(t))
        await engine.emit("test.a", {})
        assert results == ["rewritten.topic"]

    @pytest.mark.asyncio
    async def test_post_emit_runs_after(self, engine):
        order = []
        engine.on("test.*", lambda t, e: order.append("handler"))
        def post(ctx):
            order.append("post")
            return HookResult.CONTINUE
        engine.add_hook(HookStage.POST_EMIT, post)
        await engine.emit("test.a", {})
        await engine.wait_for_complete()
        assert order == ["handler", "post"]

    @pytest.mark.asyncio
    async def test_hook_chain_order(self, engine):
        order = []
        def h1(ctx): order.append("h1"); return HookResult.CONTINUE
        def h2(ctx): order.append("h2"); return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, h1)
        engine.add_hook(HookStage.PRE_EMIT, h2)
        await engine.emit("test.a", {})
        assert order == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_hook_exception_swallowed(self, engine):
        def bad_hook(ctx): raise ValueError("boom")
        engine.add_hook(HookStage.PRE_EMIT, bad_hook)
        results = []
        engine.on("test.*", lambda t, e: results.append(e))
        await engine.emit("test.a", 1)
        assert results == [1]

    @pytest.mark.asyncio
    async def test_remove_hook(self, engine):
        order = []
        def hook(ctx): order.append("hook"); return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, hook)
        await engine.emit("test.a", {})
        assert order == ["hook"]
        engine.remove_hook(HookStage.PRE_EMIT, hook)
        order.clear()
        await engine.emit("test.b", {})
        assert order == []

    @pytest.mark.asyncio
    async def test_hook_context_metadata(self, engine):
        def transfer(ctx): ctx.metadata["key"] = "value"; return HookResult.CONTINUE
        def check(ctx):
            assert ctx.metadata.get("key") == "value"
            return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, transfer)
        engine.add_hook(HookStage.PRE_EMIT, check)
        await engine.emit("test.a", {})

    @pytest.mark.asyncio
    async def test_hook_context_source(self, engine):
        def check(ctx):
            assert ctx.source == "my_source"
            return HookResult.CONTINUE
        engine.add_hook(HookStage.PRE_EMIT, check)
        await engine.emit("test.a", {}, source="my_source")


# ═══════════════════════════════════════
#  Source 管理
# ═══════════════════════════════════════

class TestSources:

    @pytest.mark.asyncio
    async def test_add_remove_source(self, engine):
        timer = TimerSource(name="t1", topic="tick", interval_ms=100)
        await engine.add_source(timer)
        assert "t1" in [s["name"] for s in engine.list_sources()]
        await engine.remove_source("t1")
        assert "t1" not in [s["name"] for s in engine.list_sources()]

    @pytest.mark.asyncio
    async def test_duplicate_source_rejected(self, engine):
        t1 = TimerSource(name="t1", topic="tick", interval_ms=100)
        t2 = TimerSource(name="t1", topic="tick2", interval_ms=200)
        r1 = await engine.add_source(t1)
        r2 = await engine.add_source(t2)
        assert r1["ok"] is True
        assert r2["ok"] is False

    @pytest.mark.asyncio
    async def test_remove_nonexistent_source(self, engine):
        r = await engine.remove_source("nope")
        assert r["ok"] is False

    @pytest.mark.asyncio
    async def test_source_emits_to_engine(self, engine):
        results = []
        engine.on("tick", lambda t, e: results.append(e))
        timer = TimerSource(name="t1", topic="tick", interval_ms=30)
        await engine.add_source(timer)
        await asyncio.sleep(0.3)
        await engine.remove_source("t1")
        assert len(results) >= 3
        assert results[0].get("tick", 0) >= 1

    @pytest.mark.asyncio
    async def test_start_stop_source(self, engine):
        timer = TimerSource(name="t1", topic="tick", interval_ms=100)
        await engine.add_source(timer)
        assert engine.get_source("t1").running is True
        await engine.stop_source("t1")
        assert engine.get_source("t1").running is False

    @pytest.mark.asyncio
    async def test_webhook_source(self, engine):
        results = []
        engine.on("wh.*", lambda t, e: results.append(e))
        wh = WebhookSource(name="wh1", path="/ingest", topic_prefix="wh")
        await engine.add_source(wh)
        await wh.ingest({"data": 123})
        assert results == [{"data": 123}]

    @pytest.mark.asyncio
    async def test_source_info(self, engine):
        timer = TimerSource(name="t1", topic="tick", interval_ms=100)
        info = timer.info()
        assert info["name"] == "t1"
        assert info["type"] == "TimerSource"
        assert info["topic"] == "tick"


# ═══════════════════════════════════════
#  Executor 管理
# ═══════════════════════════════════════

class TestExecutors:

    def _make_script(self, name="test_strategy.py", code=None):
        if code is None:
            code = 'async def on_event(topic: str, payload: dict):\n    pass\n'
        path = os.path.join(tempfile.gettempdir(), name)
        with open(path, "w") as f:
            f.write(code)
        return path

    @pytest.mark.asyncio
    async def test_add_remove_executor(self, engine):
        path = self._make_script("ex1.py")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["test.*"])
        r = await engine.add_executor(ex)
        assert r["ok"] is True
        assert "ex1" in [e["name"] for e in engine.list_executors()]
        r = await engine.remove_executor("ex1")
        assert r["ok"] is True
        os.remove(path)

    @pytest.mark.asyncio
    async def test_duplicate_executor_rejected(self, engine):
        path = self._make_script("ex_dup.py")
        e1 = ScriptExecutor(name="ex1", script_path=path)
        e2 = ScriptExecutor(name="ex1", script_path=path)
        await engine.add_executor(e1)
        r = await engine.add_executor(e2)
        assert r["ok"] is False
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_executor_executes(self, engine):
        code = 'async def on_event(topic, payload):\n    pass\n'
        path = self._make_script("ex_exec.py", code)
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["test.*"])
        await engine.add_executor(ex)
        # 检查 executor 已正确加载模块
        assert ex._module is not None
        assert ex._on_event is not None
        # 发射事件 — 应该不报错
        await engine.emit("test.hello", {"key": "value"})
        await engine.wait_for_complete()
        assert ex._executed_count >= 1
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_executor_missing_script(self, engine):
        ex = ScriptExecutor(name="bad", script_path="/nonexistent/script.py")
        r = await engine.add_executor(ex)
        assert r["ok"] is False
        assert "not found" in r["error"] or "不存在" in r["error"]

    @pytest.mark.asyncio
    async def test_executor_reload(self, engine):
        path = self._make_script("ex_reload.py", "async def on_event(topic, payload): pass\n")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["test.*"])
        await engine.add_executor(ex)
        r = await engine.reload_executor("ex1")
        assert r["ok"] is True
        await engine.remove_executor("ex1")
        os.remove(path)

    @pytest.mark.asyncio
    async def test_executor_info(self, engine):
        path = self._make_script("ex_info.py")
        ex = ScriptExecutor(name="ex1", script_path=path, patterns=["test.*"])
        info = ex.info()
        assert info["name"] == "ex1"
        assert info["patterns"] == ["test.*"]
        assert info["script_path"] == os.path.abspath(path)
        os.remove(path)


# ═══════════════════════════════════════
#  Plugin 系统
# ═══════════════════════════════════════

class TestPlugins:

    @pytest.mark.asyncio
    async def test_add_remove_plugin(self, engine):
        class MyPlugin(Plugin):
            def __init__(self):
                super().__init__("p1")
                self.attached = False
                self.detached = False
            def on_attach(self): self.attached = True
            def on_detach(self): self.detached = True
        p = MyPlugin()
        await engine.add_plugin(p)
        assert p.attached is True
        assert "p1" in engine.list_plugins()
        await engine.remove_plugin("p1")
        assert p.detached is True
        assert "p1" not in engine.list_plugins()

    @pytest.mark.asyncio
    async def test_plugin_handler(self, engine):
        results = []
        class MyPlugin(Plugin):
            def __init__(self):
                super().__init__("p1")
            def on_attach(self):
                @self.on("data.*")
                async def on_data(topic, event):
                    results.append(event)
        await engine.add_plugin(MyPlugin())
        await engine.emit("data.hello", 42)
        await engine.wait_for_complete()
        assert results == [42]

    @pytest.mark.asyncio
    async def test_plugin_emit(self, engine):
        results = []
        engine.on("plugin.*", lambda t, e: results.append(e))
        class MyPlugin(Plugin):
            def __init__(self):
                super().__init__("p1")
            async def trigger(self):
                await self.emit("plugin.out", {"from": "plugin"})
        p = MyPlugin()
        await engine.add_plugin(p)
        await p.trigger()
        await engine.wait_for_complete()
        assert results == [{"from": "plugin"}]

    @pytest.mark.asyncio
    async def test_duplicate_plugin_rejected(self, engine):
        class P(Plugin):
            def __init__(self, name):
                super().__init__(name)
        await engine.add_plugin(P("p1"))
        r = await engine.add_plugin(P("p1"))
        assert r["ok"] is False

    @pytest.mark.asyncio
    async def test_remove_nonexistent_plugin(self, engine):
        r = await engine.remove_plugin("nope")
        assert r["ok"] is False

    @pytest.mark.asyncio
    async def test_plugin_not_attached_error(self):
        p = Plugin("p1")
        with pytest.raises(RuntimeError, match="not attached"):
            _ = p.engine


# ═══════════════════════════════════════
#  异步行为
# ═══════════════════════════════════════

class TestAsyncBehavior:

    @pytest.mark.asyncio
    async def test_wait_for_complete(self, engine):
        results = []
        async def slow_handler(topic, event):
            await asyncio.sleep(0.1)
            results.append(event)
        engine.on("test.*", slow_handler)
        await engine.emit("test.a", 1)
        assert len(results) == 0
        await engine.wait_for_complete()
        assert results == [1]

    @pytest.mark.asyncio
    async def test_cancel(self, engine):
        results = []
        async def slow_handler(topic, event):
            await asyncio.sleep(5)
            results.append(event)
        engine.on("test.*", slow_handler)
        await engine.emit("test.a", 1)
        engine.cancel()
        await asyncio.sleep(0.05)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_complete_property(self, engine):
        assert engine.complete is True
        async def slow(topic, event):
            await asyncio.sleep(0.3)
        engine.on("test.*", slow)
        await engine.emit("test.a", {})
        assert engine.complete is False
        await engine.wait_for_complete()
        assert engine.complete is True

    @pytest.mark.asyncio
    async def test_handler_exception_emits_error(self, engine):
        errors = []
        engine.on("error", lambda t, e: errors.append(e))
        @engine.on("test.*")
        async def bad_handler(topic, event):
            raise ValueError("test error")
        await engine.emit("test.a", {})
        await asyncio.sleep(0.1)
        assert len(errors) >= 1


# ═══════════════════════════════════════
#  统计
# ═══════════════════════════════════════

class TestStats:

    def test_empty_stats(self, engine):
        s = engine.stats()
        assert s["sources"]["count"] == 0
        assert s["executors"]["count"] == 0
        assert s["plugins"]["count"] == 0
        assert s["handlers"]["count"] == 0
        assert s["pending_tasks"] == 0

    @pytest.mark.asyncio
    async def test_stats_with_handlers(self, engine):
        engine.on("a.*", lambda t, e: None)
        engine.on("b.*", lambda t, e: None)
        s = engine.stats()
        assert s["handlers"]["count"] == 2
        assert "a.*" in s["handlers"]["patterns"]
        assert "b.*" in s["handlers"]["patterns"]

    @pytest.mark.asyncio
    async def test_stats_with_hooks(self, engine):
        engine.add_hook(HookStage.PRE_EMIT, lambda ctx: None)
        engine.add_hook(HookStage.POST_EMIT, lambda ctx: None)
        s = engine.stats()
        assert s["hooks"]["pre_emit"] == 1
        assert s["hooks"]["post_emit"] == 1


# ═══════════════════════════════════════
#  边界情况
# ═══════════════════════════════════════

class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_emit_empty_topic(self, engine):
        engine.on("*", lambda t, e: None)
        handled = await engine.emit("", {})
        assert handled is True

    @pytest.mark.asyncio
    async def test_emit_none_payload(self, engine):
        results = []
        engine.on("test.*", lambda t, e: results.append(e))
        await engine.emit("test.a", None)
        assert results == [None]

    @pytest.mark.asyncio
    async def test_many_handlers(self, engine):
        count = 0
        def counter(t, e):
            nonlocal count
            count += 1
        for i in range(100):
            engine.on(f"test.{i}", counter)
        await engine.emit("test.50", {})
        assert count == 1

    @pytest.mark.asyncio
    async def test_concurrent_emit(self, engine):
        results = []
        async def handler(topic, event):
            await asyncio.sleep(0.01)
            results.append(event)
        engine.on("test.*", handler)
        tasks = [engine.emit("test.a", i) for i in range(10)]
        await asyncio.gather(*tasks)
        await engine.wait_for_complete()
        assert len(results) == 10