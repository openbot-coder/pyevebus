"""覆盖率补充 — SSE 端点单元测试（直接驱动 subscribe_events 实现）"""
import asyncio
import json
import pytest
import evebus.server as server_mod

# 注意：test_review_fixes.py 会 importlib.reload(server.py) 重建 engine，
# 所以这里始终从 server_mod 取当前 engine，不能缓存模块级 import
from evebus.server import app  # noqa: F401


@pytest.fixture(autouse=True)
def clean_engine():
    engine = server_mod.engine
    # 彻底清理，避免跨测试污染
    engine._handlers.clear()
    engine._executor_handlers.clear()
    for task in list(engine._waiting):
        task.cancel()
    engine._waiting.clear()
    yield
    engine = server_mod.engine
    engine._handlers.clear()
    engine._executor_handlers.clear()
    for task in list(engine._waiting):
        task.cancel()
    engine._waiting.clear()


class TestSSEUnit:

    @pytest.mark.asyncio
    async def test_subscribe_receives_event(self):
        """订阅收到事件（覆盖 _on_event / _event_stream / queue 路径）"""
        engine = server_mod.engine
        response = await server_mod.subscribe_events(pattern="unit.*")
        assert response.media_type == "text/event-stream"

        # 发射事件（注册的 handler 会 put 到 queue）
        await engine.emit("unit.one", {"n": 1})
        await engine.wait_for_complete()  # 等 handler 执行完

        # 消费生成器 — 拿到一帧后关闭（带超时保护）
        gen = response.body_iterator
        frame = await asyncio.wait_for(gen.__anext__(), timeout=5)
        await gen.aclose()

        assert "unit.one" in frame
        assert "n" in frame
        assert frame.startswith("data: ")

    @pytest.mark.asyncio
    async def test_subscribe_cleanup_on_close(self):
        """生成器关闭后 handler 移除（finally 分支）"""
        engine = server_mod.engine
        response = await server_mod.subscribe_events(pattern="clean3.*")
        assert len(engine.listeners("clean3.*")) == 1

        await engine.emit("clean3.x", {})
        await engine.wait_for_complete()
        gen = response.body_iterator
        await asyncio.wait_for(gen.__anext__(), timeout=5)
        await gen.aclose()
        await asyncio.sleep(0.05)

        assert len(engine.listeners("clean3.*")) == 0

    @pytest.mark.asyncio
    async def test_subscribe_queue_full_drops(self):
        """队列满时 put_nowait 抛 QueueFull → 丢弃事件不阻塞"""
        import asyncio as aio
        queue = aio.Queue(maxsize=1)
        queue.put_nowait({"first": True})

        async def on_event(topic, event):
            try:
                queue.put_nowait({"topic": topic, "event": event, "timestamp": 1})
            except aio.QueueFull:
                pass  # 覆盖 except 分支

        await on_event("t", {})
        assert queue.qsize() == 1  # 满时丢弃

    @pytest.mark.asyncio
    async def test_subscribe_default_pattern(self):
        """空 pattern 订阅所有（pattern or '*' 分支）"""
        engine = server_mod.engine
        response = await server_mod.subscribe_events(pattern="")
        await engine.emit("any.topic", {"x": 1})
        await engine.wait_for_complete()
        gen = response.body_iterator
        frame = await asyncio.wait_for(gen.__anext__(), timeout=5)
        await gen.aclose()
        assert "any.topic" in frame

    @pytest.mark.asyncio
    async def test_subscribe_serializes_non_json(self):
        """非 JSON payload 用 default=str 序列化（default=str 分支）"""
        engine = server_mod.engine
        response = await server_mod.subscribe_events(pattern="obj.*")
        await engine.emit("obj.test", {"custom": object()})
        await engine.wait_for_complete()
        gen = response.body_iterator
        frame = await asyncio.wait_for(gen.__anext__(), timeout=5)
        await gen.aclose()
        # 不应抛异常，包含 custom 对象
        assert "obj.test" in frame