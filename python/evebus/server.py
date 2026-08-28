"""
EventEngine HTTP API — 管理接口

基于 FastAPI，提供 Source/Executor/Plugin 实时管理。

启动: python -m evebus.server
或:   uvicorn evebus.server:app --host 0.0.0.0 --port 8080
"""

import asyncio
import hmac
import os
import sys
import json
import time
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

try:
    from fastapi import FastAPI, HTTPException, Body, Query, Request
    from fastapi.responses import JSONResponse, StreamingResponse
    from pydantic import BaseModel, Field
except ImportError:
    raise ImportError(
        "需要安装 FastAPI: pip install fastapi uvicorn"
    )

# 确保可以 import evebus
sys.path.insert(0, str(Path(__file__).parent.parent))

from evebus.engine import EventEngine
from evebus.sources import TimerSource, WebhookSource
from evebus.executors import ScriptExecutor
from evebus.plugin import Plugin

logger = logging.getLogger("evebus.server")

# ══════════════════════════════════════
#  配置（环境变量）
# ══════════════════════════════════════

# #30: 认证 token — 设置 EVEBUS_AUTH_TOKEN 后，所有管理端点需要 X-Auth-Token
AUTH_TOKEN = os.environ.get("EVEBUS_AUTH_TOKEN", "")
# #31: 请求体大小上限（字节），默认 1MB
MAX_BODY_BYTES = int(os.environ.get("EVEBUS_MAX_BODY_BYTES", str(1024 * 1024)))
# #33: 多 worker 下引擎状态分裂 — 显式拒绝
if os.environ.get("EVEBUS_ALLOW_MULTI_WORKER", "0") == "0":
    _worker_env = os.environ.get("WEB_CONCURRENCY", "")
    if _worker_env not in ("", "1"):
        raise RuntimeError(
            "EVEBUS 引擎是进程内单例：不支持多 worker（WEB_CONCURRENCY>1 会导致状态分裂）。"
            "如需横向扩展，请使用独立进程 + 外部存储。"
        )


# ══════════════════════════════════════
#  创建 Engine 和 App
# ══════════════════════════════════════

engine = EventEngine()

app = FastAPI(
    title="EventEngine API",
    description="异步事件引擎管理接口 — 实时添加 Source/Executor/Plugin",
    version="0.3.0",
)


# ══════════════════════════════════════
#  认证中间件（#30）— 保护所有 /api/v1 端点
# ══════════════════════════════════════

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if AUTH_TOKEN:
        path = request.url.path
        # 健康检查无需认证
        if path == "/api/v1/health":
            return await call_next(request)
        token = request.headers.get("X-Auth-Token", "")
        if not hmac.compare_digest(token, AUTH_TOKEN):
            return JSONResponse(
                {"ok": False, "error": "unauthorized"},
                status_code=401,
            )
    return await call_next(request)


# ══════════════════════════════════════
#  请求体大小限制中间件（#31）
# ══════════════════════════════════════

@app.middleware("http")
async def body_size_middleware(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and content_length.isdigit() and int(content_length) > MAX_BODY_BYTES:
        return JSONResponse(
            {"ok": False, "error": f"body too large (max {MAX_BODY_BYTES} bytes)"},
            status_code=413,
        )
    return await call_next(request)


# ══════════════════════════════════════
#  Pydantic 模型
# ══════════════════════════════════════

class EmitRequest(BaseModel):
    topic: str
    payload: Dict[str, Any] = {}
    source: str = ""


class TimerSourceRequest(BaseModel):
    name: str = "timer"
    topic: str = "timer.tick"
    interval_ms: int = 1000
    payload: Dict[str, Any] = {}


class WebhookSourceRequest(BaseModel):
    name: str = "webhook"
    path: str = "/events/ingest"
    topic_prefix: str = "webhook"


class ScriptExecutorRequest(BaseModel):
    name: str
    script_path: str
    patterns: List[str] = ["*"]
    auto_reload: bool = False


class SourceResponse(BaseModel):
    ok: bool
    error: Optional[str] = None
    source: Optional[Dict] = None


class ExecutorResponse(BaseModel):
    ok: bool
    error: Optional[str] = None
    executor: Optional[Dict] = None


class EmitResponse(BaseModel):
    ok: bool
    topic: str
    handled: bool


# ══════════════════════════════════════
#  Event API
# ══════════════════════════════════════

@app.post("/api/v1/events/emit", response_model=EmitResponse)
async def emit_event(req: EmitRequest):
    """发射事件到引擎"""
    handled = await engine.emit(req.topic, req.payload, source=req.source)
    return EmitResponse(ok=True, topic=req.topic, handled=handled)


@app.post("/api/v1/events/emit/{topic:path}")
async def emit_event_path(
    topic: str,
    payload: Optional[Dict[str, Any]] = Body(default=None),
):
    """通过 URL path 发射事件"""
    handled = await engine.emit(topic, payload or {})
    return EmitResponse(ok=True, topic=topic, handled=handled)


# ══════════════════════════════════════
#  SSE 订阅（RPC 流式推送）
# ══════════════════════════════════════

@app.get("/api/v1/events/subscribe")
async def subscribe_events(pattern: str = Query("", description="订阅的通配符 pattern，如 data.*.ETHUSDT")):
    """
    流式订阅事件（Server-Sent Events）

    通过 SSE 长连接持续推送匹配 pattern 的事件。任意语言/工具可消费：
      curl -N "http://host:8080/api/v1/events/subscribe?pattern=data.*"

    每个连接独立 Queue（背压上限 1024），断开自动注销 handler。
    """
    from fastapi.responses import StreamingResponse

    queue: asyncio.Queue = asyncio.Queue(maxsize=1024)

    async def _on_event(topic: str, event: Any):
        # #32: 队列满时用 put_nowait 丢弃（慢消费者不阻塞引擎），避免 handler 泄漏
        try:
            queue.put_nowait({
                "topic": topic,
                "event": event,
                "timestamp": time.time_ns(),
            })
        except asyncio.QueueFull:
            logger.warning("SSE subscriber queue full, dropping event: %s", topic)

    engine.on(pattern or "*", _on_event)

    async def _event_stream():
        try:
            while True:
                item = await queue.get()
                yield f"data: {json.dumps(item, ensure_ascii=False, default=str)}\n\n"
        finally:
            engine.off(pattern or "*", _on_event)

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ══════════════════════════════════════
#  Source API
# ══════════════════════════════════════

@app.get("/api/v1/sources")
async def list_sources():
    """列出所有事件源"""
    return {"sources": engine.list_sources()}


@app.get("/api/v1/sources/{name}")
async def get_source(name: str):
    """获取事件源详情"""
    source = engine.get_source(name)
    if not source:
        raise HTTPException(404, f"Source '{name}' not found")
    return source.info()


@app.post("/api/v1/sources/timer", response_model=SourceResponse)
async def add_timer_source(req: TimerSourceRequest):
    """添加定时器事件源"""
    source = TimerSource(
        name=req.name,
        topic=req.topic,
        interval_ms=req.interval_ms,
        payload=req.payload,
    )
    result = await engine.add_source(source)
    return SourceResponse(**result)


@app.post("/api/v1/sources/webhook", response_model=SourceResponse)
async def add_webhook_source(req: WebhookSourceRequest):
    """添加 Webhook 事件源"""
    source = WebhookSource(
        name=req.name,
        path=req.path,
        topic_prefix=req.topic_prefix,
    )
    result = await engine.add_source(source)
    return SourceResponse(**result)


@app.post("/api/v1/sources/{name}/start")
async def start_source(name: str):
    """启动事件源"""
    result = await engine.start_source(name)
    return result


@app.post("/api/v1/sources/{name}/stop")
async def stop_source(name: str):
    """停止事件源"""
    result = await engine.stop_source(name)
    return result


@app.delete("/api/v1/sources/{name}")
async def remove_source(name: str):
    """移除事件源"""
    result = await engine.remove_source(name)
    return result


# ══════════════════════════════════════
#  Executor API
# ══════════════════════════════════════

@app.get("/api/v1/executors")
async def list_executors():
    """列出所有执行器"""
    return {"executors": engine.list_executors()}


@app.get("/api/v1/executors/{name}")
async def get_executor(name: str):
    """获取执行器详情"""
    executor = engine.get_executor(name)
    if not executor:
        raise HTTPException(404, f"Executor '{name}' not found")
    return executor.info()


@app.post("/api/v1/executors/script", response_model=ExecutorResponse)
async def add_script_executor(req: ScriptExecutorRequest):
    """添加脚本执行器（动态加载）"""
    executor = ScriptExecutor(
        name=req.name,
        script_path=req.script_path,
        patterns=req.patterns,
        auto_reload=req.auto_reload,
    )
    result = await engine.add_executor(executor)
    return ExecutorResponse(**result)


@app.post("/api/v1/executors/{name}/reload")
async def reload_executor(name: str):
    """重新加载脚本执行器"""
    result = await engine.reload_executor(name)
    return result


@app.delete("/api/v1/executors/{name}")
async def remove_executor(name: str):
    """移除执行器"""
    result = await engine.remove_executor(name)
    return result


# ══════════════════════════════════════
#  Plugin API
# ══════════════════════════════════════

@app.get("/api/v1/plugins")
async def list_plugins():
    """列出所有插件"""
    return {"plugins": engine.list_plugins()}


@app.delete("/api/v1/plugins/{name}")
async def remove_plugin(name: str):
    """移除插件"""
    result = await engine.remove_plugin(name)
    return result


# ══════════════════════════════════════
#  Webhook Source 注入端点
# ══════════════════════════════════════

@app.post("/api/v1/webhook/{source_name}")
async def webhook_ingest(source_name: str, payload: Dict[str, Any] = Body({})):
    """向指定 webhook source 注入数据"""
    source = engine.get_source(source_name)
    if not source or not isinstance(source, WebhookSource):
        raise HTTPException(404, f"WebhookSource '{source_name}' not found")
    await source.ingest(payload)
    return {"ok": True, "source": source_name, "received": len(payload)}


# ══════════════════════════════════════
#  统计 / 健康检查
# ══════════════════════════════════════

@app.get("/api/v1/stats")
async def get_stats():
    """引擎统计"""
    return engine.stats()


@app.get("/api/v1/health")
async def health():
    """健康检查"""
    return {
        "status": "ok",
        "engine": "running",
        "stats": engine.stats(),
    }


# ══════════════════════════════════════
#  入口
# ══════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)