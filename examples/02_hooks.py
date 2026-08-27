"""
示例 2: Hook 系统 — 中间件

运行: python examples/02_hooks.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from evebus import EventEngine, HookStage, HookContext, HookResult


async def main():
    engine = EventEngine()

    # ── Pre-Emit Hook: 验证 ──
    async def validate_hook(ctx: HookContext):
        if "topic" not in dir(ctx):
            return HookResult.CONTINUE
        if not isinstance(ctx.payload, dict):
            print(f"  [validate] 拦截非 dict payload")
            return HookResult.INTERCEPTED
        print(f"  [validate] ✓ payload 有效")
        return HookResult.CONTINUE

    # ── Pre-Emit Hook: 添加元数据 ──
    async def enrich_hook(ctx: HookContext):
        ctx.metadata["enriched_at"] = "2026-01-01T00:00:00Z"
        ctx.payload["enriched"] = True
        print(f"  [enrich] ✓ 补充元数据")
        return HookResult.CONTINUE

    # ── Post-Emit Hook: 日志 ──
    async def log_hook(ctx: HookContext):
        print(f"  [log] ✓ {ctx.topic} 已发射")
        return HookResult.CONTINUE

    # ── 注册 hooks ──
    engine.add_hook(HookStage.PRE_EMIT, validate_hook)
    engine.add_hook(HookStage.PRE_EMIT, enrich_hook)
    engine.add_hook(HookStage.POST_EMIT, log_hook)

    # ── 注册 handler ──
    @engine.on("data.*")
    async def on_data(topic, event):
        print(f"  [handler] {topic} enriched={event.get('enriched')}")

    # ── 发射（正常） ──
    print("== 正常发射 ==")
    await engine.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})

    # ── 发射（被拦截） ──
    print("\n== 发射非 dict（被 validate 拦截） ==")
    await engine.emit("data.quotes.BINANCE.ETHUSDT", "not a dict")
    print("  （handler 应未被调用）")

    await engine.wait_for_complete()


if __name__ == "__main__":
    asyncio.run(main())