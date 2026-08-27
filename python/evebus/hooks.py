"""
Hook 系统 — 中间件模式

每个 hook 接收 HookContext，返回 HookResult。
Hook 链顺序执行，任一 hook 返回非 CONTINUE 则中断。

用法:

    from evebus import EventEngine, HookStage, HookContext, HookResult

    engine = EventEngine()

    async def rate_limit_hook(ctx: HookContext):
        if is_rate_limited(ctx.source):
            return HookResult.INTERCEPTED
        return HookResult.CONTINUE

    engine.add_hook(HookStage.PRE_EMIT, rate_limit_hook)
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Optional


class HookStage(Enum):
    """Hook 执行阶段"""
    PRE_EMIT = "pre_emit"       # 发射前（验证/过滤/修改）
    POST_EMIT = "post_emit"     # 发射后（记录/指标）
    ON_ERROR = "on_error"       # 错误时（重试/报警）
    PRE_EXECUTE = "pre_execute"     # 执行前（补充数据）
    POST_EXECUTE = "post_execute"   # 执行后（记录结果）


class HookResult(Enum):
    """Hook 返回结果"""
    CONTINUE = "continue"       # 继续执行
    INTERCEPTED = "intercepted" # 拦截（不继续分发）
    MODIFIED = "modified"       # 已修改（继续执行）


@dataclass
class HookContext:
    """Hook 上下文 — 在 hook 链间传递"""
    topic: str
    payload: Any
    source: str = ""
    metadata: dict = field(default_factory=dict)


def hook(stage: HookStage):
    """
    装饰器 — 注册 hook

    用法:
        @hook(HookStage.PRE_EMIT)
        async def validate_hook(ctx: HookContext):
            return HookResult.CONTINUE
    """
    def decorator(fn):
        fn._hook_stage = stage
        fn._hook_name = fn.__name__
        return fn
    return decorator