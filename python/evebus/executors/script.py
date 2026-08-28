"""
ScriptExecutor — 动态脚本执行器

加载外部 Python 脚本文件，将 on_event 函数作为 handler。
支持运行时重新加载脚本。

用法:

    from evebus.executors import ScriptExecutor

    executor = ScriptExecutor(
        name="my_strategy",
        script_path="strategies/momentum.py",
        patterns=["data.quotes.*.ETHUSDT"],
    )
    engine.add_executor(executor)

    # 脚本格式 (momentum.py):
    # async def on_event(topic: str, payload: dict):
    #     print(f"策略处理: {topic}")
"""

import importlib.util
import os
import sys
import asyncio
import logging
from pathlib import Path
from typing import List, Optional

from .base import EventExecutor

logger = logging.getLogger("evebus.executors")


class ScriptExecutor(EventExecutor):
    """动态脚本执行器"""

    def __init__(
        self,
        name: str,
        script_path: str,
        patterns: List[str] = None,
        auto_reload: bool = False,
        reload_interval_sec: float = 30.0,
    ):
        super().__init__(name, patterns)
        self.script_path = os.path.abspath(script_path)
        self.auto_reload = auto_reload
        self.reload_interval_sec = reload_interval_sec
        self._module = None
        self._on_event = None
        self._on_stop = None  # #22: 初始化，避免 stop() 先于 start() 抛 AttributeError
        self._reload_task: asyncio.Task = None  # type: ignore
        self._on_start_task: Optional[asyncio.Task] = None  # #23: 跟踪 on_start 任务

    def _load_script(self):
        """加载或重新加载脚本（同步，阻塞事件循环 — #24 文档说明）"""
        if not os.path.exists(self.script_path):
            raise FileNotFoundError(f"脚本不存在: {self.script_path}")

        script_name = Path(self.script_path).stem

        # 创建唯一模块名
        module_name = f"evebus_script_{self.name}_{script_name}"

        # 加载模块（#24: exec_module 同步执行顶层代码，会阻塞事件循环）
        spec = importlib.util.spec_from_file_location(
            module_name, self.script_path
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # 获取 on_event 函数
        on_event = getattr(module, "on_event", None)
        if on_event is None:
            raise AttributeError(
                f"脚本 {self.script_path} 缺少 on_event 函数"
            )

        self._module = module
        self._on_event = on_event

        # 调用 on_start（可选）— #23: 跟踪任务，异常可观测
        on_start = getattr(module, "on_start", None)
        if on_start:
            result = on_start()
            if asyncio.iscoroutine(result):
                self._on_start_task = asyncio.ensure_future(result)
                self._on_start_task.add_done_callback(self._on_start_done)

        # 调用 on_stop（可选，保存引用）
        self._on_stop = getattr(module, "on_stop", None)

    def _on_start_done(self, task: asyncio.Task):
        """#23: on_start 任务异常不再静默"""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("Executor '%s' on_start failed: %s", self.name, exc)

    async def start(self):
        """启动执行器（加载脚本 + 可选自动重载）"""
        # #25: 防重入 — 已有 reload_task 则跳过
        if self.auto_reload and self._reload_task and not self._reload_task.done():
            logger.warning("Executor '%s' already running, skip start()", self.name)
            return
        self._load_script()
        if self.auto_reload:
            self._reload_task = asyncio.create_task(self._reload_loop())

    async def stop(self):
        """停止执行器"""
        # 调用 on_stop（可选）
        if self._on_stop:
            result = self._on_stop()
            if asyncio.iscoroutine(result):
                try:
                    await result
                except Exception as e:
                    logger.error("Executor '%s' on_stop failed: %s", self.name, e)

        # #25: 停止 reload 循环并复位状态
        if self._reload_task and not self._reload_task.done():
            self._reload_task.cancel()
            try:
                await self._reload_task
            except (asyncio.CancelledError, Exception):
                pass
        self._reload_task = None
        self._running = False

    async def reload(self):
        """手动重新加载脚本"""
        self._load_script()

    async def _reload_loop(self):
        """自动重载循环（#25/#26: finally 复位 + 日志）"""
        self._running = True
        try:
            last_mtime = os.path.getmtime(self.script_path)
            while self._running:
                await asyncio.sleep(self.reload_interval_sec)
                try:
                    current_mtime = os.path.getmtime(self.script_path)
                    if current_mtime != last_mtime:
                        self._load_script()
                        last_mtime = current_mtime
                        logger.info("[%s] 脚本已重载: %s", self.name, self.script_path)
                except asyncio.CancelledError:
                    raise
                except FileNotFoundError as e:
                    # 脚本被删除等可恢复场景
                    logger.warning("[%s] 重载失败: %s", self.name, e)
                except Exception as e:
                    # #26: 记录脚本本身错误，保留旧 handler 运行
                    logger.error("[%s] 重载失败: %s", self.name, e)
        finally:
            self._running = False

    async def execute(self, topic: str, payload: dict):
        """执行脚本的 on_event 函数"""
        if self._on_event is None:
            raise RuntimeError("脚本未加载")

        result = self._on_event(topic, payload)
        if asyncio.iscoroutine(result):
            await result

    def info(self) -> dict:
        base = super().info()
        base.update({
            "script_path": self.script_path,
            "auto_reload": self.auto_reload,
            "module_loaded": self._module is not None,
        })
        return base