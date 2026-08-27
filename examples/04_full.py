"""
示例 4: 完整集成 — Source/Executor/Plugin + API 管理

展示实时添加/移除 sources、executors 和 plugins。

运行: python examples/04_full.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from evebus import (
    EventEngine,
    TimerSource,
    WebhookSource,
    ScriptExecutor,
    Plugin,
    HookStage,
)


# ═══════════════════════════════════════
#  自定义插件
# ═══════════════════════════════════════

class MetricsPlugin(Plugin):
    """指标插件 — 记录事件数"""

    def __init__(self):
        super().__init__("metrics")
        self.counts = {}

    def on_attach(self):
        @self.on("*")
        async def on_any(topic, event):
            prefix = topic.split(".")[0]
            self.counts[prefix] = self.counts.get(prefix, 0) + 1

    def report(self):
        return self.counts


class AuditPlugin(Plugin):
    """审计插件 — 记录所有事件"""

    def __init__(self, max_logs=100):
        super().__init__("audit")
        self.logs = []
        self.max_logs = max_logs

    def on_attach(self):
        async def hook(ctx):
            self.logs.append({
                "topic": ctx.topic,
                "source": ctx.source,
            })
            if len(self.logs) > self.max_logs:
                self.logs.pop(0)

        self.engine.add_hook(HookStage.POST_EMIT, hook)


# ═══════════════════════════════════════
#  主程序
# ═══════════════════════════════════════

async def main():
    engine = EventEngine()

    print("=== 1. 实时添加 Source: Timer ===")
    timer = TimerSource(name="heartbeat", topic="system.heartbeat", interval_ms=500)
    result = await engine.add_source(timer)
    print(f"  添加: {result}")

    print("\n=== 2. 实时添加 Source: Webhook ===")
    webhook = WebhookSource(name="webhook", path="/ingest", topic_prefix="external")
    result = await engine.add_source(webhook)
    print(f"  添加: {result}")

    print("\n=== 3. 实时添加 Executor: 脚本 ===")
    # 创建一个测试脚本
    test_script = os.path.join(os.path.dirname(__file__), "_test_executor.py")
    with open(test_script, "w") as f:
        f.write('''
async def on_event(topic: str, payload: dict):
    print(f"  [script] {topic} → {payload}")

def on_start():
    print("  [script] 脚本已加载")

def on_stop():
    print("  [script] 脚本已卸载")
''')

    executor = ScriptExecutor(
        name="test_script",
        script_path=test_script,
        patterns=["system.*", "external.*"],
    )
    result = await engine.add_executor(executor)
    print(f"  添加: {result}")

    print("\n=== 4. 实时添加 Plugin ===")
    metrics = MetricsPlugin()
    await engine.add_plugin(metrics)

    audit = AuditPlugin()
    await engine.add_plugin(audit)

    print("\n=== 5. 监听事件 ===")

    @engine.on("system.heartbeat")
    async def on_heartbeat(topic, event):
        print(f"  [heartbeat] tick={event.get('tick')}")

    @engine.on("external.*")
    async def on_external(topic, event):
        print(f"  [external] {topic}")

    print("\n=== 6. 运行 2 秒 ===")
    await asyncio.sleep(2)

    print("\n=== 7. 查看统计 ===")
    stats = engine.stats()
    print(f"  Sources: {stats['sources']}")
    print(f"  Executors: {stats['executors']}")
    print(f"  Plugins: {stats['plugins']}")
    print(f"  Metrics: {metrics.report()}")
    print(f"  Audit logs: {len(audit.logs)} 条")

    print("\n=== 8. 手动注入 webhook 事件 ===")
    await engine.emit("external.data", {"from": "manual"}, source="manual")

    print("\n=== 9. 实时移除 Source ===")
    await engine.remove_source("heartbeat")
    print(f"  移除后 sources: {[s['name'] for s in engine.list_sources()]}")

    print("\n=== 10. 等待完成 ===")
    await engine.wait_for_complete()

    # 清理测试脚本
    os.remove(test_script)

    print("\n✅ 全部完成")


if __name__ == "__main__":
    asyncio.run(main())