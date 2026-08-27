# EveBus

[![PyPI](https://img.shields.io/pypi/v/pyevebus.svg)](https://pypi.org/project/pyevebus/)
[![Python](https://img.shields.io/pypi/pyversions/pyevebus.svg)](https://pypi.org/project/pyevebus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

高性能异步事件引擎 — Rust 核心 + Python API + HTTP 管理接口。

参考 [pyee](https://github.com/jfhbrook/pyee) 风格，扩展通配符匹配、Hook 中间件、Source/Executor/Plugin 实时管理。

## 安装

```bash
pip install pyevebus
```

带 WebSocket 支持：

```bash
pip install pyevebus[ws]
```

## 快速开始

```python
import asyncio
from evebus import EventEngine

engine = EventEngine()

@engine.on("data.quotes.*.ETHUSDT")
async def on_quote(topic, event):
    print(f"ETH: {event}")

@engine.once("system.start")
async def on_start(topic, event):
    print("系统启动！")

async def main():
    await engine.emit("system.start", {})
    await engine.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})
    await engine.wait_for_complete()

asyncio.run(main())
```

## CLI 工具

EveBus 提供两个独立的 CLI 工具，类似 `etcd` / `etcdctl`：

| 命令 | 定位 | 说明 |
|------|------|------|
| `evebus` | **服务端** | 启动引擎、HTTP API、脚本执行器 |
| `evebusctl` | **客户端** | 远程管理引擎（源、执行器、插件） |

### `evebus` — 服务端

```bash
# 启动 HTTP API 服务
evebus serve --port 8080

# 开发模式（自动重载）
evebus serve --port 8080 --reload

# 多 Worker
evebus serve --port 8080 --workers 4

# 直接运行脚本执行器
evebus run strategy.py -t "data.*" --auto-reload
```

### `evebusctl` — 客户端管理工具

```bash
# 查看引擎状态
evebusctl status

# 发射事件
evebusctl emit "data.test" -d '{"key": "value"}'

# 管理事件源
evebusctl sources list
evebusctl sources add-timer heartbeat --topic system.heartbeat -i 5000
evebusctl sources add-webhook external --prefix external
evebusctl sources start <name>
evebusctl sources stop <name>
evebusctl sources remove <name>

# 管理执行器
evebusctl executors list
evebusctl executors add my_strat -s strategy.py -t "data.*"
evebusctl executors reload <name>
evebusctl executors remove <name>

# 管理插件
evebusctl plugins list
evebusctl plugins remove <name>
```

> 所有 `evebusctl` 命令支持 `--url` 指定远程服务地址，默认 `http://localhost:8080`

## 核心功能

### 通配符匹配

Rust 实现的 DP 算法，支持 `*`（任意字符）和 `?`（单字符）：

```python
@engine.on("data.quotes.*")           # 所有行情
@engine.on("data.quotes.*.ETHUSDT")   # 所有交易所的 ETH
@engine.on("data.*.BTCUSDT")          # 所有数据类型
```

### Hook 系统（中间件）

在事件流的各个阶段注入逻辑：

```python
from evebus import HookStage, HookContext, HookResult

# 验证 + 拦截
async def validate(ctx):
    if not isinstance(ctx.payload, dict):
        return HookResult.INTERCEPTED
    return HookResult.CONTINUE

# 补充数据
async def enrich(ctx):
    ctx.payload["enriched"] = True
    return HookResult.CONTINUE

engine.add_hook(HookStage.PRE_EMIT, validate)
engine.add_hook(HookStage.PRE_EMIT, enrich)
engine.add_hook(HookStage.POST_EMIT, log_hook)
```

**Hook 阶段：**

| 阶段 | 用途 |
|------|------|
| `PRE_EMIT` | 验证、过滤、修改事件 |
| `POST_EMIT` | 日志、指标、通知 |
| `ON_ERROR` | 错误处理、重试 |

### 事件源（Sources）

实时添加/移除事件源：

```python
from evebus import TimerSource, WebhookSource

# 定时器源
timer = TimerSource(name="heartbeat", topic="system.heartbeat", interval_ms=5000)
await engine.add_source(timer)

# Webhook 源
webhook = WebhookSource(name="external", path="/ingest", topic_prefix="external")
await engine.add_source(webhook)

# 停止/移除
await engine.stop_source("heartbeat")
await engine.remove_source("heartbeat")
```

**内置事件源：**

| 源 | 说明 |
|----|------|
| `TimerSource` | 定时事件 |
| `WebSocketSource` | WebSocket 数据（自动重连） |
| `WebhookSource` | HTTP Webhook 注入 |

### 执行器（Executors）

动态加载 Python 脚本，运行时重载：

```python
from evebus import ScriptExecutor

executor = ScriptExecutor(
    name="my_strategy",
    script_path="strategies/momentum.py",
    patterns=["data.quotes.*.ETHUSDT"],
    auto_reload=True,
)
await engine.add_executor(executor)
```

脚本格式：

```python
# strategies/momentum.py
async def on_event(topic: str, payload: dict):
    if payload.get("price", 0) > 3000:
        print("买入信号！")

def on_start():
    print("策略已加载")
```

### 插件系统

```python
from evebus import Plugin

class MetricsPlugin(Plugin):
    def __init__(self):
        super().__init__("metrics")
        self.counts = {}

    def on_attach(self):
        @self.on("*")
        async def on_any(topic, event):
            prefix = topic.split(".")[0]
            self.counts[prefix] = self.counts.get(prefix, 0) + 1

await engine.add_plugin(MetricsPlugin())
```

## HTTP API

启动服务后访问：

- **API 文档**：`http://localhost:8080/docs`（Swagger UI）
- **健康检查**：`GET http://localhost:8080/api/v1/health`

### 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/events/emit` | 发射事件 |
| `GET` | `/api/v1/sources` | 列出所有源 |
| `POST` | `/api/v1/sources/timer` | 添加定时器源 |
| `POST` | `/api/v1/sources/webhook` | 添加 Webhook 源 |
| `DELETE` | `/api/v1/sources/{name}` | 移除源 |
| `GET` | `/api/v1/executors` | 列出所有执行器 |
| `POST` | `/api/v1/executors/script` | 添加脚本执行器 |
| `POST` | `/api/v1/executors/{name}/reload` | 重载脚本 |
| `DELETE` | `/api/v1/executors/{name}` | 移除执行器 |
| `GET` | `/api/v1/plugins` | 列出所有插件 |
| `DELETE` | `/api/v1/plugins/{name}` | 移除插件 |
| `GET` | `/api/v1/stats` | 引擎统计 |

### curl 示例

```bash
# 添加定时器
curl -X POST http://localhost:8080/api/v1/sources/timer \
  -H "Content-Type: application/json" \
  -d '{"name": "heartbeat", "topic": "system.heartbeat", "interval_ms": 5000}'

# 发射事件
curl -X POST http://localhost:8080/api/v1/events/emit \
  -H "Content-Type: application/json" \
  -d '{"topic": "data.quotes.BINANCE.ETHUSDT", "payload": {"price": 3000}}'

# 查看状态
curl http://localhost:8080/api/v1/stats
```

等价于 `evebusctl`：

```bash
evebusctl sources add-timer heartbeat --topic system.heartbeat -i 5000
evebusctl emit "data.quotes.BINANCE.ETHUSDT" -d '{"price": 3000}'
evebusctl status
```

## 架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        EveBus                                   │
│                                                                 │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │ Sources  │──▶ │  Router      │──▶ │  Executors           │   │
│  │          │    │  通配符匹配   │    │  ScriptExecutor      │   │
│  │ Timer    │    │  * ? (Rust)  │    │  Handler (on)        │   │
│  │ WS       │    │              │    │                      │   │
│  │ Webhook  │    │  Hooks       │    └──────────────────────┘   │
│  └──────────┘    └──────────────┘                               │
│                      ▲                                          │
│                      │ 实时管理                                  │
│              ┌───────┴───────┐                                  │
│              │  HTTP API     │◀── evebusctl (客户端管理)         │
│              │  FastAPI      │                                  │
│              └───────────────┘                                  │
│                                                                 │
│  ┌─────────────────────────────────────────────┐                │
│  │  Plugins (metrics/audit/自定义)             │                │
│  └─────────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

## Python API

| 方法 | 说明 |
|------|------|
| `on(pattern, handler)` | 注册 handler（支持装饰器） |
| `once(pattern, handler)` | 一次性监听 |
| `off(pattern, handler)` | 移除 handler |
| `emit(topic, event)` | 发射事件（async） |
| `wait_for_complete()` | 等待所有 pending 协程 |
| `cancel()` | 取消所有 pending |
| `add_source(source)` | 添加事件源（async） |
| `remove_source(name)` | 移除事件源（async） |
| `add_executor(executor)` | 添加执行器（async） |
| `remove_executor(name)` | 移除执行器（async） |
| `add_hook(stage, hook)` | 注册 hook |
| `add_plugin(plugin)` | 添加插件（async） |
| `stats()` | 引擎统计 |

## 开发

```bash
# 环境
uv sync --dev
uv run maturin develop

# 测试 (233 用例，95% 覆盖率)
uv run python -m pytest tests/ -v

# 代码检查
uv run ruff check .
```

## License

[MIT](LICENSE)