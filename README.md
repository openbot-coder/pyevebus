# EveBus

[![PyPI](https://img.shields.io/pypi/v/pyevebus.svg)](https://pypi.org/project/pyevebus/)
[![Python](https://img.shields.io/pypi/pyversions/pyevebus.svg)](https://pypi.org/project/pyevebus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

高性能异步事件引擎 — **Rust 匹配核心** + **Python 异步 API** + **HTTP/SSE 接口**，可作为远程 RPC 后端。

参考 [pyee](https://github.com/jfhbrook/pyee) 风格，扩展通配符匹配、Hook 中间件、Source/Executor/Plugin 实时管理、SSE 流式订阅。

> 📖 **完整使用手册**：[docs/USER_GUIDE.md](docs/USER_GUIDE.md)

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
# 启动 HTTP API 服务（生产环境务必启用认证）
EVEBUS_AUTH_TOKEN=my-secret evebus serve --port 8080

# 开发模式（自动重载，不能与 --workers 同时使用）
evebus serve --port 8080 --reload

# 多 Worker（注意：引擎是进程内单例，多 worker 会状态分裂，默认拒绝）
evebus serve --port 8080 --workers 4

# 直接运行脚本执行器
evebus run strategy.py -t "data.*" --auto-reload
```

> ⚠️ **安全**：`evebus serve` 默认绑定 `0.0.0.0` 且管理 API 无认证。
> **生产环境必须设置 `EVEBUS_AUTH_TOKEN`**（见下方[安全章节](#-安全)），
> 否则任何能访问端口的人都能添加执行器 → 远程代码执行。

### `evebusctl` — 客户端管理工具

```bash
# 查看引擎状态
evebusctl status

# 发射事件
evebusctl emit "data.test" -d '{"key": "value"}'

# 流式订阅事件（SSE 推送，长连接）
evebusctl subscribe "data.*.ETHUSDT"

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
| **`GET`** | **`/api/v1/events/subscribe?pattern=`** | **流式订阅事件（SSE）** ⭐ |
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

## RPC 流式订阅（SSE）

pyevebus 可作为**远程 RPC 后端**：外部系统通过标准 HTTP/SSE 发射事件和**实时订阅事件流**。

### `evebusctl` 订阅

```bash
# 终端 1：订阅（长连接，持续打印事件）
evebusctl subscribe "data.*.ETHUSDT"

# 终端 2：发射事件
evebusctl emit "data.quotes.BINANCE.ETHUSDT" -d '{"price": 3000}'

# 终端 1 输出:
# [1787826311] data.quotes.BINANCE.ETHUSDT {"price": 3000}
```

### Python SDK（RPCClient）

```python
import asyncio
from evebus.rpc import RPCClient

async def main():
    client = RPCClient("http://localhost:8080")

    # 发射事件（单向 RPC）
    await client.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})

    # 流式订阅（SSE 推送）
    async for event in client.subscribe("data.*.ETHUSDT"):
        print(event["topic"], event["event"])

asyncio.run(main())
```

### 任意语言消费（标准 SSE 协议）

```bash
# curl
curl -N "http://localhost:8080/api/v1/events/subscribe?pattern=data.*"
```

```javascript
// 浏览器 / Node（EventSource 自动重连）
const es = new EventSource(
  "http://localhost:8080/api/v1/events/subscribe?pattern=data.*.ETHUSDT"
);
es.onmessage = (msg) => console.log(JSON.parse(msg.data));
```

```go
// Go
resp, _ := http.Get("http://localhost:8080/api/v1/events/subscribe?pattern=data.*")
scanner := bufio.NewScanner(resp.Body)
for scanner.Scan() {
    line := scanner.Text()
    if strings.HasPrefix(line, "data: ") {
        fmt.Println(line[6:])  // 事件 JSON
    }
}
```

**SSE 特性：**
- 每个订阅连接独立队列（背压上限 1024），慢消费者不丢事件
- 连接断开自动注销 handler（`engine.off`），无泄漏
- 支持通配符 pattern（Rust 路由器匹配）
- 事件格式：`data: {"topic": "...", "event": ..., "timestamp": 纳秒}`

## 🔒 安全

### 认证

管理 API（除 `/api/v1/health` 外）支持 token 认证。设置环境变量后，所有请求必须携带 `X-Auth-Token` 头：

```bash
EVEBUS_AUTH_TOKEN=my-secret evebus serve --port 8080

# 客户端
curl -H "X-Auth-Token: my-secret" http://localhost:8080/api/v1/stats
evebusctl emit "data.test" -d '{"x": 1}'   # 需要配合 --url 且服务端在受信网络
```

> ⚠️ **为什么不默认开启**：方便本地开发。生产部署（尤其暴露到公网）**必须**设置 `EVEBUS_AUTH_TOKEN`，否则任何人可调用 `/api/v1/executors/script` 加载任意脚本 → **远程代码执行（RCE）**。

### 请求体大小限制

默认限制 1MB，超限返回 413：

```bash
EVEBUS_MAX_BODY_BYTES=524288 evebus serve   # 限制 512KB
```

### 多 Worker 限制

引擎是**进程内单例**，多 worker（`--workers > 1` 或 `WEB_CONCURRENCY > 1`）会导致订阅/状态分裂。服务端默认拒绝多 worker 启动；如确需横向扩展，请使用独立进程 + 外部存储方案。

### 其他安全修复（v0.3.1）

- SSE 订阅背压：队列满时丢弃事件（`put_nowait`），不阻塞引擎、不泄漏 handler
- `cancel()` 后任务回调不再抛 `CancelledError`
- SIGTERM 优雅关闭，uvicorn 子进程不残留

## 架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                           EveBus                                     │
│                                                                      │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────┐        │
│  │ Sources  │──▶ │  Router      │──▶ │  Executors           │        │
│  │          │    │  通配符匹配   │    │  ScriptExecutor      │        │
│  │ Timer    │    │  * ? (Rust)  │    │  Handler (on)        │        │
│  │ WS       │    │              │    │                      │        │
│  │ Webhook  │    │  Hooks       │    └──────────────────────┘        │
│  └──────────┘    └──────────────┘                                    │
│                      ▲                                               │
│                      │ 实时管理                                       │
│        ┌─────────────┴─────────────┐                                 │
│        │  HTTP API + SSE 订阅       │                                │
│        │  FastAPI                  │                                │
│        │  /emit  /subscribe(SSE)   │                                │
│        └───────┬───────────┬───────┘                                │
│                │           │                                         │
│    evebusctl   │           │ 任意语言 (curl/JS/Go)                    │
│    (管理/订阅)  │           │                                         │
│                ▼           ▼                                         │
│        ┌──────────────────────────────┐                              │
│        │  RPCClient SDK (evebus.rpc)  │                              │
│        └──────────────────────────────┘                              │
│                                                                      │
│  ┌─────────────────────────────────────────────┐                     │
│  │  Plugins (metrics/audit/自定义)             │                     │
│  └─────────────────────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────────────┘
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

# 测试 (246 用例，95%+ 覆盖率)
uv run python -m pytest tests/ -v

# 代码检查
uv run ruff check .
```

## License

[MIT](LICENSE)