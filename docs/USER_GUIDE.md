# EveBus 使用手册

EveBus 是高性能异步事件引擎，采用 **Rust 通配符匹配核心 + Python 异步 API + HTTP/SSE 接口**。

本文档是完整的使用参考。快速上手见 [README](../README.md)。

## 目录

- [1. 安装](#1-安装)
- [2. 服务端：evebus](#2-服务端evebus)
- [3. 客户端：evebusctl](#3-客户端evebusctl)
- [4. Python API](#4-python-api)
- [5. HTTP API 参考](#5-http-api-参考)
- [6. SSE 流式订阅（RPC）](#6-sse-流式订阅rpc)
- [7. 事件源 / 执行器 / 插件](#7-事件源--执行器--插件)
- [8. 故障排查](#8-故障排查)

---

## 1. 安装

```bash
pip install pyevebus        # 标准安装（含 Rust 扩展）
pip install pyevebus[ws]    # 带 WebSocket 支持
```

**要求**：Python ≥ 3.9。支持 Windows / macOS / Linux。

安装后有两个命令：

| 命令 | 定位 |
|------|------|
| `evebus` | 服务端 — 启动引擎 |
| `evebusctl` | 客户端 — 远程管理 |

---

## 2. 服务端（evebus）

### `evebus serve` — 启动 HTTP API 服务

```bash
evebus serve                                  # 默认 0.0.0.0:8080
evebus serve --port 9000                      # 指定端口
evebus serve --host 127.0.0.1 --port 9000     # 仅本机访问
evebus serve --workers 4                      # 多进程
evebus serve --reload                         # 开发模式（自动重载）
```

启动后：

- **API 文档**：`http://localhost:8080/docs`（Swagger UI）
- **健康检查**：`GET http://localhost:8080/api/v1/health`

### `evebus run` — 直接运行脚本执行器

```bash
evebus run strategy.py                       # 运行脚本，订阅所有事件
evebus run strategy.py -t "data.*.ETHUSDT"   # 只订阅行情
evebus run strategy.py --auto-reload         # 脚本变更自动重载
evebus run strategy.py -n my_strategy        # 自定义名称
```

脚本格式：

```python
# strategy.py
async def on_event(topic: str, payload: dict):
    """收到事件时调用"""
    if payload.get("price", 0) > 3000:
        print("买入信号！")

def on_start():
    """执行器启动时调用（可选）"""
    print("策略已加载")

def on_stop():
    """执行器停止时调用（可选）"""
    print("策略已停止")
```

---

## 3. 客户端（evebusctl）

所有客户端命令支持 `-u/--url` 指定服务地址（默认 `http://localhost:8080`）。

### 状态与发射

```bash
# 查看引擎状态（源/执行器/插件/统计）
evebusctl status

# 发射事件
evebusctl emit "data.quotes.BINANCE.ETHUSDT" -d '{"price": 3000}'
evebusctl emit "system.start"                 # 无 payload
```

### 流式订阅（SSE）

```bash
# 终端 1：订阅（长连接，实时打印事件）
evebusctl subscribe "data.*.ETHUSDT"

# 终端 2：发射事件
evebusctl emit "data.quotes.BINANCE.ETHUSDT" -d '{"price": 3000}'

# 终端 1 输出:
# [1787826311] data.quotes.BINANCE.ETHUSDT {"price": 3000}
```

选项：

- `PATTERN` — 订阅模式，支持 `*`（任意）和 `?`（单字符），默认 `*`
- `--no-color` — 禁用彩色输出（管道重定向时使用）
- `-u/--url` — 服务地址

### 事件源管理

```bash
# 列出
evebusctl sources list

# 添加定时器源（每 5 秒发一次 system.heartbeat）
evebusctl sources add-timer heartbeat --topic system.heartbeat -i 5000

# 添加 Webhook 源（接收 HTTP POST 注入）
evebusctl sources add-webhook external --prefix external

# 启停/移除
evebusctl sources start heartbeat
evebusctl sources stop heartbeat
evebusctl sources remove heartbeat
```

### 执行器管理

```bash
# 列出
evebusctl executors list

# 添加脚本执行器
evebusctl executors add my_strat -s strategy.py -t "data.*"

# 重载脚本（热更新）
evebusctl executors reload my_strat

# 移除
evebusctl executors remove my_strat
```

### 插件管理

```bash
evebusctl plugins list
evebusctl plugins remove metrics
```

---

## 4. Python API

### 基础用法

```python
import asyncio
from evebus import EventEngine

engine = EventEngine()

@engine.on("data.quotes.*.ETHUSDT")      # 通配符订阅
async def on_quote(topic, event):
    print(f"ETH: {event}")

@engine.once("system.start")             # 只触发一次
async def on_start(topic, event):
    print("系统启动！")

async def main():
    await engine.emit("system.start", {})
    await engine.emit("data.quotes.BINANCE.ETHUSDT", {"price": 3000})
    await engine.wait_for_complete()     # 等待所有异步 handler 完成

asyncio.run(main())
```

### 方法速查

| 方法 | 说明 |
|------|------|
| `on(pattern, handler)` | 注册 handler（支持装饰器） |
| `once(pattern, handler)` | 一次性监听 |
| `off(pattern, handler)` | 移除 handler（不传 handler 则移除全部） |
| `emit(topic, event, source="")` | 发射事件（async） |
| `wait_for_complete()` | 等待所有 pending 协程 |
| `cancel()` | 取消所有 pending |
| `listeners(pattern)` | 查看某个 pattern 的 handlers |
| `event_names` | 所有已注册的 pattern |
| `add_source(source)` / `remove_source(name)` | 事件源管理（async） |
| `add_executor(executor)` / `remove_executor(name)` | 执行器管理（async） |
| `add_hook(stage, hook)` | 注册 Hook 中间件 |
| `add_plugin(plugin)` / `remove_plugin(name)` | 插件管理（async） |
| `stats()` | 引擎统计 |

### 通配符规则

| 模式 | 匹配 | 不匹配 |
|------|------|--------|
| `data.*` | `data.quotes`、`data.x.y` | `other` |
| `data.*.ETHUSDT` | `data.quotes.ETHUSDT` | `data.quotes.BTCUSDT` |
| `a?c` | `abc`、`axc` | `abbc`、`ac` |

---

## 5. HTTP API 参考

### 事件

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/events/emit` | 发射事件（body: `{"topic", "payload", "source"}`） |
| `POST` | `/api/v1/events/emit/{topic}` | 路径方式发射 |
| `GET` | `/api/v1/events/subscribe?pattern=` | SSE 流式订阅 |

### 事件源

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/sources` | 列出所有源 |
| `GET` | `/api/v1/sources/{name}` | 源详情 |
| `POST` | `/api/v1/sources/timer` | 添加定时器 |
| `POST` | `/api/v1/sources/webhook` | 添加 Webhook |
| `POST` | `/api/v1/sources/{name}/start` | 启动 |
| `POST` | `/api/v1/sources/{name}/stop` | 停止 |
| `DELETE` | `/api/v1/sources/{name}` | 移除 |

### 执行器

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/executors` | 列出 |
| `GET` | `/api/v1/executors/{name}` | 详情 |
| `POST` | `/api/v1/executors/script` | 添加脚本执行器 |
| `POST` | `/api/v1/executors/{name}/reload` | 重载 |
| `DELETE` | `/api/v1/executors/{name}` | 移除 |

### 插件 / 其他

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/plugins` | 列出插件 |
| `DELETE` | `/api/v1/plugins/{name}` | 移除插件 |
| `POST` | `/api/v1/webhook/{source_name}` | 向 Webhook 源注入 |
| `GET` | `/api/v1/stats` | 引擎统计 |
| `GET` | `/api/v1/health` | 健康检查 |

---

## 6. SSE 流式订阅（RPC）

EveBus 可作为 **远程 RPC 后端**，外部系统通过标准 HTTP/SSE 实时接收事件。

### Python SDK

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

    # 管理/查询
    await client.health()
    await client.stats()
    await client.list_sources()
    await client.list_executors()

asyncio.run(main())
```

### 任意语言

```bash
# curl
curl -N "http://localhost:8080/api/v1/events/subscribe?pattern=data.*"
```

```javascript
// 浏览器（EventSource 自动重连）
const es = new EventSource(
  "http://localhost:8080/api/v1/events/subscribe?pattern=data.*.ETHUSDT"
);
es.onmessage = (msg) => console.log(JSON.parse(msg.data));
```

### 事件格式

每个 SSE 帧：

```
data: {"topic": "data.quotes.ETH", "event": {"price": 3000}, "timestamp": 1787826311652803626}
```

`timestamp` 为纳秒。

### 特性

- **背压**：每连接独立队列（上限 1024），慢消费者不丢事件
- **自动清理**：连接断开自动注销 handler
- **通配符**：复用 Rust 路由器匹配

---

## 7. 事件源 / 执行器 / 插件

### 事件源（Sources）

```python
from evebus import TimerSource, WebSocketSource, WebhookSource

# 定时器
timer = TimerSource(name="heartbeat", topic="system.heartbeat", interval_ms=5000)
await engine.add_source(timer)

# WebSocket（自动重连）
ws = WebSocketSource(
    name="binance",
    url="wss://stream.binance.com:9943/ws/ethusdt@ticker",
    topic_prefix="data.ws",
)
await engine.add_source(ws)

# Webhook（HTTP 注入）
webhook = WebhookSource(name="external", path="/ingest", topic_prefix="external")
await engine.add_source(webhook)
```

### 执行器（Executors）

```python
from evebus import ScriptExecutor

executor = ScriptExecutor(
    name="my_strategy",
    script_path="strategies/momentum.py",
    patterns=["data.quotes.*.ETHUSDT"],
    auto_reload=True,          # 脚本变更自动重载
)
await engine.add_executor(executor)
```

### 插件（Plugins）

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

### Hook 中间件

```python
from evebus import HookStage, HookResult

# 验证 + 拦截
async def validate(ctx):
    if not isinstance(ctx.payload, dict):
        return HookResult.INTERCEPTED
    return HookResult.CONTINUE

engine.add_hook(HookStage.PRE_EMIT, validate)   # 发射前：验证/过滤/修改
engine.add_hook(HookStage.POST_EMIT, log_hook)  # 发射后：日志/指标
engine.add_hook(HookStage.ON_ERROR, retry_hook) # 错误：处理/重试
```

---

## 8. 故障排查

### 无法连接服务

```bash
evebusctl status
# ❌ 无法连接服务: http://localhost:8080
```

**解决**：先启动服务端 `evebus serve`，确认 `curl http://localhost:8080/api/v1/health` 返回 `{"status":"ok"}`。

### 事件没收到

1. 检查通配符是否正确（见 [通配符规则](#通配符规则)）
2. 检查发射端是否 `handled=true`（`evebusctl emit` 会提示）
3. SSE 订阅是长连接，确认没有代理缓冲（可加 `X-Accel-Buffering: no`）

### 订阅连接被防火墙断开

`evebusctl subscribe` 默认自动重连（指数退避 2s→4s→...→30s）。

### Windows 中文/emoji 乱码

设置环境变量 `PYTHONIOENCODING=utf-8`。

### 更多

- 版本信息：`evebus --version` / `evebusctl --version`
- 变更记录：`CHANGELOG.md`
- 贡献指南：`CONTRIBUTING.md`
