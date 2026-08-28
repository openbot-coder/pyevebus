# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed (AI 代码审查 — 40 条发现)

**安全（Critical/High）**
- **C1** `server.py`: 管理 API 增加认证 — `EVEBUS_AUTH_TOKEN` 环境变量，所有 `/api/v1/*` 端点要求 `X-Auth-Token`（健康检查免认证）
- **H1** `server.py`: SSE 订阅背压 — `put_nowait` 队列满时丢弃事件，不再无限阻塞/泄漏 handler
- **H2** `engine.py`: `cancel()` 后 done-callback 抛 CancelledError — 先判 `task.cancelled()`
- **H3** `server_cli.py`: SIGTERM 处理 — 与 SIGINT 同样触发子进程清理，不再产生孤儿进程
- **H4** `engine.py`/`executors/base.py`: Router 订阅泄漏 — `off()`/`_remove_handler`/executor `_detach()` 现在按 handler_id 从 router 卸载
- **H5** `server_cli.py`: `--reload` 与 `--workers` 互斥校验
- **H6** `executors/script.py`: `_on_stop` 在 `__init__` 初始化，`stop()` 先于 `start()` 不再抛 AttributeError

**健壮性（Medium）**
- `server.py`: 请求体大小限制（`EVEBUS_MAX_BODY_BYTES`，默认 1MB，超限 413）
- `server.py`: 拒绝多 worker 启动（引擎是进程内单例，`WEB_CONCURRENCY>1` 抛错）
- `engine.py`: `add_source` 失败时回滚（与 add_executor 一致）
- `engine.py`: `wait_for_complete` 循环等待快照，覆盖并发 emit 新增任务
- `engine.py`: hook 异常记录日志，不再静默吞掉
- `sources/timer.py`: `interval_ms<=0` 抛 ValueError；emit 失败记录日志继续循环
- `sources/base.py`: `_detach()` 取消后台任务；`run()` 附加 done-callback 上报异常；`stop()` 只捕获取消
- `sources/websocket.py`: `max_reconnect=0` 至少尝试一次；消息处理异常不触发重连
- `executors/base.py`: `_safe_execute` 错误事件发射失败不递归
- `executors/script.py`: `on_start` 任务跟踪；`start()` 防重入；`stop()` 复位 `_running`
- `rpc/client.py`: JSONDecodeError 跳过坏帧；重连计数在正常流结束时也累计

**质量（Low）**
- `hooks.py`: `@hook(stage)` 装饰器元数据被 `add_hook` 消费（stage 可省略）
- `cli.py`: 无效 JSON 友好报错；API 调用失败返回非零退出码
- `src/engine.rs`: `subscribe` 去重，避免重复分发
- `src/matching.rs`: 修正复杂度注释（DP O(n·m)）

## [0.3.0] — 2026-08-29

### Added

- **RPC 流式订阅（SSE）** — pyevebus 作为远程 RPC 后端
  - `GET /api/v1/events/subscribe?pattern=` — Server-Sent Events 长连接推送
  - 通配符 pattern 订阅（复用 Rust 路由器）
  - 每连接独立队列（背压上限 1024），慢消费者不丢事件
  - 连接断开自动注销 handler，无泄漏
- **RPCClient SDK** — `evebus.rpc.RPCClient`
  - `emit()` 单向发射事件
  - `subscribe()` 异步流式订阅（支持自动重连 + 指数退避）
  - `health()` / `stats()` / `list_sources()` / `list_executors()`
- **`evebusctl subscribe`** — CLI 流式订阅命令（彩色时间戳输出）
- **跨语言消费** — 标准 SSE 协议，任意语言可消费（curl / JS EventSource / Go）
- **示例** — `examples/05_rpc_subscribe.py`

### Changed

- **依赖** — 新增 `httpx`（RPCClient SDK 所需）

## [0.2.0] — 2026-08-28

### Added

- **CLI Split** — separated into `evebus` (server) and `evebusctl` (client) for independent distribution
  - `evebus serve/run` — server commands (engine + HTTP API + script executor)
  - `evebusctl status/emit/sources/executors/plugins` — client management commands
- **Test Suite** — 233 test cases with 95% branch coverage
  - Engine core, wildcard matching, hook system, plugin lifecycle
  - HTTP API (FastAPI TestClient), CLI (click CliRunner)
  - WebSocket/Webhook/Timer sources, ScriptExecutor reload/stop
  - Edge cases: concurrency, error propagation, async cancellation

### Changed

- **Package Renamed** — `evebus` → `pyevebus` for PyPI release
  (name `evebus` was rejected as too similar to existing projects)
  - Python import stays `from evebus import ...`
  - CLI commands stay `evebus` / `evebusctl`
- **Serve Shutdown Fix** — Ctrl+C now exits cleanly (removed deadlocking
  signal handler; replaced with `KeyboardInterrupt` + `terminate`/`kill` fallback)
- **abi3 Stable ABI** — wheels now use `cp39-abi3` (one wheel supports Python 3.9+)
- **Multi-Platform CI** — GitHub Actions matrix builds wheels for
  Windows / macOS / Linux × x86_64 / aarch64, with smoke tests and
  Trusted Publishing to PyPI

## [0.1.0] — 2026-08-26

### Added

- **Core Engine** — async event engine with pyee-compatible API (`on`/`once`/`off`/`emit`)
- **Wildcard Matching** — Rust-powered `*` and `?` pattern matching (DP algorithm)
- **Hook System** — middleware pipeline: `pre_emit`, `post_emit`, `on_error`
- **Event Sources** — pluggable sources:
  - `TimerSource` — periodic event emission
  - `WebSocketSource` — WebSocket data ingestion with auto-reconnect
  - `WebhookSource` — HTTP webhook ingestion
- **Event Executors** — pluggable handlers:
  - `ScriptExecutor` — dynamic Python script loading with hot-reload
- **Plugin System** — lifecycle-managed extensions (`on_attach`/`on_detach`)
- **HTTP API** — FastAPI management interface for runtime source/executor/plugin management
- **CLI** — command-line tool (`evebus serve/sources/executors/emit/status`)
- **Rust Core** — high-performance wildcard matching via PyO3 bindings
- **Python Fallback** — pure-Python matching for development without Rust toolchain