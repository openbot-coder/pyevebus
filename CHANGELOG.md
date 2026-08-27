# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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