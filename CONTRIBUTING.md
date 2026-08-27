# 贡献指南

欢迎为 EveBus 贡献代码！请遵循以下规范。

## 开发环境

项目使用 [uv](https://docs.astral.sh/uv/) 管理：

```bash
# 安装 uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# 克隆项目
git clone https://github.com/pyevebus/pyevebus.git
cd evebus

# 同步依赖（含开发依赖）
uv sync --dev

# 构建 Rust 扩展
uv run maturin develop
```

## 项目结构

```
evebus/
├── src/                      # Rust 核心（PyO3 绑定）
├── python/evebus/            # Python 包
│   ├── engine.py             # 核心引擎
│   ├── hooks.py              # Hook 系统
│   ├── plugin.py             # 插件系统
│   ├── cli.py                # evebusctl 客户端 CLI
│   ├── server_cli.py         # evebus 服务端 CLI
│   ├── server.py             # HTTP API (FastAPI)
│   ├── sources/              # 事件源
│   └── executors/            # 执行器
├── tests/                    # 测试 (233 用例，95% 覆盖率)
└── examples/                 # 示例
```

## 代码规范

- Python 使用 [ruff](https://docs.astral.sh/ruff/) 检查：`uv run ruff check .`
- 类型标注使用 mypy：`uv run mypy python/evebus`
- Rust 代码使用 `cargo fmt` / `cargo clippy`

## 测试

```bash
uv run python -m pytest tests/ -v
```

## 提交规范

- 遵循 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/v1.0.0/)
- 格式：`<type>(<scope>): <描述>`
- 类型：`feat` `fix` `docs` `style` `refactor` `test` `chore`

## 发布流程

```bash
# 1. 更新版本号
# pyproject.toml 和 Cargo.toml 的 version

# 2. 构建 wheel
uv run maturin build --release

# 3. 发布到 PyPI
uv run maturin publish
```

> ⚠️ 发布前请确保：
> - 所有测试通过
> - CHANGELOG.md 已更新
> - README.md 文档同步