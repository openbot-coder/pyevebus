//! EventEngine — 高性能异步事件引擎（Rust 核心）
//!
//! 设计参考：
//! - NautilusTrader MsgBus：通配符匹配 + Handler 分发
//! - pyee：API 风格（on/once/emit/off/wait_for_complete/cancel）
//!
//! 核心特点：
//! - 通配符匹配（`*` 任意字符，`?` 单字符）
//! - Handler 注册/移除
//! - 线程安全（Python 从任何线程调用）

mod engine;
mod matching;

use pyo3::prelude::*;

/// Python 模块入口（模块名必须与 pyproject.toml 的 module-name 一致）
#[pymodule]
fn _ffi(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<engine::PyRouter>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}