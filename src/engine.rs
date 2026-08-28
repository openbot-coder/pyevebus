use std::collections::HashMap;
use pyo3::prelude::*;

use crate::matching;

/// 路由器 — 管理 pattern → handler 映射，支持通配符匹配
///
/// 纯 Rust 数据结构，Python 通过 PyO3 调用。
#[pyclass]
pub struct PyRouter {
    /// pattern → handler_id 列表
    handlers: HashMap<String, Vec<String>>,
    /// 所有已注册的 pattern（去重，用于遍历匹配）
    patterns: Vec<String>,
}

#[pymethods]
impl PyRouter {
    #[new]
    fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            patterns: Vec::new(),
        }
    }

    /// 注册 handler（#28: 去重，避免同一 handler 重复分发）
    fn subscribe(&mut self, pattern: &str, handler_id: &str) {
        let list = self
            .handlers
            .entry(pattern.to_string())
            .or_insert_with(Vec::new);
        // #28: 重复订阅同一 (pattern, handler_id) 时跳过
        if !list.iter().any(|h| h == handler_id) {
            list.push(handler_id.to_string());
        }

        // #29: patterns 用 contains 检查（此处 list 已存在即 pattern 已注册）
        if !self.patterns.iter().any(|p| p == pattern) {
            self.patterns.push(pattern.to_string());
        }
    }

    /// 移除 handler
    fn unsubscribe(&mut self, pattern: &str, handler_id: &str) {
        if let Some(handlers) = self.handlers.get_mut(pattern) {
            handlers.retain(|h| h != handler_id);
            if handlers.is_empty() {
                self.handlers.remove(pattern);
                self.patterns.retain(|p| p != pattern);
            }
        }
    }

    /// 查找匹配 topic 的所有 handler_id
    fn match_handlers(&self, topic: &str) -> Vec<String> {
        let mut result = Vec::new();
        for pattern in &self.patterns {
            if matching::is_match(topic, pattern) {
                if let Some(handlers) = self.handlers.get(pattern) {
                    result.extend(handlers.iter().cloned());
                }
            }
        }
        result
    }

    /// 查找匹配 topic 的所有 pattern
    fn match_patterns(&self, topic: &str) -> Vec<String> {
        matching::find_matches(topic, &self.patterns)
    }

    /// 检查 topic 是否有任何匹配
    fn has_match(&self, topic: &str) -> bool {
        self.patterns.iter().any(|p| matching::is_match(topic, p))
    }

    /// 获取所有已注册的 pattern
    fn patterns(&self) -> Vec<String> {
        self.patterns.clone()
    }

    /// 获取指定 pattern 下的 handler_id 列表
    fn handlers_of(&self, pattern: &str) -> Vec<String> {
        self.handlers.get(pattern).cloned().unwrap_or_default()
    }

    /// 获取 pattern 数量
    fn __len__(&self) -> usize {
        self.patterns.len()
    }

    /// 是否为空
    fn __bool__(&self) -> bool {
        !self.patterns.is_empty()
    }
}