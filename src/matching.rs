/// 通配符匹配算法
///
/// 支持两种通配符：
/// - `*` 匹配任意数量的字符（包括零个）
/// - `?` 恰好匹配一个字符
///
/// 动态规划实现，时间复杂度 O(n*m)，空间复杂度 O(m)（滚动数组）。
/// 注意：`find_matches` 会对每个 pattern 调用 `is_match`，因此总复杂度为
/// O(patterns * n * m)。若 pattern 集很大，可考虑贪心回溯算法以降低
/// 常见情况复杂度，但当前 DP 实现行为确定且易于验证。

/// 检查 topic 是否匹配 pattern
///
/// ```
/// assert!(matching::is_match("data.quotes.BINANCE.ETHUSDT", "data.quotes.*.ETHUSDT"));
/// assert!(matching::is_match("data.quotes.BINANCE.ETHUSDT", "data.quotes.BINANCE.*"));
/// assert!(matching::is_match("data.quotes.BINANCE.ETHUSDT", "data.*"));
/// assert!(matching::is_match("data.quotes.BINANCE.ETHUSDT", "*"));
/// assert!(!matching::is_match("data.trades.BINANCE.ETHUSDT", "data.quotes.*"));
/// ```
pub fn is_match(topic: &str, pattern: &str) -> bool {
    let t = topic.as_bytes();
    let p = pattern.as_bytes();

    // 精确匹配快速路径
    if t == p {
        return true;
    }

    // 无通配符快速路径
    if !p.contains(&b'*') && !p.contains(&b'?') {
        return false;
    }

    let t_len = t.len();
    let p_len = p.len();

    // dp[i][j] = topic[0..i] 是否匹配 pattern[0..j]
    // 空间优化：只用两行
    let mut prev = vec![false; p_len + 1];
    let mut curr = vec![false; p_len + 1];

    // 空 pattern 匹配空 topic
    prev[0] = true;

    // 处理 pattern 前导 `*`
    for j in 1..=p_len {
        if p[j - 1] == b'*' {
            prev[j] = prev[j - 1];
        }
    }

    for i in 1..=t_len {
        curr[0] = false;
        for j in 1..=p_len {
            match p[j - 1] {
                b'*' => {
                    // `*` 匹配零个（curr[j-1]）或多个（prev[j]）
                    curr[j] = curr[j - 1] || prev[j];
                }
                b'?' => {
                    // `?` 匹配恰好一个
                    curr[j] = prev[j - 1];
                }
                c => {
                    // 精确匹配
                    curr[j] = prev[j - 1] && t[i - 1] == c;
                }
            }
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[p_len]
}

/// 从 pattern 列表中找出所有匹配 topic 的 pattern
pub fn find_matches(topic: &str, patterns: &[String]) -> Vec<String> {
    patterns
        .iter()
        .filter(|p| is_match(topic, p))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        assert!(is_match("data.quotes.BINANCE.ETHUSDT", "data.quotes.BINANCE.ETHUSDT"));
    }

    #[test]
    fn test_star_wildcard() {
        assert!(is_match("data.quotes.BINANCE.ETHUSDT", "data.quotes.*"));
        assert!(is_match("data.quotes.BINANCE.ETHUSDT", "data.*"));
        assert!(is_match("data.quotes.BINANCE.ETHUSDT", "*"));
        assert!(is_match("data.quotes.BINANCE.ETHUSDT", "data.*.ETHUSDT"));
    }

    #[test]
    fn test_question_wildcard() {
        assert!(is_match("abc", "a?c"));
        assert!(!is_match("abbc", "a?c"));
        assert!(is_match("abbc", "a??c"));
        assert!(is_match("aXc", "a?c"));
    }

    #[test]
    fn test_mixed_wildcards() {
        assert!(is_match("data.quotes.BINANCE.ETHUSDT", "data.*.*USDT"));
        assert!(is_match("data.trades.OKX.BTCUSDT", "data.*.*USDT"));
    }

    #[test]
    fn test_no_match() {
        assert!(!is_match("data.trades.BINANCE.ETHUSDT", "data.quotes.*"));
        assert!(!is_match("a", "ab"));
        assert!(!is_match("ab", "a"));
    }

    #[test]
    fn test_find_matches() {
        let patterns = vec![
            "data.quotes.*".to_string(),
            "data.trades.*".to_string(),
            "data.*.ETHUSDT".to_string(),
        ];
        let matches = find_matches("data.quotes.BINANCE.ETHUSDT", &patterns);
        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&"data.quotes.*".to_string()));
        assert!(matches.contains(&"data.*.ETHUSDT".to_string()));
    }
}