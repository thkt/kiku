pub fn truncate_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub fn redact_tokens(message: &str) -> String {
    use std::sync::LazyLock;
    use regex_lite::Regex;
    static SLACK_TOKEN_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"xox[a-z]-[a-zA-Z0-9\-]+").unwrap());
    static GEMINI_KEY_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"AIza[a-zA-Z0-9\-_]+").unwrap());

    let result = SLACK_TOKEN_RE.replace_all(message, "[REDACTED]");
    GEMINI_KEY_RE.replace_all(&result, "[REDACTED]").into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_slack_tokens() {
        let msg = "Error: invalid token xoxb-123-456-abc in request";
        assert_eq!(
            redact_tokens(msg),
            "Error: invalid token [REDACTED] in request"
        );
    }

    #[test]
    fn removes_gemini_keys() {
        let msg = "API key AIzaSyB1234567890abcdef is invalid";
        assert_eq!(
            redact_tokens(msg),
            "API key [REDACTED] is invalid"
        );
    }

    #[test]
    fn handles_no_tokens() {
        let msg = "Normal error message";
        assert_eq!(redact_tokens(msg), msg);
    }
}
