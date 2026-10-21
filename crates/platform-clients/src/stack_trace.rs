//! Summarises Java stack traces into the fields agents use: the root cause and
//! the first few lines.

/// Lines kept in a stack-trace head.
const HEAD_LINES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StackSummary {
    /// Class of the innermost `Caused by:` exception (or the top exception if
    /// there is none).
    pub root_cause_class: Option<String>,
    pub root_cause_message: Option<String>,
    /// The first lines of the trace.
    pub head: Option<String>,
}

/// Summarises a stringified Java stack trace, as Flink's REST API and its
/// JSON logs report it.
pub fn summarize(stack_trace: &str) -> StackSummary {
    let stack_trace = stack_trace.trim();
    if stack_trace.is_empty() {
        return StackSummary::default();
    }
    let head = stack_trace.lines().take(HEAD_LINES).collect::<Vec<_>>().join("\n");
    let root = stack_trace
        .lines()
        .rev()
        .find_map(|line| line.trim_start().strip_prefix("Caused by: "))
        .or_else(|| stack_trace.lines().next());
    let (root_cause_class, root_cause_message) = root.map(split_exception).unwrap_or_default();
    StackSummary { root_cause_class, root_cause_message, head: Some(head) }
}

/// Splits `java.lang.Foo: message` into its class and message.
fn split_exception(line: &str) -> (Option<String>, Option<String>) {
    let (class, message) = match line.split_once(": ") {
        Some((class, message)) => (class, Some(message.trim().to_string()).filter(|m| !m.is_empty())),
        None => (line, None),
    };
    let class = class.trim();
    let is_class = !class.is_empty() && !class.contains(char::is_whitespace);
    (is_class.then(|| class.to_string()), message)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE: &str = "java.io.IOException: Failed to deserialize consumer record due to
\tat org.apache.flink.connector.kafka.source.reader.KafkaRecordEmitter.emitRecord(KafkaRecordEmitter.java:56)
Caused by: org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException: Could not forward element to next operator
\tat org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:92)
Caused by: java.lang.NullPointerException
\tat demo.platform.jobs.PaymentsEnrichment.enrich(PaymentsEnrichment.java:91)
\t... 30 more";

    #[test]
    fn finds_the_innermost_cause() {
        let summary = summarize(TRACE);
        assert_eq!(summary.root_cause_class.as_deref(), Some("java.lang.NullPointerException"));
        assert_eq!(summary.root_cause_message, None);
        assert!(summary.head.unwrap().starts_with("java.io.IOException: Failed"));
    }

    #[test]
    fn keeps_the_cause_message() {
        let summary = summarize(
            "x.A: outer\nCaused by: demo.MalformedRecordException: Malformed record in trade-events partition 0 at offset 7: bad",
        );
        assert_eq!(summary.root_cause_class.as_deref(), Some("demo.MalformedRecordException"));
        assert_eq!(
            summary.root_cause_message.as_deref(),
            Some("Malformed record in trade-events partition 0 at offset 7: bad")
        );
    }

    #[test]
    fn without_a_cause_the_top_exception_is_the_root() {
        let summary = summarize("java.lang.IllegalStateException: boom\n\tat x.y(Z.java:1)");
        assert_eq!(summary.root_cause_class.as_deref(), Some("java.lang.IllegalStateException"));
        assert_eq!(summary.root_cause_message.as_deref(), Some("boom"));
    }

    #[test]
    fn empty_trace() {
        assert_eq!(summarize("  "), StackSummary::default());
    }
}
