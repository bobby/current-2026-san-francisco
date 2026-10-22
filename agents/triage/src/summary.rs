//! The one-line summary on a signal's card: a code template filled from the
//! evidence. Jev never writes text (docs/design.md §6.1).

use crate::evidence::{Evidence, short_class};

pub fn summary(e: &Evidence) -> String {
    let mut parts = Vec::new();

    let mut counts: Vec<(String, usize)> = Vec::new();
    for a in &e.alerts {
        match counts.iter_mut().find(|(name, _)| *name == a.alert) {
            Some((_, n)) => *n += 1,
            None => counts.push((a.alert.clone(), 1)),
        }
    }
    let alerts: Vec<String> =
        counts.iter().map(|(name, n)| if *n > 1 { format!("{name} ×{n}") } else { name.clone() }).collect();
    parts.push(alerts.join(", "));

    if let Some(f) = e.failures.first() {
        parts.push(format!(
            "{} failure{} in {} min, mostly {}",
            e.status.failures_in_window,
            if e.status.failures_in_window == 1 { "" } else { "s" },
            e.alert_history.window_minutes,
            short_class(&f.exception)
        ));
    }

    let c = &e.checkpoints;
    if c.completed_in_window == 0 && e.status.failures_in_window > 0 {
        parts.push("no checkpoint completed".to_string());
    } else if let Some(minutes) = c.last_completed_minutes_ago.filter(|m| *m >= 3.0) {
        parts.push(format!("last checkpoint {minutes:.0} min ago"));
    }
    if let (Some(recent), Some(base)) = (c.recent_max_duration_seconds, c.baseline_duration_seconds)
        && recent >= 2.0 * base.max(1.0)
    {
        parts.push(format!("checkpoints up to {recent:.1} s vs {base:.1} s"));
    }
    if let (Some(now), Some(start)) = (c.state_size_mb, c.state_size_mb_at_window_start)
        && now >= 1.5 * start.max(1.0)
    {
        parts.push(format!("state {start:.0} → {now:.0} MB"));
    }

    let m = &e.metrics;
    if let Some(lag) = m.lag_records_now.filter(|l| *l >= 10_000.0) {
        let before = m.lag_records_15_minutes_ago.unwrap_or(0.0);
        parts.push(format!("lag {} → {}", count(before), count(lag)));
    }
    if let Some(ratio) = m.input_rate_vs_15_minutes_ago.filter(|r| *r >= 2.0) {
        parts.push(format!("input ×{ratio:.1}"));
    }
    if let Some(bp) = m.backpressured_ms_per_second.filter(|b| *b >= 500.0) {
        parts.push(format!("backpressured {:.0}%", bp / 10.0));
    }

    if let Some(d) = e.deployments.first() {
        parts.push(format!("{} {} {:.0} min before", d.kind, d.to_version, d.minutes_ago));
    }
    parts.retain(|p| !p.is_empty());
    parts.join(" · ")
}

fn count(n: f64) -> String {
    if n >= 1_000_000.0 {
        format!("{:.1}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.0}k", n / 1_000.0)
    } else {
        format!("{n:.0}")
    }
}

#[cfg(test)]
mod tests {
    use super::count;

    #[test]
    fn counts_are_compact() {
        assert_eq!(count(950.0), "950");
        assert_eq!(count(81_400.0), "81k");
        assert_eq!(count(2_300_000.0), "2.3M");
    }
}
