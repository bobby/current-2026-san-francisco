//! The ticket web UI: a list and one page per incident. Deliberately
//! generic, not styled like ServiceNow. Each page carries Open Graph tags,
//! so a link to it renders a preview card.

use std::fmt::Write;

use platform_clients::tickets::{Incident, JournalEntry};

pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

const STYLE: &str = r#"
:root { --bg: #f7f7f5; --panel: #ffffff; --text: #1d1f21; --muted: #5f6368; --line: #e2e2de; --accent: #2f6fdb;
  --new: #2f6fdb; --progress: #b7791f; --hold: #6b7280; --resolved: #2f855a; --closed: #4a5568; }
@media (prefers-color-scheme: dark) { :root { --bg: #16181b; --panel: #1f2226; --text: #e8eaed; --muted: #9aa0a6;
  --line: #33373c; --accent: #7aa7ff; --new: #7aa7ff; --progress: #e0b049; --hold: #9ca3af; --resolved: #5fc28e; --closed: #a0aec0; } }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
header { padding: 14px 24px; border-bottom: 1px solid var(--line); background: var(--panel); }
header a { color: var(--text); text-decoration: none; font-weight: 600; }
main { max-width: 960px; margin: 0 auto; padding: 24px 16px; }
h1 { font-size: 22px; margin: 0 0 4px; }
.sub { color: var(--muted); margin: 0 0 20px; }
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { color: var(--muted); font-weight: 500; font-size: 13px; }
td a { color: var(--accent); text-decoration: none; font-family: ui-monospace, monospace; }
dl { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px 24px; margin: 0; }
dt { color: var(--muted); font-size: 13px; }
dd { margin: 0; overflow-wrap: anywhere; }
.state { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 13px; color: #fff; }
.s1 { background: var(--new); } .s2 { background: var(--progress); } .s3 { background: var(--hold); }
.s6 { background: var(--resolved); } .s7, .s8 { background: var(--closed); }
.note { border-left: 3px solid var(--line); padding: 2px 0 2px 12px; margin: 0 0 14px; }
.note .meta { color: var(--muted); font-size: 13px; }
.note.work_notes { border-color: var(--progress); }
pre { white-space: pre-wrap; margin: 4px 0 0; font: inherit; }
@media (max-width: 640px) { .hide-narrow { display: none; } }
"#;

struct Og<'a> {
    title: &'a str,
    description: &'a str,
    url: &'a str,
}

fn page(title: &str, og: Og<'_>, body: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<meta property="og:type" content="website">
<meta property="og:site_name" content="Platform tickets">
<meta property="og:title" content="{og_title}">
<meta property="og:description" content="{og_description}">
<meta property="og:url" content="{og_url}">
<style>{STYLE}</style>
</head>
<body>
<header><a href="/">Platform tickets</a></header>
<main>
{body}
</main>
</body>
</html>"#,
        title = escape(title),
        og_title = escape(og.title),
        og_description = escape(og.description),
        og_url = escape(og.url),
    )
}

fn state_badge(incident: &Incident) -> String {
    let label = incident.state().map_or(incident.state.as_str(), |s| s.label());
    format!(r#"<span class="state s{}">{}</span>"#, escape(&incident.state), escape(label))
}

fn priority_label(priority: &str) -> &'static str {
    match priority {
        "1" => "1 - Critical",
        "2" => "2 - High",
        "3" => "3 - Moderate",
        "4" => "4 - Low",
        _ => "5 - Planning",
    }
}

pub fn list(incidents: &[Incident], public_url: &str) -> String {
    let mut rows = String::new();
    for i in incidents {
        let _ = write!(
            rows,
            r#"<tr><td><a href="/incident/{number}">{number}</a></td><td>{short}</td><td>{state}</td><td class="hide-narrow">{ci}</td><td class="hide-narrow">{group}</td><td class="hide-narrow">{opened}</td></tr>"#,
            number = escape(&i.number),
            short = escape(&i.short_description),
            state = state_badge(i),
            ci = escape(&i.cmdb_ci),
            group = escape(&i.assignment_group),
            opened = escape(&i.opened_at),
        );
    }
    if incidents.is_empty() {
        rows.push_str(r#"<tr><td colspan="6">No incidents yet.</td></tr>"#);
    }
    let body = format!(
        r#"<h1>Incidents</h1>
<p class="sub">The {} most recent.</p>
<div class="panel"><table>
<tr><th>Number</th><th>Short description</th><th>State</th><th class="hide-narrow">Job</th><th class="hide-narrow">Assignment group</th><th class="hide-narrow">Opened (UTC)</th></tr>
{rows}
</table></div>"#,
        incidents.len()
    );
    page(
        "Incidents",
        Og { title: "Platform tickets", description: "Incidents for the streaming platform", url: public_url },
        &body,
    )
}

pub fn detail(incident: &Incident, journal: &[JournalEntry], public_url: &str) -> String {
    let field = |label: &str, value: &str| {
        format!(
            "<div><dt>{}</dt><dd>{}</dd></div>",
            escape(label),
            if value.is_empty() { "—".into() } else { escape(value) }
        )
    };
    let mut fields = String::new();
    fields.push_str(&format!("<div><dt>State</dt><dd>{}</dd></div>", state_badge(incident)));
    for (label, value) in [
        ("Priority", priority_label(&incident.priority)),
        ("Job", &incident.cmdb_ci),
        ("Assignment group", &incident.assignment_group),
        ("Assigned to", &incident.assigned_to),
        ("Caller", &incident.caller_id),
        ("Category", &incident.category),
        ("Opened (UTC)", &incident.opened_at),
        ("Resolved (UTC)", &incident.resolved_at),
        ("Close code", &incident.close_code),
        ("Correlation ID", &incident.correlation_id),
    ] {
        fields.push_str(&field(label, value));
    }
    let mut notes = String::new();
    for entry in journal.iter().rev() {
        let kind = if entry.element == "work_notes" { "Work note" } else { "Comment" };
        let _ = write!(
            notes,
            r#"<div class="note {element}"><div class="meta">{kind} · {by} · {on} UTC</div><pre>{value}</pre></div>"#,
            element = escape(&entry.element),
            by = escape(&entry.created_by),
            on = escape(&entry.created_on),
            value = escape(&entry.value),
        );
    }
    if notes.is_empty() {
        notes.push_str("<p class=\"sub\">No notes yet.</p>");
    }
    let close = if incident.close_notes.is_empty() {
        String::new()
    } else {
        format!(r#"<div class="panel"><dt>Close notes</dt><pre>{}</pre></div>"#, escape(&incident.close_notes))
    };
    let body = format!(
        r#"<h1>{number}: {short}</h1>
<p class="sub">Opened by {by} · updated {updated} UTC by {updated_by}</p>
<div class="panel"><dl>{fields}</dl></div>
<div class="panel"><dt>Description</dt><pre>{description}</pre></div>
{close}
<div class="panel"><h2 style="font-size:16px;margin:0 0 12px">Activity</h2>{notes}</div>"#,
        number = escape(&incident.number),
        short = escape(&incident.short_description),
        by = escape(&incident.sys_created_by),
        updated = escape(&incident.sys_updated_on),
        updated_by = escape(&incident.sys_updated_by),
        description = escape(if incident.description.is_empty() { "—" } else { &incident.description }),
    );
    let state = incident.state().map_or("", |s| s.label());
    let og_title = format!("{}: {}", incident.number, incident.short_description);
    let og_description = format!(
        "{state} · {} · {}",
        priority_label(&incident.priority),
        if incident.assignment_group.is_empty() { "unassigned" } else { &incident.assignment_group }
    );
    let url = format!("{public_url}/incident/{}", incident.number);
    page(&og_title, Og { title: &og_title, description: &og_description, url: &url }, &body)
}

pub fn not_found(what: &str, public_url: &str) -> String {
    page(
        "Not found",
        Og { title: "Not found", description: "No such incident", url: public_url },
        &format!("<h1>Not found</h1><p class=\"sub\">No incident {}.</p>", escape(what)),
    )
}
