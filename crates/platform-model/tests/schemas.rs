//! The committed JSON Schema files match the Rust types. Regenerate them with
//! `UPDATE_SCHEMAS=1 cargo test -p platform-model`.

use std::{fs, path::Path};

#[test]
fn schema_files_are_up_to_date() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
    let update = std::env::var_os("UPDATE_SCHEMAS").is_some();
    let mut stale = Vec::new();
    for (topic, schema) in platform_model::schemas() {
        let path = dir.join(format!("{topic}.schema.json"));
        let generated = serde_json::to_string_pretty(&schema).unwrap() + "\n";
        if update {
            fs::write(&path, &generated).unwrap();
        } else if fs::read_to_string(&path).ok().as_deref() != Some(generated.as_str()) {
            stale.push(path.display().to_string());
        }
    }
    assert!(stale.is_empty(), "stale schemas (run UPDATE_SCHEMAS=1 cargo test -p platform-model): {stale:?}");
}
