-- audit-log-archiver (tenant compliance-eng): archives audit log entries to
-- Iceberg. The healthy control job, deployed often (docs/design.md §4.2).

SET 'pipeline.name' = 'audit-log-archiver';

CREATE DATABASE IF NOT EXISTS lake.compliance_eng;

CREATE TABLE IF NOT EXISTS lake.compliance_eng.audit_log (
  audit_id STRING,
  actor STRING,
  action STRING,
  resource STRING,
  outcome STRING,
  source_ip STRING,
  logged_at TIMESTAMP_LTZ(6)
);

-- Each commit writes a new metadata file; keep only the last few.
ALTER TABLE lake.compliance_eng.audit_log SET (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
);

CREATE TEMPORARY TABLE audit_log (
  audit_id STRING,
  actor STRING,
  action STRING,
  resource STRING,
  outcome STRING,
  source_ip STRING,
  ts BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'audit-log',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'audit-log-archiver',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json'
);

INSERT INTO lake.compliance_eng.audit_log
SELECT audit_id, actor, action, resource, outcome, source_ip, TO_TIMESTAMP_LTZ(ts, 3)
FROM audit_log;
