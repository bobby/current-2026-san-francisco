-- customer-events-to-iceberg (tenant customer-360): lands customer activity
-- events in Iceberg. Schema drift upstream (F5, a stretch goal; see
-- docs/design.md §7.1) breaks this job.

SET 'pipeline.name' = 'customer-events-to-iceberg';

CREATE DATABASE IF NOT EXISTS lake.customer_360;

CREATE TABLE IF NOT EXISTS lake.customer_360.customer_events (
  event_id STRING,
  customer_id STRING,
  event_type STRING,
  channel STRING,
  session_id STRING,
  occurred_at TIMESTAMP_LTZ(6)
);

CREATE TEMPORARY TABLE customer_events (
  event_id STRING,
  customer_id STRING,
  event_type STRING,
  channel STRING,
  session_id STRING,
  ts BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'customer-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'customer-events-to-iceberg',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json'
);

INSERT INTO lake.customer_360.customer_events
SELECT event_id, customer_id, event_type, channel, session_id, TO_TIMESTAMP_LTZ(ts, 3)
FROM customer_events;
