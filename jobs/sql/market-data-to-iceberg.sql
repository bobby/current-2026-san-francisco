-- market-data-to-iceberg (tenant market-data): lands market data quotes in
-- Iceberg. Runs at parallelism 2. A traffic spike on `market-data` (F3,
-- docs/design.md §7.1) leaves it under-provisioned: backpressure and growing
-- lag, with no errors. The fix is a rescale.

SET 'pipeline.name' = 'market-data-to-iceberg';
SET 'parallelism.default' = '2';

CREATE DATABASE IF NOT EXISTS lake.market_data;

CREATE TABLE IF NOT EXISTS lake.market_data.quotes (
  symbol STRING,
  venue STRING,
  bid DOUBLE,
  ask DOUBLE,
  last_price DOUBLE,
  size BIGINT,
  spread_bps DOUBLE,
  quoted_at TIMESTAMP_LTZ(6)
);

CREATE TEMPORARY TABLE market_data (
  symbol STRING,
  venue STRING,
  bid DOUBLE,
  ask DOUBLE,
  `last` DOUBLE,
  size BIGINT,
  ts BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'market-data',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'market-data-to-iceberg',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json'
);

INSERT INTO lake.market_data.quotes
SELECT symbol, venue, bid, ask, `last`, size,
       (ask - bid) / ((ask + bid) / 2) * 10000,
       TO_TIMESTAMP_LTZ(ts, 3)
FROM market_data;
