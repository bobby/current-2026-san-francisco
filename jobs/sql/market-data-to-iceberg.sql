-- market-data-to-iceberg (tenant market-data): lands market data quotes for
-- listed instruments in Iceberg. Runs at parallelism 2. A traffic spike on
-- `market-data` (F3, docs/design.md §7.1) leaves it under-provisioned:
-- backpressure and growing lag, with no errors. The fix is a rescale.
--
-- The reference-data lookup (`reference_lookup`, from the jobs jar) costs a
-- fixed 5 ms per record, which sets the job's capacity at about 200 records/s
-- per subtask. Chaining is off, so the source backs up behind the lookup and
-- reports backpressure, as a real slow operator would.
--
-- The source runs at one subtask per partition (`scan.parallelism`), apart
-- from the job's parallelism, so records are rebalanced across the lookup's
-- subtasks. Quotes are keyed by symbol and the partitions are uneven (one
-- carries almost half); forwarded straight from the source, the hot
-- partition's lookup would cap the job at any parallelism.

SET 'pipeline.name' = 'market-data-to-iceberg';
SET 'parallelism.default' = '2';
SET 'pipeline.operator-chaining.enabled' = 'false';

ADD JAR '/jobs/java/target/platform-jobs.jar';
CREATE TEMPORARY FUNCTION reference_lookup AS 'demo.platform.jobs.QuoteReferenceLookup';

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

-- Each commit writes a new metadata file; keep only the last few.
ALTER TABLE lake.market_data.quotes SET (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '10'
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
  'scan.parallelism' = '3',
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
FROM market_data
WHERE reference_lookup(symbol) IS NOT NULL;
