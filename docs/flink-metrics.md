# Flink Metrics Reference

These are the Prometheus metrics the real Flink 2.1.2 jobs actually report. They were scraped from the local platform with all six jobs running (issue #2). The simulated fleet (#5) must emit **exactly these names and labels**, so that queries, alert rules and agents treat real and simulated jobs the same way (docs/design.md §5.2).

Flink reports about 370 `flink_*` series names. This is the curated subset the telemetry plane uses.

## Labels

The Prometheus reporter **rewrites label values**: every character other than `[A-Za-z0-9_]` becomes `_`. For example:

- job `market-data-to-iceberg` → `job_name="market_data_to_iceberg"`
- host `192.168.117.9` → `host="192_168_117_9"`
- operator `Source: market_data[30]` → `operator_name="Source:_market_data_30_"`

The simulated fleet must apply the same rewriting.

| Scope | Labels |
|---|---|
| JobManager, per job | `job_id`, `job_name`, `host` |
| TaskManager, per task | the above plus `task_id`, `task_name`, `task_attempt_id`, `task_attempt_num`, `subtask_index`, `tm_id` |
| TaskManager, per operator | the above plus `operator_id`, `operator_name` |
| Kafka source, per partition | the above plus `topic`, `partition` |
| TaskManager, process | `host`, `tm_id` |

Prometheus adds `instance` and `job="flink"` at scrape time.

## Curated metrics

### Job health (JobManager)

| Metric | Unit | Signals |
|---|---|---|
| `flink_jobmanager_job_uptime` | ms | Resets to 0 on every restart; stays low in a restart loop (F1, F2) |
| `flink_jobmanager_job_numRestarts` | count | Climbs in a restart loop (F1, F2) |
| `flink_jobmanager_job_restartingTime` | ms | Time spent in the current restart |
| `flink_jobmanager_job_downtime` | ms | Time the job has been failing or restarting |

### Checkpoints (JobManager)

| Metric | Unit | Signals |
|---|---|---|
| `flink_jobmanager_job_lastCheckpointDuration` | ms | Rises under backpressure (F3) and with state growth (F4) |
| `flink_jobmanager_job_lastCheckpointSize` | bytes | Checkpointed size; grows steadily with a state leak (F4) |
| `flink_jobmanager_job_lastCheckpointFullSize` | bytes | Full state size |
| `flink_jobmanager_job_numberOfCompletedCheckpoints` | count | Stops increasing when the job is failing (F1, F2) |
| `flink_jobmanager_job_numberOfFailedCheckpoints` | count | Checkpoint timeouts (F3, F4) |
| `flink_jobmanager_job_lastCheckpointCompletedTimestamp` | epoch ms | Staleness of the last good checkpoint |

### Throughput and backpressure (TaskManager, per task)

| Metric | Unit | Signals |
|---|---|---|
| `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | ms/s (0–1000) | Backpressure (F3) |
| `flink_taskmanager_job_task_busyTimeMsPerSecond` | ms/s (0–1000) | Saturation; near 1000 on the bottleneck task (F3) |
| `flink_taskmanager_job_task_numRecordsInPerSecond` | records/s | Input rate; drops to 0 when the job is down |
| `flink_taskmanager_job_task_numRecordsOutPerSecond` | records/s | Output rate |

### Kafka source lag (TaskManager, per source operator)

| Metric | Unit | Signals |
|---|---|---|
| `flink_taskmanager_job_task_operator_pendingRecords` | records | Consumer lag (FLIP-33 standard); grows under a traffic spike (F3) or while the job is down |
| `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max` | records | Kafka client's max partition lag |
| `flink_taskmanager_job_task_operator_KafkaSourceReader_topic_partition_currentOffset` | offset | Per-partition position (`topic`, `partition` labels) |

### TaskManager memory (per TaskManager process)

| Metric | Unit | Signals |
|---|---|---|
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | bytes | Rises with heap state growth (F4) |
| `flink_taskmanager_Status_JVM_Memory_Heap_Max` | bytes | Heap ceiling |
| `flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_TimeMsPerSecond` | ms/s | GC pressure before an OOM (F4) |

### Cluster capacity (JobManager)

| Metric | Unit | Signals |
|---|---|---|
| `flink_jobmanager_numRegisteredTaskManagers` | count | Lost TaskManagers (platform infrastructure) |
| `flink_jobmanager_taskSlotsTotal` | count | Capacity |
| `flink_jobmanager_taskSlotsAvailable` | count | Room to rescale (Scenario 3 guardrail) |

## Regenerating the list

With jobs running, list every Flink metric name with:

```sh
curl -s localhost:9090/api/v1/label/__name__/values | jq -r '.data[] | select(startswith("flink_"))'
```
