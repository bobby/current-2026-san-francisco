-- The ops DB (docs/design.md §5.4): the queryable history of the platform.*
-- topics, filled by the collector. Every table but `jobs` is append-only
-- and keyed by event ID, so a re-delivered event is stored once.

-- platform.jobs: the current registry (the topic is compacted).
CREATE TABLE jobs (
    job_name text PRIMARY KEY,
    tenant text NOT NULL,
    owner text NOT NULL,
    tier text NOT NULL,
    kind text NOT NULL,
    current_version text NOT NULL,
    flink_job_id text,
    max_parallelism integer NOT NULL,
    deployed_at timestamptz NOT NULL,
    first_deployed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_tenant ON jobs (tenant);

CREATE TABLE deployments (
    event_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    job_name text NOT NULL,
    tenant text NOT NULL,
    kind text NOT NULL,
    from_version text,
    to_version text NOT NULL,
    commit_sha text NOT NULL,
    author text NOT NULL,
    summary text NOT NULL,
    outcome text NOT NULL,
    flink_job_id text,
    restored_from text
);
CREATE INDEX deployments_job_ts ON deployments (job_name, ts);

CREATE TABLE job_status (
    event_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    job_name text NOT NULL,
    flink_job_id text NOT NULL,
    status text NOT NULL,
    previous_status text,
    restarts bigint NOT NULL,
    exception_class text,
    exception_message text,
    root_cause_class text,
    root_cause_message text,
    stack_head text,
    task text
);
CREATE INDEX job_status_job_ts ON job_status (job_name, ts);

CREATE TABLE checkpoints (
    event_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    job_name text NOT NULL,
    flink_job_id text NOT NULL,
    checkpoint_id bigint NOT NULL,
    status text NOT NULL,
    is_savepoint boolean NOT NULL,
    trigger_ts timestamptz NOT NULL,
    duration_ms bigint NOT NULL,
    checkpointed_size_bytes bigint NOT NULL,
    state_size_bytes bigint NOT NULL,
    failure_reason text
);
CREATE INDEX checkpoints_job_ts ON checkpoints (job_name, ts);

-- Recent logs only: the collector deletes rows older than its retention window.
CREATE TABLE logs (
    event_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    job_name text NOT NULL,
    flink_job_id text,
    level text NOT NULL,
    component text NOT NULL,
    host text NOT NULL,
    logger text NOT NULL,
    message text NOT NULL,
    exception_class text,
    exception_message text,
    root_cause_class text,
    root_cause_message text,
    stack_head text
);
CREATE INDEX logs_job_ts ON logs (job_name, ts);
CREATE INDEX logs_ts ON logs (ts);

CREATE TABLE alerts (
    event_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    status text NOT NULL,
    alertname text NOT NULL,
    fingerprint text NOT NULL,
    severity text,
    job_name text,
    tenant text,
    starts_at timestamptz NOT NULL,
    ends_at timestamptz,
    labels jsonb NOT NULL,
    annotations jsonb NOT NULL,
    generator_url text
);
CREATE INDEX alerts_job_ts ON alerts (job_name, ts);
CREATE INDEX alerts_fingerprint ON alerts (fingerprint);

-- Signals and actions keep the whole event as well as the columns agents
-- filter on.
CREATE TABLE signals (
    signal_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    job_name text NOT NULL,
    tenant text NOT NULL,
    disposition text NOT NULL,
    incident_id uuid,
    failure_class text,
    event jsonb NOT NULL
);
CREATE INDEX signals_job_ts ON signals (job_name, ts);
CREATE INDEX signals_incident ON signals (incident_id);

CREATE TABLE actions (
    event_id uuid PRIMARY KEY,
    ts timestamptz NOT NULL,
    action_id uuid NOT NULL,
    job_name text NOT NULL,
    incident_id uuid,
    actor text NOT NULL,
    action text NOT NULL,
    phase text NOT NULL,
    event jsonb NOT NULL
);
CREATE INDEX actions_job_ts ON actions (job_name, ts);
CREATE INDEX actions_incident ON actions (incident_id);
