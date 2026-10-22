# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project status

This repo holds demo code for the Current 2026 San Francisco talk "From Alerts to Autonomy: Advancing the Maturity Curve with Agentic Managed Streaming Platforms". README.md has the abstract and an architecture summary. The full design, including decisions, failure-mode catalogue, scenario flows and milestones, is in `docs/design.md`; read it before making changes.

## Commands

The local platform runs under Docker Compose (`compose.yml`, project name `platform`):

- `make up`: build the images and start the stack, waiting until every service is healthy.
- `make smoke`: run the end-to-end smoke test (`scripts/smoke-test.sh`) against the running stack. Run it after any change to `compose.yml`, `flink/` or service config.
- `make down` stops the stack and keeps the data. `make clean` also deletes the volumes.
- `make ps` and `make logs` show status and logs.
- `make up` requires a ShadowTraffic license in `shadowtraffic/license.env` (git-ignored; see `shadowtraffic/license.env.example`). Never read, print or commit it.

Jobs (`jobs/`; design doc §4.2):

- `make jobs`: build the Java jobs jar (`jobs/java/target/platform-jobs.jar`) in a Maven container; no local JDK needed.
- `scripts/jobs.sh submit <job> [flags]` deploys one job by hand, e.g. `scripts/jobs.sh submit payments-enrichment --fault=npe-on-null-field`. Also `submit-all`, `cancel <job>|all` and `list`, wrapped by `make jobs-submit`, `make jobs-cancel` and `make jobs-list`.
- `make jobs-test` (`scripts/jobs-test.sh`): checks that every job commits to Iceberg and that every fault fails as designed. It produces its own fixture records and takes a few minutes. Run it after changing `jobs/` or the Flink image.

Versioned deploys go through **cicd-sim** (design doc §4.3; `jobs/versions.yaml` is the version catalogue):

- `scripts/cicd.sh catalogue | deploy <job> <version> [args...] | rollback <job> [version] | baseline [job...]` calls its API on `:8091`. Each deploy emits `platform.deployments` and updates `platform.jobs`.
- `make cicd-test` (`scripts/cicd-test.sh`) covers baseline, a SQL deploy from a savepoint, F1 deploy then rollback, and deploy-time args. Run it after changing cicd-sim, the catalogue or the jobs.
- `scripts/jobs.sh` still submits jobs directly, but skips the pipeline: no deployment events, no registry. Use cicd-sim for anything a scenario or agent depends on.

How the jobs are built:

- The Java jobs are one jar with `--job=<name>`, fault flags (`--fault=npe-on-null-field|state-leak`) and `--on-deser-error=fail|dlq`.
- They reach Kafka and Iceberg **only through the Table API**, using the connectors on the cluster classpath (`flink/Dockerfile`). `flink-sql-connector-kafka` relocates Kafka's client classes, so don't add Kafka or Iceberg dependencies to the jar.
- SQL jobs are `jobs/sql/<name>.sql`, submitted through the SQL Gateway.
- Every job uses the `lake` Iceberg catalog, defined per runtime in `flink/catalogs/<runtime>/lake.yaml` and loaded through Flink's file catalog store.
- `docs/flink-metrics.md` lists the real Flink metric names and labels that the simulated fleet must match.

Telemetry plane and Rust code (design doc §5, §10):

- The Rust workspace (`Cargo.toml`) has the shared crates in `crates/` (`platform-model` holds the `platform.*` event types, `platform-clients` the Flink REST and ops DB clients) and the services in `services/`. Every service binary ships in one image, `platform/services`, built by `services/Dockerfile`; Compose picks the binary with `command`.
- `make rust-test` runs the Rust tests. The ops DB tests need the local stack's Postgres, where each test gets a throwaway database. It also needs a local Rust toolchain.
- `make telemetry-test` (`scripts/telemetry-test.sh`) checks that real job status, checkpoints, logs and alerts reach their topics and the ops DB, and that point-in-time queries work. Run it after changing the collector, Vector or the model.
- After changing a type in `platform-model`, regenerate the JSON Schemas with `UPDATE_SCHEMAS=1 cargo test -p platform-model`; a test fails while they're stale. Schema changes are breaking for ShadowTraffic's fleet configs, so update those too.
- Ops DB schema changes are new files in `crates/platform-clients/migrations/`; never edit an applied migration.

The simulated fleet and alerting (design doc §4.4, §5.2, §5.3):

- `st-fleet` runs `shadowtraffic/fleet.json`. **Don't edit it or the rosters by hand:** change `shadowtraffic/fleet/generate.py` and rerun it. The local runtime mounts the 200-job `roster-local.json`; the AWS override mounts the 1,000-job `roster-cloud.json`.
- The fleet-exporter turns `fleet.telemetry` into `platform.*` events and serves Flink-identical metrics on `:9249`. Simulated series must keep the exact names and label sets of real ones (`docs/flink-metrics.md`), with no extra labels.
- Alert rules live in `observability/prometheus/rules/` and are deliberately naive; Alertmanager's config is in `observability/alertmanager/`. `make up` reloads both configs.
- `make fleet-test` (`scripts/fleet-test.sh`) checks fleet events, metric label parity with real jobs, and the alert stream. It waits up to 15 minutes for alerts to build up.

Human interfaces (design doc §9):

- Mattermost is built for arm64 from the official release tarball (`mattermost/Dockerfile`); Mattermost publishes amd64 images only. Configuration is `MM_*` variables in `compose.yml`.
- The **chatops-bot** is the only Mattermost client. On every start it sets up, idempotently, the `oncall` admin (people log in as it), the `platform` team, the three channels and five bot accounts (`alertmanager`, `tickets`, `triage-bot`, `operator-bot`, `ops-assistant`). Agents post through its API with the typed cards in `platform_clients::chatops`; all formatting lives in `services/chatops-bot/src/cards.rs`. Don't post to Mattermost directly.
- Button clicks reach the bot through Mattermost (`MM_SERVICESETTINGS_ALLOWEDUNTRUSTEDINTERNALCONNECTIONS`), and each decision goes to `platform.actions`. Carrying out an approved action is the agents' job (#10, #11).
- The **tickets** mock follows the ServiceNow Table API (`platform_clients::tickets`). It notifies the bot after every change, so ticket cards stay current. Its schema is in `services/tickets/migrations/`.
- Grafana's dashboard is provisioned from `observability/grafana/dashboards/platform-overview.json`; edit the file, not the UI. It reads the ops DB as the read-only `grafana` role (`postgres/databases.sh`).
- `make chatops-test` (`scripts/chatops-test.sh`) covers the workspace, the alert firehose, the Table API, ticket cards, a button click through Mattermost, ticket lookups and Grafana's datasources. Run it after changing any of these.

Local endpoints (all bound to 127.0.0.1): Flink UI/REST `:8081`, SQL Gateway `:8083`, Prometheus `:9090`, Kafka `:9094` (inside Compose, `kafka:9092`), Iceberg REST catalog `:8181`, RustFS S3 API `:9000` and console `:9001`, Postgres `:5432`, collector `:8090`, cicd-sim `:8091`, fleet-exporter `:9249`, Alertmanager `:9093`, Mattermost `:8065`, chatops-bot `:8092`, tickets `:8094`, Grafana `:3000`.

Cloud runtime (`scripts/aws.sh`; design doc §4.5). These create, change or delete AWS resources, so run them only when asked:

- `make stack-aws` creates or updates the `current2026-demo` CloudFormation stack (`infra/cloudformation/demo.yaml`). `make secrets-aws` copies the ShadowTraffic license and any API keys in `.env` to SSM Parameter Store.
- `make deploy-aws` pushes the Flink, services and Mattermost images to ECR, uploads the Compose bundle to S3 and starts it on the instance over SSM Run Command. `make jobs-submit-aws` submits the jobs there.
- `scripts/aws.sh exec <command>` runs a shell command in `/opt/platform` on the instance. `make ps-aws` shows container status.
- `make tunnel` forwards the UIs to localhost: Flink, SQL Gateway, Prometheus, Mattermost, Grafana and the tickets UI, on their local ports. `make teardown-aws` deletes the stack, the bucket's contents and the secrets.

Settings that differ between runtimes, such as S3 endpoint and credentials, Flink memory and slots, and database passwords, come from environment variables with defaults in `compose.yml`; see `.env.example`. Platform-wide Flink behaviour, such as the scheduler, checkpointing, restart strategy and metrics, is in `flink/conf/config.yaml`. Flink logs go to stdout as JSON, formatted by `flink/conf/log-template.json`.

## Runtimes: local vs. cloud

The same Docker Compose stack runs in two places. Keep both working:

- **Local (development and fallback):** `compose.yml` on a MacBook Pro M3 with 18 GB. RustFS (S3-compatible) and an Iceberg REST catalog (backed by Postgres, `iceberg-rest/Dockerfile`) stand in for S3 and Glue. The simulated fleet is reduced and there's one TaskManager. Secrets come from a local, uncommitted `.env`.
- **Cloud (live demo):** `compose.yml` + `compose.aws.yml` on one EC2 Graviton instance (`m7g.2xlarge`, 32 GB) in the `evidentsystems` AWS account (`AWS_PROFILE=evidentsystems`). It uses real S3 and the AWS Glue Data Catalog, images from ECR, and secrets from SSM Parameter Store. Access is through SSM Session Manager port forwarding only; the instance has no inbound ports.

Rules for keeping them in step:

- Anything that differs between the runtimes (catalog implementation, S3 vs. RustFS endpoints, memory limits, fleet size) belongs in config or the `compose.aws.yml` override. Never branch on the runtime in code.
- Both runtimes are arm64. Build images for arm64 only.
- AWS infrastructure is plain CloudFormation YAML in `infra/cloudformation/`, not CDK. Don't create or modify AWS resources unless asked to. Check what already exists in the account first.

## Architecture decisions

These are deliberate, so don't reverse them casually:

- **Real Flink, plus simulated scale.** A handful of real Flink jobs run in Compose. ShadowTraffic feeds them input data on Kafka and also simulates telemetry for about 1,000 fictional jobs. Simulated telemetry must use the same schemas and metric names as the real Flink telemetry, so agents can't tell them apart.
- **Job logic is incidental.** Flink jobs (Java DataStream and Flink SQL) should stay minimal. Their purpose is to be operated on and to fail in controlled ways through fault flags or data-side faults.
- **Scripted and seeded.** Demo timelines must be repeatable. Avoid nondeterminism in scenario setup.
- **Each scenario builds on the last.** Scenario 1's triage signals start Scenarios 2 and 3. Don't build them as independent demos.
- **No agent harness.** Agents are Rust programs with explicit control flow. Jev (`typesafe-ai` crate) answers typed questions (`choice` / `noul` / `score`) about supplied state, and code maps those answers to actions. Jev never writes text: messages and ticket notes are code templates. Don't introduce Rig or a similar LLM tool-loop framework.
- **Swappable model endpoint.** Jev and OpenJev/SemIf share the `/v1/systemone` API, so keep the base URL configurable. Hosted Jev is the default in both runtimes.
- **Scenario 2 uses Claude Code over MCP.** It runs headless behind the Mattermost bot and talks to our Rust MCP server, built on `rmcp`. Claude can only propose mutating actions; a human approves them through Mattermost buttons.
- **ChatOps and ticketing run in Compose:** Mattermost, and a mock ticketing service shaped like the ServiceNow Table API. The ticket UI is generic and not styled to look like ServiceNow.

## Work tracking and pull requests

- Work is tracked in GitHub Issues on `bobby/current-2026-san-francisco`, grouped into the "Platform foundation" and "Agentic scenarios" milestones. Issues #1–#12 correspond to the milestones in `docs/design.md` §12.
- **Every pull request must fully auto-close at least one issue.** Put a closing keyword (`Closes #N`) in the PR description for each issue it completes. A PR that only partly finishes an issue isn't allowed. Split the issue first, so the PR can close a complete one.

## Tooling notes

- The `shadowtraffic` skill covers writing and debugging ShadowTraffic configs.
- The `typesafe-ai` skill covers designing Jev questions. Check the live docs at https://docs.typesafe.ai/llms.txt before writing integration code.
