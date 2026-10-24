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

- The Java jobs are one jar with `--job=<name>`, fault flags (`--fault=npe-on-null-field|state-leak`), `--on-deser-error=fail|dlq` and `--state-backend=hashmap|rocksdb` (F4's temporary relief).
- They reach Kafka and Iceberg **only through the Table API**, using the connectors on the cluster classpath (`flink/Dockerfile`). `flink-sql-connector-kafka` relocates Kafka's client classes, so don't add Kafka or Iceberg dependencies to the jar.
- SQL jobs are `jobs/sql/<name>.sql`, submitted through the SQL Gateway.
- Every job uses the `lake` Iceberg catalog, defined per runtime in `flink/catalogs/<runtime>/lake.yaml` and loaded through Flink's file catalog store. Each job sets its table to keep only its last 10 metadata files; a new table needs the same (`Pipeline.retainMetadata`, or `ALTER TABLE … SET` in SQL), or every checkpoint's commit leaves a metadata file behind.
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

The director and fault injection (design doc §4.4, §10.2):

- `make reset` (`scripts/director.sh reset`) returns the platform to its starting state; `make run SCENARIO=1|2|3|all` runs the timeline in `director/timeline.yaml`; `make status` shows progress and the incidents so far. On AWS, `scripts/aws.sh director <command>`.
- `reset` is destructive: it cancels every job, empties the ops history and the tickets, deletes Prometheus's Flink series and archives the Mattermost channels. It also restarts the triage and operator agents, which forget their open incidents.
- A run leaves its faults behind: F2's poison records stay ahead of `trade-events-ingest`'s committed offsets, so a redeploy fails again. `make reset` before `make jobs-test` or other tests.
- The timeline's offsets and seed are the demo. Change them deliberately, and rerun `make director-test`, which checks that two runs with the same seed produce the same incidents (about an hour for `all`).
- Fault injectors are Compose services in the `faults` profile (`st-fault-f2`, `-f3`, `-f5`), each running `shadowtraffic/faults/<fault>.json`. `scripts/director.sh inject <fault>` starts one by hand.
- F3 depends on `market-data-to-iceberg`'s capacity: a 5 ms lookup per record (`QuoteReferenceLookup`, a SQL function from the jobs jar) with operator chaining off. Changing either changes whether the spike backs it up.

Human interfaces (design doc §9):

- Mattermost is built for arm64 from the official release tarball (`mattermost/Dockerfile`); Mattermost publishes amd64 images only. Configuration is `MM_*` variables in `compose.yml`.
- The **chatops-bot** is the only Mattermost client. On every start it sets up, idempotently, the `oncall` admin (people log in as it), the `platform` team, the three channels and five bot accounts (`alertmanager`, `tickets`, `triage-bot`, `operator-bot`, `ops-assistant`). Agents post through its API with the typed cards in `platform_clients::chatops`; all formatting lives in `services/chatops-bot/src/cards.rs`. Don't post to Mattermost directly.
- Button clicks reach the bot through Mattermost (`MM_SERVICESETTINGS_ALLOWEDUNTRUSTEDINTERNALCONNECTIONS`), and each decision goes to `platform.actions`. Whoever proposed an action carries it out once approved: the operator agent for its escalation cards, the bot itself for Claude's proposals.
- The **tickets** mock follows the ServiceNow Table API (`platform_clients::tickets`). It notifies the bot after every change, so ticket cards stay current. Its schema is in `services/tickets/migrations/`.
- Grafana's dashboard is provisioned from `observability/grafana/dashboards/platform-overview.json`; edit the file, not the UI. It reads the ops DB as the read-only `grafana` role (`postgres/databases.sh`).
- `make chatops-test` (`scripts/chatops-test.sh`) covers the workspace, the alert firehose, the Table API, ticket cards, a button click through Mattermost, ticket lookups and Grafana's datasources. Run it after changing any of these.

The triage agent (Scenario 1; design doc §6.1, §8.1):

- `agents/triage` consumes `platform.alerts`, groups each job's alerts, gathers evidence from the ops DB and Prometheus, and asks Jev five questions in one request. Code policy turns the answers into `suppress`, `attach` or `raise`; every verdict goes to `platform.signals` with its state, questions and answers, and raised ones get a card in `#platform-signals`.
- Code computes features and Jev judges: summaries, digests and the one-line card summary are computed in `evidence.rs` and `summary.rs`. Jev never writes text. Leave a deployment's job arguments out of the evidence, because they carry the fault flags.
- Tuning is in `agents/triage/triage.yaml`: the platform context every judgment sees (`context`, as `triage_policy` in the state), windows, and the policy's thresholds. The known-noise notes (`known-noise.yaml`) are generated by `shadowtraffic/fleet/generate.py`; don't edit them by hand.
- Tune offline with `triage replay --from <ts> --to <ts> [--job <name>] [--evidence]` (`cargo run -p triage -- replay …`, with `JEV_API_KEY` exported from `.env`). It re-judges a past window's alerts from the ops DB and Prometheus and changes nothing. `--out <file>` writes every signal, with its answers, as JSON Lines, to compare variants against the same run.
- It needs `JEV_API_KEY` in `.env`. Without it the service stays up but doesn't triage. `JEV_BASE_URL` switches to an OpenJev/SemIf server.
- `make triage-test` (`scripts/triage-test.sh`, about 35 minutes) resets, runs the whole timeline and checks that F1–F4 and the fleet's genuine problems are raised, that the noise is suppressed or attached, and that every raised signal has a card. Rerun it after changing the questions, the evidence, the policy or the timeline.

The ops assistant (Scenario 2; design doc §6.2, §8.2):

- A human post mentioning `@ops-assistant` in `#platform-signals` or `#platform-incidents` runs headless Claude Code inside the chatops-bot container (`services/chatops-bot/src/assistant.rs`), one session per thread. Tool calls stream into the thread as status lines; the result is the answer. The system prompt is `services/chatops-bot/src/assistant-prompt.md`.
- Claude has no built-in tools, only the platform's MCP server (`services/mcp-server`, `rmcp` over stdio), which Claude Code starts per run with the thread's context in its environment. Every tool reads (the ops DB as the read-only `assistant` role) except `propose_remediation`, which only asks the bot to post an approval card (`POST /api/proposals`, `services/chatops-bot/src/remediation.rs`). Don't add mutating MCP tools: changes go through a person's Approve, which the bot carries out through cicd-sim or Flink and audits in `platform.actions`.
- Leave fault flags (`--fault=…`) out of what the tools return, as triage does; they'd give the answer away.
- Claude Code is pinned in `services/Dockerfile` (`CLAUDE_CODE_VERSION`) and installed with Node in the services image. It needs `ANTHROPIC_API_KEY` in `.env`; `CLAUDE_MODEL` picks the model. Without the key, `@ops-assistant` replies that it isn't set up.
- `make assistant-test` (`scripts/assistant-test.sh`, about 25 minutes) runs Scenario 2, asks in the F2 signal's thread, approves the proposal through Mattermost and checks the job recovers. Rerun it after changing the prompt, the tools or the bridge.

The operator agent (Scenario 3; design doc §8.3):

- `agents/operator` consumes `platform.signals` and opens an incident for each raised signal: a card in `#platform-incidents` and a ticket. Each incident runs an explicit state machine in `incident.rs`: Gather (`facts.rs`), Decide (Jev's `remediation` choice, `questions.rs`), Guardrails (`guardrails.rs`), Act (cicd-sim, or the Flink REST API for a rescale), Verify (Jev's `recovered` noul over before/after health), then Close or Escalate.
- Jev only picks; code decides. The guardrail table (allowed failure classes and tiers per action, deploy age, how soon the failures followed the deploy, the previous version's healthy time, parallelism limits, confidence thresholds, a per-job budget) is in `agents/operator/operator.yaml`, with unit tests for F1–F4 in `guardrails.rs`. Anything a guardrail stops is escalated.
- It only acts on jobs in cicd-sim's catalogue; the simulated fleet's problems are escalated. Every step is a ticket work note (the chatops-bot echoes it into the incident's thread) and an entry in `platform.actions`.
- Escalation cards have buttons; the agent consumes the decisions from `platform.actions` and carries them out (F4's temporary restart with `--state-backend=rocksdb`).
- `operator decide --from <ts> --to <ts> [--job <name>] [--evidence]` (`cargo run -p operator -- decide …`, with `JEV_API_KEY` exported from `.env`) re-decides a past window's raised signals and prints Jev's pick and the guardrails' verdict, without acting.
- `make operator-test` (`scripts/operator-test.sh`, about 40 minutes) resets, runs the whole timeline and checks that F1 is rolled back and its ticket resolved, F3 is rescaled and its lag drains, and F4 is escalated, then clicks F4's temporary restart through Mattermost. Rerun it after changing the guardrails, the questions or the timeline.

Demo day (issue #12; design doc §10.3; the procedure is `docs/runbook.md`):

- `make preflight` (`scripts/preflight.sh`) is a go/no-go that changes nothing; `make preflight-aws` runs it on the instance, then checks the tunnel. Keep it fast and side-effect free: it must not spend model tokens.
- `make rehearsal` (`scripts/rehearsal.sh`, about 45 minutes) resets, runs the whole timeline with Scenario 2's engineer played by `assistant-test.sh --in-run`, runs the triage and operator checks on that run, reports timings and peak memory to `.director/rehearsals/<start>/`, and records the run. `make rehearsal-aws` starts it detached on the instance. Rerun it after changing the timeline, the agents or the stack.
- `director record` (`make record`) saves the latest run to `recordings/<start>-<fleet>-jobs.json.gz`: the chatops-bot's transcript (`GET /admin/transcript`), the `alerts`, `signals`, `deployments`, `actions`, `incident` and `journal` rows, and the starring jobs' Grafana series. A good EC2 rehearsal's recording is committed, and the AWS bundle includes `recordings/`.
- `director replay start <file> [--from 12m] [--speed 2]` (`make replay`, `make replay-aws`) is the fallback: it quiets the platform (destructively, like `reset`) and starts the `replay` Compose service (profile `replay`, `director play`), which posts the transcript through the chatops-bot, inserts the rows and serves the metrics on `:9249` for Prometheus's `replay` job. `replay pause|resume|seek|status|stop` steer it through `:8096`; `make reset` goes live again. It needs no network or model keys.
- The bot ignores posts marked `replayed`, and a replayed card's buttons do nothing. If you add a card, a table Grafana reads or a panel, make sure a recording still captures it (`director/src/recording.rs`).

Local endpoints (all bound to 127.0.0.1): Flink UI/REST `:8081`, SQL Gateway `:8083`, Prometheus `:9090`, Kafka `:9094` (inside Compose, `kafka:9092`), Iceberg REST catalog `:8181`, RustFS S3 API `:9000` and console `:9001`, Postgres `:5432`, collector `:8090`, cicd-sim `:8091`, fleet-exporter `:9249`, Alertmanager `:9093`, Mattermost `:8065`, chatops-bot `:8092`, triage `:8093` (`/stats`), tickets `:8094`, operator `:8095` (`/stats`, `/incidents`), Grafana `:3000`, the replay's control API `:8096` (only during a replay).

Cloud runtime (`scripts/aws.sh`; design doc §4.5). These create, change or delete AWS resources, so run them only when asked:

- `make stack-aws` creates or updates the `current2026-demo` CloudFormation stack (`infra/cloudformation/demo.yaml`). `make secrets-aws` copies the ShadowTraffic license and any API keys in `.env` to SSM Parameter Store.
- `make deploy-aws` pushes the Flink, services and Mattermost images to ECR, uploads the Compose bundle to S3 and starts it on the instance over SSM Run Command. `make jobs-submit-aws` submits the jobs there.
- `scripts/aws.sh exec <command>` runs a shell command in `/opt/platform` on the instance; `detach <log> <command>` starts one in the background; `fetch <path>` copies a file back through the bucket. `make ps-aws` shows container status. `make reset-aws`, `run-aws`, `status-aws`, `rehearsal-aws`, `record-aws` and `replay-aws` are the demo-day wrappers.
- `make tunnel` forwards the UIs to localhost, reconnecting any port whose session drops: Flink, SQL Gateway, Prometheus, Mattermost, Grafana and the tickets UI, on their local ports. `make teardown-aws` deletes the stack, the bucket's contents and the secrets.

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
