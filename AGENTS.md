# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project status

This repo holds demo code for the Current 2026 San Francisco talk "From Alerts to Autonomy: Advancing the Maturity Curve with Agentic Managed Streaming Platforms". README.md has the abstract and an architecture summary. The full design, including decisions, failure-mode catalogue, scenario flows and milestones, is in `docs/design.md`; read it before making changes. No build system, source code or tests exist yet. Once they're added, put the real build, lint and test commands here. Don't make them up before then.

## Runtimes: local vs. cloud

The same Docker Compose stack runs in two places. Keep both working:

- **Local (development and fallback):** `compose.yml` on a MacBook Pro M3 with 18 GB. MinIO and an Iceberg REST catalog stand in for S3 and Glue. The simulated fleet is reduced and there's one TaskManager. Secrets come from a local, uncommitted `.env`.
- **Cloud (live demo):** `compose.yml` + `compose.aws.yml` on one EC2 Graviton instance (`m7g.2xlarge`, 32 GB) in the `evidentsystems` AWS account (`AWS_PROFILE=evidentsystems`). It uses real S3 and the AWS Glue Data Catalog, images from ECR, and secrets from SSM Parameter Store. Access is through SSM Session Manager port forwarding only; the instance has no inbound ports.

Rules for keeping them in step:

- Anything that differs between the runtimes (catalog implementation, S3 vs. MinIO endpoints, memory limits, fleet size) belongs in config or the `compose.aws.yml` override. Never branch on the runtime in code.
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

## Tooling notes

- The `shadowtraffic` skill covers writing and debugging ShadowTraffic configs.
- The `typesafe-ai` skill covers designing Jev questions. Check the live docs at https://docs.typesafe.ai/llms.txt before writing integration code.
