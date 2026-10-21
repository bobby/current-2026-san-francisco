# From Alerts to Autonomy: Advancing the Maturity Curve with Agentic Managed Streaming Platforms

This repository contains the demo code for the talk whose abstract is below.

3 demo scenarios:

1. Amplify signal/suppress noise to detect true errors (and ignore false positives)
2. Human-initiated, agent-assisted diagnosis and remediation
3. Agent-initiated diagnosis and remediation, keeping the humans in-the-loop but only requiring human intervention where absolutely necessary

## Architecture

The full design is in [docs/design.md](docs/design.md). Each scenario builds on the previous one: the signals Scenario 1 produces are what Scenario 2's engineer investigates and what Scenario 3's agent acts on.

### Runtimes: Local and Cloud

The whole stack is defined once in Docker Compose and runs in two places:

| | Local (development and fallback) | Cloud (live demo) |
|---|---|---|
| Host | MacBook Pro M3, 18 GB | One EC2 Graviton instance (`m7g.2xlarge`, 32 GB) in the `evidentsystems` AWS account |
| Compose files | `compose.yml` | `compose.yml` + `compose.aws.yml` |
| Iceberg warehouse, checkpoints, savepoints | RustFS (S3-compatible) + Iceberg REST catalog | Real S3 + AWS Glue Data Catalog |
| Images | Built locally (arm64) | The same arm64 images, pushed to ECR |
| Secrets (Jev and Anthropic API keys, ShadowTraffic license) | Local `.env` (not committed) | SSM Parameter Store |
| Access | `localhost` | SSM Session Manager port forwarding (no inbound ports) |
| Scale | Reduced fleet (about 200 simulated jobs), one TaskManager | Full fleet (about 1,000 simulated jobs), two TaskManagers |

AWS infrastructure is defined in CloudFormation (`infra/cloudformation/`). Both runtimes are arm64, so the images built on the laptop run unchanged on EC2.

#### Running Locally

You need Docker Desktop (or another Docker engine with Compose v2), `make`, `curl` and `jq`. You also need a [ShadowTraffic license](https://shadowtraffic.io/pricing.html): copy `shadowtraffic/license.env.example` to `shadowtraffic/license.env` and fill it in.

```sh
make up           # build and start the platform; waits until it's healthy (~20 s warm)
make smoke        # end-to-end check: Flink job → S3 checkpoint → Prometheus, Iceberg, Kafka, Postgres
make jobs-submit  # build the Java jobs jar and submit all six jobs
make jobs-test    # every job commits to Iceberg; every fault fails as designed (a few minutes)
make telemetry-test  # job status, checkpoints, logs and alerts reach Kafka and the ops DB
make fleet-test   # the simulated fleet looks like real jobs; the naive alerts flow (~15 min)
make rust-test    # Rust unit tests and ops DB tests (needs a Rust toolchain)
make down         # stop (keep data); `make clean` also deletes volumes
```

Versioned deploys and rollbacks go through cicd-sim, the platform's deployment pipeline. The versions are listed in `jobs/versions.yaml`:

```sh
scripts/cicd.sh catalogue                           # every job's versions
scripts/cicd.sh baseline                            # put every job on its baseline version
scripts/cicd.sh deploy payments-enrichment 1.42.0   # the F1 bad deploy
scripts/cicd.sh rollback payments-enrichment        # back to 1.41.0
scripts/cicd.sh deploy trade-events-ingest 2.18.0 --on-deser-error=dlq
make cicd-test                                      # deploys and rollbacks against real Flink
```

To submit a job by hand, outside the pipeline (no deployment events), use `scripts/jobs.sh`:

```sh
scripts/jobs.sh submit payments-enrichment --fault=npe-on-null-field
scripts/jobs.sh submit trade-events-ingest --on-deser-error=dlq
scripts/jobs.sh list
scripts/jobs.sh cancel positions-aggregator
```

Then open the Flink UI at http://localhost:8081, Prometheus at http://localhost:9090, Alertmanager at http://localhost:9093, and the RustFS console at http://localhost:9001. Copy `.env.example` to `.env` to override the defaults.

#### Running on AWS

You also need the AWS CLI with the `evidentsystems` profile and the [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html) (`brew install --cask session-manager-plugin`), which `make tunnel` uses.

```sh
make stack-aws        # create or update the CloudFormation stack (~5 min the first time)
make secrets-aws      # copy the ShadowTraffic license (and any API keys in .env) to SSM Parameter Store
make deploy-aws       # push the Flink image to ECR, upload the Compose bundle and start the stack
make jobs-submit-aws  # submit every job on the instance
make tunnel           # forward the Flink UI (:8081), SQL Gateway (:8083) and Prometheus (:9090)
make teardown-aws     # delete everything: stack, bucket contents, images, secrets
```

`scripts/aws.sh` has more subcommands: `exec <command>` and `jobs <args>` run on the instance over SSM Run Command, `shell` opens a session, and `stop`/`start` park the instance between rehearsals. Stop the local stack before `make tunnel`, because both use the same ports.

### A Real Flink Platform

We run a real (small) managed-Flink platform: Kafka, a Flink session cluster, and Iceberg tables (on RustFS locally, on S3 with Glue in the cloud). A handful of real Flink jobs (a mix of Java DataStream and Flink SQL) read from Kafka and write to Iceberg. The jobs' business logic is deliberately incidental, since the talk is about operating many user-provided, opaque jobs, not about the jobs themselves.

Real telemetry comes from Flink itself: metrics via the Prometheus reporter, logs, and checkpoint stats via the Flink REST API. Remediations (restart, rollback, rescale, stop-with-savepoint) call the real Flink REST API, so the audience can watch the telemetry recover.

A small **CI/CD simulator** deploys job versions to Flink on a scripted timeline and emits deployment events, because a recently deployed job is much more suspicious than one that has run error-free for a long time. Faults are injected by deploying job versions with fault flags enabled, or from the data side via poison records, schema drift and traffic spikes.

### Data Generation by ShadowTraffic

[ShadowTraffic](https://docs.shadowtraffic.io/) plays two roles:

- **Foreground:** it generates the input data for the real Flink jobs on Kafka topics, including injected data-side faults.
- **Background:** it simulates telemetry from a large fleet (hundreds to thousands) of fictional jobs, using the same schemas and metric names as the real ones. These cover logs/traces, checkpoints, metrics and CI/CD deployments, and supply the scale and noise that Scenario 1 needs.

Real and simulated telemetry land in the same stores, so the agents can't tell them apart. Demo scenarios are scripted and seeded so they are repeatable live.

### Jev (or OpenJev/SemIf) as the Decision Model

For Scenarios 1 and 3, we build lightweight agents that use [Jev](https://typesafe.ai/) to map unstructured telemetry onto a finite set of typed outcomes: classifications, yes/no judgments with probabilities, and choices among remediation actions such as "File a ServiceNow ticket". [OpenJev/SemIf](https://huggingface.co/openjev/openjev) (an open-weights rebrand of OpenJev, not endorsed by TypeSafe AI) uses the same `/v1/systemone` API, so it can be swapped in by changing the base URL. We default to hosted Jev in both runtimes.

Scenario 2 is conversational: [Claude Code](https://claude.com/claude-code) reaches the Scenario 1 signals, tickets, Flink state and deployment history through an MCP server we build.

### Agents Written in Rust, Without a Harness

We implement the agents in Rust **without an agent harness**. Jev isn't a chat model that chooses tools in a loop: it answers typed questions about supplied state, and code owns the workflow. Each agent is therefore an explicit state machine that:

- calls Jev via the [`typesafe-ai`](https://crates.io/crates/typesafe-ai) crate;
- maps Jev's answers onto a Rust `Action` enum;
- calls the Flink REST API, Prometheus, the ticketing service and ChatOps with plain `reqwest` clients.

The MCP server for Scenario 2 uses the official Rust MCP SDK ([`rmcp`](https://crates.io/crates/rmcp)).

### ChatOps and Ticketing

- **[Mattermost](https://mattermost.com/)** runs in Compose as the ChatOps UI. Agents post alerts, incident threads and status updates there. All three scenarios happen in Mattermost, including the Scenario 2 conversation with Claude Code.
- **A ServiceNow-shaped mock ticketing service** runs in Compose. Agents open tickets and add notes as they diagnose and remediate. Ticket links render as rich previews in Mattermost.

## Talk Abstract

Double the number of jobs on a multi-tenant stream processing platform, and you'll more than double the alerts, false positives, and real incidents your team must triage. Signal and noise both grow super-linearly with scale, consuming an ever-larger scale of the platform team’s bandwidth. Applying AI at the right points can bring that support growth curve back toward linear.

At JPMorgan Chase, a small platform team supports thousands of user-submitted Flink jobs processing more than 100 billion events per month. In this talk, we share the three-stage agentic support maturity curve that made this possible, and how you can apply it to your own platform:

Amplify the signals that matter. Learn AI/ML techniques can enrich platform telemetry and filter out alert noise so real problems stand out.
Human-initiated, AI-assisted diagnosis and remediation. See how correlating logs, metrics, and historical metadata with point-in-time state provides the context an agent needs to help engineers find root causes and fixes faster.
Agent-initiated diagnosis and remediation. Discover how agents can resolve routine issues on their own, keep the right people informed, and bring in humans only when necessary.
You'll leave this session with a practical blueprint for making support scale with your platform usage instead of outrunning it.

## Speakers

- Bobby Calderwood -- Senior Director of Architecture, Data Technology
- Andrew Lee – Software Engineer, Data Technology

### Bobby’s Bio

Bobby works as a Senior Director of Architecture at JPMorgan Chase where he helps design streaming data platforms.

Bobby is also the founder of Evident Systems, which helps companies solve complex problems using simple event-driven systems. As a Confluent Partner, Bobby developed the course [Practical Event Modeling](https://developer.confluent.io/courses/event-modeling/intro/) as well as consulting with Confluent customers. Bobby has been a Confluent Community Catalyst since the program’s inception.

Before JPMorgan Chase and Evident Systems, Bobby was a Distinguished Engineer on the Technology Fellows team at Capital One, and before that worked on the original Product Team at Cognitect that built Clojure, ClojureScript, and Datomic.

Bobby enjoys writing software in Rust and Clojure.

### Andrew’s Bio

Andrew Lee is a software engineer at JP Morgan Chase leading the managed data streaming platform, focusing on real time data ingestion and processing into data lake architectures. Before joining JP Morgan, Andrew had specialized in data streaming for tax and regulatory use cases at Expedia.
