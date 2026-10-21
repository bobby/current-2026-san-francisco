.PHONY: up down clean ps logs smoke jobs jobs-submit jobs-cancel jobs-list jobs-test license rust-test telemetry-test cicd-test \
	stack-aws secrets-aws deploy-aws jobs-submit-aws ps-aws tunnel teardown-aws

MAVEN_IMAGE := maven:3.9.16-eclipse-temurin-17

# Build images and start the local platform, waiting until it's healthy.
up: license
	docker compose up -d --build --wait

# Stop the platform, keeping data volumes.
down:
	docker compose down

# Stop the platform and delete all data volumes.
clean:
	docker compose down -v

ps:
	docker compose ps -a

logs:
	docker compose logs -f

# End-to-end check of the running platform (issue #1).
smoke:
	./scripts/smoke-test.sh

# Build the Java jobs jar (jobs/java/target/platform-jobs.jar) in a Maven
# container, so no local JDK or Maven is needed.
jobs:
	docker run --rm -v "$(CURDIR)/jobs/java:/src" -v platform-maven-cache:/root/.m2 -w /src \
		$(MAVEN_IMAGE) mvn -q -B package

# Submit every job, healthy.
jobs-submit: jobs
	./scripts/jobs.sh submit-all

jobs-cancel:
	./scripts/jobs.sh cancel all

jobs-list:
	./scripts/jobs.sh list

# Every job runs and commits to Iceberg; every fault flag fails as designed
# (issue #2). Takes a few minutes.
jobs-test: jobs
	./scripts/jobs-test.sh

# Rust unit tests, plus the ops DB tests against the local stack's Postgres
# (each gets a throwaway database). Needs a Rust toolchain.
rust-test:
	DATABASE_URL=postgres://postgres:$${POSTGRES_PASSWORD:-postgres}@localhost:5432/postgres \
		cargo test --workspace -- --include-ignored

# Real Flink activity reaches the platform.* topics and the ops DB; alerts,
# logs and point-in-time queries work (issue #4).
telemetry-test: jobs
	./scripts/telemetry-test.sh

# Versioned deploys and rollbacks through cicd-sim, against real Flink
# (issue #6). Takes a few minutes.
cicd-test: jobs
	./scripts/cicd-test.sh

license:
	@test -f shadowtraffic/license.env || { \
		echo "shadowtraffic/license.env is missing: copy shadowtraffic/license.env.example and fill in your ShadowTraffic license."; \
		exit 1; }

# The cloud runtime (issue #3; docs/design.md §4.5), in the evidentsystems AWS
# account. See scripts/aws.sh for the other subcommands (exec, shell, stop,
# start).

# Create or update the CloudFormation stack (VPC, instance, bucket, Glue, ECR).
stack-aws:
	./scripts/aws.sh stack

# Copy the ShadowTraffic license and any API keys in .env to SSM Parameter Store.
secrets-aws: license
	./scripts/aws.sh secrets

# Push the Flink image to ECR, upload the Compose bundle and start it.
deploy-aws: jobs
	./scripts/aws.sh deploy

# Submit every job, healthy, on the instance.
jobs-submit-aws:
	./scripts/aws.sh jobs submit-all

ps-aws:
	./scripts/aws.sh exec docker compose ps -a

# Forward the Flink UI, SQL Gateway and Prometheus to localhost. Stop the
# local stack first, since it uses the same ports.
tunnel:
	./scripts/aws.sh tunnel

# Delete the stack, the bucket's contents, the images and the secrets.
teardown-aws:
	./scripts/aws.sh teardown
