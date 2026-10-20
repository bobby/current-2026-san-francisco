.PHONY: up down clean ps logs smoke jobs jobs-submit jobs-cancel jobs-list jobs-test license

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

license:
	@test -f shadowtraffic/license.env || { \
		echo "shadowtraffic/license.env is missing: copy shadowtraffic/license.env.example and fill in your ShadowTraffic license."; \
		exit 1; }
