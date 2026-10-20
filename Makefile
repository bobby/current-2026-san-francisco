.PHONY: up down clean ps logs smoke

# Build images and start the local platform, waiting until it's healthy.
up:
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
