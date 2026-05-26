# ==============================================================================
# govScape Pipeline Automation Shortcut Hub
# ==============================================================================

.PHONY: build run test clean

# 1. Build the production multi-stage image
build:
	docker build -t govscape-pipeline:prod .

# 2. Run the production pipeline with local data volume and env variables
run:
	docker run --rm --env-file .env -v "$$(pwd)/data:/app/data" govscape-pipeline:prod

# 3. Run the unit tests locally by mounting the tests folder on the fly
test:
	docker run --rm --env-file .env -v "$$(pwd)/tests:/app/tests" govscape-pipeline:prod pytest tests/test_transform_to_silver.py

# 4. Clean up dangling docker images to save disk space
clean:
	docker image prune -f