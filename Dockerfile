# =======================================================
# STAGE 1: The Build Environment (Builder)
# =======================================================
FROM python:3.12-slim AS builder

WORKDIR /app

# Install project dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project context into the builder stage
COPY . .

# Quality Gate: Run tests with a temporary dummy key strictly for this command
RUN CONGRESS_API_KEY=dummy_key_for_testing pytest tests/test_transform_to_silver.py

# =======================================================
# STAGE 2: The Production Runtime (Runner)
# =======================================================
FROM python:3.12-slim AS runner

WORKDIR /app

# Copy installed Python packages from the builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Copy only the production source code (excludes test files and overhead)
COPY ./src ./src

# Set the entry point for the production data pipeline
CMD ["python", "src/main.py"]