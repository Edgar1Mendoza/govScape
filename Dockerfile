# =======================================================
# STAGE 1: The Build Environment (Builder)
# =======================================================
# Pull the clean base image to act as our compiling and testing kitchen
FROM python:3.12-slim AS builder

# Set the working directory for the build environment
WORKDIR /app

# Install project dependencies into the builder stage
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project context into the builder stage
COPY . .

# Quality Gate: Execute test suite using a dummy environment variable to prevent build failure
RUN CONGRESS_API_KEY=dummy_key_for_testing pytest tests/test_transform_to_silver.py

# =======================================================
# STAGE 2: The Production Runtime (Runner)
# =======================================================
# Pull a fresh, isolated slim image to serve as the lean production runtime
FROM python:3.12-slim AS runner

# Set the execution directory for the production container
WORKDIR /app

# Copy compiled third-party Python packages directly from the builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# MASTER TRICK: Copy binary executables (such as pytest, black, or custom CLIs) from builder
COPY --from=builder /usr/local/bin /usr/local/bin

# MASTER TRICK: Copy the Python project configuration metadata file so paths remain mapped
COPY pyproject.toml .

# Copy production-ready source code only (strictly excluding test overhead and raw data)
COPY ./src ./src

# Set the primary runtime entry point for the data pipeline
CMD ["python", "src/main.py"]