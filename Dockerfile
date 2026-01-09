FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app ./app

# Create non-root user for security
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8080

# Health check - use simple /health endpoint that doesn't load data
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Run with Gunicorn - OPTIMIZED FOR SPEED
# - 1 worker with preload = shared memory cache across all threads
# - 8 threads for concurrency
# - preload loads data ONCE at startup, shared by all threads
# - 300s timeout for long-running data queries
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "300", "--preload", "--access-logfile", "-", "--error-logfile", "-", "app.app:server"]
