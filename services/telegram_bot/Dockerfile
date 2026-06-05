# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Keeps Python from generating .pyc files and enables stdout/stderr logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better layer caching — only re-runs on requirements changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY bot/ bot/

# Cloud Run injects PORT; gunicorn binds to it
ENV PORT=8080

# Use gunicorn with a single worker — Cloud Run scales by spawning new instances,
# not threads, so 1 worker + 4 threads is the right fit here.
CMD exec gunicorn \
    --bind "0.0.0.0:$PORT" \
    --workers 1 \
    --threads 4 \
    --timeout 120 \
    main:app
