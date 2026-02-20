# Minimal Python image for running MeshBot
# by Till Vennefrohne - https://github.com/mokny/meshbot
FROM python:3.11-slim

# - PYTHONDONTWRITEBYTECODE: avoid __pycache__ files
# - PYTHONUNBUFFERED: log output immediately (useful in Docker logs)
# - TZ: make sure timezone-aware code defaults to Europe/Berlin
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Berlin

WORKDIR /app

# Install certificates (HTTPS), tzdata (timezone info)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY app /app/app

# Start the bot
CMD ["python", "-m", "app.main"]
