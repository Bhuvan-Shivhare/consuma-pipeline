# Single image shared by every service (gateway, worker, relay, reaper).
# docker-compose runs the same image with different commands.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

# Default command is the gateway; overridden per-service in docker-compose.yml.
CMD ["uvicorn", "app.gateway:app", "--host", "0.0.0.0", "--port", "8000"]
