FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app/backend
COPY frontend /app/frontend
COPY scripts /app/scripts
COPY cloudv2-config.json /app/cloudv2-config.json

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/logs_mqtt /data /app/certs \
    && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 8008

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 CMD curl -fsS http://127.0.0.1:8008/login || exit 1

CMD ["python", "backend/run_monitor.py"]
