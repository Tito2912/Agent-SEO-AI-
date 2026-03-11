FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY seo-agent-web/requirements.txt seo-agent-web/requirements.txt
RUN pip install --no-cache-dir -r seo-agent-web/requirements.txt

COPY seo-agent-web seo-agent-web
COPY skills skills
COPY seo-autopilot.yml seo-autopilot.yml

WORKDIR /app/seo-agent-web
CMD ["sh", "-c", "uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
