FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY seo-agent-web/requirements.txt seo-agent-web/requirements.txt
RUN pip install --no-cache-dir -r seo-agent-web/requirements.txt

COPY seo-agent-web seo-agent-web
COPY skills skills
COPY seo-autopilot.yml seo-autopilot.yml

WORKDIR /app/seo-agent-web
CMD ["sh", "-c", "sh ./entrypoint.sh"]
