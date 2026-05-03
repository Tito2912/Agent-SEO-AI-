FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget gnupg \
    && install -d /usr/share/postgresql-common/pgdg \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg \
    && . /etc/os-release \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

COPY seo-agent-web/requirements.txt seo-agent-web/requirements.txt
RUN pip install --no-cache-dir -r seo-agent-web/requirements.txt

COPY seo-agent-web seo-agent-web
COPY skills skills
COPY seo-autopilot.yml seo-autopilot.yml

WORKDIR /app/seo-agent-web
CMD ["sh", "-c", "sh ./entrypoint.sh"]
