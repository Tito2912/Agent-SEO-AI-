#!/bin/sh
set -eu

MODE="${SEO_AGENT_SERVICE_MODE:-web}"

if [ "$MODE" = "web" ]; then
  echo "[ENTRYPOINT] mode=web"
  alembic -c alembic.ini upgrade head
  exec uvicorn backend.app:app --host 0.0.0.0 --port "${PORT:-8000}"
fi

if [ "$MODE" = "worker" ]; then
  echo "[ENTRYPOINT] mode=worker"
  exec python -m backend.worker_main
fi

echo "[ENTRYPOINT] unknown SEO_AGENT_SERVICE_MODE: $MODE" >&2
exit 2

