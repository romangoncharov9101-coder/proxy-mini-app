#!/bin/sh
set -e

echo "[entrypoint] Запуск: $APP_ROLE"

case "$APP_ROLE" in
  api)
    echo "[entrypoint] Применяем миграции Alembic..."
    alembic upgrade head
    echo "[entrypoint] Миграции применены. Запуск FastAPI..."
    exec uvicorn backend.main:app \
      --host 0.0.0.0 \
      --port 8000 \
      --workers 2 \
      --log-level warning \
      --no-access-log
    ;;

  worker)
    echo "[entrypoint] Запуск Celery Worker..."
    exec celery -A backend.utils.celery_core.celery_app worker \
      --loglevel=info \
      --concurrency=4 \
      -n worker@%h
    ;;

  beat)
    echo "[entrypoint] Запуск Celery Beat..."
    exec celery -A backend.utils.celery_core.celery_app beat \
      --loglevel=info \
      --scheduler celery.beat:PersistentScheduler \
      --schedule /tmp/celerybeat-schedule
    ;;

  *)
    echo "[entrypoint] Неизвестная роль: $APP_ROLE"
    exit 1
    ;;
esac
