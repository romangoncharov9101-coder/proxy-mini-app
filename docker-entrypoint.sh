#!/bin/sh
set -e

echo "[entrypoint] Запуск: $APP_ROLE"

# Создаём папку для логов здесь, а не в Dockerfile.
# Docker монтирует volume ДО запуска entrypoint, поэтому mkdir в Dockerfile
# бесполезен — volume перекрывает созданную папку пустым хранилищем.
LOG_DIR="${LOG_DIR:-/app/logs}"
mkdir -p "$LOG_DIR"
echo "[entrypoint] Папка логов: $LOG_DIR"

# Вспомогательная функция: ждём доступности Redis перед стартом воркера/beat.
# Даже при наличии healthcheck в compose — между "healthy" и реальной
# готовностью принимать соединения может быть небольшая задержка.
wait_for_redis() {
    REDIS_HOST=$(echo "$REDIS_URL" | sed 's|redis://||' | cut -d: -f1)
    REDIS_PORT=$(echo "$REDIS_URL" | sed 's|redis://||' | cut -d: -f2 | cut -d/ -f1)
    REDIS_PORT=${REDIS_PORT:-6379}

    echo "[entrypoint] Ожидаем Redis ${REDIS_HOST}:${REDIS_PORT}..."
    for i in $(seq 1 30); do
        if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping 2>/dev/null | grep -q PONG; then
            echo "[entrypoint] Redis готов (попытка $i)"
            return 0
        fi
        echo "[entrypoint] Redis недоступен, ждём 2s... (попытка $i/30)"
        sleep 2
    done
    echo "[entrypoint] Redis не ответил за 60 секунд — аварийный выход"
    exit 1
}

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
    wait_for_redis
    echo "[entrypoint] Запуск Celery Worker..."
    exec celery -A backend.utils.celery_core.celery_app worker \
      --loglevel=info \
      --concurrency=4 \
      -n worker@%h
    ;;

  beat)
    wait_for_redis
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