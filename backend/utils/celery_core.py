from celery import Celery
from celery.schedules import crontab
from backend.utils.config import settings

celery_app = Celery(
    'proxy_tasks',
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        'backend.tasks.sync_tasks',
    ],
)

celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        'socket_connect_timeout': 30,
        'socket_keepalive': True,
    },
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    result_expires=3600,
    worker_enable_remote_control=False,
    worker_send_task_events=False
)

celery_app.conf.beat_schedule = {
    'daily_sync_all_data': {
        'task': 'backend.tasks.sync_tasks.sync_regions_task',
        'schedule': crontab(hour=0, minute=0),
    },
    'sync_balances_every_15min': {
        'task': 'backend.tasks.sync_tasks.sync_balances_task',
        'schedule': crontab(minute='*/15'),
    },
}
