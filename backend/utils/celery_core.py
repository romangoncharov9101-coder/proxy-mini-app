import logging
import logging.handlers
import os
import sys
from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready
from backend.utils.config import settings

LOG_DIR = os.getenv('LOG_DIR', '/app/logs')
LOG_FILE = os.path.join(LOG_DIR, 'celery.log')

def _setup_celery_logging():
    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=3, encoding='utf-8'
        ),
    ]
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in handlers:
        h.setFormatter(formatter)
        root.addHandler(h)

_setup_celery_logging()
logger = logging.getLogger('celery.core')
logger.info(f'[CELERY] Подключение к Redis: {settings.REDIS_URL}')

celery_app = Celery(
    'proxy_tasks',
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        'backend.tasks.sync_tasks',
        'backend.tasks.notifications_tasks',
    ],
)

celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10,
    broker_transport_options={
        'socket_connect_timeout': 30,
        'socket_keepalive': True,
        'socket_keepalive_options': {},
        'visibility_timeout': 3600,
    },
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    result_expires=3600,
    worker_enable_remote_control=False,
    worker_send_task_events=False,
    task_soft_time_limit=540,
    task_time_limit=600,
)

celery_app.conf.beat_schedule = {
    'daily_sync_regions': {
        'task': 'backend.tasks.sync_tasks.sync_regions_task',
        'schedule': crontab(hour=0, minute=0),
    },
    'sync_balances_every_15min': {
        'task': 'backend.tasks.sync_tasks.sync_balances_task',
        'schedule': crontab(minute='*/15'),
    },
    'daily_notify_expiring_proxies': {
        'task': 'backend.tasks.notifications_tasks.notify_expiring_proxies_task',
        'schedule': crontab(hour=9, minute=0),
    },
    'daily_auto_renew_proxies': {
        'task': 'backend.tasks.notifications_tasks.auto_renew_proxies_task',
        'schedule': crontab(hour=3, minute=0),
    },
}

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info('[CELERY] Воркер готов — запускаем первичную синхронизацию регионов')
    celery_app.send_task('backend.tasks.sync_tasks.sync_regions_task')