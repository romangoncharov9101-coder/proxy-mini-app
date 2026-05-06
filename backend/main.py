import logging
import logging.handlers
import sys
import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from backend.routes import user, pages, admin
from backend.utils.config import settings

LOG_DIR = os.getenv('LOG_DIR', '/app/logs')
LOG_FILE = os.path.join(LOG_DIR, 'app.log')

def setup_logging():
    log_level = logging.DEBUG if settings.ENVIRONMENT != 'production' else logging.INFO

    formatter = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=20 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )

    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(handler)
    root_logger.addHandler(file_handler)

    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.engine').setLevel(
        logging.INFO if settings.ENVIRONMENT != 'production' else logging.WARNING
    )

setup_logging()
logger = logging.getLogger('app')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

@app.middleware('http')
async def log_request(request: Request, call_next):
    import time
    t_start = time.monotonic()
    response = await call_next(request)
    duration_ms = int((time.monotonic() - t_start) * 1000)

    logger.info(
        "[HTTP] %s %s — %d (%dms) from %s",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
        request.client.host if request.client else "unknown",
    )
    return response

app.include_router(pages.router)
app.include_router(user.router, prefix='/api/v1')
app.include_router(admin.router, prefix='/api/v1')

logger.info(f'PRX_CORE запущен, окружение: {settings.ENVIRONMENT}')