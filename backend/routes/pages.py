import logging
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.database import schemas
from backend.utils.security import get_current_user
from backend.database.database import get_db
from backend.database.models import User, Proxy, UserRole

templates = Jinja2Templates(directory='frontend')
logger = logging.getLogger("routes.pages")
router = APIRouter()

@router.get('/', response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse(request=request, name='index.html', context={'request': request})

@router.get('/403', response_class=HTMLResponse)
async def forbidden_page(request: Request):
    return templates.TemplateResponse(request=request, name='403.html', context={'request': request}, status_code=403)

@router.get('/404', response_class=HTMLResponse)
async def forbidden_page(request: Request):
    return templates.TemplateResponse(request=request, name='404.html', context={'request': request}, status_code=404)

@router.patch('/proxies/{proxy_id}/auto-extend')
async def set_auto_extend(
    proxy_id: int,
    request: schemas.AutoExtendRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Выключить / включить автопродление прокси.
    Доступен и пользователю и администратору
    """

    if current_user.role == UserRole.admin:
        stmt = select(Proxy).where(Proxy.id == proxy_id)
    else:
        stmt = select(Proxy).where(Proxy.id == proxy_id, Proxy.owner_id == current_user.id)

    result = await db.execute(stmt)
    proxy = result.scalar_one_or_none()

    if not proxy:
        raise HTTPException(status_code=404, detail='Прокси не найден')
    
    proxy.auto_extend_local = request.auto_extend
    await db.commit()
    logger.info(f'[AUTO_EXTEND] proxy_id={proxy_id} auto_extend={request.auto_extend}')
    return {'proxy_id': proxy_id, 'auto_extend': request.auto_extend}