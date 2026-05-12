import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select, func, String, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.models import Proxy, User, ApiKey, UserRole
from backend.database import schemas
from backend.api_services.ipfoxy import IPFoxyService

logger = logging.getLogger('services.proxy')

def _build_proxy_list_stmt(
        *,
        user_api_key_id: Optional[int] = None,
        last_id: Optional[int] = None,
        limit: int = 20,
        search: Optional[str] = None,
        key_id: Optional[int] = None,
        filter_owner_id: Optional[int] = None,
        proxy_status: Optional[str] = None,
):
    """
    Строит SQLAlchemy-statement для выборки прокси.
    user_api_key_id - api_key_id текущего пользователя (для user-роута);
                      None означает "все прокси" (для admin-роута).
    owner_search    - поиск по username/first_name/telegram_id владельца
                      ИЛИ key_name/api_id ключа (только для admin-роута).
    """
    now = datetime.now(timezone.utc)
    expiration_threshold = now - timedelta(days=2)

    if user_api_key_id is None:
        stmt = (
            select(Proxy)
            .outerjoin(User, Proxy.owner_id == User.id)
            .outerjoin(ApiKey, Proxy.api_key_id == ApiKey.id)
            .where(Proxy.expires_at > expiration_threshold)
            .order_by(Proxy.purchased_at.desc(), Proxy.id.desc())
        )
    else:
        stmt = (
            select(Proxy)
            .where(Proxy.expires_at > expiration_threshold)
            .order_by(Proxy.purchased_at.desc(), Proxy.id.desc())
        )

    if user_api_key_id is not None:
        stmt = stmt.where(
            Proxy.api_key_id == user_api_key_id,
            Proxy.is_active == True,
        )

    if search:
        q = f'%{search}%'
        conditions = [
            Proxy.username.ilike(q),
            func.cast(Proxy.ipfoxy_proxy_id, String).ilike(q),
            func.cast(Proxy.ipfoxy_order_id, String).ilike(q),
            Proxy.host.ilike(q),
            Proxy.note.ilike(q),
        ]
        if user_api_key_id is None:
            conditions += [
                User.username.ilike(q),
                User.first_name.ilike(q),
                ApiKey.key_name.ilike(q),
                ApiKey.api_id.ilike(q),
            ]
            if search.strip().lstrip('-').isdigit():
                conditions.append(User.telegram_id == int(search.strip()))
        stmt = stmt.where(or_(*conditions))

    if last_id:
        stmt = stmt.where(Proxy.id < last_id)

    if key_id:
        stmt = stmt.where(Proxy.api_key_id == key_id)
    if filter_owner_id:
        stmt = stmt.where(Proxy.owner_id == filter_owner_id)

    if proxy_status == 'active':
        stmt = stmt.where(Proxy.is_active == True, Proxy.expires_at > now)
    elif proxy_status == 'inactive':
        stmt = stmt.where(Proxy.is_active == False)
    elif proxy_status == 'expired':
        stmt = stmt.where(Proxy.expires_at <= now)

    stmt = stmt.limit(limit + 1)
    return stmt


async def get_proxy_page(
        db: AsyncSession,
        *,
        user_api_key_id: Optional[int] = None,
        last_id: Optional[int] = None,
        limit: int = 20,
        search: Optional[str] = None,
        key_id: Optional[int] = None,
        filter_owner_id: Optional[int] = None,
        proxy_status: Optional[str] = None,
) -> dict:
    """Возвращает страницу прокси с cursor-пагинацией."""
    stmt = _build_proxy_list_stmt(
        user_api_key_id=user_api_key_id,
        last_id=last_id,
        limit=limit,
        search=search,
        key_id=key_id,
        filter_owner_id=filter_owner_id,
        proxy_status=proxy_status,
    )

    result = await db.execute(stmt)
    rows = result.unique().scalars().all()
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = items[-1].id if (has_more and items) else None

    logger.debug(f'[PROXY_PAGE] user_api_key_id={user_api_key_id} returned={len(items)} has_more={has_more}')
    return {'items': items, 'next_cursor': next_cursor, 'has_more': has_more}


def build_proxy_detail(
        proxy: Proxy,
        *,
        owner_username: Optional[str] = None,
        owner_tg_id: Optional[int] = None,
        api_key_name: Optional[str] = None
) -> schemas.ProxyDetail:
    """Формирует ProxyDetail из ORM-объекта. Общий код для user и admin."""
    return schemas.ProxyDetail(
        id=proxy.id,
        ipfoxy_proxy_id=proxy.ipfoxy_proxy_id,
        ipfoxy_order_id=proxy.ipfoxy_order_id,
        host=proxy.host,
        public_ip=proxy.public_ip,
        port=proxy.port,
        type=proxy.type,
        username=proxy.username,
        password=proxy.password,
        ip_type=proxy.ip_type,
        ip_version=proxy.ip_version,
        country_code=proxy.country_code,
        area_id=proxy.area_id,
        auto_extend=proxy.auto_extend,
        is_active=proxy.is_active,
        purchased_at=proxy.purchased_at,
        expires_at=proxy.expires_at,
        renewal_at=proxy.renewal_at,
        checked_location=proxy.checked_location,
        location_match=proxy.location_match,
        note=proxy.note,
        owner_username=owner_username,
        owner_tg_id=owner_tg_id,
        api_key_name=api_key_name,
    )

async def get_proxy_or_404(
        db: AsyncSession,
        proxy_id: int,
        *,
        api_key_id: Optional[int] = None,
) -> Proxy:
    """Загружаем прокси, кидает 404 если не найден."""
    stmt = select(Proxy).where(Proxy.id == proxy_id)
    if api_key_id:
        stmt = stmt.where(Proxy.api_key_id == api_key_id)

    result = await db.execute(stmt)
    proxy = result.scalar_one_or_none()

    if not proxy:
        raise HTTPException(status_code=404, detail='Прокси не найден')
    return proxy


async def get_proxy_detail_for_user(
        db: AsyncSession,
        proxy_id: int,
        current_user: User
) -> schemas.ProxyDetail:
    """Детальная карточка прокси для обычного пользователя."""
    proxy = await get_proxy_or_404(db, proxy_id, api_key_id=current_user.api_key_id)
    return build_proxy_detail(proxy)


async def get_proxy_detail_for_admin(
        db: AsyncSession,
        proxy_id: int,
) -> schemas.ProxyDetail:
    """Детальная карточка прокси для администратора (с владельцем и ключом)."""
    proxy = await get_proxy_or_404(db, proxy_id)

    owner_username: Optional[str] = None
    owner_tg_id: Optional[int] = None
    if proxy.owner_id:
        owner_res = await db.execute(select(User).where(User.id == proxy.owner_id))
        owner = owner_res.scalar_one_or_none()
        if owner:
            owner_username = owner.username or owner.first_name or 'Anonymous'
            owner_tg_id = owner.telegram_id

    api_key_name: Optional[str] = None
    if proxy.api_key_id:
        key_res = await db.execute(select(ApiKey).where(ApiKey.id == proxy.api_key_id))
        key_obj = key_res.scalar_one_or_none()
        if key_obj:
            api_key_name = key_obj.key_name

    return build_proxy_detail(
        proxy,
        owner_username=owner_username,
        owner_tg_id=owner_tg_id,
        api_key_name=api_key_name,
    )

async def toggle_auto_extend(
        db: AsyncSession,
        proxy_ids: list[str],
        enable: bool,
        current_user: User
) -> dict:
    ipfoxy_proxy_ids = None
    if isinstance(proxy_ids, int):
        proxy_ids = [proxy_ids]

    if proxy_ids:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids))
        result = await db.execute(stmt)
        proxies_for_toggle_auto = result.scalars().all()

        ipfoxy_proxy_ids = [p.ipfoxy_proxy_id for p in proxies_for_toggle_auto if p.ipfoxy_proxy_id]
        if not ipfoxy_proxy_ids:
            raise HTTPException(status_code=400, detail="Не удалось найти внешние ID для выбранных прокси")
        
    try:
        service_data = await IPFoxyService.get_service_by_user(db, current_user)
        if not service_data:
            raise HTTPException(status_code=400, detail='API ключ не привязан.')
        service, user_api_key = service_data

        str_ipfoxy_proxy_id = ",".join(str(pid) for pid in ipfoxy_proxy_ids)

        code, msg = await service.automatic_renew(int(enable), str_ipfoxy_proxy_id)
        if code not in [0, 200]:
            raise HTTPException(status_code=code, detail=f'Ошибка сервиса IpFoxy: {msg}')
        
        for proxy in proxies_for_toggle_auto:
            proxy.auto_extend = enable
        await db.commit()

    except Exception as exc:
        logger.info(f'[TOGGLE_AUTO] ошибка продления прокси: {exc}')
        raise HTTPException(status_code=500, detail=f'Ошибка продления прокси: {exc}')

    return {'proxy_id': proxy_ids, 'auto_extend': enable}
        

# async def set_auto_extend(
#         db: AsyncSession,
#         proxy_id: int,
#         auto_extend: bool,
#         current_user: User
# ) -> dict:
#     """Включить/выключить автопродление. Admin может менять любой прокси."""
#     api_key_id = None if current_user.role == UserRole.admin else current_user.api_key_id
#     proxy = await get_proxy_or_404(db, proxy_id, api_key_id=api_key_id)
#     proxy.auto_extend_local = auto_extend
#     await db.commit()
#     logger.info(f'[AUTO_EXTEND] proxy_id={proxy_id} auto_extend={auto_extend} by user={current_user.id}')
#     return {'proxy_id': proxy_id, 'auto_extend': auto_extend}