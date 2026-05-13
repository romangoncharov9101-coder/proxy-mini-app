import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select, func, String, or_
from sqlalchemy.ext.asyncio import AsyncSession
from decimal import Decimal

from backend.database.models import Proxy, User, ApiKey, UserRole, Transaction, TransactionType
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
        sort_by: Optional[str] = None,
        country_code: Optional[str] = None,
):
    """
    Строит SQLAlchemy-statement для выборки прокси.
    user_api_key_id - api_key_id текущего пользователя (для user-роута);
                      None означает "все прокси" (для admin-роута).
    sort_by: newest | oldest | expires_asc | expires_desc
    country_code: фильтр по коду страны
    """
    now = datetime.now(timezone.utc)
    expiration_threshold = now - timedelta(days=2)

    # Определяем порядок сортировки
    if sort_by == 'oldest':
        order_clause = [Proxy.purchased_at.asc(), Proxy.id.asc()]
    elif sort_by == 'expires_asc':
        order_clause = [Proxy.expires_at.asc(), Proxy.id.asc()]
    elif sort_by == 'expires_desc':
        order_clause = [Proxy.expires_at.desc(), Proxy.id.desc()]
    else:  # newest (default)
        order_clause = [Proxy.purchased_at.desc(), Proxy.id.desc()]

    if user_api_key_id is None:
        stmt = (
            select(Proxy)
            .outerjoin(User, Proxy.owner_id == User.id)
            .outerjoin(ApiKey, Proxy.api_key_id == ApiKey.id)
            .where(Proxy.expires_at > expiration_threshold)
            .order_by(*order_clause)
        )
    else:
        stmt = (
            select(Proxy)
            .where(Proxy.expires_at > expiration_threshold)
            .order_by(*order_clause)
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

    if country_code:
        stmt = stmt.where(Proxy.country_code.ilike(country_code))

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
        sort_by: Optional[str] = None,
        country_code: Optional[str] = None,
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
        sort_by=sort_by,
        country_code=country_code,
    )

    result = await db.execute(stmt)
    rows = result.unique().scalars().all()
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = items[-1].id if (has_more and items) else None

    # Подгружаем owner info для всех режимов (owner lazy='selectin' — уже загружен)
    proxy_items = []
    for proxy in items:
        owner_username = None
        owner_tg_id = None
        if proxy.owner:
            owner_username = proxy.owner.username
            owner_first_name= proxy.owner.first_name
        # Создаём dict из proxy + owner fields
        from backend.database.schemas import ProxyListItem
        item = ProxyListItem(
            id=proxy.id,
            host=proxy.host,
            port=proxy.port,
            type=proxy.type,
            ip_type=proxy.ip_type,
            ip_version=proxy.ip_version,
            country_code=proxy.country_code,
            is_active=proxy.is_active,
            expires_at=proxy.expires_at,
            purchased_at=proxy.purchased_at,
            note=proxy.note,
            auto_extend=proxy.auto_extend,
            username=proxy.username,
            password=proxy.password,
            owner_username=owner_username,
            owner_first_name=owner_first_name,
        )
        proxy_items.append(item)

    logger.debug(f'[PROXY_PAGE] user_api_key_id={user_api_key_id} returned={len(proxy_items)} has_more={has_more}')
    return {'items': proxy_items, 'next_cursor': next_cursor, 'has_more': has_more}


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


async def resolve_ipfoxy_ids(
        db: AsyncSession,
        current_user: User,
        proxy_ids: list[int]
) -> str:
    """
    По списку внутренних DB-id прокси возвращает строку внешних ipfoxy-id
    через запятую без пробелов (например: "101,202,303").
    Для admin — без ограничения по ключу, для user — только свои прокси.
    """
    if current_user.role == UserRole.admin:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids))
    else:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids), Proxy.api_key_id == current_user.api_key_id)

    result = await db.execute(stmt)
    proxies = result.scalars().all()

    ipfoxy_ids = [p.ipfoxy_proxy_id for p in proxies if p.ipfoxy_proxy_id]
    if not ipfoxy_ids:
        raise HTTPException(status_code=400, detail='Не удалось найти внешние ID для выбранных прокси.')
    return ",".join(str(pid) for pid in ipfoxy_ids)


async def toggle_auto_extend(
        db: AsyncSession,
        proxy_ids: list[str],
        enable: bool,
        current_user: User
) -> dict:
    if isinstance(proxy_ids, int):
        proxy_ids = [proxy_ids]

    if not proxy_ids:
        raise HTTPException(status_code=400, detail='Список proxy_ids пуст')

    stmt = select(Proxy).where(Proxy.id.in_(proxy_ids))
    result = await db.execute(stmt)
    proxies_for_toggle_auto = result.scalars().all()

    # Используем resolve_ipfoxy_ids для получения строки внешних ID
    str_ipfoxy_proxy_id = await resolve_ipfoxy_ids(db, current_user, proxy_ids)

    try:
        service_data = await IPFoxyService.get_service_by_user(db, current_user)
        if not service_data:
            raise HTTPException(status_code=400, detail='API ключ не привязан.')
        service, user_api_key = service_data

        code, msg = await service.automatic_renew(int(enable), str_ipfoxy_proxy_id)
        if code not in [0, 200]:
            raise HTTPException(status_code=code, detail=f'Ошибка сервиса IpFoxy: {msg}')

        for proxy in proxies_for_toggle_auto:
            proxy.auto_extend = enable
        await db.commit()

    except HTTPException:
        raise
    except Exception as exc:
        logger.info(f'[TOGGLE_AUTO] ошибка продления прокси: {exc}')
        raise HTTPException(status_code=500, detail=f'Ошибка продления прокси: {exc}')

    return {'proxy_id': proxy_ids, 'auto_extend': enable}


async def extend_proxies_service(
        db: AsyncSession,
        current_user: User,
        proxy_ids: list[int],
        days: int
) -> dict:
    if not proxy_ids:
        raise HTTPException(status_code=400, detail="Список proxy_ids пуст")
    if days < 1:
        raise HTTPException(status_code=400, detail="days должен быть >= 1")

    logger.info(f'[EXTEND] user_id={current_user.id} proxy_ids={proxy_ids} days={days}')

    if current_user.role == UserRole.admin:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids))
    else:
        stmt = select(Proxy).where(
            Proxy.id.in_(proxy_ids),
            Proxy.api_key_id == current_user.api_key_id,
        )

    result = await db.execute(stmt)
    proxies = result.scalars().all()

    if not proxies:
        raise HTTPException(status_code=404, detail='Прокси не найдены или у вас нет доступа.')

    if current_user.role != UserRole.admin and len(proxies) != len(proxy_ids):
        raise HTTPException(status_code=403, detail='Некоторые прокси не найдены или принадлежат другому пользователю.')

    service_data = await IPFoxyService.get_service_by_user(db, current_user)
    if not service_data:
        raise HTTPException(status_code=400, detail='К аккаунту не привязан активный АПИ ключ. Обратитесь к администратору.')
    service, user_api_key = service_data

    # resolve_ipfoxy_ids возвращает строку через запятую
    proxy_ids_str = await resolve_ipfoxy_ids(db, current_user, proxy_ids)
    ipfoxy_ids = [int(x) for x in proxy_ids_str.split(',')]

    try:
        total_cost: Decimal = await service.get_order_price(order_type='EXTEND', days=days, proxy_ids=ipfoxy_ids)
    except Exception as exc:
        logger.error(f'[EXTEND] ошибка расчета цены: {exc}')
        raise HTTPException(status_code=500, detail='Ошибка при расчете стоимости продления.')

    current_balance: Decimal = user_api_key.balance or Decimal('0.00')
    if current_balance < total_cost:
        raise HTTPException(status_code=400, detail=f'Недостаточно средств. Баланс: {current_balance} USD, требуется: {total_cost} USD')

    order_id = ''
    try:
        order_id = await service.renew_proxy(proxy_ids=proxy_ids_str, days=days)
    except Exception as exc:
        logger.error(f'[EXTEND] ошибка renew_proxy: {exc}')
        raise HTTPException(status_code=500, detail=f'Ошибка продления прокси: {exc}')

    now = datetime.now(timezone.utc)
    for proxy in proxies:
        old_expires = proxy.expires_at
        if old_expires and old_expires.tzinfo is None:
            old_expires = old_expires.replace(tzinfo=timezone.utc)
        if old_expires and old_expires > now:
            proxy.expires_at = old_expires + timedelta(days=days)
        else:
            proxy.expires_at = now + timedelta(days=days)
            proxy.renewal_at = now

    try:
        new_balance = await service.get_balance()
        user_api_key.balance = new_balance
    except Exception as exc:
        logger.warning(f'[EXTEND] не удалось обновить баланс: {exc}')
        user_api_key.balance = current_balance - total_cost

    renew_order_id = None
    if isinstance(order_id, dict):
        renew_order_id = order_id.get('data', {}).get('order_id')
    elif order_id:
        renew_order_id = str(order_id)

    tx = Transaction(
        user_id=current_user.id,
        order_id=renew_order_id,
        api_key_id=user_api_key.id,
        type=TransactionType.extend,
        amount=-total_cost,
        description=f'Продление {len(proxies)} прокси на {days} дн. ({renew_order_id})',
    )
    db.add(tx)
    await db.commit()

    logger.info(f'[EXTEND] OK user_id={current_user.id} proxies={len(proxies)} days={days} cost={total_cost}')
    return {
        'status': 'success',
        'extended': len(proxies),
        'days': days,
        'total_cost': str(total_cost),
        'order_id': renew_order_id
    }