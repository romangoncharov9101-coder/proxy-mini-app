import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from redis.asyncio import Redis

from backend.database.database import get_db
from backend.database import schemas
from backend.database.models import User, Regions, Proxy, Transaction, TransactionType, ApiKey, UserRole, AppSettings
from backend.utils.security import get_current_user
from backend.utils.config import settings
from backend.api_services.ipfoxy import IPFoxyService
from backend.tasks.sync_tasks import ts_to_dt
from backend.api_services.proxy_service import (
    get_proxy_page,
    get_proxy_detail_for_user,
    toggle_auto_extend as proxy_set_auto_extend,
    extend_proxies_service,
    resolve_ipfoxy_ids,
)

router = APIRouter(prefix="/user", tags=["User"])
logger = logging.getLogger("routes.user")

CACHE_KEY_COUNTRIES = "all_countries_cache"
CACHE_EXPIRE = 3600  

@router.get("/me", response_model=schemas.UserProfileResponse)
async def get_me(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    force_refresh: bool = Query(False, description="Принудительно обновить баланс"),
):
    """
    Возвращает профиль текущего пользователя.
    Если к аккаунту привязан API ключ — обновляет баланс
    (не чаще раза в 60 минут, если не передан force_refresh=true).
    """
    logger.debug("[ME] user_id=%s tg_id=%s", user.id, user.telegram_id)

    current_balance = Decimal("0.00")

    if user.api_key_id:
        await db.refresh(user)
        stmt = select(ApiKey).where(ApiKey.id == user.api_key_id, ApiKey.is_active.is_(True))
        res = await db.execute(stmt)
        api_key = res.scalar_one_or_none()

        if api_key:
            needs_refresh = (
                force_refresh
                or api_key.balance is None
                or api_key.last_checked is None
                or (datetime.now(timezone.utc) - api_key.last_checked) > timedelta(minutes=60)
            )

            if needs_refresh:
                logger.info("[ME] user_id=%s — обновляем баланс key_id=%s (force=%s)", user.id, api_key.id, force_refresh)
                try:
                    service = IPFoxyService.get_service_by_key_obj(api_key)
                    real_balance = await service.get_balance()
                    api_key.balance = real_balance
                    api_key.last_checked = func.now()
                    await db.commit()
                    current_balance = real_balance
                except Exception as exc:
                    logger.error("[ME] user_id=%s — ошибка обновления баланса: %s", user.id, exc)
                    current_balance = api_key.balance or Decimal("0.00")
            else:
                current_balance = api_key.balance or Decimal("0.00")

    if user.api_key_id:
        try:
            from redis.asyncio import Redis as _SyncRedis
            async with _SyncRedis.from_url(
                settings.REDIS_URL, decode_responses=True,
                socket_connect_timeout=1, socket_timeout=1,
            ) as _r:
                cache_exists = await _r.exists(f"user:proxies:{user.id}")
            if not cache_exists:
                from backend.tasks.sync_tasks import sync_proxies_task
                sync_proxies_task.delay(api_key_db_id=user.api_key_id)
                logger.info("[ME] user_id=%s — запущена фоновая синхронизация прокси", user.id)
        except Exception as _exc:
            logger.warning("[ME] Не удалось запустить sync_proxies_task: %s", _exc)

    return {
        "first_name": user.first_name,
        "username":   user.username,
        "balance":    current_balance,
        "role":       user.role,
        "api_key_id": user.api_key_id,
    }

@router.get("/countries", response_model=schemas.CountriesResponse)
async def get_countries(
    last_id: Optional[int] = Query(None),
    limit:   int = Query(20, ge=1, le=100),
    db:      AsyncSession = Depends(get_db),
    search:  Optional[str] = Query(None, description="Поиск по названию страны"),
    current_user: User = Depends(get_current_user),
):
    """
    Пагинированный список регионов.
    Redis используется только как необязательный кеш.
    Если Redis недоступен — данные берутся напрямую из БД.
    Если регионов нет в БД — запускает Celery задачу синхронизации.
    """
    logger.debug("[COUNTRIES] user_id=%s last_id=%s limit=%s", current_user.id, last_id, limit)

    countries = None

    try:
        async with Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        ) as redis_client:
            cached_raw = await redis_client.get(CACHE_KEY_COUNTRIES)

            if cached_raw:
                try:
                    countries = json.loads(cached_raw)
                    logger.debug("[COUNTRIES] из кеша: %d записей", len(countries))
                except Exception as exc:
                    logger.warning("[COUNTRIES] ошибка парсинга кеша: %s", exc)
                    countries = None

    except Exception as exc:
        logger.warning("[COUNTRIES] Redis недоступен, читаем из БД: %s", exc)
        countries = None

    try:
        if not countries:
            stmt = (
                select(Regions)
                .where(Regions.status.is_(True))
                .order_by(asc(Regions.id))
            )
            result = await db.execute(stmt)
            all_regions = result.scalars().all()

            if not all_regions:
                logger.info("[COUNTRIES] БД пуста — запускаем sync_regions_task")
                try:
                    from backend.tasks.sync_tasks import sync_regions_task
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, sync_regions_task.delay)
                except Exception as exc:
                    logger.error("[COUNTRIES] ошибка запуска задачи: %s", exc)

            countries = [
                {
                    "id":           r.id,
                    "area_id":      r.area_id,
                    "ip_type":      r.ip_type,
                    "ip_version":   r.ip_version,
                    "country":      r.country or "",
                    "country_code": r.country_code or "",
                    "retail_price": float(r.retail_price) if r.retail_price else 0.0,
                }
                for r in all_regions
            ]

            if countries:
                try:
                    async with Redis.from_url(
                        settings.REDIS_URL,
                        decode_responses=True,
                        socket_connect_timeout=2,
                        socket_timeout=2,
                    ) as redis_client:
                        await redis_client.setex(
                            CACHE_KEY_COUNTRIES,
                            CACHE_EXPIRE,
                            json.dumps(countries),
                        )
                        logger.debug("[COUNTRIES] сохранено в кеш: %d записей", len(countries))
                except Exception as exc:
                    logger.warning("[COUNTRIES] не удалось сохранить кеш Redis: %s", exc)

        # Фильтрация по allowed_area_ids (только для пользователей, не для администраторов)
        if current_user.role.value != 'admin':
            try:
                settings_res = await db.execute(select(AppSettings).where(AppSettings.id == 1))
                app_settings = settings_res.scalar_one_or_none()
                if app_settings and app_settings.allowed_area_ids:
                    allowed_ids = set(x.strip() for x in app_settings.allowed_area_ids.split(',') if x.strip())
                    if allowed_ids:
                        countries = [c for c in countries if str(c["area_id"]) in allowed_ids]
                        logger.debug("[COUNTRIES] фильтр area_ids: осталось %d", len(countries))
            except Exception as exc:
                logger.warning("[COUNTRIES] ошибка чтения AppSettings: %s", exc)

        if search:
            search_val = search.lower().strip()
            countries = [
                c for c in countries
                if search_val in (c["country"] or "").lower()
                or search_val in (c["country_code"] or "").lower()
            ]

            logger.debug("[COUNTRIES] поиск '%s' — найдено %d", search_val, len(countries))

            return {
                "items": countries,
                "next_cursor": None,
                "has_more": False,
            }

        start_index = 0

        if last_id is not None:
            for i, c in enumerate(countries):
                if c["id"] == last_id:
                    start_index = i + 1
                    break
            else:
                return {
                    "items": [],
                    "next_cursor": None,
                    "has_more": False,
                }

        page = countries[start_index:start_index + limit]
        has_more = (start_index + limit) < len(countries)
        next_cursor = page[-1]["id"] if page and has_more else None

        logger.debug(
            "[COUNTRIES] страница: %d элем., has_more=%s, next_cursor=%s",
            len(page),
            has_more,
            next_cursor,
        )

        return {
            "items": page,
            "next_cursor": next_cursor,
            "has_more": has_more,
        }

    except Exception as exc:
        logger.error("[COUNTRIES] критическая ошибка: %s", exc, exc_info=True)
        return {
            "items": [],
            "next_cursor": None,
            "has_more": False,
        }

@router.get("/proxies", response_model=schemas.ProxyPageResponse)
async def get_my_proxies(
    last_id: Optional[int] = Query(None, description="cursor — id последнего полученного прокси"),
    limit:   int = Query(20, ge=1, le=50),
    db:      AsyncSession = Depends(get_db),
    search:       Optional[str] = Query(None, description="Поиск по юзеру, proxy_id, order_id"),
    sort_by:      Optional[str] = Query(None, description="newest | oldest | expires_asc | expires_desc"),
    country_code: Optional[str] = Query(None, description="фильтр по коду страны"),
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает прокси текущего пользователя, отсортированные по id desc (новые первые).
    """
    return await get_proxy_page(
        db,
        user_api_key_id=current_user.api_key_id,
        last_id=last_id,
        search=search,
        sort_by=sort_by,
        country_code=country_code,
    )

@router.get("/proxies/countries")
async def get_proxy_countries_user(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Список уникальных стран из купленных прокси пользователя (для фильтра)."""
    from sqlalchemy import distinct
    result = await db.execute(
        select(distinct(Proxy.country_code))
        .where(
            Proxy.country_code.isnot(None),
            Proxy.api_key_id == current_user.api_key_id,
            Proxy.is_active == True,
        )
        .order_by(Proxy.country_code)
    )
    codes = [row[0] for row in result.all() if row[0]]
    return [{"country_code": code} for code in codes]

@router.get("/proxies/{proxy_id}", response_model=schemas.ProxyDetail)
async def get_proxy_detail(
    proxy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает полную информацию о конкретном прокси пользователя
    включая логин/пароль и техническую информацию.
    """
    logger.debug("[PROXY_DETAIL] user_id=%s proxy_id=%s", current_user.id, proxy_id)
    return await get_proxy_detail_for_user(db, proxy_id, current_user)

@router.post("/calculate-price")
async def calculate_order_price(
    order_data: schemas.OrderPriceRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Рассчитывает стоимость заказа через API ключ пользователя."""
    logger.info(
        "[CALC_PRICE] user_id=%s area_id=%s num=%s days=%s",
        current_user.id, order_data.area_id, order_data.num, order_data.days,
    )

    service_data = await IPFoxyService.get_service_by_user(db, current_user)
    if not service_data:
        raise HTTPException(
            status_code=400,
            detail="К вашему аккаунту не привязан активный API ключ. Обратитесь к администратору.",
        )
    service, _ = service_data

    ipfoxy_ids_str = None
    if order_data.proxy_ids:
        # resolve_ipfoxy_ids уже возвращает готовую строку через запятую
        ipfoxy_ids_str = await resolve_ipfoxy_ids(db, current_user, order_data.proxy_ids)
        logger.info(
            "[CALC_PRICE] конвертация DB-ids=%s -> ipfoxy_ids_str=%s",
            order_data.proxy_ids, ipfoxy_ids_str,
        )

    try:
        price = await service.get_order_price(
            order_type=order_data.order_type,
            days=order_data.days,
            area_id=order_data.area_id,
            proxy_ids=ipfoxy_ids_str,
            num=order_data.num,
        )
        return {
            "status":      "success",
            "order_price": price,
            "currency":    "USD",
            "details": {
                "days":    order_data.days,
                "num":     order_data.num,
                "area_id": order_data.area_id,
            },
        }
    except Exception as exc:
        logger.error("[CALC_PRICE] user_id=%s — ошибка: %s", current_user.id, exc)
        raise HTTPException(status_code=500, detail="Ошибка при обращении к поставщику")

@router.post("/purchase-proxy")
async def purchase_proxy_endpoint(
    request: schemas.ProxyPurchaseRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    user_id = current_user.id
    logger.info("[PURCHASE] Start: user_id=%s area_id=%s num=%d", user_id, request.area_id, request.num)

    try:
        service_data = await IPFoxyService.get_service_by_user(db, current_user)
        if not service_data:
            raise HTTPException(status_code=400, detail='API ключ не привязан.')
        service, user_api_key = service_data

        total_cost = await service.get_order_price(
            order_type="BUY",
            area_id=request.area_id,
            num=request.num,
            days=request.days,
        )
        current_balance = user_api_key.balance or Decimal("0.00")

        if current_balance < total_cost:
            raise HTTPException(status_code=400, detail=f'Недостаточно средств. Баланс: {current_balance} USD. Необходимо: {total_cost}')

        region_res = await db.execute(select(Regions).where(Regions.area_id == str(request.area_id)))
        region = region_res.scalar_one_or_none()
        if not region:
            raise HTTPException(status_code=400, detail='Регион не найден')

        expected_country_code = region.country_code

        order_id = await service.purchase_proxy(
            area_id=request.area_id,
            num=request.num,
            days=request.days,
        )
        if not order_id:
            raise ValueError('Не получен order_id от провайдера')

        try:
            user_api_key.balance = await service.get_balance()
        except Exception as bal_exc:
            logger.warning("[PURCHASE] Не удалось обновить баланс: %s", bal_exc)
            user_api_key.balance = current_balance - total_cost

        transaction = Transaction(
            user_id=current_user.id,
            order_id=str(order_id),
            api_key_id=user_api_key.id,
            type=TransactionType.purchase,
            amount=total_cost,
            description=f"Order {order_id}: {request.num} proxies × {request.days}d — АКТИВАЦИЯ",
        )
        db.add(transaction)
        await db.commit()

        from backend.tasks.sync_tasks import activate_proxies_task
        activate_proxies_task.delay(
            order_id=str(order_id),
            user_id=current_user.id,
            api_key_id=user_api_key.id,
            expected_country_code=expected_country_code,
            area_id=str(request.area_id),
            days=request.days,
        )

        return {
            'status': 'pending',
            'order_id': order_id,
            'message': 'Заказ оформлен, прокси появятся в течение минуты',
            'count': request.num
        }

    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        logger.error('[PURCHASE] Критическая ошибка: %s', exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@router.patch("/proxies/{proxy_id}/auto-extend")
async def set_auto_extend(
    proxy_id: int,
    data: schemas.AutoExtendRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Включить / выключить автопродление прокси.
    Администратор может менять любой прокси, пользователь — только свои.
    """
    logger.info("[AUTO_EXTEND] user_id=%s proxy_id=%s → %s", current_user.id, proxy_id, data.auto_extend)
    return await proxy_set_auto_extend(db, proxy_id, data.auto_extend, current_user)

@router.patch("/proxies/{proxy_id}/note")
async def update_proxy_note(
    proxy_id: int,
    data: schemas.ProxyNoteUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Установить или очистить нотатку для прокси.
    Пользователь — только для своих прокси. Администратор — для любых.
    """
    stmt = select(Proxy).where(Proxy.id == proxy_id)
    if current_user.role != UserRole.admin:
        stmt = stmt.where(Proxy.api_key_id == current_user.api_key_id)
    result = await db.execute(stmt)
    proxy = result.scalar_one_or_none()
    if not proxy:
        raise HTTPException(status_code=404, detail='Прокси не найден')
    note_val = (data.note or '').strip() or None
    proxy.note = note_val
    await db.commit()
    logger.info("[PROXY_NOTE] proxy_id=%s user_id=%s note=%s", proxy_id, current_user.id, note_val)
    return {'proxy_id': proxy_id, 'note': note_val}

@router.post('/proxies/extend')
async def extend_proxies(
    request: schemas.ExtendProxyRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return await extend_proxies_service(
        db=db,
        current_user=current_user,
        proxy_ids=request.proxy_ids,
        days=request.days
    )