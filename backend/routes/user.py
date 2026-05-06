import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from redis.asyncio import Redis

from backend.database.database import AssyncSessionLocal, get_db
from backend.database import schemas
from backend.database.models import User, Regions, Proxy, Transaction, TransactionType, ApiKey, UserRole
from backend.utils.security import get_current_user
from backend.utils.config import settings
from backend.api_services.ipfoxy import IPFoxyService
from backend.api_services.extend_service import extend_proxies_service
from backend.utils.check_location import check_proxy_country_with_ip_api
from backend.api_services.proxy_service import (
    get_proxy_page,
    get_proxy_detail_for_user,
    set_auto_extend as proxy_set_auto_extend
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
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает прокси текущего пользователя, отсортированные по id desc (новые первые).
    Поддерживает cursor-пагинацию: передавай last_id для следующей страницы.
    """
    return await get_proxy_page(
        db,
        owner_id=current_user.id,
        last_id=last_id,
        search=search,
    )

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

    ipfoxy_proxy_ids = None
    if order_data.proxy_ids:
        if current_user.role == UserRole.admin:
            stmt = select(Proxy).where(Proxy.id.in_(order_data.proxy_ids))
        else:
            stmt = select(Proxy).where(
                Proxy.id.in_(order_data.proxy_ids),
                Proxy.owner_id == current_user.id,
            )
        result = await db.execute(stmt)
        proxies_for_price = result.scalars().all()

        ipfoxy_proxy_ids = [p.ipfoxy_proxy_id for p in proxies_for_price if p.ipfoxy_proxy_id]
        if not ipfoxy_proxy_ids:
            raise HTTPException(status_code=400, detail="Не удалось найти внешние ID для выбранных прокси")

        logger.info(
            "[CALC_PRICE] конвертация DB-ids=%s -> ipfoxy_ids=%s",
            order_data.proxy_ids, ipfoxy_proxy_ids,
        )

    try:
        price = await service.get_order_price(
            order_type=order_data.order_type,
            days=order_data.days,
            area_id=order_data.area_id,
            proxy_ids=",".join(str(pid) for pid in ipfoxy_proxy_ids) if order_data.proxy_ids else None,
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

        asyncio.create_task(
            activate_proxies_background(
                order_id=str(order_id),
                user_id=current_user.id,
                api_key_id=user_api_key.id,
                expected_country_code=expected_country_code,
                area_id=str(request.area_id),
                days=request.days,
            )
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
        raise HTTPException(status_code=500, detail=f"Ошибка: {exc}")

async def activate_proxies_background(
    order_id: str,
    user_id: int,
    api_key_id: int,
    expected_country_code: str,
    area_id: str,
    days: int,
):
    """Фоновая задача: ждёт активации прокси у провайдера и сохраняет в БД."""
    logger.info("[ACTIVATE] Старт фоновой активации order_id=%s user_id=%s", order_id, user_id)

    async with AssyncSessionLocal() as db:
        try:
            service_data = await IPFoxyService.get_service_by_key_id(db, api_key_id)
            if not service_data:
                logger.error("[ACTIVATE] order_id=%s — API ключ id=%s не найден", order_id, api_key_id)
                return
            service, _ = service_data

            order_details = await service.get_order_information(order_id)
            order_data_ids = order_details.get('data', {}).get('proxy_ids', [])
            proxy_ids_str = ",".join(str(pid) for pid in order_data_ids) if order_data_ids else None

            if not proxy_ids_str:
                logger.error("[ACTIVATE] order_id=%s — proxy_ids не найдены в order-info: %s", order_id, order_details)
                return

            proxies_data = []
            for attempt in range(1, 21):
                await asyncio.sleep(15)
                proxies_data = await service.get_proxies_list(1, 50, proxy_ids_str)
                if proxies_data:
                    logger.info("[ACTIVATE] order_id=%s — прокси готовы на попытке %d", order_id, attempt)
                    break
                logger.warning("[ACTIVATE] order_id=%s — попытка %d/20, прокси ещё не готовы", order_id, attempt)

            if not proxies_data:
                logger.error("[ACTIVATE] order_id=%s — прокси не появились за 5 минут!", order_id)
                return

            added_count = 0
            for p in proxies_data:
                try:
                    raw_expire = p.get("expire_time")
                    proxy_public_ip = p.get("public_ip") or p.get("host") or p.get("server")

                    try:
                        checked_location, location_match = await check_proxy_country_with_ip_api(
                            ip_or_host=proxy_public_ip,
                            expected_country_code=expected_country_code,
                        )
                    except Exception as loc_exc:
                        logger.warning("[ACTIVATE] Ошибка проверки IP %s: %s", proxy_public_ip, loc_exc)
                        checked_location, location_match = "Error", False

                    new_proxy = Proxy(
                        owner_id=user_id,
                        api_key_id=api_key_id,
                        ipfoxy_proxy_id=str(p.get("id") or p.get("proxy_id")),
                        ipfoxy_order_id=str(order_id),
                        host=p.get("host") or p.get("server"),
                        public_ip=p.get("public_ip"),
                        port=int(p.get("port")),
                        username=p.get("user") or p.get("username"),
                        password=p.get("password"),
                        type=p.get("type"),
                        area_id=str(area_id),
                        country_code=expected_country_code,
                        expires_at=datetime.fromtimestamp(int(raw_expire), tz=timezone.utc) if raw_expire else None,
                        checked_location=checked_location,
                        location_match=location_match,
                        auto_extend=False,
                        auto_extend_local=False,
                        ip_version=p.get("ip_version"),
                        ip_type=p.get("ip_type"),
                    )
                    db.add(new_proxy)
                    added_count += 1
                except Exception as proxy_exc:
                    logger.error("[ACTIVATE] Ошибка подготовки прокси к сохранению: %s", proxy_exc, exc_info=True)
                    continue

            await db.commit()
            logger.info("[ACTIVATE] order_id=%s — %d прокси успешно сохранены в БД", order_id, added_count)

        except Exception as exc:
            logger.error("[ACTIVATE] order_id=%s — критическая ошибка: %s", order_id, exc, exc_info=True)
            await db.rollback()

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