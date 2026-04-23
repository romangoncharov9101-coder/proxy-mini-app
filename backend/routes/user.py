import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, String
from redis.asyncio import Redis

from backend.database.database import AssyncSessionLocal, get_db
from backend.database import schemas
from backend.database.models import User, Regions, Proxy, Transaction, TransactionType, ApiKey, UserRole
from backend.utils.security import get_current_user
from backend.utils.config import settings
from backend.api_services.ipfoxy import IPFoxyService

router = APIRouter(prefix="/user", tags=["User"])
logger = logging.getLogger("routes.user")

CACHE_KEY_COUNTRIES = "all_countries_cache"
CACHE_EXPIRE = 3600  

@router.get("/me", response_model=schemas.UserProfileResponse)
async def get_me(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Возвращает профиль текущего пользователя.
    Если к аккаунту привязан API ключ — обновляет баланс
    (не чаще раза в 5 минут, чтобы не перегружать IPFoxy).
    """
    logger.debug("[ME] user_id=%s tg_id=%s", user.id, user.telegram_id)

    current_balance = Decimal("0.00")

    if user.api_key_id:
        await db.refresh(user)
        stmt = select(ApiKey).where(ApiKey.id == user.api_key_id, ApiKey.is_active.is_(True))
        res = await db.execute(stmt)
        api_key = res.scalar_one_or_none()
        print(api_key)

        if api_key:
            needs_refresh = (
                api_key.balance is None
                or api_key.last_checked is None
                or (datetime.now(timezone.utc) - api_key.last_checked) > timedelta(minutes=60)
            )

            if needs_refresh:
                logger.info("[ME] user_id=%s — обновляем баланс key_id=%s", user.id, api_key.id)
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
    Пагинированный список регионов с кешированием в Redis.
    Если регионов нет в БД — запускает Celery задачу синхронизации.
    """
    logger.debug("[COUNTRIES] user_id=%s last_id=%s limit=%s", current_user.id, last_id, limit)

    async with Redis.from_url(
        settings.REDIS_URL, decode_responses=True, socket_connect_timeout=2
    ) as redis_client:
        try:
            countries = None
            cached_raw = await redis_client.get(CACHE_KEY_COUNTRIES)

            if cached_raw:
                try:
                    countries = json.loads(cached_raw)
                    logger.debug("[COUNTRIES] из кеша: %d записей", len(countries))
                except Exception as exc:
                    logger.warning("[COUNTRIES] ошибка парсинга кеша: %s", exc)

            if not countries:
                async with AssyncSessionLocal() as inner_db:
                    stmt = select(Regions).where(Regions.status.is_(True)).order_by(asc(Regions.id))
                    result = await inner_db.execute(stmt)
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
                            "ip_type":      r.ip_type or "STATIC",
                            "ip_version":   r.ip_version or "IPv4",
                            "country":      r.country,
                            "country_code": r.country_code,
                            "retail_price": float(r.retail_price) if r.retail_price else 0.0,
                        }
                        for r in all_regions
                    ]

                    if countries:
                        await redis_client.setex(CACHE_KEY_COUNTRIES, CACHE_EXPIRE, json.dumps(countries))

            if search and countries:
                search_val = search.lower()
                countries = [
                    c for c in countries 
                    if search_val in c["country"].lower() or search_val in c["country_code"].lower()
                ]

            # ── Cursor пагинация ──────────────────────────────────────────
            start_index = 0
            if last_id is not None:
                for i, c in enumerate(countries):
                    if c["id"] == last_id:
                        start_index = i + 1
                        break
                else:
                    return {"items": [], "next_cursor": None, "has_more": False}

            page = countries[start_index: start_index + limit]
            has_more = (start_index + limit) < len(countries)
            next_cursor = page[-1]["id"] if page and has_more else None

            logger.debug(
                "[COUNTRIES] страница: %d элем., has_more=%s, next_cursor=%s",
                len(page), has_more, next_cursor,
            )
            return {"items": page, "next_cursor": next_cursor, "has_more": has_more}

        except Exception as exc:
            logger.error("[COUNTRIES] критическая ошибка: %s", exc, exc_info=True)
            return {"items": [], "next_cursor": None, "has_more": False}

@router.get("/proxies", response_model=schemas.ProxyPageResponse)
async def get_my_proxies(
    last_id: Optional[int] = Query(None, description="cursor — id последнего полученного прокси"),
    limit:   int = Query(20, ge=1, le=50),
    db:      AsyncSession = Depends(get_db),
    country_code: Optional[str] = Query(None, description="Фильтр по коду страны"),
    expired:      Optional[bool]= Query(None, description="True - только истекшие, False - только активные"),
    search:       Optional[str] = Query(None, description="Поиск по юзеру, proxy_id, order_id"),
    current_user: User = Depends(get_current_user),
):
    """
    Возвращает прокси текущего пользователя, отсортированные по id desc (новые первые).
    Поддерживает cursor-пагинацию: передавай last_id для следующей страницы.
    """
    now = datetime.now(timezone.utc)
    stmt = (
        select(Proxy)
        .where(
            Proxy.owner_id == current_user.id,
            Proxy.is_active == True,
            Proxy.expires_at > now
            )
        .order_by(Proxy.id.desc())
    )

    if search:
        search_filter = f"%{search}%"
        stmt = stmt.where(
            (Proxy.username.ilike(search_filter)) |
            (func.cast(Proxy.ipfoxy_proxy_id, String).ilike(search_filter)) |
            (func.cast(Proxy.ipfoxy_order_id, String).ilike(search_filter)) |
            (Proxy.host.ilike(search_filter))
        )

    if country_code:
        stmt = stmt.where(Proxy.country_code == country_code.upper())

    if expired is True:
        stmt = stmt.where(Proxy.expires_at < now)
    elif expired is False:
        stmt = stmt.where(Proxy.expires_at > now)

    if last_id is not None:
        stmt = stmt.where(Proxy.id < last_id)

    stmt = stmt.limit(limit + 1)

    result = await db.execute(stmt)
    rows = result.scalars().all()

    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = items[-1].id if has_more and items else None

    logger.debug("[PROXIES] user_id=%s — %d прокси, has_more=%s", current_user.id, len(items), has_more)

    return {
        "items":       items,
        "next_cursor": next_cursor,
        "has_more":    has_more,
    }

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

    stmt = select(Proxy).where(
        Proxy.id == proxy_id,
        Proxy.owner_id == current_user.id,
    )
    result = await db.execute(stmt)
    proxy = result.scalar_one_or_none()

    if not proxy:
        logger.warning("[PROXY_DETAIL] proxy_id=%s не найден для user_id=%s", proxy_id, current_user.id)
        raise HTTPException(status_code=404, detail="Прокси не найден")

    return proxy

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

    try:
        price = await service.get_order_price(
            order_type=order_data.order_type,
            days=order_data.days,
            area_id=order_data.area_id,
            proxy_ids=[str(p) for p in order_data.proxy_ids] if order_data.proxy_ids else None,
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
    """
    Покупает прокси через API ключ пользователя.
    1. Проверяет наличие привязанного ключа
    2. Проверяет достаточность баланса
    3. Создаёт заказ, сохраняет прокси и транзакцию в БД
    """
    logger.info(
        "[PURCHASE] user_id=%s area_id=%s num=%d days=%d",
        current_user.id, request.area_id, request.num, request.days,
    )

    service_data = await IPFoxyService.get_service_by_user(db, current_user)
    if not service_data:
        raise HTTPException(
            status_code=400,
            detail="К вашему аккаунту не привязан активный API ключ. Обратитесь к администратору.",
        )
    service, user_api_key = service_data

    # ── Проверка баланса ──────────────────────────────────────────────────
    total_cost = await service.get_order_price(
        order_type="BUY",
        area_id=request.area_id,
        num=request.num,
        days=request.days,
    )

    current_balance = user_api_key.balance or Decimal("0.00")
    if current_balance < total_cost:
        logger.warning(
            "[PURCHASE] user_id=%s — недостаточно средств: баланс=%s цена=%s",
            current_user.id, current_balance, total_cost,
        )
        raise HTTPException(
            status_code=400,
            detail=f"Недостаточно средств на балансе ключа. Баланс: {current_balance} USD, требуется: {total_cost} USD.",
        )

    # ── Покупка ───────────────────────────────────────────────────────────
    try:
        order_id = await service.purchase_proxy(
            area_id=request.area_id,
            num=request.num,
            days=request.days,
        )
        if not order_id:
            raise ValueError("Не получен order_id от провайдера")

        order_details = await service.get_order_information(order_id)
        if order_details.get("code") not in (0, 200):
            raise ValueError(f"Ошибка получения деталей заказа: {order_details.get('msg')}")

        # Обновляем баланс ключа
        new_balance = await service.get_balance()
        user_api_key.balance = new_balance

        proxies_data = order_details.get("data", {}).get("list", [])

        for p in proxies_data:
            raw_expire = p.get("expire_time")
            new_proxy = Proxy(
                owner_id=current_user.id,
                api_key_id=user_api_key.id,
                ipfoxy_proxy_id=str(p.get("proxy_id")),
                ipfoxy_order_id=str(order_id),
                host=p.get("server"),
                port=p.get("port"),
                username=p.get("username"),
                password=p.get("password"),
                ip_type=p.get("ip_type"),
                expires_at=datetime.fromtimestamp(raw_expire) if raw_expire else None,
                area_id=str(request.area_id),
            )
            db.add(new_proxy)
            logger.debug("[PURCHASE] добавлен прокси host=%s:%s", p.get("server"), p.get("port"))

        transaction = Transaction(
            user_id=current_user.id,
            api_key_id=user_api_key.id,
            type=TransactionType.purchase,
            amount=total_cost,
            description=f"Order {order_id}: {request.num} proxies × {request.days}d",
        )
        db.add(transaction)

        await db.commit()
        logger.info(
            "[PURCHASE] user_id=%s — заказ %s создан, %d прокси",
            current_user.id, order_id, len(proxies_data),
        )
        return {"status": "success", "order_id": order_id, "count": len(proxies_data)}

    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        logger.error("[PURCHASE] user_id=%s — ошибка: %s", current_user.id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при создании заказа: {exc}")