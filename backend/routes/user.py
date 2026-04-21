import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func
from redis.asyncio import Redis

from backend.database.database import AssyncSessionLocal, get_db
from backend.database import schemas
from backend.utils.security import get_current_user
from backend.database.models import User, Regions, Proxy, Transaction, TransactionType, ApiKey
from backend.utils.config import settings
from backend.api_services.ipfoxy import IPFoxyService

router = APIRouter(prefix='/user', tags=['User'])
logger = logging.getLogger('routes.user')

CACHE_KEY_COUNTRIES = 'all_countries_cache'
CACHE_EXPIRE = 3600

@router.get('/me', response_model=schemas.UserProfileResponse)
async def get_me(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Возвращает профиль текущего пользователя.
    Если к аккаунту привязан API ключ — обновляет баланс
    """
    current_balance = Decimal('0.00')

    if user.api_key_id:
        await db.refresh(user)
        stmt = select(ApiKey).where(ApiKey.id == user.api_key_id, ApiKey.is_active.is_(True))
        res = await db.execute(stmt)
        api_key = res.scalar_one_or_none()

        if api_key:
            needs_refresh = (
                api_key.balance is None
                or api_key.last_checked is None
                or (datetime.now(timezone.now() - api_key.last_checked.replace(tzinfo=None)) > timedelta(minutes=60))
            )
            if needs_refresh:
                logger.info(f'[ME] {user.telegram_id} - Обновляем баланс key_id={api_key.api_id}')
                try:
                    service = IPFoxyService.get_service_by_key_obj(api_key)
                    real_balance = await service.get_balance()
                    api_key.balance = real_balance
                    api_key.last_checked = func.now()
                    await db.commit()
                    current_balance = real_balance
                except Exception as exc:
                    current_balance = api_key.balance or Decimal('0.00')
            else:
                current_balance = api_key.balance or Decimal('0.00')
    return {
        'first_name': user.first_name,
        'username': user.username,
        'balance': current_balance,
        'role': user.role,
        'api_key_id': user.api_key.api_id
    }

@router.get('/countries', response_model=schemas.CountriesResponse)
async def get_countries(
    last_id: Optional[int] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Пагинированный список регионов с кешированием в Redis.
    Если регионов нет в БД — запускает Celery задачу синхронизации.
    """
    async with Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_connect_timeout=2) as redis_client:
        try:
            cached_data = await redis_client.get(CACHE_KEY_COUNTRIES)
            countries = None
            
            if cached_data:
                try:
                    countries = json.loads(cached_data)
                except Exception as e:
                    logger.warning(f'[COUNTRIES] ошибка парсинга кеша: {e}')

            if not countries:
                async with AssyncSessionLocal() as inner_db:
                    stmt = select(Regions).where(Regions.status.is_(True)).order_by(asc(Regions.id))
                    result = await inner_db.execute(stmt)
                    all_objs = result.scalars().all()

                    if not all_objs:
                        try:
                            from backend.tasks.sync_tasks import sync_regions_task
                            loop = asyncio.get_running_loop()
                            await loop.run_in_executor(None, sync_regions_task.delay)
                        except Exception as e:
                            logger.error(f'[COUNTRIES] ошибка запуска задачи: {e}')
                    
                    countries = [
                        {
                            'id': c.id,
                            'area_id': c.area_id,
                            'ip_type': getattr(c, 'ip_type', 'STATIC'),
                            'ip_version': getattr(c, 'ip_version', 'IPv4'),
                            'country': c.country,
                            'country_code': c.country_code,
                            'retail_price': float(c.retail_price) if c.retail_price else 0.0,
                        }
                        for c in all_objs
                    ]
                    if countries:
                        await redis_client.setex(CACHE_KEY_COUNTRIES, CACHE_EXPIRE, json.dumps(countries))

            start_index = 0
            if last_id is not None:
                for i, country in enumerate(countries):
                    if country['id'] == last_id:
                        start_index = i + 1
                        break
                else:
                    return {"items": [], "next_cursor": None, "has_more": False}

            paginated_data = countries[start_index : start_index + limit]
            has_more = (start_index + limit) < len(countries)
            next_cursor = paginated_data[-1]['id'] if paginated_data and has_more else None

            logger.debug(f'[COUNTRIES] страница: {len(paginated_data)} элем., has_more={has_more}, next_cursor={next_cursor}')

            return {
                'items': paginated_data,
                'next_cursor': next_cursor,
                'has_more': has_more
            }

        except Exception as e:
            print(f"Критическая ошибка в get_countries: {e}")
            return {'items': [], 'next_cursor': None, 'has_more': False}
        
@router.post('/calculate-price')
async def calculate_order_price(
    order_data: OrderPriceRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    service = await IPFoxyService.get_working_service(db)

    if not service:
        raise HTTPException(status_code=503, detail='Сервис временно недоступе: нет рабочих API ключей')
    
    try:
        price = await service.get_order_price(
            order_type=order_data.order_type,
            days=order_data.days,
            area_id=order_data.area_id,
            proxy_ids=order_data.proxy_ids,
            num=order_data.num
        )

        return {
            "status": "success",
            "order_price": price,
            "currency": "USD",
            "details": {
                "days": order_data.days,
                "num": order_data.num,
                "area_id": order_data.area_id
            }
        }
    
    except Exception as e:
        print(f"Ошибка при расчете цены: {e}")
        raise HTTPException(status_code=500, detail="ОШИБКА ПРИ ОБРАЩЕНИИ К ПОСТАВЩИКУ")
    
@router.post('/purchase-proxy')
async def purchase_proxy_endpoint(
    request: ProxyPurchaseRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    service_data = await IPFoxyService.get_service_by_user(db, current_user)
    if not service_data:
        raise HTTPException(status_code=400, detail='К вашему аккаунту не привязан активный API ключ. Обратитесь к администратору')
    service, user_api_key = service_data

    price_val = await service.get_order_price(
        order_type='BUY',
        area_id=request.area_id,
        num=request.num,
        days=request.days
    )
    total_cost = Decimal(str(price_val))

    if user_api_key.balance < total_cost:
        raise HTTPException(status_code=400, detail=f'Недостаточно средств на балансе ключа {user_api_key.api_id}. Обратитесь к администратору.')
    
    try:
        order_id = await service.purchase_proxy(
            area_id=request.area_id,
            num=request.num,
            days=request.days
        )

        if not order_id:
            raise Exception("Не удалось получить Order ID от провайдера")
        
        order_details = await service.get_order_information(order_id)
        if order_details.get('code') not in [0, 200]:
            raise Exception(f"Заказ {order_id} создан, но данные не получены: {order_details.get('msg')}")
        
        new_balance = await service.get_balance()
        user_api_key.balance = new_balance

        proxies_data = order_details.get('data', {}).get('list', [])

        for p in proxies_data:
            new_proxy = Proxy(
                owner_id=current_user.id,
                api_key_id=user_api_key.id,
                ipfoxy_proxy_id=str(p.get('proxy_id')),
                ipfoxy_order_id=str(order_id),
                host=p.get('server'),
                port=p.get('port'),
                username=p.get('username'),
                password=p.get('password'),
                ip_type=p.get('ip_type'),
                expires_at=datetime.fromtimestamp(p.get('expire_time')) if p.get('expire_time') else None,
                area_id=str(request.area_id)
            )
            db.add(new_proxy)

        transaction = Transaction(
            user_id=current_user.id,
            type=TransactionType.purchase,
            amount=total_cost,
            description=f"Order {order_id}: {request.num} proxies"
        )
        db.add(transaction)

        await db.commit()
        return {"status": "success", "order_id": order_id, "count": len(proxies_data)}
    
    except Exception as e:
        await db.rollback()
        print(f"Purchase error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")