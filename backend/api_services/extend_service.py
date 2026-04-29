import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.models import Proxy, Transaction, TransactionType, User, UserRole, ApiKey
from backend.api_services.ipfoxy import IPFoxyService

logger = logging.getLogger('services.extend')

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

    # Строим запрос в зависимости от роли
    if current_user.role == UserRole.admin:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids))
    else:
        stmt = select(Proxy).where(
            Proxy.id.in_(proxy_ids),
            Proxy.owner_id == current_user.id,
        )

    # Выполняем запрос ВСЕГДА (не только для обычных юзеров)
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

    ipfoxy_ids = [p.ipfoxy_proxy_id for p in proxies if p.ipfoxy_proxy_id]
    if not ipfoxy_ids:
        raise HTTPException(status_code=400, detail='У выбранных прокси нет внешних ID для продления')

    proxy_ids_str = ",".join(str(pid) for pid in ipfoxy_ids)

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

    # Обновляем баланс: сначала пробуем получить реальный, иначе вычитаем
    try:
        new_balance = await service.get_balance()
        user_api_key.balance = new_balance
    except Exception as exc:
        logger.warning(f'[EXTEND] не удалось обновить баланс: {exc}')
        user_api_key.balance = current_balance - total_cost

    # Безопасно извлекаем order_id из ответа renew_proxy
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

async def resolve_ipfoxy_ids(
        db: AsyncSession,
        current_user: User,
        proxy_ids: list[int]
) -> list[str]:
    if current_user.role == UserRole.admin:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids))
    else:
        stmt = select(Proxy).where(Proxy.id.in_(proxy_ids), Proxy.owner_id == current_user.id)

    result = await db.execute(stmt)
    proxies = result.scalars().all()

    ipfoxy_ids = [p.ipfoxy_proxy_id for p in proxies if p.ipfoxy_proxy_id]
    if not ipfoxy_ids:
        raise HTTPException(status_code=400, detail='Не удалось найти внешние ID для выбранных прокси.')
    return ipfoxy_ids
