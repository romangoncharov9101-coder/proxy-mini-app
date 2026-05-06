import logging
import asyncio
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone
from sqlalchemy.orm import aliased
from sqlalchemy import or_

from backend.database.database import get_db
from backend.database import schemas
from backend.database.models import (
    User, UserRole, ApiKey, Proxy, Transaction, Whitelist
)
from backend.utils.security import require_admin
from backend.utils.crypto import encrypt_data
from backend.api_services.ipfoxy import IPFoxyService
from backend.api_services.extend_service import extend_proxies_service
from backend.api_services.proxy_service import (
    get_proxy_page,
    get_proxy_detail_for_admin
)

router = APIRouter(prefix='/admin', tags=['Admin'])
logger = logging.getLogger('routes.admin')

@router.get('/keys', response_model=list[schemas.ApiKeyResponse])
async def get_api_keys(
    search: Optional[str] = Query(None, description="Поиск по api_id или key_name"),
    is_active: Optional[bool] = Query(None, description="Фильтр: True - активные, False - выключенные"),
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Список всех API ключей, новые первые.
    Поиск: по названию ключа, и по api_id.
    Фильтрация: Выбрать активные ключи или деактивированные.
    '''
    stmt = select(ApiKey).order_by(ApiKey.created_at.desc())

    if search:
        stmt = stmt.where(
            (ApiKey.key_name.ilike(f'%{search}%')) | (ApiKey.api_id.ilike(f'%{search}%'))
        )

    if is_active is not None:
        stmt = stmt.where(ApiKey.is_active == is_active)
    
    result = await db.execute(stmt)
    keys = result.scalars().all()

    async def _fetch_balance(key: ApiKey) -> None:
        try:
            service = IPFoxyService.get_service_by_key_obj(key)
            key.balance = await service.get_balance()
            key.last_checked = datetime.now(timezone.utc)
        except Exception as exc:
            logger.warning(f'[KEYS] не удалось получить баланс key_id={key.api_id} name={key.key_name}: {exc}')

    await asyncio.gather(*(_fetch_balance(k) for k in keys))

    try:
        await db.commit()
    except Exception as exc:
        logger.error(f'[KEYS] ошибка сохранения баланса: {exc}')

    return keys

@router.post('/keys', response_model=schemas.ApiKeyResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    key_data: schemas.ApiKeyCreate,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''Создать новый API ключ. Секрет шифруется через Fernet перед сохранением.'''
    new_key = ApiKey(
        key=encrypt_data(key_data.key),
        api_id=key_data.api_id,
        key_name=key_data.key_name,
        balance=0,
        last_checked=None
    )

    try:
        service = IPFoxyService.get_service_by_key_obj(new_key)
        is_ok = await service.check_connection()
        if not is_ok:
            raise HTTPException(status_code=400, detail='Неверный API ID / TOKEN либо API недоступен')
        balance = await service.get_balance()
        new_key.balance = balance
        new_key.last_checked = datetime.now(timezone.utc)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f'[ADMIN_KEYS] ошибка проверки ключа api_id={key_data.api_id}: {exc}')
        raise HTTPException(status_code=400, detail='Не удалось проверить ключ через IPFoxy API')
    
    db.add(new_key)

    try:
        await db.commit()
        await db.refresh(new_key)
        return new_key
    except Exception as exc:
        await db.rollback()
        logger.error(f'[ADMIN_KEYS] ошибка создания ключа: {exc}', exc)
        raise HTTPException(status_code=400, detail='Ошибка базы данных при создании ключа.')

@router.get('/keys/{key_id}/stats', response_model=schemas.ApiKeyStatsResponse)
async def get_api_key_stats(
    key_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Расширенная карточка ключа:
    — баланс (актуальный запрос к IPFoxy)
    — количество прокси купленных по этому ключу
    — количество пользователей привязанных к ключу
    '''
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    api_key = result.scalar_one_or_none()
    if not api_key:
        raise HTTPException(status_code=404, detail='Ключ не найден')

    current_balance = api_key.balance
    try:
        service = IPFoxyService.get_service_by_key_obj(api_key)
        balance = await service.get_balance()
        api_key.balance = balance
        await db.commit()
    except Exception as exc:
        logger.error(f'[ADMIN_KEY_STATS] key_id={api_key.api_id} ошибка обновления баланса: {exc}')

    proxy_count_res = await db.execute(
        select(func.count()).where(Proxy.api_key_id == key_id)
    )
    proxy_count = proxy_count_res.scalar() or 0

    user_count_res = await db.execute(
        select(func.count()).where(User.api_key_id == key_id)
    )
    user_count = user_count_res.scalar() or 0

    return {
        "id": api_key.id,
        "api_id": api_key.api_id,
        "key_name": api_key.key_name,
        "is_active": bool(api_key.is_active),
        "balance": current_balance,
        "last_checked": api_key.last_checked,
        "proxy_count": proxy_count,
        "user_count": user_count,
    }

@router.patch('/keys/{key_id}', response_model=schemas.ApiKeyResponse)
async def update_api_key(
    key_id: int,
    updated_data: schemas.ApiKeyUpdate,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''Изменить название, секреты или статус ключа.'''
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    db_key = result.scalar_one_or_none()
    if not db_key:
        raise HTTPException(status_code=404, detail='Ключ не найден')

    payload = updated_data.model_dump(exclude_unset=True)
    for field, value in payload.items():
        if field == 'key' and value:
            setattr(db_key, field, encrypt_data(value))
            logger.debug(f'[ADMIN_KEYS] key_id={db_key.api_id} — секрет перешифрован')
        else:
            setattr(db_key, field, value)

    try:
        await db.commit()
        await db.refresh(db_key)
        logger.info(f'[ADMIN_KEYS] ключ id={db_key.api_id} обновлён: {list(payload.keys())}')
        return db_key
    except Exception as exc:
        await db.rollback()
        logger.error('[ADMIN_KEYS] ошибка обновления key_id=%s: %s', key_id, exc)
        raise HTTPException(status_code=400, detail='Ошибка при обновлении ключа.')

@router.delete('/keys/{key_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    key_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Удалить API ключ. Пользователи с этим ключом останутся, но потеряют привязку
    (api_key_id → NULL через FK SET NULL-like поведение на уровне Python).
    '''
    logger.info('[ADMIN_KEYS] admin_id=%s — удаляет ключ id=%s', admin.id, key_id)

    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    db_key = result.scalar_one_or_none()
    if not db_key:
        raise HTTPException(status_code=404, detail='Ключ не найден')

    users_result = await db.execute(select(User).where(User.api_key_id == key_id))
    affected_users = users_result.scalars().all()
    for u in affected_users:
        u.api_key_id = None
        logger.debug('[ADMIN_KEYS] user_id=%s отвязан от ключа id=%s', u.id, key_id)

    try:
        await db.delete(db_key)
        await db.commit()
        logger.info('[ADMIN_KEYS] ключ id=%s удалён, затронуто пользователей: %d', key_id, len(affected_users))
    except Exception as exc:
        await db.rollback()
        logger.error('[ADMIN_KEYS] ошибка удаления key_id=%s: %s', key_id, exc)
        raise HTTPException(status_code=400, detail='Ошибка при удалении ключа.')

@router.get('/users', response_model=list[schemas.UserListItem])
async def get_users(
    last_id:  Optional[int] = Query(None),
    limit:    int = Query(30, ge=1, le=100),
    key_id:   Optional[int] = Query(None),
    search: Optional[str] = Query(None, description='Поиск по TG ID, username и first_name'),
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Список всех пользователей с cursor-пагинацией.
    Опционально: фильтр по привязанному ключу (key_id).
    Включает название ключа (join).
    Поиск: по TG ID, username и first_name.
    Фильтрация: По API ключам.
    '''

    stmt = select(User, ApiKey.key_name).outerjoin(ApiKey, User.api_key_id == ApiKey.id)

    if last_id:
        stmt = stmt.where(User.id > last_id)

    if search:
        stmt = stmt.where(
            or_(
                User.telegram_id == search if search.isdigit() else False,
                User.username.ilike(f"%{search}%"),
                User.first_name.ilike(f"%{search}%")
            )
        )
    if key_id is not None:
        stmt = stmt.where(User.api_key_id == key_id)

    stmt = stmt.order_by(User.id.desc())
    stmt = stmt.limit(limit + 1)
    result = await db.execute(stmt)
    rows = result.all()

    has_more = len(rows) > limit
    raw_items = rows[:limit]
    next_cursor = raw_items[-1][0].id if (has_more and raw_items) else None

    items = []
    for user_obj, key_name in rows:
        items.append(schemas.UserListItem(
            id=user_obj.id,
            telegram_id=user_obj.telegram_id,
            username=user_obj.username,
            first_name=user_obj.first_name,
            role=user_obj.role,
            api_key_id=user_obj.api_key_id,
            api_key_name=key_name,
            created_at=user_obj.created_at,
        ))

    return items

@router.post('/whitelist', response_model=schemas.WhitelistResponse, status_code=status.HTTP_201_CREATED)
async def add_to_whitelist(
    data: schemas.WhitelistAddRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Добавить telegram_id в whitelist → открыть доступ к приложению.
    Если уже есть — возвращает 409.
    '''
    exists_stmt = select(Whitelist).where(Whitelist.telegram_id == data.telegram_id)
    exists_res = await db.execute(exists_stmt)
    if exists_res.scalar_one_or_none():
        raise HTTPException(status_code=409, detail='Пользователь уже в белом списке.')

    entry = Whitelist(
        telegram_id=data.telegram_id,
        role=data.role,
        added_by=admin.telegram_id,
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)

    return entry

@router.delete('/users/{user_id}/block', status_code=status.HTTP_204_NO_CONTENT)
async def block_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    user_result = await db.execute(select(User).where(User.id == user_id))
    target_user = user_result.scalar_one_or_none()
    
    if not target_user:
        raise HTTPException(status_code=404, detail='Пользователь не найден')

    if target_user.role == UserRole.admin and target_user.id != admin.id:
        raise HTTPException(status_code=403, detail='Нельзя удалить другого администратора.')

    wl_result = await db.execute(
        select(Whitelist).where(Whitelist.telegram_id == target_user.telegram_id)
    )
    wl_entry = wl_result.scalars().first()

    if wl_entry:
        await db.delete(wl_entry)
        logger.info('Запись удалена из whitelist для tg_id=%s', target_user.telegram_id)

    await db.delete(target_user)
    await db.commit()
    
    return None

@router.post('/users/assign-key', status_code=status.HTTP_200_OK)
async def assign_key_to_users(
    data: schemas.AssignKeyRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Назначить API ключ одному или нескольким пользователям.
    Принимает список user_id (internal id, не telegram_id).
    '''
    key_result = await db.execute(
        select(ApiKey).where(ApiKey.id == data.key_id, ApiKey.is_active.is_(True))
    )
    api_key = key_result.scalar_one_or_none()
    if not api_key:
        raise HTTPException(status_code=404, detail='API ключ не найден или неактивен.')

    users_result = await db.execute(select(User).where(User.id.in_(data.user_ids)))
    target_users = users_result.scalars().all()

    if not target_users:
        raise HTTPException(status_code=404, detail='Пользователи не найдены.')

    for u in target_users:
        old_key = u.api_key_id
        u.api_key_id = data.key_id

    await db.commit()

    logger.info(
        '[ADMIN_ASSIGN] ключ id=%s назначен %d пользователям',
        data.key_id, len(target_users),
    )
    return {
        'status':   'ok',
        'assigned': len(target_users),
        'key_id':   data.key_id,
        'key_name': api_key.key_name,
    }

@router.get('/proxies', response_model=schemas.ProxyPageResponse)
async def get_all_proxies(
    last_id:      Optional[int] = Query(None),
    limit:        int           = Query(20, ge=1, le=50),
    key_id:       Optional[int] = Query(None, description='фильтр по api_key_id'),
    owner_id:     Optional[int] = Query(None, description='фильтр по user_id владельца'),
    search:       Optional[str] = Query(None, description='Поиск по host, proxy_id, order_id, username/tg_id владельца, key_name/api_id ключа'),
    proxy_status: Optional[str] = Query(None, description='active | inactive | expired | all'),
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Все прокси системы с cursor-пагинацией.
    Фильтры: по ключу, по владельцу, по статусу (active/inactive/expired/all).
    Поиск: по host/proxy_id/order_id прокси, username/tg_id владельца, key_name/api_id ключа.
    Сортировка: новые первые.
    '''
    return await get_proxy_page(
        db,
        last_id=last_id,
        limit=limit,
        search=search,
        key_id=key_id,
        filter_owner_id=owner_id,
        proxy_status=proxy_status if proxy_status != 'all' else None,
    )

@router.get('/proxies/{proxy_id}', response_model=schemas.ProxyDetail)
async def get_proxy_detail_admin(
    proxy_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Детальная карточка прокси для администратора.
    Включает данные о владельце и о ключе.
    '''
    return await get_proxy_detail_for_admin(db, proxy_id)

@router.patch('/proxies/{proxy_id}/active')
async def set_proxy_active_admin(
    proxy_id: int,
    data: schemas.ProxyActiveRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin)
):
    """
    Активировать / деактивирвоать выбранный прокси.
    Доступно только администратору при просмотре карточки с детальной информацией о прокси.
    """
    result = await db.execute(
        select(Proxy).where(Proxy.id == proxy_id)
    )
    proxy = result.scalar_one_or_none()

    if not proxy:
        raise HTTPException(status_code=404, detail='Прокси не найден')

    proxy.is_active = data.is_active

    try:
        await db.commit()
        await db.refresh(proxy)

        return {
            'status': 'success',
            'proxy_id': proxy.id,
            'is_active': proxy.is_active,
        }

    except Exception as exc:
        await db.rollback()
        logger.error(
            '[ADMIN_PROXY_ACTIVE] ошибка proxy_id=%s: %s',
            proxy_id, exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail='Ошибка при изменении статуса прокси',
        )

@router.delete('/proxies/{proxy_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_proxy_admin(
    proxy_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Удалить прокси из базы данных.
    Используется администратором для очистки устаревших или некорректных записей.
    '''
    result = await db.execute(select(Proxy).where(Proxy.id == proxy_id))
    proxy = result.scalar_one_or_none()

    if not proxy:
        raise HTTPException(status_code=404, detail='Прокси не найден')

    try:
        await db.delete(proxy)
        await db.commit()
        logger.info('[ADMIN_DELETE_PROXY] proxy_id=%s удалён', proxy_id)
    except Exception as exc:
        await db.rollback()
        logger.error('[ADMIN_DELETE_PROXY] ошибка proxy_id=%s: %s', proxy_id, exc)
        raise HTTPException(status_code=500, detail='Ошибка при удалении прокси')

@router.post('/proxies/extend')
async def extend_proxies_admin(
    request: schemas.ExtendProxyRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin)
):
    return await extend_proxies_service(
        db=db,
        current_user=current_user,
        proxy_ids=request.proxy_ids,
        days=request.days
    )

@router.get('/transactions', response_model=schemas.TransactionPageResponse)
async def get_all_transactions(
    last_id:  Optional[int] = Query(None),
    limit:    int           = Query(20, ge=1, le=50),
    key_id:   Optional[int] = Query(None, description='фильтр по api_key_id'),
    user_id_filter: Optional[int] = Query(None, alias='user_id'),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(require_admin),
):
    '''
    Все транзакции системы с cursor-пагинацией.
    Включает telegram_id пользователя и название ключа.
    Сортировка: новые первые.
    '''
    UserAlias = aliased(User)

    stmt = (
        select(Transaction, User.telegram_id, ApiKey.key_name)
        .outerjoin(User, Transaction.user_id == User.id)
        .outerjoin(ApiKey, Transaction.api_key_id == ApiKey.id)
    )

    if date_from:
        stmt = stmt.where(Transaction.created_at >= date_from)
    if date_to:
        stmt = stmt.where(Transaction.created_at <= date_to)

    if last_id:
        stmt = stmt.where(Transaction.id < last_id)

    if key_id:
        stmt = stmt.where(Transaction.api_key_id == key_id)

    if user_id_filter:
        stmt = stmt.where(User.telegram_id == user_id_filter)

    stmt = stmt.order_by(Transaction.id.desc())

    stmt = stmt.limit(limit + 1)
    result = await db.execute(stmt)
    rows = result.all()

    has_more = len(rows) > limit
    raw_items = rows[:limit]
    next_cursor = raw_items[-1][0].id if (has_more and raw_items) else None

    items = [
        schemas.TransactionItem(
            id=tx.id,
            type=tx.type,
            amount=tx.amount,
            description=tx.description,
            created_at=tx.created_at,
            user_tg_id=tg_id,
            api_key_name=key_name,
        )
        for tx, tg_id, key_name in raw_items
    ]

    logger.debug('[ADMIN_TX] возвращено %d транзакций, has_more=%s', len(items), has_more)
    return {'items': items, 'next_cursor': next_cursor, 'has_more': has_more}