import hmac
import hashlib
import json
import logging
from urllib.parse import unquote
from fastapi import HTTPException, Header, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.utils.config import settings
from backend.database.database import get_db
from backend.database.models import User, UserRole, Whitelist

logger = logging.getLogger("security")

def verify_telegram_init_data(init_data: str) -> dict:
    """
    Проверяет подпись Telegram WebApp initData.
    Возвращает словарь user{} из initData.
    Бросает HTTPException(401) при невалидной подписи.
    """
    try:
        parsed: dict[str, str] = {}
        for part in init_data.split('&'):
            if '=' in part:
                k, v = part.split('=', 1)
                parsed[k] = unquote(v)
        
        received_hash = parsed.pop('hash', None)
        if not received_hash:
            logger.warning('[AUTH] initData не содержит hash')
            raise ValueError('No hash in init data')
        
        data_ckeck_string = '\n'.join(f'{k}={v}' for k, v in sorted(parsed.items()))
        secret_key = hmac.new(
            b'WebAppData',
            settings.BOT_TOKEN.encode(),
            hashlib.sha256, 
        ).digest()

        expected_hash = hmac.new(
            secret_key,
            data_ckeck_string.encode(),
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(expected_hash, received_hash):
            logger.warning('[AUTH] Hash mismatch - возможная подделка initData')
            raise ValueError('Hash mismatch')
        
        user_data = json.loads(parsed.get('user', '{}'))
        logger.debug(f'[AUTH] initData валидна для tg_id={user_data.get('id')}')
        return user_data
    
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f'[AUTH] Ошибка верификации initData: {exc}')
        raise HTTPException(status_code=401, detail='Invalid Telegram auth.')
    
async def get_current_user(
        request: Request,
        x_init_data: str = Header(None, alias='X-TG-Init-Data'),
        db: AsyncSession = Depends(get_db),
) -> User:
    """
    FastAPI dependency. Возвращает текущего аутентифицированного пользователя.

    Порядок проверок:
      1. Нет заголовка X-TG-Init-Data → 403 (браузер)
      2. Невалидный HMAC → 401
      3. telegram_id не в Whitelist → 403
      4. Ищем / создаём User, синхронизируем role
    """
    if not x_init_data:
        logger.info(
            '[AUTH] Запрос без X-TG-Init-Data от %s - отклонен как браузерный',
            request.client.host if request.client else 'unknown'
        )
        raise HTTPException(status_code=403, detail='ACCESS_DENIED')
    
    tg_user_data = verify_telegram_init_data(x_init_data)
    tg_id: int | None = tg_user_data.get('id')
    if not tg_id:
        logger.warning('[AUTH] initData не содержит user.id')
        raise HTTPException(status_code=401, detail='User ID missing')
    
    wl_stmt = select(Whitelist).where(Whitelist.telegram_id == tg_id)
    wl_result = await db.execute(wl_stmt)
    whitelist_entry = wl_result.scalar_one_or_none()

    if not whitelist_entry:
        logger.warning(f'[AUTH] tg_id={tg_id} не найден в Whitelist - доступ запрещен')
        raise HTTPException(status_code=403, detail='ACCES DENIED')
    
    role_from_whitelist: UserRole = whitelist_entry.role

    user_stmt = select(User).where(User.telegram_id == tg_id)
    user_result = await db.execute(user_stmt)
    user = user_result.scalar_one_or_none()

    if not user:
        logger.info(f'[AUTH] Первый вход tg_id={tg_id} username={tg_user_data.get('username', '')} role={role_from_whitelist}')
        user = User(
            telegram_id=tg_id,
            username=tg_user_data.get('username', ''),
            first_name=tg_user_data.get('first_name', ''),
            role=role_from_whitelist,
            api_key_id=whitelist_entry.pending_api_key_id,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        if whitelist_entry.pending_api_key_id:
            logger.info(f'[AUTH] tg_id={tg_id} — применён pending_api_key_id={whitelist_entry.pending_api_key_id}')
    else:
        if user.role != role_from_whitelist:
            logger.info(f'[AUTH] tg_id={tg_id} - роль изменилась {user.role} -> {role_from_whitelist} (Синхронизация с Whitelist)')
            user.role = role_from_whitelist
            await db.commit()
            await db.refresh(user)
    return user

async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """
    Dependency для admin-only endpoint'ов.
    Бросает 403 если пользователь не администратор.
    """
    if current_user.role != UserRole.admin:
        logger.warning(f'[AUTH] user_id={current_user.id} tg_id={current_user.telegram_id} попытка доступа к admin endpoint без прав')
        raise HTTPException(status_code=403, detail='DENIED')
    return current_user