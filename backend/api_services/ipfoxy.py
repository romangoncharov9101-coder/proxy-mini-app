import time
import httpx
import logging
from typing import Optional, Any
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from backend.database.models import ApiKey, Proxy, User
from backend.utils.config import settings
from backend.utils.crypto import decrypt_data

logger = logging.getLogger('ipfoxy.service')

def _safe_decimal(value: Any, fallback: str = '0.00') -> Decimal:
    """Конвертирует любое значение в Decimal без исключений."""
    if value is None or value == '':
        return Decimal(fallback)
    try:
        return Decimal(str(value).strip().replace(',', '.'))
    except (ValueError, InvalidOperation):
        logger.warning(f'Не удалось конвертировать {value} в Decimal, fallback={fallback}')
        return Decimal(fallback)

class IPFoxyService:
    """
    Клиент для IPFoxy Open API.
    Stateless — не хранит состояние между вызовами.
    """
    def __init__(self, api_token: str, api_id: str, key_name: Optional[str] = None):
        self.api_token = api_token
        self.api_id = api_id
        self.key_name = key_name 
        self.base_url = settings.IPFOXY_API_BASE

    @classmethod
    async def get_service_by_user(cls, db: AsyncSession, user: User) -> Optional[tuple['IPFoxyService', ApiKey]]:
        """
        Возвращает (IPFoxyService, ApiKey) по привязанному к пользователю ключу.
        Возвращает None если ключ не задан или неактивен.
        """
        if not user.api_key_id:
            logger.warning(f'[KEY_LOOKUP] user_id={user.id} tg_id={user.telegram_id}, api_key_id не задан')
            return None
        
        stmt = select(ApiKey).where(ApiKey.id == user.api_key_id, ApiKey.is_active.is_(True))
        result = await db.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            logger.warning(f'[KEY_LOOKUP] user_id={user.id}, - ключ api_key_id={user.api_key_id} ключ не найден или не активен.')
            return None
        
        logger.debug(f'[KEY_LOOKUP] user_id={user.id} - используется ключ id={api_key.id}, name={api_key.key_name}')
        return cls._from_key_obj(api_key), api_key
    
    @classmethod
    async def get_service_by_key_id(cls, db: AsyncSession, api_id: int) -> Optional[tuple['IPFoxyService', ApiKey]]:
        """
        Возвращает (IPFoxyService, ApiKey) по id ключа.
        Используется в Celery воркерах.
        """
        stmt = select(ApiKey).where(ApiKey.id == api_id, ApiKey.is_active.is_(True))
        result = await db.execute(stmt)
        api_key = result.scalar_one_or_none()

        if not api_key:
            logger.warning(f'[KEY_LOOKUP] key_id={api_id} - не найден или не активен')
            return None
        
        logger.debug(f'[KEY_LOOKUP] key_id={api_id}, name={api_key.key_name} - OK')
        return cls._from_key_obj(api_key), api_key
    
    @classmethod
    def get_service_by_key_obj(cls, api_key: ApiKey) -> 'IPFoxyService':
        """Создает сервис из уже загруженного объекта ApiKey (без доп. запроса к бд)"""
        return cls._from_key_obj(api_key)
    
    @classmethod
    def _from_key_obj(cls, api_key: ApiKey) -> 'IPFoxyService':
        return cls(
            api_token=decrypt_data(api_key.key),
            api_id=api_key.api_id,
            key_name=api_key.key_name
        )
    
    async def check_connection(self) -> bool:
        """Проверят что ключ валидный и API отвечает. Используется в Celery"""
        logger.debug(f'[CHECK] key_name={self.key_name} - проверка соединения')
        try:
            data = await self._make_request('GET', '/ip/open-api/account-info')
            ok = data.get('code') in (0, 200) and 'data' in data
            if ok:
                logger.info(f'[CHECK] key_name={self.key_name} - соединение ОК')
            else:
                logger.warning(f"[CHECK] key_name={self.key_name} - ответ API: code={data.get('code')}")
            return ok
        except Exception as exc:
            logger.error(f'[CHECK] key_name={self.key_name} - исключение: {exc}')
            return False
        
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None
    ) -> dict:

        url = f"{self.base_url}{endpoint}"

        headers = {
            "api-token": self.api_token,
            "api-id": self.api_id,
        }

        async with httpx.AsyncClient(timeout=20) as client:
            try:
                if method.lower() == "get":
                    response = await client.get(
                        url,
                        headers=headers,
                        params=params
                    )

                else:
                    payload = {
                        k: str(v)
                        for k, v in (params or {}).items()
                        if v is not None
                    }

                    response = await client.post(
                        url,
                        headers=headers,
                        data=payload
                    )

                response.raise_for_status()
                return response.json()

            except Exception as exc:
                logger.error(f"IPFoxy error: {exc}")
                return {"code": 500, "msg": str(exc), "data": {}}
            
    async def get_balance(self) -> Decimal:
        """ Получить баланс аккаунта IPFoxy"""
        data = await self._make_request('GET', '/ip/open-api/account-info')
        balance = data.get('data', {}).get('total_balance', '0.00')
        logger.info(f'[BALANCE] key_name={self.key_name} - {Decimal(str(balance))} USD')
        return Decimal(str(balance))
    
    async def get_proxies_list(self, page: int = 1, page_size: int = 20, proxy_ids: str = None) -> list[dict]:
        """ Получить список всех прокси связанных с АПИ ключами """
        if isinstance(proxy_ids, list):
            proxy_ids = ','.join(str(pid) for pid in proxy_ids)

        data = await self._make_request(
            'GET',
            '/ip/open-api/proxy-list',
            params={'page': page, 'page_size': page_size, 'proxy_ids': proxy_ids}
        )
        proxies_list = data.get('data', {}).get('list', [])
        logger.info(f'[PROXY_LIST] key_name={self.key_name} — {len(list)} прокси (стр. {page})')
        return proxies_list
    
    async def renew_proxy(self, proxy_ids: str, days: int = 30) -> dict:
        """ Продлить купленный ранее прокси """
        return await self._make_request(
            'POST',
            '/ip/open-api/proxy-extend',
            params={'days': days, 'proxy_ids': proxy_ids}
        )
    
    async def purchase_proxy(self, area_id: int, num: int, days: int = 30, auto_extend: int = 0) -> Optional[str]:
        """ Метод на покупку прокси """
        payload = {
            "days": int(days),
            "area_id": int(area_id),
            "auto_extend": int(auto_extend),
            "num": int(num),
        }
        data = await self._make_request(
            'POST',
            '/ip/open-api/proxy-buy',
            params=payload
        )
        order_id = data.get('data', {}).get('order_id')
        if order_id:
            logger.info(f'[PURCHASE] key_name={self.key_name} — order_id={order_id}')
        else:
            logger.error(f'[PURCHASE] key_name={self.key_name} — order_id не получен. Ответ: {data}')
        return order_id
    
    async def get_order_information(self, order_id) -> dict:
        """ Получить информацию по одному СУЩЕСТВУЮЩЕМУ заказу """
        return await self._make_request(
            'GET',
            '/ip/open-api/order-info',
            params={'order_id': order_id}
        )
    
    async def get_order_price(self, order_type: str, area_id: int = None, proxy_ids: list[str] = None, num: int = None, days: int = 30) -> Decimal:
        """
        Получить стоимость заказа перед покупкой.
        ВАЖНО: IPFoxy возвращает ключ 'order price' (с пробелом).
        """
        if proxy_ids and isinstance(proxy_ids, list):
            proxy_ids = ','.join(str(pid) for pid in proxy_ids)

        data = await self._make_request(
            'GET',
            '/ip/open-api/order-price',
            params={
                'order_type': order_type,
                'days': days,
                'area_id': area_id,
                'proxy_ids': proxy_ids,
                'num': num
            }
        )
        raw_price = data.get('data', {}).get('order price', 0)
        price = _safe_decimal(raw_price)
        logger.info(f'[ORDER_PRICE] key_name={self.key_name} - {price} USD (type={order_type} area_id={area_id} num={num} days={days})')
        return price

    async def get_regions(self) -> list[dict]:
        """
        Получить список всех доступных регионов.
        Возвращает список dict с полями: area_id, ip_type, ip_version,
        country, country_code, region, list_price, retail_price, status.
        """
        data = await self._make_request(
            'GET',
            '/ip/open-api/area-list',
        )
        regions: list = data.get('data', [])
        logger.info(f'[REGIONS] key_name={self.key_name} - {len(regions)} регионов')
        return regions