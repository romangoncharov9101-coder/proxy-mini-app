import asyncio
import logging
import logging.handlers
import os
from celery import shared_task
from sqlalchemy import select
from decimal import Decimal, InvalidOperation
from sqlalchemy.sql import func
from backend.database.database import AssyncSessionLocal
from backend.database.models import Regions, ApiKey
from backend.api_services.ipfoxy import IPFoxyService

import json
from datetime import datetime, timezone
from backend.database.models import Proxy, Regions
from redis.asyncio import Redis
from backend.utils.config import settings as cfg
from backend.utils.check_location import check_proxy_country_with_ip_api

logger = logging.getLogger('celery.tasks')

def run_async(coro):
    """Запускает async coroutine в sync-контексте Celery воркера."""
    return asyncio.run(coro)


def _to_decimal(value):
    if value is None or value == '':
        return Decimal('0.00')
    try:
        return Decimal(str(value).strip().replace(',', '.'))
    except (ValueError, InvalidOperation):
        return Decimal('0.00')

def ts_to_dt(val):
    if not val:
        return None
    try:
        return datetime.fromtimestamp(int(val), tz=timezone.utc)
    except Exception:
        return None

@shared_task(name='backend.tasks.sync_tasks.sync_regions_task', bind=True, max_retries=3, default_retry_delay=60)
def sync_regions_task(self):
    """
    Синхронизирует таблицу regions из IPFoxy API.

    Логика выбора ключа:
      - Загружаем все активные ключи из БД.
      - Перебираем по одному, проверяем check_connection().
      - Используем ПЕРВЫЙ прошедший проверку ключ.
      - Регионы одинаковы для всех аккаунтов IPFoxy.
    """
    logger.info('[SYNC_REGIONS] Задача запущена')

    async def logic():
        async with AssyncSessionLocal() as db:
            stmt = select(ApiKey).where(ApiKey.is_active.is_(True)).order_by(ApiKey.id)
            result = await db.execute(stmt)
            all_keys: list[ApiKey] = result.scalars().all()

            if not all_keys:
                logger.error('[SYNC_REGIONS] В БД нет активных API ключей — задача отменена')
                return {'status': 'error', 'reason': 'no_active_keys'}

            logger.info(f'[SYNC_REGIONS] Найдено {len(all_keys)} активных ключей')

            working_service = None
            for key_obj in all_keys:
                service = IPFoxyService.get_service_by_key_obj(key_obj)
                try:
                    ok = await service.check_connection()
                    if ok:
                        working_service = service
                        logger.info(f'[SYNC_REGIONS] Используется ключ id={key_obj.api_id} name={key_obj.key_name}')
                        break
                    else:
                        logger.warning(f'[SYNC_REGIONS] Ключ id={key_obj.api_id} не прошёл check_connection — пропускаем')
                except Exception as exc:
                    logger.error(f'[SYNC_REGIONS] Ошибка проверки ключа id={key_obj.api_id}: {exc}')

            if working_service is None:
                logger.error('[SYNC_REGIONS] Ни один ключ не прошёл проверку соединения — отмена')
                return {'status': 'error', 'reason': 'no_working_key'}

            regions_data = await working_service.get_regions()
            if not regions_data:
                logger.warning('[SYNC_REGIONS] API вернул пустой список регионов')
                return {'status': 'warning', 'reason': 'empty_regions'}

            for reg in regions_data:
                raw_area_id = reg.get('id')
                area_id_str = str(raw_area_id)

                stmt_find = select(Regions).where(Regions.area_id == area_id_str)
                res_find = await db.execute(stmt_find)
                db_reg = res_find.scalar_one_or_none()

                if not db_reg:
                    db_reg = Regions(area_id=str(reg['id']))
                    db.add(db_reg)

                db_reg.ip_type = reg.get('ip_type', 'STATIC_DATACENTER')
                db_reg.list_price = _to_decimal(reg.get('list_price', 0.00))
                db_reg.ip_version = reg.get('ip_version')
                db_reg.country = reg.get('country', 'Unknown')
                db_reg.country_code = reg.get('country_code', 'XX')
                db_reg.region = reg.get('region', 'Unknown')
                db_reg.retail_price = _to_decimal(reg.get('retail_price', 0.00))

                raw_status = reg.get('status', True)
                if isinstance(raw_status, str):
                    db_reg.status = raw_status.lower() == 'true'
                else:
                    db_reg.status = bool(raw_status)

            await db.commit()
            logger.info('[SYNC_REGIONS] Завершено')
            return {'status': 'ok', 'total': len(regions_data)}

    try:
        return run_async(logic())
    except Exception as exc:
        logger.error(f'[SYNC_REGIONS] Неперехваченная ошибка: {exc}')
        raise self.retry(exc=exc)


@shared_task(name='backend.tasks.sync_tasks.sync_balances_task', bind=True, max_retries=2, default_retry_delay=30)
def sync_balances_task(self):
    """
    Обновляет balance для КАЖДОГО активного API ключа независимо.

    Важно: каждый ключ работает со своим аккаунтом IPFoxy — у каждого свой баланс.
    Ошибка одного ключа не останавливает обновление остальных.
    """
    logger.info('[SYNC_BALANCES] Задача запущена')

    async def logic():
        async with AssyncSessionLocal() as db:
            try:
                stmt = select(ApiKey).where(ApiKey.is_active.is_(True)).order_by(ApiKey.id)
                result = await db.execute(stmt)
                all_keys: list[ApiKey] = result.scalars().all()

                if not all_keys:
                    logger.warning('[SYNC_BALANCES] Нет активных ключей')
                    return {'status': 'warning', 'reason': 'no_active_keys'}

                for key_obj in all_keys:
                    try:
                        service = IPFoxyService.get_service_by_key_obj(key_obj)
                        new_balance = await service.get_balance()
                        key_obj.balance = new_balance
                        key_obj.last_checked = func.now()
                    except Exception as exc:
                        logger.error(f'[SYNC_BALANCES] key_id={key_obj.api_id} name={key_obj.key_name} — Ошибка: {exc}')

                await db.commit()
                return {'status': 'ok'}
            except Exception as e:
                await db.rollback()
                logger.error(f'[SYNC_BALANCES] Ошибка БД: {e}')
            finally:
                await db.close()

    try:
        return run_async(logic())
    except Exception as exc:
        logger.error(f'[SYNC_BALANCES] Неперехваченная ошибка: {exc}')
        raise self.retry(exc=exc)

@shared_task(name='backend.tasks.sync_tasks.sync_proxies_task', bind=True, max_retries=3, default_retry_delay=60)
def sync_proxies_task(self, api_key_db_id: int = None):
    """
    Синхронизирует прокси из IPFoxy.

    Если передан api_key_db_id — синхронизирует только этот ключ (вызов при открытии приложения).
    Если не передан — синхронизирует все активные ключи (вызов по расписанию каждые 30 мин).

    Правила:
      - page_size=50 (максимум IPFoxy), обходим все страницы до конца
      - Открываем соединение с БД только если нашли proxy_id которого ещё нет в таблице
      - Уже существующие записи НЕ трогаем — только INSERT новых
    """
    logger.info(f'[SYNC_PROXIES] Задача запущена api_key_db_id={api_key_db_id}')

    async def logic():
        # Получаем список ключей для обработки
        async with AssyncSessionLocal() as db:
            if api_key_db_id:
                stmt = select(ApiKey).where(ApiKey.id == api_key_db_id, ApiKey.is_active.is_(True))
            else:
                stmt = select(ApiKey).where(ApiKey.is_active.is_(True)).order_by(ApiKey.id)
            result = await db.execute(stmt)
            keys: list[ApiKey] = result.scalars().all()

            if not keys:
                logger.warning('[SYNC_PROXIES] Нет активных ключей для синхронизации')
                return {'status': 'warning', 'reason': 'no_active_keys'}

            # Загружаем множество уже известных ipfoxy_proxy_id из БД одним запросом
            existing_res = await db.execute(select(Proxy.ipfoxy_proxy_id))
            existing_ids: set[str] = {r for r in existing_res.scalars().all() if r}
            logger.info(f'[SYNC_PROXIES] В БД уже есть {len(existing_ids)} прокси')

            # Загружаем валидные area_id для проверки FK
            reg_res = await db.execute(select(Regions.area_id))
            valid_area_ids: set[str] = {str(r) for r in reg_res.scalars().all()}

        total_new = 0

        for key_obj in keys:
            service = IPFoxyService.get_service_by_key_obj(key_obj)
            logger.info(f'[SYNC_PROXIES] Обрабатываю ключ id={key_obj.id} name={key_obj.key_name}')

            # Собираем все прокси ключа постранично (page_size=50 — максимум IPFoxy)
            new_proxies_to_insert: list[dict] = []
            page = 1
            page_size = 50

            while True:
                try:
                    batch = await service.get_proxies_list(page=page, page_size=page_size)
                except Exception as exc:
                    logger.error(f'[SYNC_PROXIES] Ошибка получения стр.{page} ключ={key_obj.key_name}: {exc}')
                    break

                if not batch:
                    break

                logger.debug(f'[SYNC_PROXIES] key={key_obj.key_name} стр.{page}: получено {len(batch)} шт.')

                for p in batch:
                    proxy_id = str(p.get('id') or '').strip()
                    if not proxy_id:
                        continue

                    if proxy_id in existing_ids:
                        existing_proxy_res = await db.execute(
                            select(Proxy).where(Proxy.ipfoxy_proxy_id == proxy_id)
                        )
                        existing_proxy = existing_proxy_res.scalar_one_or_none()
                        if existing_proxy:
                            existing_proxy.auto_extend = bool(int(p.get('auto_extend', 0)))
                            existing_proxy.expires_at = ts_to_dt(p.get('expire_time'))
                            existing_proxy.renewal_at = ts_to_dt(p.get('renewal_time'))

                            # Заполняем геолокацию если она ещё не была проверена
                            if existing_proxy.checked_location is None:
                                geo_ip = str(existing_proxy.public_ip or existing_proxy.host or '')
                                expected_cc = str(existing_proxy.country_code or '').strip().upper() or None
                                if geo_ip:
                                    try:
                                        checked_location, location_match = await check_proxy_country_with_ip_api(
                                            ip_or_host=geo_ip,
                                            expected_country_code=expected_cc,
                                        )
                                        existing_proxy.checked_location = checked_location
                                        existing_proxy.location_match   = location_match
                                        logger.debug(f'[SYNC_PROXIES] Геолокация заполнена proxy_id={proxy_id} → {checked_location} match={location_match}')
                                    except Exception as loc_exc:
                                        logger.warning(f'[SYNC_PROXIES] Ошибка геопроверки existing ip={geo_ip}: {loc_exc}')

                            await db.commit()
                        continue

                    host = str(p.get('host') or '').strip()
                    port_raw = p.get('port')
                    try:
                        port = int(port_raw)
                    except (TypeError, ValueError):
                        continue  # нет порта — пропускаем

                    if not host or not port:
                        continue

                    area_id = str(p.get('area_id') or '').strip()
                    if not area_id or area_id not in valid_area_ids:
                        logger.warning(f'[SYNC_PROXIES] area_id={area_id} не в БД, пропускаю proxy_id={proxy_id}')
                        continue

                    # Проверяем геолокацию нового прокси (как при покупке)
                    public_ip_for_check = str(p.get('public_ip') or host)
                    expected_cc = str(p.get('country_code') or '').strip().upper() or None
                    try:
                        checked_location, location_match = await check_proxy_country_with_ip_api(
                            ip_or_host=public_ip_for_check,
                            expected_country_code=expected_cc,
                        )
                    except Exception as loc_exc:
                        logger.warning(f'[SYNC_PROXIES] Ошибка геопроверки ip={public_ip_for_check}: {loc_exc}')
                        checked_location, location_match = None, None

                    new_proxies_to_insert.append({
                        'ipfoxy_proxy_id':  proxy_id,
                        'host':             host,
                        'public_ip':        public_ip_for_check,
                        'port':             port,
                        'type':             str(p.get('type') or 'http'),
                        'username':         str(p.get('user') or ''),
                        'password':         str(p.get('password') or ''),
                        'auto_extend':      bool(int(p.get('auto_extend', 0))),
                        'ip_type':          str(p.get('ip_type') or ''),
                        'ip_version':       str(p.get('ip_version') or 'IPv4'),
                        'country_code':     str(p.get('country_code') or ''),
                        'area_id':          area_id,
                        'api_key_id':       key_obj.id,
                        'expires_at':       ts_to_dt(p.get('expire_time')),
                        'purchased_at':     ts_to_dt(p.get('buy_time')),
                        'renewal_at':       ts_to_dt(p.get('renewal_time')),
                        'is_active':        True,
                        'checked_location': checked_location,
                        'location_match':   location_match,
                    })
                    existing_ids.add(proxy_id)

                if len(batch) < page_size:
                    break  # последняя страница
                page += 1

            logger.info(f'[SYNC_PROXIES] key={key_obj.key_name}: новых прокси для вставки — {len(new_proxies_to_insert)}')

            # Открываем БД только если есть что вставить
            if new_proxies_to_insert:
                async with AssyncSessionLocal() as db:
                    for data in new_proxies_to_insert:
                        db.add(Proxy(**data))
                    await db.commit()
                    logger.info(f'[SYNC_PROXIES] key={key_obj.key_name}: вставлено {len(new_proxies_to_insert)} прокси')
                total_new += len(new_proxies_to_insert)

        logger.info(f'[SYNC_PROXIES] Завершено. Всего новых: {total_new}')

        # Инвалидируем Redis-кеш только если что-то добавили
        if total_new > 0:
            try:
                async with Redis.from_url(
                    cfg.REDIS_URL, decode_responses=True,
                    socket_connect_timeout=2, socket_timeout=2,
                ) as redis:
                    # Удаляем кеш всех пользователей и администратора
                    keys_to_del = await redis.keys('user:proxies:*')
                    keys_to_del.append('admin:proxies:all')
                    if keys_to_del:
                        await redis.delete(*keys_to_del)
                    logger.info(f'[SYNC_PROXIES] Redis-кеш инвалидирован: {len(keys_to_del)} ключей')
            except Exception as exc:
                logger.warning(f'[SYNC_PROXIES] Не удалось инвалидировать Redis-кеш: {exc}')

        return {'status': 'ok', 'new': total_new}

    try:
        return run_async(logic())
    except Exception as exc:
        logger.error(f'[SYNC_PROXIES] Неперехваченная ошибка: {exc}', exc_info=True)
        raise self.retry(exc=exc)