import asyncio
import logging
from celery import shared_task
from sqlalchemy import select
from decimal import Decimal, InvalidOperation
from sqlalchemy.sql import func
from backend.database.database import AssyncSessionLocal
from backend.database.models import Regions, ApiKey
from backend.api_services.ipfoxy import IPFoxyService

logger = logging.getLogger('celery.tasks')

def run_async(coro):
    """Запускает async coroutine в sync-контексте Celery воркера."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result()
    else:
        return loop.run_until_complete(coro)

def _to_decimal(value):
    if value is None or value == "":
        return Decimal("0.00")
    try:
        return Decimal(str(value).strip().replace(',', '.'))
    except (ValueError, InvalidOperation):
        return Decimal("0.00")

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
                logger.eeror('[SYNC_REGIONS] В БД нет активных API ключей - задача отменена')
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
            
            regions_data = await service.get_regions()
            if not regions_data:
                logger.warning("[SYNC_REGIONS] API вернул пустой список регионов")
                return {"status": "warning", "reason": "empty_regions"}
            
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
                db_reg.ip_version = reg.get('ip_version', 'IPv4')
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
            logger.info("[SYNC_REGIONS] Завершено")
            return {
                'status': 'ok',
                'total': len(regions_data)
            }
    try:
        return run_async(logic())
    except Exception as exc:
        raise self.retry(exc=exc)
    
@shared_task(name='backend.tasks.sync_tasks.sync_balances_task', bind=True, max_retries=2, default_retry_delay=30)
def sync_balances_task(self):
    """
    Обновляет balance для КАЖДОГО активного API ключа независимо.

    Важно: каждый ключ работает со своим аккаунтом IPFoxy — у каждого свой баланс.
    Ошибка одного ключа не останавливает обновление остальных.
    """
    logger.info("[SYNC_BALANCES] Задача запущена")

    async def logic():
        async with AssyncSessionLocal() as db:
            stmt = select(ApiKey).where(ApiKey.is_active.is_(True)).order_by(ApiKey)
            result = await db.execute(stmt)
            all_keys: list[ApiKey] = result.scalars().all()

            if not all_keys:
                logger.warning("[SYNC_BALANCES] Нет активных ключей")
                return {"status": "warning", "reason": "no_active_keys"}
            
            for key_obj in all_keys:
                try:
                    service = IPFoxyService.get_service_by_key_obj(key_obj)
                    new_balance = await service.get_balance()

                    old_balance = key_obj.balance
                    key_obj.balance = new_balance
                    key_obj.last_checked = func.now()

                except Exception as exc:
                    logger.error(f'[SYNC_BALANCES] key_id={key_obj.api_id} name={key_obj.key_name} - Ошибка: {exc}')
            
            await db.commit()
            return {'status': 'ok'}
    try:
        return run_async(logic())
    except Exception as exc:
        raise self.retry(exc=exc)