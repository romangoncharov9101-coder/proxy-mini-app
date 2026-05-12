import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx
from celery import shared_task
from sqlalchemy import select, delete

from backend.database.database import AssyncSessionLocal
from backend.database.models import Proxy, User, Notifications, ApiKey
from backend.api_services.ipfoxy import IPFoxyService
from backend.utils.config import settings

logger = logging.getLogger('celery.tasks')

WARN_BEFORE_DAYS = 3
IPFOXY_GRACE_DAYS = 2
DEACTIVATE_AFTER_GRACE_DAYS = IPFOXY_GRACE_DAYS
REMIND_AFTER_EXPIRY_DAYS = IPFOXY_GRACE_DAYS
AUTO_RENEW_BEFORE_DAYS = 7
DELETE_NOTIFICATION_AFTER_HOURS = 48
EDIT_THRESHOLD_HOURS = 24

def run_async(coro):
    return asyncio.run(coro)
    
async def _tg_send_message(telegram_id: int, text: str) -> int | None:
    url = f'https://api.telegram.org/bot{settings.BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': telegram_id,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json=payload)
            data = r.json()
            if data.get('ok'):
                return data['result']['message_id']
            logger.warning(f'[TG_SEND] tg_id={telegram_id} ответ API: {data}')
    except Exception as exc:
        logger.error(f'[TG_SEND] tg_id={telegram_id} ошибка: {exc}')
    return None

async def _tg_edit_message(telegram_id: int, message_id: int, text: str) -> bool:
    url = f'https://api.telegram.org/bot{settings.BOT_TOKEN}/editMessageText'
    payload = {
        'chat_id': telegram_id,
        'message_id': message_id,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json=payload)
            data = r.json()
            if data.get('ok'):
                return True
            err = data.get('description', '')
            logger.warning(f'[TG_EDIT] tg_id={telegram_id} msg_id={message_id} ответ: {err}')
    except Exception as exc:
        logger.error(f'[TG_EDIT] tg_id={telegram_id} шибка: {exc}')
    return False

async def _tg_delete_message(telegram_id: int, message_id: int) -> bool:
    url = f'https://api.telegram.org/bot{settings.BOT_TOKEN}/deleteMessage'
    payload = {'chat_id': telegram_id, 'message_id': message_id}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(url, json=payload)
            return r.json().get('ok', False)
    except Exception as exc:
        logger.error(f'[TG_DELETE] tg_id={telegram_id} msg_id={message_id} ошибка: {exc}')
    return False

def _fmt_dt(dt: datetime | None) -> str:
    if not dt:
        return '-'
    dt_local = dt.astimezone(timezone.utc)
    return dt_local.strftime("%d.%m.%Y %H:%M UTC")

def _build_notification_text(proxies: list[Proxy], is_admin: bool = False) -> str:
    now = datetime.now(timezone.utc)
    lines = ['<b>ПРОКСИ ТРЕБУЮТ ВНИМАНИЯ</b>\n']
    for p in proxies:
        expires = p.expires_at
        if expires:
            expires_aware = expires if expires.tzinfo else expires.replace(tzinfo=timezone.utc)
            diff = expires_aware - now
            if diff.total_seconds() < 0:
                status_str = f'ИСТЁК {abs(int(diff.total_seconds() // 3600))} ч. назад'
            else:
                hours_left = int(diff.total_seconds() // 3600)
                status_str = f'ИСТЕКАЕТ ЧЕРЕЗ {hours_left}'
        else:
            status_str = 'ДАТА НЕИЗВЕСТНА'

        host_str = f"{p.host}:{p.port}" if p.host else f"ID {p.ipfoxy_proxy_id}"
        owner_str = ""
        if is_admin and p.owner:
            uname = p.owner.username or p.owner.first_name or f"id{p.owner.telegram_id}"
            owner_str = f"\n   👤 Владелец: @{uname}"

        note_str = f"\n   📝 {p.note}" if p.note else ""

        lines.append(
            f"🔹 <code>{host_str}</code>\n"
            f"   {status_str}\n"
            f"   Истекает: {_fmt_dt(expires)}"
            f"{owner_str}"
            f"{note_str}"
        )

    lines.append('\n<i>ДЛЯ ПРОДЛЕНИЯ ОТКРОЙТЕ ПРИЛОЖЕНИЕ.</i>')
    return '\n'.join(lines)

@shared_task(
    name='backend.tasks.notifications_tasks.notify_expiring_proxies_task',
    bind=True, max_retries=2, default_retry_delay=60
)
def notify_expiring_proxies_task(self):
    """
    Ежедневная задача.
    Проходит по всем пользователям, у которых есть прокси требующие внимания,
    и отправляет / редактирует Telegram-уведомление.
    """
    logger.info('[NOTIFY] Задача запущена')
    async def logic():
        async with AssyncSessionLocal() as db:
            now = datetime.now(timezone.utc)
            expiry_warn_threshold = now + timedelta(days=WARN_BEFORE_DAYS)
            # Граница деактивации: expires_at + 2 дня grace от IPFoxy уже прошли
            deactivate_threshold = now - timedelta(days=DEACTIVATE_AFTER_GRACE_DAYS)
            # Граница напоминания: прокси просрочен, но ещё в grace period или чуть позже
            expired_remind_threshold = now - timedelta(days=REMIND_AFTER_EXPIRY_DAYS)

            # Деактивируем прокси только после истечения grace period IPFoxy (2 дня после expires_at).
            # Это защищает от ложной деактивации: IPFoxy продолжает работать 2 дня после expire.
            stmt_overdue = select(Proxy).where(
                Proxy.is_active.is_(True),
                Proxy.expires_at < deactivate_threshold,
            )
            res_overdue = await db.execute(stmt_overdue)
            overdue_proxies = res_overdue.scalars().all()
            for p in overdue_proxies:
                p.is_active = False
                logger.info(f'[NOTIFY] proxy_id={p.id} помечен неактивным (просрочен > {REMIND_AFTER_EXPIRY_DAYS} дн.)')
            if overdue_proxies:
                await db.commit()

            stmt_warm = select(Proxy).where(
                Proxy.auto_extend.is_(False),
                Proxy.expires_at > expired_remind_threshold,
                Proxy.expires_at < expiry_warn_threshold
            )
            res_warn = await db.execute(stmt_warm)
            warning_proxies: list[Proxy] = res_warn.scalars().all()
            by_user: dict[int, list[Proxy]] = {}

            all_owner_ids = {p.owner_id for p in warning_proxies if p.owner_id}
            owners_map: dict[int, User] = {}
            if all_owner_ids:
                stmt_users = select(User).where(User.id.in_(all_owner_ids))
                res_users = await db.execute(stmt_users)
                for u in res_users.scalars().all():
                    owners_map[u.id] = u

            for p in warning_proxies:
                if p.owner_id and p.owner_id in owners_map:
                    tg_id = owners_map[p.owner_id].telegram_id
                    by_user.setdefault(tg_id, []).append(p)

            for admin_tg_id in settings.ADMIN_TELEGRAM_IDS:
                by_user[admin_tg_id] = warning_proxies

            all_tg_ids = set(by_user.keys())
            stmt_logs = select(Notifications)
            res_logs = await db.execute(stmt_logs)
            logs_map: dict[int, Notifications] = {
                log.telegram_id: log for log in res_logs.scalars().all()
            }

            stale_threshold = now - timedelta(hours=DELETE_NOTIFICATION_AFTER_HOURS)
            for tg_id, log in list(logs_map.items()):
                if tg_id not in all_tg_ids:
                    if log.sent_at and log.sent_at < stale_threshold:
                        if log.message_id:
                            await _tg_delete_message(tg_id, log.message_id)
                        await db.delete(log)
                        del logs_map[tg_id]
                        logger.info(f'[NOTIFY] tg_id={tg_id} уведомление удалено (неактуально).')

            if logs_map != {}:
                await db.commit()

            for tg_id, proxies in by_user.items():
                if not proxies:
                    continue

                is_admin_notify = tg_id in settings.ADMIN_TELEGRAM_IDS
                text = _build_notification_text(proxies, is_admin=is_admin_notify)
                log = logs_map.get(tg_id)

                if log and log.message_id:
                    sent_ago = (now - log.sent_at).total_seconds() / 3600 if log.sent_at else 999
                    if sent_ago < EDIT_THRESHOLD_HOURS:
                        success = await _tg_edit_message(tg_id, log.message_id, text)
                        if success:
                            log.sent_at = now
                            logger.info(f'[NOTIFY] tg_id={tg_id} сообщение отредактирование')
                            continue
                    else:
                        await _tg_delete_message(tg_id, log.message_id)

                new_msg_id = await _tg_send_message(tg_id, text)
                if new_msg_id:
                    if log:
                        log.message_id = new_msg_id
                        log.sent_at = now
                    else:
                        new_log = Notifications(
                            telegram_id=tg_id,
                            message_id=new_msg_id,
                            sent_at=now
                        )
                        db.add(new_log)
                    logger.info(f'[NOTIFY] tg_id={tg_id} новое сообщение msg_id={new_msg_id}')
            
            await db.commit()
            logger.info('[NOTIFY] Задача завершена')
            return {'status': 'ok', 'users_notified': len(by_user)}
    try:
        return run_async(logic())
    except Exception as exc:
        logger.error(f'[NOTIFY] Критическая ошибка: {exc}', exc_info=True)
        raise self.retry(exc=exc)