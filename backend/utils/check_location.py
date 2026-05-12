import httpx
import logging
from backend.utils.config import settings

logger = logging.getLogger('utils.check_location')


async def check_proxy_country_with_ip_api(
        ip_or_host: str,
        expected_country_code: str | None
) -> tuple[str | None, bool | None]:
    """
    Проверяет геолокацию IP через ipinfo.io/lite.

    Возвращает:
        checked_location  — название страны, например 'Canada'
        location_match    — совпадает ли country_code с ожидаемым (None если expected не задан)
    """
    if not ip_or_host:
        return None, None

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            res = await client.get(
                f'https://api.ipinfo.io/lite/{ip_or_host}',
                params={'token': settings.IPINFO_TOKEN},
            )

        if res.status_code != 200:
            logger.warning(f'[IPINFO] status={res.status_code} ip={ip_or_host} body={res.text}')
            return None, None

        data = res.json()

        real_country      = data.get('country')
        real_country_code = (data.get('country_code') or '').upper()
        expected          = (expected_country_code or '').upper()

        return real_country, real_country_code == expected if expected else None

    except Exception as exc:
        logger.warning(f'[IPINFO] ошибка проверки ip={ip_or_host}: {exc}')
        return None, None