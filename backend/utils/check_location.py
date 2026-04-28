import httpx
import logging
from backend.database.models import Regions

logger = logging.getLogger('utils.check_location')

async def check_proxy_country_with_ip_api(
        ip_or_host: str,
        expected_country_code: str | None
) -> tuple[str | None, bool | None]:
    """
    Возвращает:
    checked_location - фактическая страна, например 'United States'
    location_match - совпадает ли country_code с ожидаемым значением
    """
    if not ip_or_host:
        return None, None
    
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            res = await client.get(
                f'http:/ip-api.com/json/{{ip_or_host}}',
                params = {
                    'fields': 'status,message,country,countryCode,query',
                    'lang': 'en',
                },
            )
        
        if res.status_code != 200:
            logger.warning(f'[IP_API] status={res.status_code} ip={ip_or_host} body={res.text}')
            return None, None
        
        data = res.json()

        if data.get('status') != 'success':
            logger.warning(f'[IP_API] fail ip={ip_or_host} msg={data.get("message")}')
            return None, None
        
        real_country = data.get('country')
        real_country_code = (data.get('countryCode') or '').upper()
        expected = (expected_country_code or '').upper()

        return real_country, real_country_code == expected if expected else None
    
    except Exception as exc:
        logger.warning(f'[IP_API] ошибка проверки ip={ip_or_host}: {exc}')
        return None, None