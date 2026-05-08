from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional
from datetime import datetime
from backend.database.models import UserRole, TransactionType

# ---- User ----------------------------------------------------
class UserProfileResponse(BaseModel):
    first_name: str | None
    username: str | None
    balance: Decimal
    role: UserRole
    api_key_id: Optional[int] = None

    class Config:
        from_attributes = True

class UserListItem(BaseModel):
    id: int
    telegram_id: int
    username: Optional[str]
    first_name: Optional[str]
    role: UserRole
    api_key_id: Optional[int]
    api_key_name: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

# ---- Whitelist--------------------------------------------------
class WhitelistAddRequest(BaseModel):
    telegram_id: int = Field(..., description='Telegram ID пользователя')
    role: UserRole = UserRole.user

class WhitelistResponse(BaseModel):
    id: int
    telegram_id: int
    role: UserRole
    added_at: datetime
    added_by: Optional[int]

    class Config:
        from_attributes = True 

# ---- ApiKey ----------------------------------------------------
class ApiKeyCreate(BaseModel):
    key: str = Field(..., min_length=1, max_length=500, description='API ключ для сервиса IpFoxy')
    api_id: str = Field(..., min_length=1, max_length=100, description='API id для системы IpFoxy')
    key_name: str = Field(..., min_length=1, max_length=255, description='Внутренне название для API ключа')

class ApiKeyUpdate(BaseModel):
    key: Optional[str] = None
    api_id: Optional[str] = None
    key_name: Optional[str] = None
    is_active: Optional[bool] = None

class ApiKeyResponse(BaseModel):
    id: int
    api_id: str
    key_name: str
    is_active: bool
    balance: Optional[Decimal] = None
    last_checked: Optional[datetime] = None
    created_at: datetime

    class Config:
        from_attributes = True

class ApiKeyStatsResponse(BaseModel):
    id: int
    api_id: str
    key_name: str
    is_active: bool
    balance: Optional[float] = None
    proxy_count: int = 0
    user_count: int = 0

class AssignKeyRequest(BaseModel):
    user_ids: list[int] = Field(..., min_length=1, description='Список internal user.id')
    key_id: int

# ---- Proxy -----------------------------------------------------
class ProxyListItem(BaseModel):
    id: int
    host: str
    port: int
    type: str
    ip_type: Optional[str]
    ip_version: Optional[str]
    country_code: Optional[str]
    is_active: bool
    expires_at: Optional[datetime]
    purchased_at: datetime

    class Config:
        from_attributes = True

class ProxyDetail(BaseModel):
    id:                int
    ipfoxy_proxy_id:   Optional[str]
    ipfoxy_order_id:   Optional[str]
    host:              str
    public_ip:         Optional[str]
    port:              int
    type:              str
    username:          Optional[str]
    password:          Optional[str]
    ip_type:           Optional[str]
    ip_version:        Optional[str]
    country_code:      Optional[str]
    area_id:           Optional[str]
    auto_extend:       bool
    is_active:         bool
    purchased_at:      datetime
    expires_at:        Optional[datetime]
    renewal_at:        Optional[datetime]
    checked_location:  Optional[str]
    location_match:    Optional[bool]

    owner_username:    Optional[str] = None
    owner_tg_id:       Optional[int] = None
    api_key_name:      Optional[str] = None

    class Config:
        from_attributes = True

class ProxyPageResponse(BaseModel):
    items: list[ProxyListItem]
    next_cursor: Optional[int] = None
    has_more: bool

class ExtendProxyRequest(BaseModel):
    proxy_ids: list[int]
    days: int

class AutoExtendRequest(BaseModel):
    auto_extend: bool

class ProxyActiveRequest(BaseModel):
    is_active: bool

# ---- Countries -------------------------------------------------
class CountryItem(BaseModel):
    id: int
    area_id: str
    ip_type: str
    ip_version: str
    country: str
    country_code: str
    retail_price: Decimal

    class Config:
        from_attributes = True

class CountriesResponse(BaseModel):
    items: list[CountryItem]
    next_cursor: Optional[int] = None
    has_more: bool

# ---- Order info -------------------------------------------------
class OrderPriceRequest(BaseModel):
    order_type: str
    days: int
    area_id: int | None = None
    proxy_ids: list[int] | None = None
    num: int | None = Field(None, ge=1, le=20)

class ProxyPurchaseRequest(BaseModel):
    area_id: int
    days: int
    num: int = Field(..., ge=1, le=20)

# ---- Transactions -----------------------------------------------
class TransactionItem(BaseModel):
    id: int
    type: TransactionType
    amount: Decimal
    description: Optional[str]
    created_at: datetime
    user_tg_id: Optional[int] = None
    api_key_name: Optional[str] = None

    class Config:
        from_attributes = True

class TransactionPageResponse(BaseModel):
    items: list[TransactionItem]
    next_cursor: Optional[int] = None
    has_more: bool

# ---- AppSettings ------------------------------------------------
class AppSettingsResponse(BaseModel):
    allowed_area_ids: Optional[str] = None

    class Config:
        from_attributes = True

class AppSettingsUpdate(BaseModel):
    allowed_area_ids: Optional[str] = None

class SyncProxiesRequest(BaseModel):
    api_key_id: int
    owner_id: Optional[int] = None