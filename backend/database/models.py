from sqlalchemy import Column, Integer, BigInteger, String, Boolean, DateTime, Numeric, ForeignKey, Enum as SAEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

from backend.database.database import Base

class UserRole(str, enum.Enum):
    admin = 'admin'
    user = 'user'

class TransactionType(str, enum.Enum):
    purchase = 'purchase'
    renewal = 'renewal'
    extend = 'extend'

class Whitelist(Base):
    """
    Таблица разрешенных пользователей.

    Если telegram_id НЕ присутствует в этой таблице - доступ запрещен (403).
    Блокировка пользователя = удаление строки из этой таблицы.
    Поле role здесь является источником истины для роли пользователя.
    """

    __tablename__ = 'whitelist'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    role = Column(SAEnum(UserRole), default=UserRole.user, nullable=False)
    added_at = Column(DateTime(timezone=True), server_default=func.now())
    added_by = Column(BigInteger, nullable=True)

    def __repr__(self):
        return f'<Whitelist tg_id={self.telegram_id} role={self.role}>'

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    username = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    role = Column(SAEnum(UserRole), default=UserRole.user, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    api_key_id = Column(Integer, ForeignKey('api_keys.id'), nullable=True)

    api_key = relationship('ApiKey', back_populates='users')
    proxies = relationship('Proxy', back_populates='owner')
    transactions = relationship('Transaction', back_populates='user')

    def __repr__(self):
        return f"<User id={self.id} tg_id={self.telegram_id} role={self.role}>"

class Proxy(Base):
    __tablename__ = 'proxies'

    id = Column(Integer, primary_key=True, index=True)
    ipfoxy_proxy_id = Column(String(50), unique=True, index=True)
    ipfoxy_order_id = Column(String(255), nullable=True)
    host = Column(String(100), nullable=False)
    public_ip = Column(String(100))
    port = Column(Integer, nullable=False)
    type = Column(String(20))

    username = Column(String(255), nullable=True)
    password = Column(String(255), nullable=True)

    auto_extend = Column(Boolean, default=False)
    auto_extend_local = Column(Boolean, default=False)
    ip_type = Column(String(50))
    ip_version = Column(String(20))

    area_id = Column(String(10), ForeignKey('regions.area_id'), nullable=False)
    country_code = Column(String(10), nullable=True)

    owner_id = Column(Integer, ForeignKey('users.id', ondelete='SET NULL'), nullable=True, index=True)
    api_key_id = Column(Integer, ForeignKey('api_keys.id'), nullable=True)

    purchased_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True)
    renewal_at = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)

    checked_location = Column(String(50), nullable=True)
    location_match = Column(Boolean, nullable=True)

    owner = relationship('User', back_populates='proxies', foreign_keys=[owner_id], lazy='selectin')
    api_key = relationship('ApiKey', back_populates='proxies')
    transactions = relationship('Transaction', back_populates='proxy')
    region_info = relationship('Regions', back_populates='proxies')

    def __repr__(self):
        return f"<Proxy id={self.id} host={self.host}:{self.port} owner_id={self.owner_id}>"

class ApiKey(Base):
    __tablename__ = 'api_keys'

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(500), nullable=False)
    api_id = Column(String(100))
    key_name = Column(String(255), nullable=False, index=True)
    is_active = Column(Boolean, default=True)
    balance = Column(Numeric(precision=10, scale=2), nullable=True)
    last_checked = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    users = relationship('User', back_populates='api_key')
    proxies = relationship('Proxy', back_populates='api_key')

    def __repr__(self):
        return f"<ApiKey id={self.id} name='{self.key_name}' active={self.is_active}>"

class Transaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False, index=True)
    order_id = Column(String(50), nullable=False)
    proxy_id = Column(Integer, ForeignKey('proxies.id'), nullable=True, index=True)
    api_key_id  = Column(Integer, ForeignKey("api_keys.id"), nullable=True, index=True)
    type = Column(SAEnum(TransactionType), nullable=False)
    amount = Column(Numeric(precision=10, scale=2), nullable=False)
    description = Column(String(500), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship('User', back_populates='transactions')
    proxy = relationship('Proxy', back_populates='transactions')

    def __repr__(self):
        return f"<Transaction id={self.id} type={self.type} amount={self.amount}>"

class Regions(Base):
    __tablename__ = 'regions'

    id = Column(Integer, primary_key=True)
    area_id = Column(String(10), nullable=False, unique=True, index=True)
    ip_type = Column(String(50), nullable=False)
    status = Column(Boolean, default=True)
    list_price = Column(Numeric(precision=10, scale=2), nullable=False)
    ip_version = Column(String(50), nullable=False)
    country = Column(String(100), nullable=False)
    country_code = Column(String(10), nullable=False)
    region = Column(String(100), nullable=False)
    retail_price = Column(Numeric(precision=10, scale=2), nullable=False)

    proxies = relationship('Proxy', back_populates='region_info')

    def __repr__(self):
        return f"<Regions area_id={self.area_id} country={self.country}>"
    
class AppSettings(Base):
    __tablename__ = 'app_settings'

    id = Column(Integer, primary_key=True, index=True)
    allowed_area_ids = Column(String(2000), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<AppSettings allowed_area_ids=[self.allowed_area_ids]>'
    
class Notifications(Base):
    __tablename__ = 'notifications'

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    message_id = Column(BigInteger, nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f'<NotificationLog tg_id={self.telegram_id} msg_id={self.message_id}>'
