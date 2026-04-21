from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from backend.database.database import get_db
from backend.database.models import User, ApiKey, UserRole
from backend.database import schemas
from backend.utils.security import get_current_user
from backend.utils.crypto import encrypt_data

router = APIRouter(prefix='/admin', tags=['admin'])

@router.get('/keys', response_model=list[schemas.ApiKeyResponse])
async def get_api_keys(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail='Запрещено.')
    
    result = await db.execute(select(ApiKey).order_by(ApiKey.created_at.desc()))
    return result.scalars().all()

@router.post('/keys', response_model=schemas.ApiKeyCreate)
async def create_api_key(
    key_data: schemas.ApiKeyCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail='Запрещено.')
    
    encrypted_key = encrypt_data(key_data.key)

    new_key = ApiKey(
        key=encrypted_key,
        api_id=key_data.api_id,
        key_name=key_data.key_name
    )

    db.add(new_key)
    try:
        await db.commit()
        await db.refresh(new_key)
        return new_key
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=400, detail='Ошибка базы данных.')
    
@router.patch('/keys/{key_id}', response_model=schemas.ApiKeyResponse)
async def update_api_key(
    key_id: int,
    updated_data: schemas.ApiKeyUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail='Запрещено.')
    
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    db_key = result.scalar_one_or_none()
    if not db_key:
        raise HTTPException(status_code=404, detail='Ключ не найден')
    
    updated_dict = updated_data.model_dump(exclude_unset=True)
    for field, value in updated_dict.items():
        if field in ['key'] and value:
            setattr(db_key, field, encrypt_data(value))
        else:
            setattr(db_key, field, value)

    try:
        await db.commit()
        await db.refresh(db_key)
        return db_key
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail='Ошибка при обновлении базы данных.')
    
@router.delete('/keys/{key_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_api_key(
    key_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if current_user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail='Запрещено.')
    
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    db_key = result.scalar_one_or_none()

    if not db_key:
        raise HTTPException(status_code=404, detail='Ключ не найден')
        
    try:
        await db.delete(db_key)
        await db.commit()
        return None
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail='ОШибка при удалении.')