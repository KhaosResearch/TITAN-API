from datetime import datetime, timedelta
from typing import Union

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jwt import PyJWTError
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.status import HTTP_401_UNAUTHORIZED

from titan.database import get_connection
from titan.models.user import UserCreateRequest, UserInDB

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"  # $ openssl rand -hex 32
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v3/auth/login")


def create_access_token(*, data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_user_by_username(db: AsyncIOMotorClient, username: str) -> UserInDB:
    """
    Gets user from database.
    """
    user = await db.users.find_one({"username": username})
    if user:
        return UserInDB(**user)


async def get_user_by_email(db: AsyncIOMotorClient, email: str) -> UserInDB:
    """
    Gets user from database.
    """
    user = await db.users.find_one({"email": email})
    if user:
        return UserInDB(**user)


async def register_user(db: AsyncIOMotorClient, user: UserCreateRequest):
    """
    Saves user to database.
    """
    user_dict = user.dict()
    user_dict["password"] = user.hashed_password

    await db.users.insert_one(user_dict)

    return UserInDB(**user_dict)


async def authenticate_user(db: AsyncIOMotorClient, username: str, password: str) -> Union[UserInDB, bool]:
    user = await get_user_by_username(db, username)
    if not user:
        return False
    if not user.verify_password(password):
        return False
    return user


async def get_current_user(
    token: str = Depends(oauth2_scheme), db: AsyncIOMotorClient = Depends(get_connection)
) -> UserInDB:
    """
    Get current user based on JWT.
    """
    credentials_exception = HTTPException(
        status_code=HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except PyJWTError:
        raise credentials_exception
    user = await get_user_by_username(db, username=username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: UserInDB = Depends(get_current_user),
) -> UserInDB:
    """
    Checks if current user is active (i.e., field `disabled` is False).
    """
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
