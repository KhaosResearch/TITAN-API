from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_409_CONFLICT

from titan.auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    authenticate_user,
    create_access_token,
    get_current_active_user,
    get_user_by_email,
    get_user_by_username,
    register_user,
)
from titan.database import get_connection
from titan.models.user import Token, UserCreateRequest, UserInDB

router = APIRouter()


@router.post(
    "/register",
    summary="Sing-up authentication endpoint",
    tags=["auth"],
    response_model=UserInDB,
    response_description="User model from database",
)
async def register_to_system(user: UserCreateRequest, db: AsyncIOMotorClient = Depends(get_connection)):
    user_by_email = await get_user_by_email(db, user.email)
    if user_by_email:
        raise HTTPException(
            status_code=HTTP_409_CONFLICT,
            detail="Email already in use",
        )
    user_by_username = await get_user_by_username(db, user.username)
    if user_by_username:
        raise HTTPException(
            status_code=HTTP_409_CONFLICT,
            detail="User already in use",
        )
    user_in_db = await register_user(db, user)
    return user_in_db


@router.post(
    "/login",
    summary="Log-in authentication endpoint",
    tags=["auth"],
    response_model=Token,
)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncIOMotorClient = Depends(get_connection),
):
    user = await authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/user/me", summary="Retrieves user data", tags=["user"], response_model=UserInDB)
async def read_users_me(
    current_user: UserInDB = Depends(get_current_active_user),
):
    return current_user
