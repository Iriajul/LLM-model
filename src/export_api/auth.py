import uuid
import jwt
from datetime import datetime,timezone
from fastapi import APIRouter, HTTPException, Depends, status, Response, Cookie
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext

from .config import JWT_SECRET_KEY, JWT_ALGORITHM, ACCESS_TOKEN_EXPIRE, redis_client
from .models import RegisterRequest, LoginRequest, RefreshRequest, TokenResponse, LogoutResponse
from src.db_utils import get_user_by_email, get_user_by_username, create_user

router = APIRouter(tags=["auth"])
bearer = HTTPBearer()
pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_ctx.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)


def create_access_token(sub: str) -> str:
    expire = datetime.now(timezone.utc) + ACCESS_TOKEN_EXPIRE
    payload = {"sub": sub, "exp": expire}
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def create_refresh_token() -> str:
    return str(uuid.uuid4())


@router.post("/auth/register", status_code=201)
def register(req: RegisterRequest):
    if get_user_by_email(req.email) or get_user_by_username(req.username):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "User already exists")
    hashed = hash_password(req.password)
    create_user(username=req.username, email=req.email, hashed_password=hashed)
    return {"msg": "Registration successful"}


@router.post("/auth/login", response_model=TokenResponse)
def login(req: LoginRequest, response: Response):
    # Try to find user by email or username
    user = get_user_by_email(req.login)
    if not user:
        user = get_user_by_username(req.login)
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    access_token = create_access_token(user.email)
    refresh_token = create_refresh_token()
    if not redis_client:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Redis not available")
    redis_client.set(
        f"refresh:{refresh_token}",
        user.email,
        ex=int(ACCESS_TOKEN_EXPIRE.total_seconds()) * 24
    )
    response.set_cookie("refresh_token", refresh_token, httponly=True, secure=True)
    return TokenResponse(access_token=access_token)


@router.post("/auth/refresh", response_model=TokenResponse)
def refresh(refresh_token: str = Cookie(None)):
    if not refresh_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Refresh token missing")
    email = redis_client.get(f"refresh:{refresh_token}")
    if not email:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid refresh token")
    access_token = create_access_token(email)
    return TokenResponse(access_token=access_token)

@router.post("/auth/logout", response_model=LogoutResponse)
def logout(response: Response, refresh_token: str = Cookie(None)):
    if refresh_token:
        redis_client.delete(f"refresh:{refresh_token}")
        response.delete_cookie("refresh_token")
    return LogoutResponse(msg="Logged out")

def jwt_auth(
    credentials: HTTPAuthorizationCredentials = Depends(bearer)
) -> str:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload.get("sub")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Access token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid access token")