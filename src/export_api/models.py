from pydantic import BaseModel, Field
from typing import List, Dict, Any

class ExportRequest(BaseModel):
    data: List[Dict[str, Any]] = Field(
        ...,
        example=[{"customer_name":"Alice","years_with_company":5}]
    )

class ExportResponse(BaseModel):
    csv_url: str
    excel_url: str
    expires: str

class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str

class LoginRequest(BaseModel):
    login: str
    password: str

class RefreshRequest(BaseModel):
    refresh_token: str = None

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
class LogoutResponse(BaseModel):
    msg: str