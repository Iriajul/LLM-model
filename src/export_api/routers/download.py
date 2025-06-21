import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse

from ..config import EXPORT_DIR
from ..utils import safe_path
from ..auth import jwt_auth

router = APIRouter(prefix="/download", tags=["download"])

@router.get("/{file_id}.{fmt}", dependencies=[Depends(jwt_auth)])
def download_file(file_id: str, fmt: str):
    path = safe_path(file_id, fmt, EXPORT_DIR)
    if not os.path.exists(path):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found or expired")
    return FileResponse(path,
                        media_type="application/octet-stream",
                        filename=os.path.basename(path))