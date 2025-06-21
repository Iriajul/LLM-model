import uuid
import os
import pandas as pd
from datetime import datetime, timedelta
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from ..config import EXPORT_DIR
from ..utils import safe_path, cleanup_old_files
from ..models import ExportRequest, ExportResponse
from ..auth import jwt_auth

router = APIRouter(prefix="/export", tags=["export"])

@router.post("", response_model=ExportResponse, dependencies=[Depends(jwt_auth)])
async def export_data(req: ExportRequest, background_tasks: BackgroundTasks):
    if not req.data or not isinstance(req.data, list):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid data format")
    if len(req.data) > 100_000:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Data exceeds maximum size limit"
        )

    cleanup_old_files(EXPORT_DIR)
    fid   = uuid.uuid4().hex
    csvp  = safe_path(fid, "csv", EXPORT_DIR)
    xlsxp = safe_path(fid, "xlsx", EXPORT_DIR)

    try:
        df = pd.DataFrame(req.data)
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid data structure")

    try:
        df.to_csv(csvp, index=False)
        df.to_excel(xlsxp, index=False, engine="openpyxl")
    except Exception:
        for p in (csvp, xlsxp):
            if os.path.exists(p): os.remove(p)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to generate export files")

    background_tasks.add_task(cleanup_old_files, EXPORT_DIR)
    expires = (datetime.utcnow() + timedelta(hours=24)).isoformat()

    return ExportResponse(
        csv_url=f"/download/{fid}.csv",
        excel_url=f"/download/{fid}.xlsx",
        expires=expires
    )