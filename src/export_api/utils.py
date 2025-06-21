import os, uuid, logging
from datetime import datetime, timedelta
from fastapi import HTTPException, status

logger = logging.getLogger("export_api.utils")

def safe_path(file_id: str, fmt: str, export_dir: str) -> str:
    # Validate UUID v4
    try:
        uuid.UUID(file_id, version=4)
    except ValueError:
        logger.warning(f"Invalid file ID format: {file_id}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Invalid file identifier format")
    # Validate format
    if fmt not in ("csv", "xlsx"):
        logger.warning(f"Invalid file format requested: {fmt}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Invalid file format")
    return os.path.join(export_dir, f"{file_id}.{fmt}")

def cleanup_old_files(export_dir: str, hours: int = 24):
    now = datetime.now()
    for fn in os.listdir(export_dir):
        path = os.path.join(export_dir, fn)
        if os.path.isfile(path):
            created = datetime.fromtimestamp(os.path.getctime(path))
            if now - created > timedelta(hours=hours):
                try:
                    os.remove(path)
                    logger.info(f"Removed old file: {fn}")
                except Exception as ex:
                    logger.error(f"Error removing {fn}: {ex}")