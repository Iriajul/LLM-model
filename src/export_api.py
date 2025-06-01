import os
import uuid
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import secrets
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("export_api")

app = FastAPI()

# ===== HTTPS ENFORCEMENT IN PRODUCTION =====
if os.getenv("ENVIRONMENT") == "production":
    logger.info("Enabling HTTPS enforcement for production environment")
    from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
    app.add_middleware(HTTPSRedirectMiddleware)

# Security setup
security = HTTPBasic()
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Get credentials from environment variables
def get_credentials():
    return (
        os.getenv("EXPORT_API_USER", "admin"),
        os.getenv("EXPORT_API_PASS", "securepassword")
    )

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    """Secure authentication with timing-attack resistance"""
    correct_username, correct_password = get_credentials()
    
    # Secure credential comparison
    username_correct = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        correct_username.encode("utf-8")
    )
    password_correct = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        correct_password.encode("utf-8")
    )
    
    if not (username_correct and password_correct):
        logger.warning(f"Authentication failed for user: {credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def safe_path(file_id: str, format: str) -> str:
    """Validate and sanitize file paths to prevent traversal attacks"""
    # Validate UUID format
    try:
        uuid.UUID(file_id, version=4)
    except ValueError:
        logger.warning(f"Invalid file ID format: {file_id}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file identifier format"
        )
    
    # Validate file format
    if format not in ["csv", "xlsx"]:
        logger.warning(f"Invalid file format requested: {format}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format"
        )
    
    # Construct safe path
    return os.path.join(EXPORT_DIR, f"{file_id}.{format}")

def cleanup_old_files():
    """Remove files older than 24 hours"""
    now = datetime.now()
    for filename in os.listdir(EXPORT_DIR):
        file_path = os.path.join(EXPORT_DIR, filename)
        if os.path.isfile(file_path):
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if (now - file_time) > timedelta(hours=24):
                try:
                    os.remove(file_path)
                    logger.info(f"Cleaned up expired file: {filename}")
                except Exception as e:
                    logger.error(f"Error deleting {filename}: {str(e)}")

@app.post("/export")
async def export_data(
    data: dict, 
    background_tasks: BackgroundTasks,
    username: str = Depends(authenticate)
):
    """Securely export data to CSV and Excel formats"""
    try:
        # Input validation
        if not data or not isinstance(data.get("data"), list):
            logger.warning("Invalid data format received")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid data format"
            )
        
        # Size validation
        if len(data["data"]) > 100000:  # 100k row limit
            logger.warning("Data size exceeds limit")
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Data exceeds maximum size limit"
            )
        
        # Cleanup old files
        cleanup_old_files()
        
        # Generate secure file IDs
        file_id = uuid.uuid4().hex
        csv_path = safe_path(file_id, "csv")
        excel_path = safe_path(file_id, "xlsx")

        # Convert to DataFrame
        try:
            df = pd.DataFrame(data["data"])
        except Exception as e:
            logger.error(f"Data conversion failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid data structure"
            )
        
        # Save files
        try:
            df.to_csv(csv_path, index=False)
            df.to_excel(excel_path, index=False, engine="openpyxl")
        except Exception as e:
            logger.error(f"File save failed: {str(e)}")
            # Clean up partial files
            for path in [csv_path, excel_path]:
                if os.path.exists(path):
                    os.remove(path)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to generate export files"
            )
        
        # Schedule cleanup
        background_tasks.add_task(cleanup_old_files)
        
        logger.info(f"Export successful for user {username} - File ID: {file_id}")
        return {
            "csv_url": f"/download/{file_id}.csv",
            "excel_url": f"/download/{file_id}.xlsx",
            "expires": (datetime.now() + timedelta(hours=24)).isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected export error")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/download/{file_id}.{format}")
async def download_file(
    file_id: str, 
    format: str,
    username: str = Depends(authenticate)
):
    """Securely download exported files"""
    try:
        file_path = safe_path(file_id, format)
        
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_id}.{format}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found or expired"
            )
            
        logger.info(f"Download served for {username} - {file_id}.{format}")
        return FileResponse(
            file_path,
            media_type="application/octet-stream",
            filename=f"export_{file_id[:8]}.{format}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/health")
async def health_check():
    """Security-neutral health check"""
    return JSONResponse(content={"status": "ok", "timestamp": datetime.utcnow().isoformat()})