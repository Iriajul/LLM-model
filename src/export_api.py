import os
import uuid
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import pandas as pd
import shutil

app = FastAPI()
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Cleanup old files every 24 hours
def cleanup_old_files():
    now = datetime.now()
    for filename in os.listdir(EXPORT_DIR):
        file_path = os.path.join(EXPORT_DIR, filename)
        file_time = datetime.fromtimestamp(os.path.getctime(file_path))
        if now - file_time > timedelta(hours=24):
            os.remove(file_path)

@app.post("/export")
async def export_data(data: dict, background_tasks: BackgroundTasks):
    try:
        # Cleanup old files first
        cleanup_old_files()
        
        # Generate unique ID
        file_id = uuid.uuid4().hex
        csv_path = os.path.join(EXPORT_DIR, f"{file_id}.csv")
        excel_path = os.path.join(EXPORT_DIR, f"{file_id}.xlsx")

        # Convert to DataFrame
        df = pd.DataFrame(data["data"])
        
        # Save files
        df.to_csv(csv_path, index=False)
        df.to_excel(excel_path, index=False, engine="openpyxl")

        # Schedule cleanup
        background_tasks.add_task(cleanup_old_files)
        
        return {
            "csv_url": f"/download/{file_id}.csv",
            "excel_url": f"/download/{file_id}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{file_id}.{format}")
async def download_file(file_id: str, format: str):
    valid_formats = ["csv", "xlsx"]
    if format not in valid_formats:
        raise HTTPException(status_code=400, detail="Invalid format")
    
    file_path = os.path.join(EXPORT_DIR, f"{file_id}.{format}")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        file_path,
        media_type="application/octet-stream",
        filename=f"query_result.{format}"
    )