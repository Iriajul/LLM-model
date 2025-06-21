from fastapi import FastAPI
from .routers.export import router as export_router
from .routers.download import router as download_router
from .auth import router as auth_router
from fastapi.responses import JSONResponse
from datetime import datetime

app = FastAPI()
app.include_router(auth_router)
app.include_router(export_router)
app.include_router(download_router)

@app.get("/health")
async def health_check():
    return JSONResponse({"status": "ok", "timestamp": datetime.utcnow().isoformat()})

@app.get("/")
async def root():
    return JSONResponse({
        "message": "Welcome to the NL2SQL Export API",
        "available_endpoints": [
            "/auth/login",
            "/export",
            "/download/{file_id}.{format}",
            "/health",
            "/docs"
        ]
    })

@app.get("/ping")
def ping():
    return {"msg": "pong"}