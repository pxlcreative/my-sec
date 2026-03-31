import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import settings
from routes import firms, match, sync
from routes.alerts import router as alerts_router
from routes.excel import router as excel_router
from routes.export import router as export_router
from routes.external import router as external_router
from routes.platforms import firm_platforms_router, match_router as platform_match_router, platforms_router

logging.basicConfig(level=settings.log_level.upper())
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SEC Adviser Database Platform",
    version="0.1.0",
    description="Self-hosted mirror of SEC-registered investment adviser data.",
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
_origins = (
    ["*"]
    if settings.cors_origins.strip() == "*"
    else [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Global error handler for unhandled exceptions (DB errors etc.)
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception on %s %s", request.method, request.url)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(firms.router, prefix="/api")
app.include_router(sync.router, prefix="/api")
app.include_router(match.router, prefix="/api")
app.include_router(platforms_router, prefix="/api")
app.include_router(firm_platforms_router, prefix="/api")
app.include_router(platform_match_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(alerts_router, prefix="/api")
app.include_router(excel_router, prefix="/api")
app.include_router(external_router, prefix="/api")


# ---------------------------------------------------------------------------
# Meta endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}
