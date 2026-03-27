from fastapi import APIRouter
from fastapi.responses import JSONResponse

from routes.home_routes import APP_VERSION
from settings import (
    APP_BASE_URL,
    APP_ENV,
    ENABLE_EXTERNAL_SIDE_EFFECTS,
    ENABLE_GOOGLE_CALENDAR_INTEGRATION,
    MONGODB_DB_NAME,
)


router = APIRouter()


@router.get("/health/config")
def health_config():
    return JSONResponse(
        {
            "status": "ok",
            "app_version": APP_VERSION,
            "app_env": APP_ENV,
            "app_base_url": APP_BASE_URL or None,
            "mongodb_db_name": MONGODB_DB_NAME,
            "external_side_effects_enabled": ENABLE_EXTERNAL_SIDE_EFFECTS,
            "google_calendar_integration_enabled": ENABLE_GOOGLE_CALENDAR_INTEGRATION,
        }
    )
