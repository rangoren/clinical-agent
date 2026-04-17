from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from routes.calendar_routes import router as calendar_router
from routes.feedback_routes import router as feedback_router
from routes.home_routes import router as home_router
from routes.system_routes import router as system_router
from routes.message_routes import router as message_router
from routes.study_routes import router as study_router
from routes.undo_routes import router as undo_router
from routes.home_routes import APP_VERSION
from services.logging_service import log_event
from services.study_service import ensure_study_content_seed
from services.web_push_service import start_duty_sync_push_poller, web_push_configured
from settings import APP_BASE_URL, APP_ENV, ENABLE_EXTERNAL_SIDE_EFFECTS, ENABLE_GOOGLE_CALENDAR_INTEGRATION, MONGODB_DB_NAME


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def log_startup_version():
    ensure_study_content_seed()
    push_poller_started = start_duty_sync_push_poller()
    log_event(
        "app_startup",
        payload={
            "app_version": APP_VERSION,
            "app_env": APP_ENV,
            "mongodb_db_name": MONGODB_DB_NAME,
            "app_base_url": APP_BASE_URL or "(not set)",
            "external_side_effects_enabled": ENABLE_EXTERNAL_SIDE_EFFECTS,
            "google_calendar_integration_enabled": ENABLE_GOOGLE_CALENDAR_INTEGRATION,
            "web_push_configured": web_push_configured(),
            "duty_sync_push_poller_started": push_poller_started,
        },
    )


app.include_router(home_router)
app.include_router(system_router)
app.include_router(message_router)
app.include_router(calendar_router)
app.include_router(feedback_router)
app.include_router(study_router)
app.include_router(undo_router)
