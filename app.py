from fastapi import FastAPI

from routes.calendar_routes import router as calendar_router
from routes.feedback_routes import router as feedback_router
from routes.home_routes import router as home_router
from routes.message_routes import router as message_router
from routes.study_routes import router as study_router
from routes.undo_routes import router as undo_router
from routes.home_routes import APP_VERSION
from services.logging_service import log_event
from services.study_service import ensure_study_content_seed


app = FastAPI()


@app.on_event("startup")
def log_startup_version():
    ensure_study_content_seed()
    log_event("app_startup", payload={"app_version": APP_VERSION})


app.include_router(home_router)
app.include_router(message_router)
app.include_router(calendar_router)
app.include_router(feedback_router)
app.include_router(study_router)
app.include_router(undo_router)
