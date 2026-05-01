from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from settings import APP_ENV

router = APIRouter()
templates = Jinja2Templates(directory="templates")
APP_VERSION = "v0.3.389"


@router.get("/")
def home(request: Request):
    response = templates.TemplateResponse(
        "index.html",
        {"request": request, "app_version": APP_VERSION, "app_env": APP_ENV},
    )
    response.headers["X-App-Version"] = APP_VERSION
    response.headers["X-App-Env"] = APP_ENV
    return response


@router.get("/duty-sync-sw.js")
def duty_sync_service_worker():
    response = FileResponse("static/sw.js", media_type="application/javascript")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["X-App-Version"] = APP_VERSION
    return response
