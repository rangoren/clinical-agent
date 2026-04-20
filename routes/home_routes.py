from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from settings import APP_ENV

router = APIRouter()
templates = Jinja2Templates(directory="templates")
APP_VERSION = "v0.3.195"


@router.get("/")
def home(request: Request):
    response = templates.TemplateResponse(
        "index.html",
        {"request": request, "app_version": APP_VERSION, "app_env": APP_ENV},
    )
    response.headers["X-App-Version"] = APP_VERSION
    response.headers["X-App-Env"] = APP_ENV
    return response
