from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse


router = APIRouter()
templates = Jinja2Templates(directory="templates")
APP_VERSION = "v0.3.10"


@router.get("/")
def home(request: Request):
    response = templates.TemplateResponse("index.html", {"request": request, "app_version": APP_VERSION})
    response.headers["X-App-Version"] = APP_VERSION
    return response
