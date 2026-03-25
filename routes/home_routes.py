from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates


router = APIRouter()
templates = Jinja2Templates(directory="templates")
APP_VERSION = "v0.2.6"


@router.get("/")
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "app_version": APP_VERSION})
