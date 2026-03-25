from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from settings import APP_BASE_URL
from services.google_calendar_service import (
    begin_google_calendar_connect,
    complete_google_calendar_connect,
    disconnect_google_calendar,
    get_google_calendar_status,
)
from services.logging_service import log_event


router = APIRouter()


@router.post("/calendar/status")
async def handle_calendar_status(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        provider = data.get("provider", "google")
        if provider != "google":
            return JSONResponse({"connected": False, "available": False, "provider": provider})
        return JSONResponse(get_google_calendar_status(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/status", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/connect/google")
async def handle_google_calendar_connect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(begin_google_calendar_connect(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/connect/google", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.get("/calendar/google/callback")
async def handle_google_calendar_callback(code: str = "", state: str = "", error: str = ""):
    base_redirect = APP_BASE_URL or "/"
    try:
        if error:
            return RedirectResponse(url=f"{base_redirect}?calendar_status=error")
        result = complete_google_calendar_connect(code=code, state=state)
        if result.get("status") == "connected":
            return RedirectResponse(url=f"{base_redirect}?calendar_status=connected&provider=google")
        return RedirectResponse(url=f"{base_redirect}?calendar_status=error")
    except Exception:
        return RedirectResponse(url=f"{base_redirect}?calendar_status=error")


@router.post("/calendar/disconnect/google")
async def handle_google_calendar_disconnect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(disconnect_google_calendar(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/disconnect/google", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})
