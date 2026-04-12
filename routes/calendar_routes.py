from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from settings import APP_BASE_URL
from services.google_calendar_service import (
    begin_google_calendar_connect,
    complete_google_calendar_connect,
    disconnect_google_calendar,
    get_google_calendar_status,
)
from services.duty_sync_service import (
    approve_pending_duty_review,
    connect_duty_sheet,
    disconnect_duty_sheet,
    get_duty_sync_status,
    ignore_pending_duty_review,
    toggle_pending_review_change,
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
            log_event("google_calendar_callback_error", payload={"reason": "provider_error", "error": error}, level="error")
            return RedirectResponse(url=f"{base_redirect}?calendar_status=error")
        result = complete_google_calendar_connect(code=code, state=state)
        if result.get("status") == "connected":
            return RedirectResponse(url=f"{base_redirect}?calendar_status=connected&provider=google")
        log_event(
            "google_calendar_callback_error",
            session_id=result.get("session_id"),
            payload={"reason": result.get("status"), "reply": result.get("reply")},
            level="error",
        )
        return RedirectResponse(url=f"{base_redirect}?calendar_status=error")
    except Exception as exc:
        log_event("google_calendar_callback_error", payload={"reason": "exception", "error": str(exc)}, level="error")
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


@router.post("/calendar/duty-sync/status")
async def handle_duty_sync_status(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(get_duty_sync_status(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/status", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/connect")
async def handle_duty_sync_connect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        sheet_url = data.get("sheet_url")
        full_name = data.get("full_name")
        return JSONResponse(connect_duty_sheet(session_id, sheet_url=sheet_url, full_name=full_name))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/connect", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/disconnect")
async def handle_duty_sync_disconnect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(disconnect_duty_sheet(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/disconnect", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/approve")
async def handle_duty_sync_review_approve(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            approve_pending_duty_review(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/approve", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/ignore")
async def handle_duty_sync_review_ignore(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            ignore_pending_duty_review(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/ignore", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/toggle-item")
async def handle_duty_sync_review_toggle_item(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            toggle_pending_review_change(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                change_key=data.get("change_key"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/toggle-item", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})
