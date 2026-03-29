from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.logging_service import log_event
from services.message_handler_service import continue_onboarding, get_session_state, process_message, reset_session, start_clean_chat_mode
from services.scheduling_service import build_scheduling_welcome, confirm_scheduling_draft, dismiss_scheduling_draft, handle_scheduling_message
from services.study_service import get_idle_study_cards
from settings import APP_ENV


router = APIRouter()


def _friendly_route_error_reply(exc):
    message = str(exc or "").strip()
    normalized = message.lower()

    if any(marker in normalized for marker in ("overloaded", "overloaded_error", "529", "rate limit", "timeout")):
        friendly = "I’m a bit busy right now. Please try again in a few seconds."
        if APP_ENV != "production":
            return f"{friendly}\n\nDEV error: {exc.__class__.__name__}: {message}"
        return friendly

    friendly = "Something went wrong while generating the reply. Please try again."
    if APP_ENV != "production":
        return f"{friendly}\n\nDEV error: {exc.__class__.__name__}: {message}"
    return friendly


@router.post("/message")
async def handle_message(request: Request):
    try:
        data = await request.json()
        user_message = data.get("message", "").strip()
        session_id = data.get("session_id")
        app_mode = data.get("app_mode", "clinical")
        if app_mode == "scheduling":
            return JSONResponse(handle_scheduling_message(session_id=session_id, user_message=user_message))
        return JSONResponse(process_message(user_message=user_message, session_id=session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/message", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})


@router.post("/session-state")
async def handle_session_state(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        app_mode = data.get("app_mode", "clinical")
        if app_mode == "scheduling":
            return JSONResponse({"state": "ready", "needs_onboarding": False, "reply": build_scheduling_welcome(session_id)})
        session_state = get_session_state(session_id)
        if session_state.get("state") == "ready" and not session_state.get("reply"):
            session_state.update(get_idle_study_cards(session_id))
        return JSONResponse(session_state)
    except Exception as exc:
        log_event("route_error", payload={"route": "/session-state", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})


@router.post("/continue-onboarding")
async def handle_continue_onboarding(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(continue_onboarding(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/continue-onboarding", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})


@router.post("/chat-mode")
async def handle_chat_mode(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(start_clean_chat_mode(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/chat-mode", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})


@router.post("/reset-session")
async def handle_reset_session(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(reset_session(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/reset-session", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})


@router.post("/scheduling/confirm")
async def handle_scheduling_confirm(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        draft_id = data.get("draft_id")
        selected_calendar_id = data.get("selected_calendar_id")
        return JSONResponse(
            confirm_scheduling_draft(
                session_id=session_id,
                draft_id=draft_id,
                selected_calendar_id=selected_calendar_id,
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/scheduling/confirm", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})


@router.post("/scheduling/dismiss")
async def handle_scheduling_dismiss(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        draft_id = data.get("draft_id")
        return JSONResponse(dismiss_scheduling_draft(session_id=session_id, draft_id=draft_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/scheduling/dismiss", "error": str(exc)}, level="error")
        return JSONResponse({"reply": _friendly_route_error_reply(exc)})
