from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.logging_service import log_event
from services.message_handler_service import continue_onboarding, get_session_state, process_message, reset_session, start_clean_chat_mode


router = APIRouter()


@router.post("/message")
async def handle_message(request: Request):
    try:
        data = await request.json()
        user_message = data.get("message", "").strip()
        session_id = data.get("session_id")
        return JSONResponse(process_message(user_message=user_message, session_id=session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/message", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/session-state")
async def handle_session_state(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(get_session_state(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/session-state", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/continue-onboarding")
async def handle_continue_onboarding(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(continue_onboarding(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/continue-onboarding", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/chat-mode")
async def handle_chat_mode(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(start_clean_chat_mode(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/chat-mode", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/reset-session")
async def handle_reset_session(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(reset_session(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/reset-session", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})
