from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.logging_service import log_event
from services.study_service import answer_mcq, get_idle_study_cards, handle_study_action, open_study_card


router = APIRouter()


@router.post("/study/cards")
async def handle_study_cards(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(get_idle_study_cards(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/study/cards", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/study/open")
async def handle_study_open(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        content_item_id = data.get("content_item_id")
        card_type = data.get("card_type", "practice")
        return JSONResponse(open_study_card(session_id, content_item_id, card_type))
    except Exception as exc:
        log_event("route_error", payload={"route": "/study/open", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/study/answer")
async def handle_study_answer(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        content_item_id = data.get("content_item_id")
        selected_option = data.get("selected_option")
        return JSONResponse(answer_mcq(session_id, content_item_id, selected_option))
    except Exception as exc:
        log_event("route_error", payload={"route": "/study/answer", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/study/action")
async def handle_study_action_route(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        content_item_id = data.get("content_item_id")
        action = data.get("action")
        return JSONResponse(handle_study_action(session_id, content_item_id, action))
    except Exception as exc:
        log_event("route_error", payload={"route": "/study/action", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})
