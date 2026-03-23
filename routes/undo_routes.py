from fastapi import APIRouter, Request

from services.logging_service import log_event
from services.undo_service import undo_last_saved


router = APIRouter()


@router.post("/undo")
async def undo(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return {"status": undo_last_saved(session_id)}
    except Exception as exc:
        log_event("route_error", payload={"route": "/undo", "error": str(exc)}, level="error")
        return {"status": f"error: {str(exc)}"}
