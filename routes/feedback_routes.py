from bson import ObjectId
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.feedback_service import apply_feedback
from services.logging_service import log_event


router = APIRouter()


@router.post("/feedback")
async def feedback(request: Request):
    try:
        data = await request.json()
        message_id = data.get("message_id")
        direction = data.get("direction")

        if not message_id or direction not in {"up", "down"}:
            return JSONResponse({"status": "invalid request"})

        return JSONResponse(apply_feedback(ObjectId(message_id), direction))
    except Exception as exc:
        log_event("route_error", payload={"route": "/feedback", "error": str(exc)}, level="error")
        return JSONResponse({"status": f"error: {str(exc)}"})
