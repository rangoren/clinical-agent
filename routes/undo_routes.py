from fastapi import APIRouter, Request

from services.undo_service import undo_last_saved


router = APIRouter()


@router.post("/undo")
async def undo(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return {"status": undo_last_saved(session_id)}
    except Exception as exc:
        return {"status": f"error: {str(exc)}"}
