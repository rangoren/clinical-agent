from services.memory_service import delete_last_knowledge, delete_last_principle, delete_last_protocol
from services.logging_service import log_event


_last_saved_items_by_session = {}


def record_last_saved(session_id, item_type, text):
    if not session_id:
        return
    _last_saved_items_by_session[session_id] = {"type": item_type, "text": text}


def clear_last_saved(session_id):
    if not session_id:
        return
    _last_saved_items_by_session.pop(session_id, None)


def undo_last_saved(session_id):
    if not session_id:
        return "missing session_id"

    last_saved_item = _last_saved_items_by_session.get(session_id, {})
    item_type = last_saved_item.get("type")
    text = last_saved_item.get("text")

    if not item_type or not text:
        return "nothing to undo"

    if item_type == "protocol":
        delete_last_protocol(text)
        clear_last_saved(session_id)
        log_event("undo_applied", session_id, {"item_type": item_type})
        return "undone protocol"

    if item_type == "knowledge":
        delete_last_knowledge(text)
        clear_last_saved(session_id)
        log_event("undo_applied", session_id, {"item_type": item_type})
        return "undone knowledge"

    if item_type == "principle":
        delete_last_principle(text)
        clear_last_saved(session_id)
        log_event("undo_applied", session_id, {"item_type": item_type})
        return "undone principle"

    clear_last_saved(session_id)
    return "nothing to undo"
