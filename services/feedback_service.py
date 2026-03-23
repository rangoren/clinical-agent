from services.chat_service import get_message_by_id, save_feedback_log
from services.logging_service import log_event
from services.memory_service import (
    decrease_knowledge_weight,
    decrease_protocol_weight,
    increase_knowledge_weight,
    increase_protocol_weight,
)


def apply_feedback(message_id, direction):
    message_doc = get_message_by_id(message_id)
    if not message_doc:
        return {"status": "message not found"}

    metadata = message_doc.get("metadata", {})
    used_knowledge = metadata.get("used_knowledge", [])
    used_protocols = metadata.get("used_protocols", [])

    save_feedback_log(
        message_id=str(message_id),
        direction=direction,
        used_knowledge=used_knowledge,
        used_protocols=used_protocols,
    )
    log_event(
        "feedback_applied",
        payload={
            "message_id": str(message_id),
            "direction": direction,
            "knowledge_count": len(used_knowledge),
            "protocol_count": len(used_protocols),
        },
    )

    if direction == "up":
        for item in used_knowledge:
            increase_knowledge_weight(item, 1)
        for item in used_protocols:
            increase_protocol_weight(item, 1)
        return {"status": "marked useful"}

    for item in used_knowledge:
        decrease_knowledge_weight(item, 1)
    for item in used_protocols:
        decrease_protocol_weight(item, 1)

    return {"status": "marked off"}
