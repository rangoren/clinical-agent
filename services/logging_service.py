import logging
from datetime import datetime

from db import interaction_logs_collection


logger = logging.getLogger("clinical_assistant")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def log_event(event_type, session_id=None, payload=None, level="info"):
    event = {
        "event_type": event_type,
        "session_id": session_id,
        "payload": payload or {},
        "created_at": datetime.utcnow(),
    }

    try:
        interaction_logs_collection.insert_one(event)
    except Exception:
        pass

    log_fn = getattr(logger, level, logger.info)
    try:
        log_fn("%s | session=%s | payload=%s", event_type, session_id, event["payload"])
    except Exception:
        logger.info("%s | session=%s", event_type, session_id)
