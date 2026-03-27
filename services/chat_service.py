from datetime import datetime

from db import feedback_logs_collection, messages_collection


def load_chat(session_id, limit=None):
    docs = list(
        messages_collection.find({"session_id": session_id})
        .sort("created_at", -1)
        .limit(limit or 0)
    )
    docs.reverse()
    return [{"role": doc["role"], "content": doc["content"]} for doc in docs]


def save_message(role, content, session_id, metadata=None):
    doc = {
        "role": role,
        "content": content,
        "session_id": session_id,
        "created_at": datetime.utcnow(),
    }
    if metadata:
        doc["metadata"] = metadata

    result = messages_collection.insert_one(doc)
    return str(result.inserted_id)


def delete_messages_for_session(session_id):
    messages_collection.delete_many({"session_id": session_id})


def get_message_by_id(message_id):
    return messages_collection.find_one({"_id": message_id})


def save_feedback_log(message_id, direction, used_knowledge, used_protocols, used_sources=None):
    feedback_logs_collection.insert_one(
        {
            "message_id": message_id,
            "direction": direction,
            "used_knowledge": used_knowledge,
            "used_protocols": used_protocols,
            "used_sources": used_sources or [],
            "created_at": datetime.utcnow(),
        }
    )
