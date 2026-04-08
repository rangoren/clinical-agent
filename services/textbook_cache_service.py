from datetime import datetime

from db import textbook_cache_collection


def get_textbook_cache(cache_key):
    doc = textbook_cache_collection.find_one({"cache_key": cache_key}, {"_id": 0})
    if not doc:
        return None
    return doc


def save_textbook_cache(cache_key, payload):
    document = {
        "cache_key": cache_key,
        "payload": payload,
        "updated_at": datetime.utcnow(),
    }
    textbook_cache_collection.update_one(
        {"cache_key": cache_key},
        {"$set": document},
        upsert=True,
    )
    return document


def merge_textbook_cache_topics(cache_key, topic_entries, metadata=None):
    existing = get_textbook_cache(cache_key) or {}
    existing_payload = existing.get("payload") or {}
    existing_topics = existing_payload.get("topics") or []
    by_topic = {entry.get("topic"): entry for entry in existing_topics if entry.get("topic")}

    for entry in topic_entries:
        if entry.get("topic"):
            by_topic[entry["topic"]] = entry

    merged_topics = list(by_topic.values())
    payload = {
        "book_id": existing_payload.get("book_id") or (metadata or {}).get("book_id") or "gabbe_9",
        "topics": merged_topics,
        "topic_count": len(merged_topics),
        "mapped_count": sum(1 for item in merged_topics if item.get("status") == "mapped"),
        "unmapped_count": sum(1 for item in merged_topics if item.get("status") != "mapped"),
    }
    if metadata:
        payload.update(metadata)
    return save_textbook_cache(cache_key, payload)
