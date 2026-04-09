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


def append_textbook_page_cache(cache_key, pages, metadata=None):
    existing = get_textbook_cache(cache_key) or {}
    existing_payload = existing.get("payload") or {}
    existing_pages = existing_payload.get("pages") or []
    by_page = {entry.get("page"): entry for entry in existing_pages if entry.get("page")}

    for entry in pages:
        if entry.get("page"):
            by_page[entry["page"]] = entry

    merged_pages = [by_page[page] for page in sorted(by_page)]
    payload = {
        "book_id": existing_payload.get("book_id") or (metadata or {}).get("book_id") or "gabbe_9",
        "pages": merged_pages,
        "page_count": len(merged_pages),
    }
    if metadata:
        payload.update(metadata)
    return save_textbook_cache(cache_key, payload)


def get_textbook_page_cache_progress(cache_key):
    cached = get_textbook_cache(cache_key) or {}
    payload = cached.get("payload") or {}
    pages = payload.get("pages") or []
    if not pages:
        return {
            "page_count": 0,
            "cached_through_page": 0,
            "total_pages": payload.get("total_pages"),
            "updated_at": cached.get("updated_at"),
        }

    last_page = max((entry.get("page") or 0) for entry in pages)
    return {
        "page_count": len(pages),
        "cached_through_page": last_page,
        "total_pages": payload.get("total_pages"),
        "updated_at": cached.get("updated_at"),
    }
