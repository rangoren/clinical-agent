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
