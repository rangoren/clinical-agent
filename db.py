from pymongo import MongoClient
from pymongo import ASCENDING

from settings import MONGODB_URI


mongo_client = MongoClient(MONGODB_URI)
db = mongo_client["clinical_assistant"]

messages_collection = db["messages"]
principles_collection = db["principles"]
knowledge_collection = db["knowledge"]
protocols_collection = db["protocols"]
feedback_logs_collection = db["feedback_logs"]
user_profiles_collection = db["user_profiles"]
interaction_logs_collection = db["interaction_logs"]
search_cache_collection = db["search_cache"]
scheduling_drafts_collection = db["scheduling_drafts"]
scheduled_events_collection = db["scheduled_events"]
scheduling_preferences_collection = db["scheduling_preferences"]


def _ensure_indexes():
    try:
        search_cache_collection.create_index([("cache_type", ASCENDING), ("cache_key", ASCENDING)], unique=True)
        search_cache_collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
        search_cache_collection.create_index([("last_used_at", ASCENDING)])
        scheduling_drafts_collection.create_index([("session_id", ASCENDING), ("status", ASCENDING), ("created_at", ASCENDING)])
        scheduled_events_collection.create_index([("session_id", ASCENDING), ("start_at", ASCENDING), ("end_at", ASCENDING)])
        scheduling_preferences_collection.create_index([("session_id", ASCENDING)], unique=True)
    except Exception:
        pass


_ensure_indexes()
