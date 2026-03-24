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


def _ensure_indexes():
    try:
        search_cache_collection.create_index([("cache_type", ASCENDING), ("cache_key", ASCENDING)], unique=True)
        search_cache_collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
        search_cache_collection.create_index([("last_used_at", ASCENDING)])
    except Exception:
        pass


_ensure_indexes()
