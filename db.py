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
calendar_connections_collection = db["calendar_connections"]
user_calendars_collection = db["user_calendars"]
oauth_states_collection = db["oauth_states"]
study_content_collection = db["study_content"]
study_user_state_collection = db["study_user_state"]


def _ensure_indexes():
    try:
        messages_collection.create_index([("session_id", ASCENDING), ("created_at", ASCENDING)])
        search_cache_collection.create_index([("cache_type", ASCENDING), ("cache_key", ASCENDING)], unique=True)
        search_cache_collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
        search_cache_collection.create_index([("last_used_at", ASCENDING)])
        user_profiles_collection.create_index([("session_id", ASCENDING)], unique=True)
        scheduling_drafts_collection.create_index([("session_id", ASCENDING), ("status", ASCENDING), ("created_at", ASCENDING)])
        scheduled_events_collection.create_index([("session_id", ASCENDING), ("start_at", ASCENDING), ("end_at", ASCENDING)])
        scheduling_preferences_collection.create_index([("session_id", ASCENDING)], unique=True)
        calendar_connections_collection.create_index([("session_id", ASCENDING), ("provider", ASCENDING)], unique=True)
        user_calendars_collection.create_index([("session_id", ASCENDING), ("provider", ASCENDING), ("provider_calendar_id", ASCENDING)], unique=True)
        oauth_states_collection.create_index([("state", ASCENDING)], unique=True)
        oauth_states_collection.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
        study_content_collection.create_index([("id", ASCENDING)], unique=True)
        study_content_collection.create_index([("approved_for_stage_b", ASCENDING), ("topic", ASCENDING), ("item_type", ASCENDING)])
        study_user_state_collection.create_index([("session_id", ASCENDING)], unique=True)
        interaction_logs_collection.create_index([("session_id", ASCENDING), ("event_type", ASCENDING), ("created_at", ASCENDING)])
    except Exception:
        pass


_ensure_indexes()
