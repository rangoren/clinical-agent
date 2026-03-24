from pymongo import MongoClient

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
