from dotenv import load_dotenv
from pymongo import MongoClient
import os

load_dotenv()

mongo_uri = os.getenv("MONGODB_URI")

print("URI loaded:", mongo_uri[:40] + "...")

client = MongoClient(mongo_uri)

try:
    result = client.admin.command("ping")
    print("Mongo connected successfully:", result)
except Exception as e:
    print("Mongo connection failed:")
    print(e)