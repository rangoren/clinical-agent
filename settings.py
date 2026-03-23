import os

from dotenv import load_dotenv


load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")

if not ANTHROPIC_API_KEY:
    raise ValueError("Missing ANTHROPIC_API_KEY in .env file")

if not MONGODB_URI:
    raise ValueError("Missing MONGODB_URI in .env file")
