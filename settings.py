import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PRODUCTION_DB_NAME = "clinical_assistant"
VALID_APP_ENVS = {"development", "staging", "production"}


def _read_bool(name, default):
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


load_dotenv(BASE_DIR / ".env")

APP_ENV = os.getenv("APP_ENV", "production").strip().lower()
if APP_ENV not in VALID_APP_ENVS:
    raise ValueError(f"Invalid APP_ENV '{APP_ENV}'. Expected one of: development, staging, production.")

env_specific_file = BASE_DIR / f".env.{APP_ENV}"
if env_specific_file.exists():
    load_dotenv(env_specific_file, override=True)

APP_ENV = os.getenv("APP_ENV", APP_ENV).strip().lower()
if APP_ENV not in VALID_APP_ENVS:
    raise ValueError(f"Invalid APP_ENV '{APP_ENV}'. Expected one of: development, staging, production.")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", DEFAULT_PRODUCTION_DB_NAME).strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
APP_BASE_URL = os.getenv("APP_BASE_URL", "").rstrip("/")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", f"{APP_BASE_URL}/calendar/google/callback" if APP_BASE_URL else "")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Jerusalem")
ALLOW_NON_PROD_PROD_DB = _read_bool("ALLOW_NON_PROD_PROD_DB", False)
ENABLE_EXTERNAL_SIDE_EFFECTS = _read_bool("ENABLE_EXTERNAL_SIDE_EFFECTS", APP_ENV == "production")
ENABLE_GOOGLE_CALENDAR_INTEGRATION = _read_bool("ENABLE_GOOGLE_CALENDAR_INTEGRATION", APP_ENV == "production")

if not ANTHROPIC_API_KEY:
    raise ValueError("Missing ANTHROPIC_API_KEY in environment configuration.")

if not MONGODB_URI:
    raise ValueError("Missing MONGODB_URI in environment configuration.")

if not MONGODB_DB_NAME:
    raise ValueError("Missing MONGODB_DB_NAME in environment configuration.")

if APP_ENV != "production" and MONGODB_DB_NAME == DEFAULT_PRODUCTION_DB_NAME and not ALLOW_NON_PROD_PROD_DB:
    raise ValueError(
        "Refusing to start non-production APP_ENV against the default production DB name. "
        "Set MONGODB_DB_NAME to a non-production database, or explicitly set ALLOW_NON_PROD_PROD_DB=true."
    )
