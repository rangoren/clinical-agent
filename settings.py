import os
import re
import base64
from pathlib import Path

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PRODUCTION_DB_NAME = "clinical_assistant"
VALID_APP_ENVS = {"development", "staging", "production"}


def _read_bool(name, default):
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _read_env_text(name):
    raw_value = os.getenv(name, "")
    cleaned = raw_value.strip()
    if (
        len(cleaned) >= 2
        and cleaned[0] == cleaned[-1]
        and cleaned[0] in {'"', "'"}
    ):
        cleaned = cleaned[1:-1].strip()
    return cleaned


def _read_env_pem(name):
    cleaned = _read_env_text(name)
    return _normalize_pem_text(cleaned)


def _normalize_pem_text(cleaned):
    if not cleaned:
        return ""
    cleaned = cleaned.replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n").strip()
    if "BEGIN" in cleaned and "END" in cleaned:
        begin_match = re.search(r"-----BEGIN ([A-Z ]+)-----", cleaned)
        end_match = re.search(r"-----END ([A-Z ]+)-----", cleaned)
        if begin_match and end_match:
            begin_label = begin_match.group(1)
            end_label = end_match.group(1)
            body = cleaned
            body = re.sub(r"-----BEGIN [A-Z ]+-----", "", body)
            body = re.sub(r"-----END [A-Z ]+-----", "", body)
            body = re.sub(r"\s+", "", body)
            wrapped_lines = [body[index : index + 64] for index in range(0, len(body), 64)]
            return "\n".join(
                [f"-----BEGIN {begin_label}-----", *wrapped_lines, f"-----END {end_label}-----"]
            )
    return cleaned


def _to_base64url(raw_bytes):
    return base64.urlsafe_b64encode(raw_bytes).rstrip(b"=").decode()


def _normalize_web_push_private_key(name):
    cleaned = _normalize_pem_text(_read_env_text(name))
    if not cleaned:
        return "", ""
    try:
        if "BEGIN" in cleaned and "PRIVATE KEY" in cleaned:
            key = serialization.load_pem_private_key(cleaned.encode(), password=None)
        else:
            padded = cleaned + ("=" * ((4 - len(cleaned) % 4) % 4))
            decoded = base64.urlsafe_b64decode(padded.encode())
            if len(decoded) == 32:
                private_value = int.from_bytes(decoded, "big")
                key = ec.derive_private_key(private_value, ec.SECP256R1())
            else:
                key = serialization.load_der_private_key(decoded, password=None)
        private_raw = key.private_numbers().private_value.to_bytes(32, "big")
        public_numbers = key.public_key().public_numbers()
        public_raw = b"\x04" + public_numbers.x.to_bytes(32, "big") + public_numbers.y.to_bytes(32, "big")
        return _to_base64url(private_raw), _to_base64url(public_raw)
    except Exception:
        return cleaned, ""


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
WEB_PUSH_PRIVATE_KEY, _DERIVED_WEB_PUSH_PUBLIC_KEY = _normalize_web_push_private_key("WEB_PUSH_PRIVATE_KEY")
WEB_PUSH_PUBLIC_KEY = _DERIVED_WEB_PUSH_PUBLIC_KEY or _read_env_text("WEB_PUSH_PUBLIC_KEY")
WEB_PUSH_SUBJECT = _read_env_text("WEB_PUSH_SUBJECT")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "").strip()
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "").strip()
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "").strip().rstrip("/")

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
