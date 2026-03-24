import re
from urllib.parse import urlparse

from services.trusted_source_registry import get_active_country, get_country_domains


def normalize_text(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def is_israel_relevant(user_message, user_profile=None):
    return get_active_country(user_message, user_profile=user_profile) == "Israel"


def is_local_source_url(url, user_message, user_profile=None):
    if not url:
        return False

    country = get_active_country(user_message, user_profile=user_profile)
    if not country:
        return False

    hostname = urlparse(url).netloc.lower().replace("www.", "")
    allowed_domains = get_country_domains(country)
    return any(hostname == domain or hostname.endswith(f".{domain}") for domain in allowed_domains)


def is_israeli_source_url(url):
    return is_local_source_url(url, "ישראל", user_profile={"country": "Israel"})


def preferred_local_source_bonus(url, user_message, user_profile=None):
    if is_local_source_url(url, user_message, user_profile=user_profile):
        return 20
    return 0
