import re


PROFILE_STATUS_PATTERNS = (
    "what residency year do you have saved",
    "what residency year is saved",
    "what year residency am i",
    "what year resident am i",
    "what year am i in residency",
    "which year residency am i",
    "which year resident am i",
    "what do you have saved for me",
    "what you have saved on me",
    "what do you have saved on me",
    "what have you saved on me",
    "what have you saved for me",
    "what do you know about my training",
    "what do you know about me",
    "what is saved in my profile",
    "what's saved in my profile",
    "what training level do you have",
    "what training stage do you have",
)

RESIDENCY_YEAR_STATUS_PATTERNS = (
    "what residency year do you have saved",
    "what residency year is saved",
    "which residency year do you have",
    "which residency year is saved",
    "what year residency am i",
    "what year resident am i",
    "what year am i in residency",
    "which year residency am i",
    "which year resident am i",
)

TRAINING_STAGE_STATUS_PATTERNS = (
    "what training level do you have",
    "what training stage do you have",
    "what do you know about my training",
)

PROFILE_UPDATE_FIRST_PERSON_MARKERS = (
    "i am",
    "i'm",
    "im ",
    "my residency year is",
    "i am in",
    "i’m",
    "אני",
)

NON_PROFILE_CHAT_MARKERS = {
    "what do you think",
    "what should i do",
    "can you help",
    "patient",
    "bleeding",
    "pain",
    "pregnant",
    "bp",
    "blood pressure",
    "fever",
}


def normalize_profile_prompt(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def looks_like_residency_year_question(normalized):
    if "residency" not in normalized and "resident" not in normalized:
        return False
    year_markers = ("what year", "which year", "year am i", "am i", "do you have saved", "is saved")
    return any(marker in normalized for marker in year_markers)


def detect_profile_status_intent(user_message):
    normalized = normalize_profile_prompt(user_message)
    if not normalized:
        return None

    if any(pattern in normalized for pattern in RESIDENCY_YEAR_STATUS_PATTERNS) or looks_like_residency_year_question(normalized):
        return "residency_year"
    if any(pattern in normalized for pattern in TRAINING_STAGE_STATUS_PATTERNS):
        return "training_stage"
    if any(pattern in normalized for pattern in PROFILE_STATUS_PATTERNS):
        return "profile_status"
    if "saved" in normalized and any(token in normalized for token in (" me", "profile", "training", "residency")):
        return "profile_status"
    if "what do you know" in normalized and any(token in normalized for token in ("me", "training", "profile")):
        return "profile_status"
    return None


def looks_like_profile_update_message(user_message, extracted_fields):
    normalized = normalize_profile_prompt(user_message)
    if not extracted_fields or not normalized:
        return False
    if detect_profile_status_intent(user_message):
        return False
    if "?" in normalized:
        return False
    if any(marker in normalized for marker in NON_PROFILE_CHAT_MARKERS):
        return False

    if any(marker in normalized for marker in PROFILE_UPDATE_FIRST_PERSON_MARKERS):
        return True

    # Allow compact declarative updates like "r6" or "resident" during onboarding/profile edits.
    compact_tokens = re.findall(r"[a-z0-9\u0590-\u05ff]+", normalized)
    return len(compact_tokens) <= 6
