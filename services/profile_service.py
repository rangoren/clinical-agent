import re
from datetime import datetime

from db import user_profiles_collection
from services.profile_prompt_resolver import looks_like_profile_update_message


TRAINING_STAGE_ALIASES = {
    "resident": "resident",
    "residency": "resident",
    "מתמחה": "resident",
    "specialist": "specialist",
    "attending": "specialist",
    "consultant": "specialist",
    "מומחה": "specialist",
    "מומחית": "specialist",
    "fellowship": "fellowship",
    "fellow": "fellowship",
    "פלושיפ": "fellowship",
    "פלואושיפ": "fellowship",
    "פלו": "fellowship",
}

RESIDENCY_YEAR_WORD_ALIASES = {
    "r1": "R1",
    "first": "R1",
    "1st": "R1",
    "one": "R1",
    "firest": "R1",
    "frist": "R1",
    "yr1": "R1",
    "year 1": "R1",
    "first year": "R1",
    "r2": "R2",
    "second": "R2",
    "2nd": "R2",
    "two": "R2",
    "seccond": "R2",
    "second year": "R2",
    "year 2": "R2",
    "yr2": "R2",
    "r3": "R3",
    "third": "R3",
    "3rd": "R3",
    "three": "R3",
    "thrid": "R3",
    "third year": "R3",
    "year 3": "R3",
    "yr3": "R3",
    "r4": "R4",
    "fourth": "R4",
    "4th": "R4",
    "four": "R4",
    "forth": "R4",
    "fourth year": "R4",
    "year 4": "R4",
    "yr4": "R4",
    "r5": "R5",
    "fifth": "R5",
    "5th": "R5",
    "five": "R5",
    "fivth": "R5",
    "fifth year": "R5",
    "year 5": "R5",
    "yr5": "R5",
    "r6": "R6",
    "sixth": "R6",
    "6th": "R6",
    "six": "R6",
    "sixt": "R6",
    "sixth year": "R6",
    "year 6": "R6",
    "yr6": "R6",
    "r7": "R7",
    "seventh": "R7",
    "7th": "R7",
    "seven": "R7",
    "seventh year": "R7",
    "year 7": "R7",
    "yr7": "R7",
}

ANSWER_STYLE_ALIASES = {
    "concise": "concise",
    "brief": "concise",
    "short": "concise",
    "balanced": "balanced",
    "standard": "balanced",
    "teaching": "teaching",
    "teach": "teaching",
    "detailed": "teaching",
}

COUNTRY_PLACEHOLDER_WORDS = {
    "hello",
    "hi",
    "hey",
    "yo",
    "shalom",
    "thanks",
    "thank you",
    "ok",
    "okay",
    "start",
}

ONBOARDING_GREETING_WORDS = COUNTRY_PLACEHOLDER_WORDS | {
    "good morning",
    "good afternoon",
    "good evening",
}

COUNTRY_ALIASES = {
    "israel": "Israel",
    "ישראל": "Israel",
    "uk": "UK",
    "united kingdom": "UK",
    "england": "UK",
    "usa": "USA",
    "us": "USA",
    "united states": "USA",
    "canada": "Canada",
    "australia": "Australia",
    "new zealand": "New Zealand",
    "india": "India",
    "germany": "Germany",
    "france": "France",
    "italy": "Italy",
    "spain": "Spain",
}

SUBSPECIALTY_ALIASES = {
    "mfm": "Maternal-Fetal Medicine",
    "maternal fetal medicine": "Maternal-Fetal Medicine",
    "urogyn": "Urogynecology",
    "urogynecology": "Urogynecology",
    "fertility": "Reproductive Endocrinology and Infertility",
    "rei": "Reproductive Endocrinology and Infertility",
    "oncology": "Gynecologic Oncology",
    "gyn oncology": "Gynecologic Oncology",
    "gynaecologic oncology": "Gynecologic Oncology",
    "ultrasound": "Obstetric and Gynecologic Ultrasound",
}


def get_user_profile(session_id):
    return user_profiles_collection.find_one({"session_id": session_id})


def create_user_profile(session_id, profile_data):
    now = datetime.utcnow()
    doc = {
        "session_id": session_id,
        "country": profile_data.get("country"),
        "training_stage": profile_data.get("training_stage"),
        "residency_year": profile_data.get("residency_year"),
        "subspecialty": profile_data.get("subspecialty"),
        "answer_style": profile_data.get("answer_style"),
        "onboarding_step": profile_data.get("onboarding_step"),
        "onboarding_done": profile_data.get("onboarding_done", False),
        "chat_mode": profile_data.get("chat_mode", False),
        "last_active_at": now,
        "created_at": now,
        "updated_at": now,
    }
    result = user_profiles_collection.insert_one(doc)
    return str(result.inserted_id)


def update_user_profile(session_id, updates):
    payload = dict(updates)
    now = datetime.utcnow()
    payload["updated_at"] = now
    payload["last_active_at"] = now
    user_profiles_collection.update_one({"session_id": session_id}, {"$set": payload})


def touch_user_profile(session_id):
    if not session_id:
        return
    now = datetime.utcnow()
    user_profiles_collection.update_one(
        {"session_id": session_id},
        {"$set": {"last_active_at": now, "updated_at": now}},
    )


def delete_user_profile(session_id):
    user_profiles_collection.delete_one({"session_id": session_id})


def normalize_training_stage(value):
    cleaned = value.strip().lower()
    direct_match = TRAINING_STAGE_ALIASES.get(cleaned)
    if direct_match:
        return direct_match

    for alias, normalized in TRAINING_STAGE_ALIASES.items():
        if alias in cleaned:
            return normalized

    return None


def normalize_answer_style(value):
    cleaned = value.strip().lower()
    direct_match = ANSWER_STYLE_ALIASES.get(cleaned)
    if direct_match:
        return direct_match

    for alias, normalized in ANSWER_STYLE_ALIASES.items():
        if alias in cleaned:
            return normalized

    return None


def normalize_residency_year(value):
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    year_phrase_match = re.search(r"\b([1-7])\s*(?:st|nd|rd|th)?\s*year\b", cleaned)
    if year_phrase_match:
        return f"R{year_phrase_match.group(1)}"

    resident_phrase_match = re.search(r"\b([1-7])\s*(?:st|nd|rd|th)?\s*(?:year\s+)?resident\b", cleaned)
    if resident_phrase_match:
        return f"R{resident_phrase_match.group(1)}"
    r_format_match = re.search(r"\br\s*([1-7])\b", cleaned)
    if r_format_match:
        return f"R{r_format_match.group(1)}"

    for alias, normalized in RESIDENCY_YEAR_WORD_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", cleaned):
            return normalized

    return None


def normalize_country(value):
    cleaned = re.sub(r"\s+", " ", value.strip())
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered in COUNTRY_PLACEHOLDER_WORDS:
        return None

    direct_match = COUNTRY_ALIASES.get(lowered)
    if direct_match:
        return direct_match

    if not re.fullmatch(r"[A-Za-z][A-Za-z .,'()-]{1,59}", cleaned):
        return None

    alpha_chars = re.sub(r"[^A-Za-z]", "", cleaned)
    if len(alpha_chars) < 2:
        return None

    return cleaned


def is_onboarding_greeting(value):
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    return cleaned in ONBOARDING_GREETING_WORDS


def start_onboarding(session_id):
    existing_profile = get_user_profile(session_id)
    if existing_profile:
        update_user_profile(
            session_id,
            {
                "country": None,
                "training_stage": None,
                "residency_year": None,
                "subspecialty": None,
                "answer_style": None,
                "onboarding_step": "country",
                "onboarding_done": False,
                "chat_mode": False,
            },
        )
        return

    create_user_profile(
        session_id,
        {
            "onboarding_step": "country",
            "onboarding_done": False,
            "chat_mode": False,
        },
    )


def build_onboarding_intro():
    return (
        "Hi, I’m your OB-GYN clinical assistant.<br><br>"
        "I can adapt the level and style of answers to your training stage and setting.<br><br>"
        "You can answer naturally in one sentence, or just start asking your question and I’ll adapt as we go.<br><br>"
        "To start, tell me where you work and what stage you're in."
    )


def _build_training_stage_question():
    return (
        "<b>Next question:</b><br>"
        "What best describes your current stage?<br>"
        "Resident / Specialist / Fellowship"
    )


def _build_residency_year_question():
    return (
        "<b>Next question:</b><br>"
        "Which residency year are you in?<br>"
        "For example: R1, R2, R3, R4, R5 or first year, second year"
    )


def _build_subspecialty_question():
    return (
        "<b>Next question:</b><br>"
        "What is your subspecialty or main clinical focus?<br>"
        "If none, you can write General OB-GYN."
    )


def _build_answer_style_question():
    return (
        "<b>Last question:</b><br>"
        "What answer style do you prefer?<br>"
        "Concise / Balanced / Teaching"
    )


def build_onboarding_question(step):
    current_step = step or "country"
    if current_step == "country":
        return build_onboarding_intro()
    if current_step == "training_stage":
        return _build_training_stage_question()
    if current_step == "residency_year":
        return _build_residency_year_question()
    if current_step == "subspecialty":
        return _build_subspecialty_question()
    if current_step == "answer_style":
        return _build_answer_style_question()
    return build_onboarding_intro()


def activate_chat_mode(session_id):
    profile = get_user_profile(session_id)
    payload = {
        "country": None,
        "training_stage": None,
        "residency_year": None,
        "subspecialty": None,
        "answer_style": None,
        "onboarding_step": None,
        "onboarding_done": True,
        "chat_mode": True,
    }
    if profile:
        update_user_profile(session_id, payload)
        return

    create_user_profile(session_id, payload)


def _build_onboarding_complete_message(profile):
    training_stage = profile.get("training_stage", "not set").title()
    answer_style = profile.get("answer_style", "balanced").title()
    subspecialty = profile.get("subspecialty") or "General OB-GYN"
    country = profile.get("country") or "your setting"

    residency_line = ""
    if profile.get("residency_year"):
        residency_line = f"<br>Residency year: {profile['residency_year']}"

    return (
        "Great, I’ve got what I need.<br><br>"
        "I’m ready when you are."
    )


def infer_country_from_text(value):
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    for alias, normalized in COUNTRY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", cleaned):
            return normalized
    return None


def infer_subspecialty_from_text(value):
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    for alias, normalized in SUBSPECIALTY_ALIASES.items():
        if alias in cleaned:
            return normalized
    return None


def extract_profile_updates_from_message(user_message, profile):
    cleaned = user_message.strip()
    lowered = cleaned.lower()
    updates = {}
    extracted_fields = []

    country = infer_country_from_text(cleaned)
    if country and not profile.get("country"):
        updates["country"] = country
        extracted_fields.append("country")

    training_stage = normalize_training_stage(cleaned)
    if training_stage:
        updates["training_stage"] = training_stage
        extracted_fields.append("training_stage")

    residency_year = normalize_residency_year(cleaned)
    stage_for_year = updates.get("training_stage") or profile.get("training_stage")
    if residency_year and not stage_for_year:
        updates["training_stage"] = "resident"
        stage_for_year = "resident"
        if "training_stage" not in extracted_fields:
            extracted_fields.append("training_stage")
    if residency_year and stage_for_year == "resident":
        updates["residency_year"] = residency_year
        extracted_fields.append("residency_year")

    answer_style = normalize_answer_style(cleaned)
    if answer_style:
        updates["answer_style"] = answer_style
        extracted_fields.append("answer_style")

    subspecialty = infer_subspecialty_from_text(cleaned)
    if subspecialty:
        updates["subspecialty"] = subspecialty
        extracted_fields.append("subspecialty")
    elif any(phrase in lowered for phrase in {"general ob-gyn", "general obgyn", "general gyn", "general ob gyn"}):
        updates["subspecialty"] = "General OB-GYN"
        extracted_fields.append("subspecialty")

    if updates.get("training_stage") == "resident" and "subspecialty" not in updates and not profile.get("subspecialty"):
        updates["subspecialty"] = "General OB-GYN"

    return updates, extracted_fields


def is_profile_only_message(user_message, extracted_fields):
    return looks_like_profile_update_message(user_message, extracted_fields)


def is_general_greeting_message(user_message):
    cleaned = re.sub(r"\s+", " ", user_message.strip().lower())
    greeting_phrases = ONBOARDING_GREETING_WORDS | {"how are you", "מה שלומך", "מה נשמע", "שלום"}
    if cleaned in greeting_phrases:
        return True

    tokens = cleaned.split()
    if len(tokens) > 3:
        return False

    return cleaned in {"hi there", "hey there", "hello there", "good morning", "good evening", "good afternoon"}


def is_core_profile_complete(profile):
    training_stage = profile.get("training_stage")
    if not training_stage:
        return False
    if training_stage == "resident":
        return bool(profile.get("residency_year"))
    return True


def finalize_onboarding_profile(session_id, profile):
    updates = {
        "onboarding_done": True,
        "onboarding_step": None,
    }
    if not profile.get("answer_style"):
        updates["answer_style"] = "balanced"
    if profile.get("training_stage") == "resident" and not profile.get("subspecialty"):
        updates["subspecialty"] = "General OB-GYN"

    update_user_profile(session_id, updates)
    return get_user_profile(session_id)


def build_soft_onboarding_followup(profile):
    training_stage = profile.get("training_stage")
    if not training_stage:
        return "To tailor the level properly, are you currently a resident, specialist, or in fellowship?"
    if training_stage == "resident" and not profile.get("residency_year"):
        return "What residency year are you in right now?"
    if not profile.get("country"):
        return "Which country are you currently working in?"
    if not profile.get("answer_style"):
        return "What answer style do you prefer: concise, balanced, or teaching?"
    if training_stage != "resident" and not profile.get("subspecialty"):
        return "What is your subspecialty or main clinical focus?"
    return "Great, I’ve got what I need. I’m ready when you are."


def handle_onboarding_step(session_id, profile, user_message):
    step = profile.get("onboarding_step") or "country"
    cleaned = user_message.strip()

    if is_onboarding_greeting(cleaned):
        return {
            "reply": build_onboarding_question(step),
            "completed": False,
            "intent": "onboarding_question",
        }

    if step == "country":
        country = normalize_country(cleaned)
        if not country:
            return {
                "reply": (
                    "I still need your country so I can tailor guidance to your setting.<br><br>"
                    "Please answer with the country you work in.<br>"
                    "For example: Israel / UK / USA"
                ),
                "completed": False,
                "intent": "onboarding_retry",
            }

        update_user_profile(
            session_id,
            {
                "country": country,
                "onboarding_step": "training_stage",
            },
        )
        return {"reply": _build_training_stage_question(), "completed": False, "intent": "onboarding_question"}

    if step == "training_stage":
        training_stage = normalize_training_stage(cleaned)
        if not training_stage:
            return {
                "reply": (
                    "I didn’t catch that stage yet.<br><br>"
                    "Please answer with one of these:<br>"
                    "Resident / Specialist / Fellowship"
                ),
                "completed": False,
                "intent": "onboarding_retry",
            }

        next_step = "residency_year" if training_stage == "resident" else "subspecialty"
        update_user_profile(
            session_id,
            {
                "training_stage": training_stage,
                "residency_year": None if training_stage != "resident" else profile.get("residency_year"),
                "subspecialty": "General OB-GYN" if training_stage == "resident" else profile.get("subspecialty"),
                "onboarding_step": next_step,
            },
        )

        reply = _build_residency_year_question() if next_step == "residency_year" else _build_subspecialty_question()
        return {"reply": reply, "completed": False, "intent": "onboarding_question"}

    if step == "residency_year":
        residency_year = normalize_residency_year(cleaned)
        if not residency_year:
            return {
                "reply": (
                    "I need a residency year in a simple format.<br><br>"
                    "Please answer like:<br>"
                    "R1 / R2 / R3 / R4 / R5<br>"
                    "or first year / second year"
                ),
                "completed": False,
                "intent": "onboarding_retry",
            }

        update_user_profile(
            session_id,
            {
                "residency_year": residency_year,
                "onboarding_step": "answer_style",
            },
        )
        return {"reply": _build_answer_style_question(), "completed": False, "intent": "onboarding_question"}

    if step == "subspecialty":
        subspecialty = cleaned
        if cleaned.lower() in {"none", "general", "general obgyn", "general ob-gyn"}:
            subspecialty = "General OB-GYN"

        update_user_profile(
            session_id,
            {
                "subspecialty": subspecialty,
                "onboarding_step": "answer_style",
            },
        )
        return {"reply": _build_answer_style_question(), "completed": False, "intent": "onboarding_question"}

    if step == "answer_style":
        answer_style = normalize_answer_style(cleaned)
        if not answer_style:
            return {
                "reply": (
                    "Please choose one answer style.<br><br>"
                    "Concise / Balanced / Teaching"
                ),
                "completed": False,
                "intent": "onboarding_retry",
            }

        update_user_profile(
            session_id,
            {
                "answer_style": answer_style,
                "onboarding_step": None,
                "onboarding_done": True,
            },
        )
        updated_profile = get_user_profile(session_id)
        return {
            "reply": _build_onboarding_complete_message(updated_profile),
            "completed": True,
            "intent": "onboarding_complete",
        }

    update_user_profile(session_id, {"onboarding_step": "country", "onboarding_done": False})
    return {"reply": build_onboarding_intro(), "completed": False, "intent": "onboarding_intro"}


def build_user_profile_context(profile):
    if not profile:
        return "- No user profile available"

    country = profile.get("country") or "Not provided"
    training_stage = profile.get("training_stage") or "Not provided"
    if profile.get("training_stage") in {"specialist", "fellowship"}:
        residency_year = "Not applicable"
    else:
        residency_year = profile.get("residency_year") or "Not saved"
    subspecialty = profile.get("subspecialty") or "Not provided"
    answer_style = profile.get("answer_style") or "balanced"

    return "\n".join(
        [
            f"- Country: {country}",
            f"- Training stage: {training_stage}",
            f"- Residency year: {residency_year}",
            f"- Subspecialty: {subspecialty}",
            f"- Preferred answer style: {answer_style}",
        ]
    )
