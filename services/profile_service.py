import re
from datetime import datetime

from db import user_profiles_collection


TRAINING_STAGE_ALIASES = {
    "resident": "resident",
    "residency": "resident",
    "specialist": "specialist",
    "attending": "specialist",
    "consultant": "specialist",
    "fellowship": "fellowship",
    "fellow": "fellowship",
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
        "created_at": now,
        "updated_at": now,
    }
    result = user_profiles_collection.insert_one(doc)
    return str(result.inserted_id)


def update_user_profile(session_id, updates):
    payload = dict(updates)
    payload["updated_at"] = datetime.utcnow()
    user_profiles_collection.update_one({"session_id": session_id}, {"$set": payload})


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
    cleaned = value.strip().lower()
    match = re.search(r"([1-7])", cleaned)
    if not match:
        return None
    return f"R{match.group(1)}"


def normalize_country(value):
    cleaned = re.sub(r"\s+", " ", value.strip())
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered in COUNTRY_PLACEHOLDER_WORDS:
        return None

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
    create_user_profile(
        session_id,
        {
            "onboarding_step": "country",
            "onboarding_done": False,
        },
    )


def build_onboarding_intro():
    return (
        "Hi, I’m your OB-GYN clinical assistant.<br><br>"
        "I’m here to help you think through cases, sharpen risk assessment, and suggest focused next steps.<br><br>"
        "Before we start, I need a few short details so I can adapt the level and style of my answers to you.<br><br>"
        "<b>First question:</b><br>"
        "Which country are you currently working in?"
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
        "For example: R1, R2, R3, R4, R5"
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
        "Onboarding complete.<br><br>"
        "I’ll adapt responses using your profile:<br>"
        f"Country: {country}<br>"
        f"Stage: {training_stage}{residency_line}<br>"
        f"Focus: {subspecialty}<br>"
        f"Style: {answer_style}<br><br>"
        "You can start with any clinical question."
    )


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
                    "R1 / R2 / R3 / R4 / R5"
                ),
                "completed": False,
                "intent": "onboarding_retry",
            }

        update_user_profile(
            session_id,
            {
                "residency_year": residency_year,
                "onboarding_step": "subspecialty",
            },
        )
        return {"reply": _build_subspecialty_question(), "completed": False, "intent": "onboarding_question"}

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
    residency_year = profile.get("residency_year") or "Not applicable"
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
