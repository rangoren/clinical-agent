import json
import re

from llm_client import client
from settings import ANTHROPIC_MODEL


GREETING_TERMS = {"hi", "hello", "hey", "thanks", "thank you", "good morning", "good evening"}
MEMORY_DIRECTIVE_PREFIXES = (
    "remember that",
    "save this",
    "save that",
    "use this",
    "from now on",
    "please remember",
)
PROTOCOL_HINTS = ("protocol", "department", "hospital", "our unit", "our practice", "local policy")
PRINCIPLE_HINTS = ("always", "never", "when i ask", "please answer", "in replies", "respond like")
LOCAL_PROTOCOL_PATTERNS = (
    "at our hospital",
    "in our hospital",
    "our hospital",
    "in our department",
    "our department",
    "in our unit",
    "our unit",
    "our protocol",
    "we give",
    "we use",
    "we do",
)
CLINICAL_HINTS = (
    "patient",
    "pregnant",
    "bleeding",
    "bp",
    "blood pressure",
    "weeks",
    "ga",
    "fever",
    "pain",
    "headache",
    "ultrasound",
    "management",
    "next step",
    "differential",
    "pap",
    "pap smear",
    "paps smear",
    "hpv",
    "screening",
    "cervix",
    "cervical",
    "smear",
)

PROFILE_OR_ACCOUNT_HINTS = (
    "profile",
    "training level",
    "training stage",
    "residency year",
    "what do you know about me",
    "what do you have saved",
    "what is saved",
    "saved for me",
    "remember me as",
    "i am r",
    "i'm r",
    "year resident",
)


def build_intent_classifier_prompt(user_message, recent_context=""):
    return f"""
You are an intent classifier for a medical assistant.

Your task is to classify the user's latest message into exactly one label.

Allowed labels:
- clinical_consult
- general_chat
- principle
- knowledge
- protocol

Definitions:

clinical_consult:
A clinical question, case, interpretation request, next-step decision, risk assessment, or follow-up on a patient scenario.

general_chat:
Greeting, thanks, casual chat, question about the tool, or non-clinical conversation.

principle:
A general instruction about how the assistant should think or respond.

knowledge:
A medical fact, rule, insight, or reusable clinical information that should be stored as knowledge.

protocol:
A department, hospital, local, or team-specific way of practicing medicine or making decisions.

Confidence:
- high
- medium
- low

Rules:
- Return exactly one label
- Return exactly one confidence value
- Do not answer the user
- Do not explain your reasoning
- Do not add extra text
- Output must be valid JSON only

Important routing policy:
- If the message is a patient case or clinical decision request, label it clinical_consult
- If uncertain between clinical_consult and a memory label, prefer clinical_consult
- Only use principle / knowledge / protocol when the user is clearly trying to teach or store something
- If uncertain between general_chat and clinical_consult, prefer clinical_consult
- For memory labels, use high confidence only when the intent is clearly explicit

Recent context:
{recent_context if recent_context else "None"}

Latest user message:
{user_message}

Return only JSON in this exact shape:
{{
  "label": "clinical_consult",
  "confidence": "high"
}}
"""


def _normalize_message(user_message):
    return re.sub(r"\s+", " ", user_message.strip().lower())


def _is_greeting_message(cleaned_message):
    return cleaned_message in GREETING_TERMS


def _looks_like_clinical_consult(cleaned_message):
    if any(hint in cleaned_message for hint in PROFILE_OR_ACCOUNT_HINTS):
        return False
    return any(hint in cleaned_message for hint in CLINICAL_HINTS) or "?" in cleaned_message


def _looks_like_local_protocol_statement(cleaned_message):
    if "?" in cleaned_message:
        return False

    has_local_marker = any(pattern in cleaned_message for pattern in LOCAL_PROTOCOL_PATTERNS)
    has_clinical_content = any(
        marker in cleaned_message
        for marker in ("mg", "dose", "iv", "txa", "tranexamic", "antibiotic", "bleeding", "cesarean", "c-section", "vaginal")
    )
    return has_local_marker and has_clinical_content


def _detect_rule_based_intent(user_message):
    cleaned_message = _normalize_message(user_message)
    if not cleaned_message:
        return {"label": "general_chat", "confidence": "high", "source": "rule"}

    if _is_greeting_message(cleaned_message):
        return {"label": "general_chat", "confidence": "high", "source": "rule"}

    if cleaned_message.startswith(MEMORY_DIRECTIVE_PREFIXES):
        if any(hint in cleaned_message for hint in PROTOCOL_HINTS):
            return {"label": "protocol", "confidence": "high", "source": "rule"}
        if any(hint in cleaned_message for hint in PRINCIPLE_HINTS):
            return {"label": "principle", "confidence": "high", "source": "rule"}
        return {"label": "knowledge", "confidence": "high", "source": "rule"}

    if any(hint in cleaned_message for hint in PROTOCOL_HINTS) and any(
        phrase in cleaned_message for phrase in ("save", "remember", "use")
    ):
        return {"label": "protocol", "confidence": "high", "source": "rule"}

    if _looks_like_local_protocol_statement(cleaned_message):
        return {"label": "protocol", "confidence": "medium", "source": "rule"}

    if any(hint in cleaned_message for hint in PRINCIPLE_HINTS) and any(
        phrase in cleaned_message for phrase in ("save", "remember", "from now on", "always")
    ):
        return {"label": "principle", "confidence": "medium", "source": "rule"}

    if _looks_like_clinical_consult(cleaned_message):
        return {"label": "clinical_consult", "confidence": "medium", "source": "rule"}

    return None


def _apply_post_classification_guards(user_message, parsed_result):
    cleaned_message = _normalize_message(user_message)
    if _looks_like_local_protocol_statement(cleaned_message):
        return {"label": "protocol", "confidence": "medium", "source": "protocol_guard"}

    if parsed_result["label"] in {"protocol", "principle", "knowledge"}:
        explicit_memory_intent = any(prefix in cleaned_message for prefix in MEMORY_DIRECTIVE_PREFIXES) or "save" in cleaned_message
        if not explicit_memory_intent:
            return {"label": "clinical_consult" if _looks_like_clinical_consult(cleaned_message) else "general_chat", "confidence": "medium", "source": "guard"}

    if parsed_result["label"] == "general_chat" and _looks_like_clinical_consult(cleaned_message):
        return {"label": "clinical_consult", "confidence": "medium", "source": "guard"}

    return parsed_result


def classify_message_intent(user_message, chat_history):
    rule_based = _detect_rule_based_intent(user_message)
    if rule_based and rule_based["confidence"] == "high":
        return rule_based

    recent_context_items = chat_history[-4:] if chat_history else []
    recent_context = "\n".join(f"{item['role']}: {item['content']}" for item in recent_context_items)
    prompt = build_intent_classifier_prompt(user_message=user_message, recent_context=recent_context)

    try:
        response = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=120,
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        )
        parsed = json.loads(response.content[0].text.strip())
    except Exception:
        if rule_based:
            return rule_based
        return {"label": "general_chat", "confidence": "low", "source": "fallback"}

    allowed_labels = {"clinical_consult", "general_chat", "principle", "knowledge", "protocol"}
    allowed_confidence = {"high", "medium", "low"}

    label = parsed.get("label")
    confidence = parsed.get("confidence")
    if label not in allowed_labels or confidence not in allowed_confidence:
        if rule_based:
            return rule_based
        return {"label": "general_chat", "confidence": "low", "source": "fallback"}

    result = {"label": label, "confidence": confidence, "source": "llm"}
    if rule_based and rule_based["label"] == "clinical_consult" and result["label"] != "clinical_consult":
        result = {"label": "clinical_consult", "confidence": "medium", "source": "rule_override"}

    return _apply_post_classification_guards(user_message, result)
