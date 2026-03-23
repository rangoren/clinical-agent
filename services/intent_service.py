import json

from llm_client import client
from settings import ANTHROPIC_MODEL


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


def classify_message_intent(user_message, chat_history):
    recent_context_items = chat_history[-4:] if chat_history else []
    recent_context = "\n".join(f"{item['role']}: {item['content']}" for item in recent_context_items)
    prompt = build_intent_classifier_prompt(user_message=user_message, recent_context=recent_context)

    try:
        response = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )
        parsed = json.loads(response.content[0].text.strip())
    except Exception:
        return {"label": "general_chat", "confidence": "low"}

    allowed_labels = {"clinical_consult", "general_chat", "principle", "knowledge", "protocol"}
    allowed_confidence = {"high", "medium", "low"}

    label = parsed.get("label")
    confidence = parsed.get("confidence")
    if label not in allowed_labels or confidence not in allowed_confidence:
        return {"label": "general_chat", "confidence": "low"}

    return {"label": label, "confidence": confidence}
