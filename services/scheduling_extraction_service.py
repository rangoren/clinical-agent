import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from llm_client import client
from settings import ANTHROPIC_MODEL, APP_TIMEZONE
from services.logging_service import log_event


EXTRACTION_MODEL = ANTHROPIC_MODEL


def _current_context():
    now = datetime.now(ZoneInfo(APP_TIMEZONE))
    return {
        "iso_now": now.isoformat(timespec="minutes"),
        "date": now.date().isoformat(),
        "weekday": now.strftime("%A"),
        "timezone": APP_TIMEZONE,
    }


def _strip_json_fence(text):
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _safe_json_loads(text):
    cleaned = _strip_json_fence(text)
    try:
        return json.loads(cleaned)
    except Exception:
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except Exception:
            return None


def _normalize_extraction(payload):
    if not isinstance(payload, dict):
        return None

    extraction = {
        "action": str(payload.get("action") or "").lower() or None,
        "title": (payload.get("title") or "").strip() or None,
        "calendar_type": str(payload.get("calendar_type") or "").lower() or None,
        "location": (payload.get("location") or "").strip() or None,
        "is_shift": bool(payload.get("is_shift")),
        "is_bulk": bool(payload.get("is_bulk")),
        "date": (payload.get("date") or "").strip() or None,
        "source_date": (payload.get("source_date") or "").strip() or None,
        "target_date": (payload.get("target_date") or "").strip() or None,
        "start_time": (payload.get("start_time") or "").strip() or None,
        "end_time": (payload.get("end_time") or "").strip() or None,
        "duration_minutes": payload.get("duration_minutes"),
        "bulk_dates": payload.get("bulk_dates") or [],
        "missing_fields": [str(item).lower() for item in (payload.get("missing_fields") or []) if item],
        "references_previous": bool(payload.get("references_previous")),
        "confidence": str(payload.get("confidence") or "low").lower(),
        "notes": (payload.get("notes") or "").strip() or None,
    }

    if extraction["action"] not in {"create", "update", "delete", "bulk_create", "bulk_delete", None}:
        extraction["action"] = None
    if extraction["calendar_type"] not in {"work", "kids", "family", "personal", "shared", None}:
        extraction["calendar_type"] = None
    if extraction["confidence"] not in {"high", "medium", "low"}:
        extraction["confidence"] = "low"
    if not isinstance(extraction["bulk_dates"], list):
        extraction["bulk_dates"] = []
    extraction["bulk_dates"] = [str(item).strip() for item in extraction["bulk_dates"] if str(item).strip()]

    try:
        if extraction["duration_minutes"] is not None:
            extraction["duration_minutes"] = int(extraction["duration_minutes"])
    except Exception:
        extraction["duration_minutes"] = None

    return extraction


def extract_scheduling_intent(user_message, pending_message=None, last_reference=None):
    context = _current_context()
    combined_message = user_message.strip()
    if pending_message:
        combined_message = f"{pending_message.strip()}\nFollow-up message: {user_message.strip()}".strip()
    last_reference_block = "- None"
    if last_reference:
        last_reference_block = (
            f"- title: {last_reference.get('title')}\n"
            f"- start_at: {last_reference.get('start_at')}\n"
            f"- end_at: {last_reference.get('end_at')}\n"
            f"- calendar_type: {last_reference.get('calendar_type')}\n"
            f"- location: {last_reference.get('location') or ''}"
        )

    system_prompt = f"""
You extract structured scheduling intent for a personal scheduling copilot.

Current local context:
- Timezone: {context['timezone']}
- Current date: {context['date']}
- Current local datetime: {context['iso_now']}
- Current weekday: {context['weekday']}

Most recent scheduling reference in the conversation:
{last_reference_block}

Return JSON only.

Rules:
- Understand Hebrew and English naturally.
- Resolve relative dates like today, tomorrow, next Thursday, יום חמישי הקרוב into ISO dates.
- If the user is continuing a previous message, combine the intent naturally.
- Decide whether the user is referring to the most recent scheduling reference.
- Detect action as one of: create, update, delete, bulk_create, bulk_delete.
- For shifts/on-call/night duty/תורנות/תורנויות/תורניות:
  - is_shift = true
  - title should be exactly "תורנות"
  - default location should be "שיבא" unless the user explicitly gave another location
  - default start_time should be "08:00"
  - default end_time should be "09:00"
  - default duration_minutes should be 1500
- For "תורנות חצי" / "חצי תורנות" / "half shift":
  - title should be exactly "תורנות חצי"
  - default location should be "שיבא" unless the user explicitly gave another location
  - default start_time should be "15:00"
  - default end_time should be "23:00"
  - default duration_minutes should be 480
- For "מחלקות" / "משמרת מחלקות" / "department shift" / "ward shift":
  - title should be exactly "מחלקות"
  - default location should be "שיבא" unless the user explicitly gave another location
  - default start_time should be "15:00"
  - default end_time should be "23:00"
  - default duration_minutes should be 480
- If the user writes dates like "4,7,9 and half shift on 15", treat 15 as a date when the request is about scheduling events, not as an hour.
- If the user gives multiple dates for the same event, use is_bulk=true and fill bulk_dates with ISO dates.
- For update:
  - source_date is the current event date if stated
  - target_date is the new date if stated
  - start_time is the current/or retained time only if clearly stated
  - end_time is the new end time only if clearly stated
- For delete:
  - identify the event title and/or source_date if possible
- If information is missing, include missing_fields using only: title, date, time
- Prefer the actual event title, not the command text.
- If the user says "דייט עם רן" or "date with Ran", that is the title.
- If the user says things like "it", "that", "אותה", "אותו", "את זה", or clearly refers to the last event without repeating details, set references_previous = true.
- If confidence is not at least medium, return confidence low.

JSON schema:
{{
  "action": "create|update|delete|bulk_create|bulk_delete",
  "title": string|null,
  "calendar_type": "work|kids|family|personal|shared"|null,
  "location": string|null,
  "is_shift": boolean,
  "is_bulk": boolean,
  "date": "YYYY-MM-DD"|null,
  "source_date": "YYYY-MM-DD"|null,
  "target_date": "YYYY-MM-DD"|null,
  "start_time": "HH:MM"|null,
  "end_time": "HH:MM"|null,
  "duration_minutes": number|null,
  "bulk_dates": ["YYYY-MM-DD"],
  "missing_fields": ["title"|"date"|"time"],
  "references_previous": boolean,
  "confidence": "high|medium|low",
  "notes": string|null
}}
"""

    try:
        response = client.messages.create(
            model=EXTRACTION_MODEL,
            max_tokens=350,
            system=system_prompt,
            messages=[{"role": "user", "content": [{"type": "text", "text": combined_message}]}],
        )
        raw_text = response.content[0].text
    except Exception as exc:
        log_event(
            "scheduling_llm_extract_failed",
            payload={"message": user_message, "error": str(exc)},
            level="error",
        )
        return None

    extraction = _normalize_extraction(_safe_json_loads(raw_text))
    log_event(
        "scheduling_llm_extract",
        payload={
            "message": user_message,
            "pending_message": pending_message,
            "last_reference": last_reference,
            "raw_reply": raw_text,
            "parsed": extraction,
        },
    )
    return extraction
