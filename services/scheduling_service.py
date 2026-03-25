import re
from datetime import datetime, timedelta
from uuid import uuid4

from db import scheduled_events_collection, scheduling_drafts_collection


DEFAULT_EVENT_MINUTES = 60
CALENDAR_KEYWORDS = {
    "work": ("work", "shift", "clinic", "ward", "call", "hospital", "meeting"),
    "kids": ("kids", "child", "children", "school", "kindergarten", "pickup", "dropoff", "pediatrician"),
    "family": ("family", "parents", "in-laws", "dinner", "birthday", "shared"),
    "personal": ("gym", "dentist", "doctor", "hair", "friend", "date", "personal"),
    "shared": ("shared", "together", "with maya", "with my husband", "with my wife"),
}
REMINDER_DEFAULTS = {
    "work": ["30 minutes before"],
    "kids": ["1 day before", "2 hours before"],
    "personal": ["1 hour before"],
    "family": ["2 hours before"],
    "shared": ["2 hours before"],
}
WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def _utcnow():
    return datetime.utcnow()


def _normalize_text(text):
    return re.sub(r"\s+", " ", (text or "").strip())


def _infer_calendar_type(text):
    lowered = text.lower()
    for calendar_type, keywords in CALENDAR_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return calendar_type
    return "personal"


def _infer_reminders(calendar_type):
    return REMINDER_DEFAULTS.get(calendar_type, ["1 hour before"])


def _extract_time(text):
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", text, flags=re.IGNORECASE)
    if not match:
        return None

    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    suffix = (match.group(3) or "").lower()
    if suffix == "pm" and hour < 12:
        hour += 12
    if suffix == "am" and hour == 12:
        hour = 0
    if hour > 23 or minute > 59:
        return None
    return hour, minute


def _next_weekday(base_dt, weekday):
    days_ahead = weekday - base_dt.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return base_dt + timedelta(days=days_ahead)


def _extract_date(text):
    lowered = text.lower()
    now = _utcnow()

    if "today" in lowered:
        return now.date()
    if "tomorrow" in lowered:
        return (now + timedelta(days=1)).date()

    for weekday_name, weekday_index in WEEKDAYS.items():
        if f"next {weekday_name}" in lowered or weekday_name in lowered:
            return _next_weekday(now, weekday_index).date()

    iso_match = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if iso_match:
        return datetime(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))).date()

    short_match = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b", text)
    if short_match:
        day = int(short_match.group(1))
        month = int(short_match.group(2))
        year = int(short_match.group(3)) if short_match.group(3) else now.year
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None

    return None


def _clean_title(text):
    cleaned = re.sub(r"\b(today|tomorrow|next\s+\w+)\b", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d{1,2}(?::\d{2})?\s*(am|pm)?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(20\d{2})-(\d{2})-(\d{2})\b", "", cleaned)
    cleaned = re.sub(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", "", cleaned)
    cleaned = re.sub(r"\b(at|on|for|with|schedule|book|set|create|add)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
    return cleaned or "Untitled event"


def _build_event_from_message(message):
    normalized = _normalize_text(message)
    calendar_type = _infer_calendar_type(normalized)
    reminders = _infer_reminders(calendar_type)
    event_date = _extract_date(normalized)
    event_time = _extract_time(normalized)

    missing = []
    if not event_date:
        missing.append("date")
    if not event_time:
        missing.append("time")

    if missing:
        return {
            "status": "needs_details",
            "missing_fields": missing,
            "calendar_type": calendar_type,
            "title": _clean_title(normalized),
        }

    start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=event_time[0], minute=event_time[1])
    end_at = start_at + timedelta(minutes=DEFAULT_EVENT_MINUTES)

    return {
        "status": "ready",
        "title": _clean_title(normalized),
        "calendar_type": calendar_type,
        "reminders": reminders,
        "start_at": start_at,
        "end_at": end_at,
    }


def _serialize_event(event):
    return {
        "title": event["title"],
        "calendar_type": event["calendar_type"],
        "start_at": event["start_at"].isoformat(timespec="minutes"),
        "end_at": event["end_at"].isoformat(timespec="minutes"),
        "reminders": event["reminders"],
    }


def _find_conflicts(session_id, event):
    conflicts = list(
        scheduled_events_collection.find(
            {
                "session_id": session_id,
                "status": "confirmed",
                "start_at": {"$lt": event["end_at"]},
                "end_at": {"$gt": event["start_at"]},
            }
        ).sort("start_at", 1)
    )
    serialized = []
    for conflict in conflicts:
        serialized.append(
            {
                "title": conflict.get("title", "Existing event"),
                "calendar_type": conflict.get("calendar_type", "personal"),
                "start_at": conflict["start_at"].isoformat(timespec="minutes"),
                "end_at": conflict["end_at"].isoformat(timespec="minutes"),
            }
        )
    return serialized


def _save_draft(session_id, raw_message, parsed_event, conflicts):
    now = _utcnow()
    draft_id = str(uuid4())
    doc = {
        "draft_id": draft_id,
        "session_id": session_id,
        "raw_message": raw_message,
        "parsed_event": parsed_event,
        "conflicts": conflicts,
        "status": "pending",
        "created_at": now,
        "updated_at": now,
    }
    scheduling_drafts_collection.insert_one(doc)
    return draft_id


def _format_missing_fields_reply(parsed):
    if parsed["missing_fields"] == ["date", "time"]:
        return "I can draft that, but I still need a date and time before I create anything."
    if "date" in parsed["missing_fields"]:
        return "I can draft that, but I still need the date."
    return "I can draft that, but I still need the time."


def _format_draft_reply(parsed_event, conflicts):
    start_label = parsed_event["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"I drafted this in your {parsed_event['calendar_type']} calendar for {start_label}. "
            f"I also found {len(conflicts)} conflict{'s' if len(conflicts) != 1 else ''}. Review before creating."
        )
    return f"I drafted this in your {parsed_event['calendar_type']} calendar for {start_label}. Review it before I create it."


def build_scheduling_welcome():
    return (
        "Scheduling Copilot is active.<br><br>"
        "Try something like:<br>"
        "- Add dinner with Maya tomorrow at 19:30<br>"
        "- Schedule school pickup next Tuesday at 16:00<br>"
        "- Add clinic meeting next Monday at 08:00"
    )


def handle_scheduling_message(session_id, user_message):
    parsed = _build_event_from_message(user_message)
    if parsed["status"] == "needs_details":
        return {
            "reply": _format_missing_fields_reply(parsed),
            "scheduling_draft": None,
        }

    conflicts = _find_conflicts(session_id, parsed)
    draft_id = _save_draft(session_id, user_message, _serialize_event(parsed), conflicts)

    return {
        "reply": _format_draft_reply(parsed, conflicts),
        "scheduling_draft": {
            "draft_id": draft_id,
            "title": parsed["title"],
            "calendar_type": parsed["calendar_type"].title(),
            "start_at": parsed["start_at"].strftime("%a, %d %b %Y · %H:%M"),
            "end_at": parsed["end_at"].strftime("%H:%M"),
            "reminders": parsed["reminders"],
            "conflicts": conflicts,
            "status": "needs_review",
        },
    }


def confirm_scheduling_draft(session_id, draft_id):
    draft = scheduling_drafts_collection.find_one({"session_id": session_id, "draft_id": draft_id, "status": "pending"})
    if not draft:
        return {"status": "missing"}

    parsed_event = draft["parsed_event"]
    now = _utcnow()
    event_doc = {
        "session_id": session_id,
        "draft_id": draft_id,
        "title": parsed_event["title"],
        "calendar_type": parsed_event["calendar_type"],
        "start_at": datetime.fromisoformat(parsed_event["start_at"]),
        "end_at": datetime.fromisoformat(parsed_event["end_at"]),
        "reminders": parsed_event["reminders"],
        "status": "confirmed",
        "created_at": now,
        "updated_at": now,
    }
    scheduled_events_collection.insert_one(event_doc)
    scheduling_drafts_collection.update_one(
        {"draft_id": draft_id, "session_id": session_id},
        {"$set": {"status": "confirmed", "updated_at": now}},
    )
    return {
        "status": "confirmed",
        "reply": f"Created {parsed_event['title']} in your {parsed_event['calendar_type']} calendar.",
    }


def dismiss_scheduling_draft(session_id, draft_id):
    now = _utcnow()
    result = scheduling_drafts_collection.update_one(
        {"session_id": session_id, "draft_id": draft_id, "status": "pending"},
        {"$set": {"status": "dismissed", "updated_at": now}},
    )
    if not result.modified_count:
        return {"status": "missing"}
    return {"status": "dismissed", "reply": "Draft dismissed."}
