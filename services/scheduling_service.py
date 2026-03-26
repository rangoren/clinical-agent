import re
from calendar import monthrange
from datetime import datetime, timedelta
from uuid import uuid4

from bson import ObjectId

from db import scheduled_events_collection, scheduling_drafts_collection, scheduling_preferences_collection
from services.scheduling_extraction_service import extract_scheduling_intent
from services.logging_service import log_event
from services.google_calendar_service import (
    get_google_calendar_name,
    get_google_calendars,
    has_google_calendar_connection,
    sync_google_create_event,
    sync_google_delete_event,
    sync_google_update_event,
)


DEFAULT_EVENT_MINUTES = 60
DEFAULT_SHIFT_START_HOUR = 8
DEFAULT_SHIFT_START_MINUTE = 0
DEFAULT_SHIFT_DURATION_HOURS = 25
DEFAULT_SHIFT_LOCATION = "שיבא"
HALF_SHIFT_START_HOUR = 15
HALF_SHIFT_START_MINUTE = 0
HALF_SHIFT_END_HOUR = 23
HALF_SHIFT_END_MINUTE = 0
HALF_SHIFT_DURATION_MINUTES = 8 * 60
KNOWN_LOCATIONS = {
    "שיבא": ("shiba", "sheba", "tel hashomer", "תל השומר", "שיבא"),
}
CALENDAR_KEYWORDS = {
    "work": ("work", "shift", "clinic", "ward", "call", "hospital", "meeting", "on-call", "on call", "night shift", "night shifts", "call shift", "call shifts", "תורנות", "תורנויות", "תורנות חצי", "חצי תורנות", "מחלקות", "משמרת מחלקות", "כוננות", "משמרת לילה", "תורנית"),
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
    "mon": 0,
    "שני": 0,
    "יום שני": 0,
    "tuesday": 1,
    "tue": 1,
    "שלישי": 1,
    "יום שלישי": 1,
    "wednesday": 2,
    "wed": 2,
    "רביעי": 2,
    "יום רביעי": 2,
    "thursday": 3,
    "thu": 3,
    "חמישי": 3,
    "יום חמישי": 3,
    "friday": 4,
    "fri": 4,
    "שישי": 4,
    "יום שישי": 4,
    "saturday": 5,
    "sat": 5,
    "שבת": 5,
    "sunday": 6,
    "sun": 6,
    "ראשון": 6,
    "יום ראשון": 6,
}
MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "ינואר": 1,
    "פברואר": 2,
    "מרץ": 3,
    "אפריל": 4,
    "מאי": 5,
    "יוני": 6,
    "יולי": 7,
    "אוגוסט": 8,
    "ספטמבר": 9,
    "אוקטובר": 10,
    "נובמבר": 11,
    "דצמבר": 12,
}
DELETE_KEYWORDS = (
    "delete",
    "remove",
    "cancel",
    "drop",
    "מחק",
    "תמחק",
    "תמחקי",
    "למחוק",
    "בטל",
    "תבטל",
    "תבטלי",
    "לבטל",
    "הסר",
    "תסיר",
)
UPDATE_KEYWORDS = (
    "move",
    "reschedule",
    "change",
    "update",
    "push",
    "switch",
    "replace",
    "delay",
    "postpone",
    "advance",
    "bring forward",
    "move up",
    "move back",
    "תעדכן",
    "תעדכני",
    "לשנות",
    "תזיז",
    "תזיזי",
    "שנה",
    "תשנה",
    "תשני",
    "תחליף",
    "תחליפי",
    "תדחה",
    "דחה",
    "תקדים",
    "תקדימי",
)
SUMMARY_KEYWORDS = (
    "daily summary",
    "summary for today",
    "what's on today",
    "whats on today",
    "today summary",
    "today's schedule",
    "todays schedule",
    "schedule today",
    "what do i have today",
    "what do we have today",
)
SHIFT_KEYWORDS = (
    "on-call",
    "on call",
    "call shift",
    "call shifts",
    "night shift",
    "night shifts",
    "night duty",
    "duty shift",
    "תורנות",
    "תורנויות",
    "תורניות",
    "תורנית",
    "כוננות",
    "משמרת לילה",
)
HALF_SHIFT_KEYWORDS = (
    "תורנות חצי",
    "חצי תורנות",
    "תורנות חצי יום",
    "half shift",
    "half-call",
    "half call",
    "partial call",
)
DEPARTMENT_SHIFT_KEYWORDS = (
    "מחלקות",
    "משמרת מחלקות",
    "מחלקה ערב",
    "מחלקת ערב",
    "department shift",
    "ward shift",
    "ward evening shift",
)


def _utcnow():
    return datetime.utcnow()


def _get_scheduling_preferences(session_id):
    return scheduling_preferences_collection.find_one({"session_id": session_id}) or {}


def _get_preferred_google_calendar(session_id, calendar_type):
    preferences = _get_scheduling_preferences(session_id)
    preferred_map = preferences.get("google_calendar_preferences", {})
    return preferred_map.get(calendar_type)


def _get_last_scheduling_reference(session_id):
    preferences = _get_scheduling_preferences(session_id)
    return preferences.get("last_scheduling_reference")


def _save_preferred_google_calendar(session_id, calendar_type, provider_calendar_id):
    if not provider_calendar_id or not calendar_type:
        return
    scheduling_preferences_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {
                f"google_calendar_preferences.{calendar_type}": provider_calendar_id,
                "updated_at": _utcnow(),
            },
            "$setOnInsert": {"created_at": _utcnow()},
        },
        upsert=True,
    )


def _save_last_scheduling_reference(session_id, event_doc):
    if not event_doc:
        return
    reference = {
        "event_id": str(event_doc.get("_id") or event_doc.get("event_id") or ""),
        "title": event_doc.get("title"),
        "calendar_type": event_doc.get("calendar_type"),
        "start_at": event_doc.get("start_at").isoformat(timespec="minutes") if isinstance(event_doc.get("start_at"), datetime) else event_doc.get("start_at"),
        "end_at": event_doc.get("end_at").isoformat(timespec="minutes") if isinstance(event_doc.get("end_at"), datetime) else event_doc.get("end_at"),
        "location": event_doc.get("location"),
        "provider_event_id": event_doc.get("provider_event_id"),
        "provider_calendar_id": event_doc.get("provider_calendar_id"),
        "status": event_doc.get("status"),
    }
    scheduling_preferences_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "last_scheduling_reference": reference,
                "updated_at": _utcnow(),
            },
            "$setOnInsert": {"created_at": _utcnow()},
        },
        upsert=True,
    )


def _normalize_text(text):
    normalized = (text or "").strip()
    replacements = {
        "–": "-",
        "—": "-",
        "־": "-",
        "“": '"',
        "”": '"',
        "׳": "'",
        "״": '"',
        " ,": ",",
        " .": ".",
    }
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)
    return re.sub(r"\s+", " ", normalized)


def _tokenize_title(text):
    tokens = []
    for token in re.findall(r"[A-Za-z0-9\u0590-\u05FF]+", (text or "").lower()):
        if not token:
            continue
        variants = [token]
        if token.startswith("ה") and len(token) > 2:
            variants.append(token[1:])
        for variant in variants:
            if variant in {
            "add",
            "schedule",
            "book",
            "set",
            "create",
            "event",
            "move",
            "reschedule",
            "change",
            "update",
            "delete",
            "remove",
            "cancel",
            "drop",
            "to",
            "from",
            "at",
            "on",
            "for",
            "my",
            "our",
            "the",
            "a",
            "an",
            "next",
            "today",
            "tomorrow",
            "פגישה",
            "שיחה",
            "אירוע",
            "יומן",
            "ליומן",
            "תכניס",
            "תכניסי",
            "תוסיף",
            "תוסיפי",
            "תשים",
            "תשימי",
            "תקבע",
            "תקבעי",
            "לקבוע",
            "ביום",
            "בתאריך",
            "בשעה",
            "משעה",
            "עד",
            "היום",
            "מחר",
            }:
                continue
            tokens.append(variant)
    return tokens


def _detect_action(message):
    lowered = (message or "").lower()
    if any(keyword in lowered for keyword in DELETE_KEYWORDS):
        return "delete"
    if any(keyword in lowered for keyword in UPDATE_KEYWORDS):
        return "update"
    return "create"


def _is_daily_summary_request(message):
    lowered = _normalize_text(message).lower()
    if any(keyword in lowered for keyword in SUMMARY_KEYWORDS):
        return True
    return "today" in lowered and any(
        phrase in lowered
        for phrase in ("do i have", "meetings", "meeting", "schedule", "events", "what do i", "what's on", "whats on")
    )


def _infer_calendar_type(text):
    lowered = text.lower()
    for calendar_type, keywords in CALENDAR_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return calendar_type
    return "personal"


def _infer_reminders(calendar_type):
    return REMINDER_DEFAULTS.get(calendar_type, ["1 hour before"])


def _is_shift_template(text):
    lowered = (text or "").lower()
    if any(keyword in lowered for keyword in SHIFT_KEYWORDS):
        return True
    return bool(re.search(r"תורנ(?:ות|יות|ית|י(?:ו)?ת)?", text or ""))


def _match_scheduling_template(text):
    normalized = _normalize_text(text)
    lowered = normalized.lower()

    if any(keyword in lowered for keyword in HALF_SHIFT_KEYWORDS) or re.search(r"תורנות\s+חצי|חצי\s+תורנות", normalized):
        return {
            "title": "תורנות חצי",
            "location": DEFAULT_SHIFT_LOCATION,
            "start_time": "15:00",
            "end_time": "23:00",
            "duration_minutes": HALF_SHIFT_DURATION_MINUTES,
            "is_shift": False,
        }

    if any(keyword in lowered for keyword in DEPARTMENT_SHIFT_KEYWORDS) or re.search(r"משמרת\s+מחלקות|מחלקות", normalized):
        return {
            "title": "מחלקות",
            "location": DEFAULT_SHIFT_LOCATION,
            "start_time": "15:00",
            "end_time": "23:00",
            "duration_minutes": HALF_SHIFT_DURATION_MINUTES,
            "is_shift": False,
        }

    if _is_shift_template(normalized):
        return {
            "title": "תורנות",
            "location": DEFAULT_SHIFT_LOCATION,
            "start_time": "08:00",
            "end_time": "09:00",
            "duration_minutes": DEFAULT_SHIFT_DURATION_HOURS * 60,
            "is_shift": True,
        }

    return None


def _build_shift_window(event_date):
    start_at = datetime.combine(event_date, datetime.min.time()).replace(
        hour=DEFAULT_SHIFT_START_HOUR,
        minute=DEFAULT_SHIFT_START_MINUTE,
    )
    end_at = start_at + timedelta(hours=DEFAULT_SHIFT_DURATION_HOURS)
    return start_at, end_at


def _infer_default_location(text):
    template = _match_scheduling_template(text)
    if template:
        return template["location"]
    return _extract_location(text)


def _extract_location(text):
    normalized = _normalize_text(text)
    lowered = normalized.lower()
    for canonical_location, aliases in KNOWN_LOCATIONS.items():
        for alias in aliases:
            alias_lower = alias.lower()
            if re.search(rf"\bat\s+{re.escape(alias_lower)}\b", lowered):
                return canonical_location
            if re.search(rf"(?<!\w)ב{re.escape(alias_lower)}(?!\w)", lowered):
                return canonical_location
            if alias_lower in lowered and lowered.strip().endswith(alias_lower):
                return canonical_location

    generic_patterns = [
        r"\b(?:at|in)\s+([A-Za-z\u0590-\u05FF][A-Za-z\u0590-\u05FF0-9\s'\"-]{1,40})",
        r"(?:בבית חולים|במרפאה|בקליניקה|במחלקה|במשרד|בזום|בzoom)\s*([A-Za-z\u0590-\u05FF0-9\s'\"-]{1,40})",
    ]
    stop_pattern = r"\b(?:today|tomorrow|next|בשעה|בתאריך|for|עד|to|\d{1,2}(?::\d{2})?)\b"
    for pattern in generic_patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        location = re.split(stop_pattern, match.group(1), maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,.-")
        if location:
            return location
    return None


def _strip_location_from_title(text):
    cleaned = text
    for aliases in KNOWN_LOCATIONS.values():
        for alias in aliases:
            cleaned = re.sub(rf"\bat\s+{re.escape(alias)}\b", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(rf"(?<!\w)ב{re.escape(alias)}(?!\w)", "", cleaned, flags=re.IGNORECASE)
    return cleaned


def _extract_duration_minutes(text):
    normalized = _normalize_text(text)
    if re.search(r"\b(?:half an hour|half-hour|חצי שעה)\b", normalized, flags=re.IGNORECASE):
        return 30
    if re.search(r"\b(?:hour and a half|1\.5 hours?|שעה וחצי)\b", normalized, flags=re.IGNORECASE):
        return 90

    minute_match = re.search(r"\b(\d{1,3})\s*(minutes?|mins?|דקות|דקה)\b", normalized, flags=re.IGNORECASE)
    if minute_match:
        return int(minute_match.group(1))

    short_minute_match = re.search(r"\b(\d{1,3})\s*(?:דק|דק'|min)\b", normalized, flags=re.IGNORECASE)
    if short_minute_match:
        return int(short_minute_match.group(1))

    hour_match = re.search(r"\b(\d{1,2})\s*(hours?|hrs?|שעות|שעה)\b", normalized, flags=re.IGNORECASE)
    if hour_match:
        return int(hour_match.group(1)) * 60

    short_hour_match = re.search(r"\b(\d{1,2})\s*(?:h|hr|hrs|ש')\b", normalized, flags=re.IGNORECASE)
    if short_hour_match:
        return int(short_hour_match.group(1)) * 60

    return None


def _strip_duration_from_title(text):
    cleaned = re.sub(r"\bfor\s+\d{1,3}\s*(minutes?|mins?|hours?|hrs?)\b", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bof\s+\d{1,3}\s*(minutes?|mins?|hours?|hrs?)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bשל\s+\d{1,3}\s*(דקות|דקה|שעות|שעה)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d{1,3}\s*(minutes?|mins?|hours?|hrs?|דקות|דקה|שעות|שעה)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:half an hour|half-hour|חצי שעה|hour and a half|1\.5 hours?|שעה וחצי)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d{1,3}\s*(?:דק|דק'|min|h|hr|hrs|ש')\b", "", cleaned, flags=re.IGNORECASE)
    return cleaned


def _extract_participant(text):
    normalized = _normalize_text(text)
    patterns = [
        r"\b(?:meeting|appointment|call|dinner)\s+(?:with|about)\s+([A-Za-z\u0590-\u05FF][A-Za-z\u0590-\u05FF\s'-]{0,40})",
        r"(?:פגישה|שיחה)\s+(?:עם|על)\s+([\u0590-\u05FFA-Za-z][\u0590-\u05FFA-Za-z\s'-]{0,40})",
        r"(?:meeting|appointment)\s+(?:regarding|for)\s+([A-Za-z\u0590-\u05FF][A-Za-z\u0590-\u05FF\s'-]{0,40})",
        r"(?:פגישה|שיחה)\s+(?:לגבי|מול)\s+([\u0590-\u05FFA-Za-z][\u0590-\u05FFA-Za-z\s'-]{0,40})",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        participant = re.split(
            r"\b(?:today|tomorrow|at|for|on|in|של|minutes?|hours?|דקות|דקה|שעות|שעה)\b",
            match.group(1),
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip(" ,.-")
        if participant:
            return participant
    return None


def _build_semantic_title(text):
    participant = _extract_participant(text)
    lowered = (text or "").lower()
    date_match = re.search(r"(?:ל)?(?:date|דייט)\s+(?:with|עם)\s+([A-Za-z\u0590-\u05FF][A-Za-z\u0590-\u05FF\s'-]{0,40})", _normalize_text(text), flags=re.IGNORECASE)
    if date_match:
        person = re.split(
            r"\b(?:today|tomorrow|at|for|on|in|של|minutes?|hours?|דקות|דקה|שעות|שעה)\b",
            date_match.group(1),
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip(" ,.-")
        if person:
            return f"דייט עם {person}" if "דייט" in text else f"Date with {person}"
    if participant and any(keyword in lowered for keyword in ("meeting", "appointment", "פגישה", "שיחה", "זום", "zoom")):
        if any(keyword in text for keyword in ("פגישה", "שיחה")):
            return f"פגישה עם {participant}"
        return f"Meeting with {participant}"
    return None


def _extract_explicit_title(text):
    normalized = _normalize_text(text)
    patterns = [
        r"(?:בשם(?: האירוע)?|שם האירוע)\s+(.+?)(?=\s+(?:ביום|בתאריך|בשעה|משעה|מהשעה|היום|מחר|next|today|tomorrow|at|on|from)\b|$)",
        r"\b(?:event named|event called|named|called)\s+(.+?)(?=\s+(?:on|at|from|today|tomorrow|next)\b|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        title = match.group(1).strip(" ,.-")
        if title:
            return title
    return None


def _normalize_event_title(text):
    template = _match_scheduling_template(text)
    if template:
        return template["title"]
    semantic_title = _build_semantic_title(text)
    if semantic_title:
        return semantic_title
    explicit_title = _extract_explicit_title(text)
    if explicit_title:
        return explicit_title
    return _clean_title(text)


def _infer_event_minutes(text, is_shift_template=False):
    template = _match_scheduling_template(text)
    if template:
        return template["duration_minutes"]
    if is_shift_template:
        return DEFAULT_SHIFT_DURATION_HOURS * 60
    return _extract_duration_minutes(text) or DEFAULT_EVENT_MINUTES


def _duration_label(minutes):
    if not minutes:
        return None
    hours, mins = divmod(minutes, 60)
    if hours and mins:
        return f"{hours}h {mins}m"
    if hours:
        return f"{hours}h"
    return f"{mins}m"


def _extract_time(text):
    if _extract_time_range(text):
        return _extract_time_range(text)[0]
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


def _extract_time_range(text):
    match = re.search(
        r"(?:בשעה|בשעות|בין|מ|at|from)?\s*(\d{1,2})(?::(\d{2}))?\s*(?:עד|to|ל|-)\s*(\d{1,2})(?::(\d{2}))?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    start_hour = int(match.group(1))
    start_minute = int(match.group(2) or 0)
    end_hour = int(match.group(3))
    end_minute = int(match.group(4) or 0)
    if any(value > 23 for value in (start_hour, end_hour)) or any(value > 59 for value in (start_minute, end_minute)):
        return None
    return (start_hour, start_minute), (end_hour, end_minute)


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

    dotted_match = re.search(r"\b(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\b", text)
    if dotted_match:
        day = int(dotted_match.group(1))
        month = int(dotted_match.group(2))
        year = int(dotted_match.group(3)) if dotted_match.group(3) else now.year
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None

    dashed_match = re.search(r"\b(\d{1,2})-(\d{1,2})(?:-(\d{2,4}))?\b", text)
    if dashed_match:
        day = int(dashed_match.group(1))
        month = int(dashed_match.group(2))
        year = int(dashed_match.group(3)) if dashed_match.group(3) else now.year
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None

    month_names_pattern = "|".join(sorted((re.escape(name) for name in MONTHS.keys()), key=len, reverse=True))
    month_first_match = re.search(rf"\b({month_names_pattern})\s+(\d{{1,2}})(?:,?\s*(20\d{{2}}))?\b", text, flags=re.IGNORECASE)
    if month_first_match:
        month = MONTHS[month_first_match.group(1).lower()]
        day = int(month_first_match.group(2))
        year = int(month_first_match.group(3) or now.year)
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None

    day_first_named_month_match = re.search(rf"\b(\d{{1,2}})\s+({month_names_pattern})(?:\s+(20\d{{2}}))?\b", text, flags=re.IGNORECASE)
    if day_first_named_month_match:
        day = int(day_first_named_month_match.group(1))
        month = MONTHS[day_first_named_month_match.group(2).lower()]
        year = int(day_first_named_month_match.group(3) or now.year)
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None

    hebrew_day_month_match = re.search(rf"\b(?:ה\s*)?(\d{{1,2}})\s+ל({month_names_pattern})(?:\s+(20\d{{2}}))?\b", text, flags=re.IGNORECASE)
    if hebrew_day_month_match:
        day = int(hebrew_day_month_match.group(1))
        month = MONTHS[hebrew_day_month_match.group(2).lower()]
        year = int(hebrew_day_month_match.group(3) or now.year)
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None

    return None


def _extract_all_dates(text):
    now = _utcnow()
    found_dates = []

    for pattern, builder in (
        (r"\b(20\d{2})-(\d{2})-(\d{2})\b", lambda m: datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()),
        (
            r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b",
            lambda m: datetime(
                (int(m.group(3)) + 2000) if m.group(3) and int(m.group(3)) < 100 else int(m.group(3) or now.year),
                int(m.group(2)),
                int(m.group(1)),
            ).date(),
        ),
        (
            r"\b(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\b",
            lambda m: datetime(
                (int(m.group(3)) + 2000) if m.group(3) and int(m.group(3)) < 100 else int(m.group(3) or now.year),
                int(m.group(2)),
                int(m.group(1)),
            ).date(),
        ),
        (
            r"\b(\d{1,2})-(\d{1,2})(?:-(\d{2,4}))?\b",
            lambda m: datetime(
                (int(m.group(3)) + 2000) if m.group(3) and int(m.group(3)) < 100 else int(m.group(3) or now.year),
                int(m.group(2)),
                int(m.group(1)),
            ).date(),
        ),
    ):
        for match in re.finditer(pattern, text):
            try:
                found_dates.append((match.start(), builder(match)))
            except ValueError:
                continue

    month_names_pattern = "|".join(sorted((re.escape(name) for name in MONTHS.keys()), key=len, reverse=True))
    for match in re.finditer(rf"\b({month_names_pattern})\s+(\d{{1,2}})(?:,?\s*(20\d{{2}}))?\b", text, flags=re.IGNORECASE):
        try:
            found_dates.append(
                (
                    match.start(),
                    datetime(int(match.group(3) or now.year), MONTHS[match.group(1).lower()], int(match.group(2))).date(),
                )
            )
        except ValueError:
            continue
    for match in re.finditer(rf"\b(\d{{1,2}})\s+({month_names_pattern})(?:\s+(20\d{{2}}))?\b", text, flags=re.IGNORECASE):
        try:
            found_dates.append(
                (
                    match.start(),
                    datetime(int(match.group(3) or now.year), MONTHS[match.group(2).lower()], int(match.group(1))).date(),
                )
            )
        except ValueError:
            continue
    for match in re.finditer(rf"\b(?:ה\s*)?(\d{{1,2}})\s+ל({month_names_pattern})(?:\s+(20\d{{2}}))?\b", text, flags=re.IGNORECASE):
        try:
            found_dates.append(
                (
                    match.start(),
                    datetime(int(match.group(3) or now.year), MONTHS[match.group(2).lower()], int(match.group(1))).date(),
                )
            )
        except ValueError:
            continue

    return [value for _, value in sorted(found_dates, key=lambda item: item[0])]


def _extract_weekday_mentions(text):
    lowered = text.lower()
    mentions = []
    for weekday_name, weekday_index in WEEKDAYS.items():
        for match in re.finditer(rf"(?<!\w){re.escape(weekday_name.lower())}(?!\w)", lowered):
            mentions.append((match.start(), weekday_index))
    return [value for _, value in sorted(mentions, key=lambda item: item[0])]


def _extract_update_target_date(text):
    explicit_dates = _extract_all_dates(text)
    if len(explicit_dates) >= 2:
        return explicit_dates[-1]

    weekday_mentions = _extract_weekday_mentions(text)
    if len(weekday_mentions) >= 2:
        return _next_weekday(_utcnow(), weekday_mentions[-1]).date()
    if len(weekday_mentions) == 1:
        return _next_weekday(_utcnow(), weekday_mentions[0]).date()
    return _extract_date(text)


def _extract_update_target_time(text):
    replacement_match = re.search(
        r"(?:מ(?:שעה)?|from)\s*(\d{1,2}(?::\d{2})?)\s*(?:ל(?:שעה)?|to)\s*(\d{1,2}(?::\d{2})?)",
        text,
        flags=re.IGNORECASE,
    )
    if replacement_match:
        target = replacement_match.group(2)
        return _extract_time(target)

    time_range = _extract_time_range(text)
    if time_range:
        return time_range[1]
    return _extract_time(text)


def _extract_date_phrase(text):
    lowered = text.lower()
    if "today" in lowered:
        return "today"
    if "tomorrow" in lowered:
        return "tomorrow"
    for weekday_name in WEEKDAYS:
        if f"next {weekday_name}" in lowered:
            return f"next {weekday_name}"
        if weekday_name in lowered:
            return weekday_name

    iso_match = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if iso_match:
        return iso_match.group(0)

    short_match = re.search(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", text)
    if short_match:
        return short_match.group(0)

    dotted_match = re.search(r"\b\d{1,2}\.\d{1,2}(?:\.\d{2,4})?\b", text)
    if dotted_match:
        return dotted_match.group(0)

    dashed_match = re.search(r"\b\d{1,2}-\d{1,2}(?:-\d{2,4})?\b", text)
    if dashed_match:
        return dashed_match.group(0)

    month_names_pattern = "|".join(sorted((re.escape(name) for name in MONTHS.keys() if name.isascii()), key=len, reverse=True))
    month_first_match = re.search(rf"\b(?:{month_names_pattern})\s+\d{{1,2}}(?:,?\s*20\d{{2}})?\b", text, flags=re.IGNORECASE)
    if month_first_match:
        return month_first_match.group(0)

    day_first_named_month_match = re.search(rf"\b\d{{1,2}}\s+(?:{month_names_pattern})(?:\s+20\d{{2}})?\b", text, flags=re.IGNORECASE)
    if day_first_named_month_match:
        return day_first_named_month_match.group(0)

    return None


def _extract_month_year(text):
    lowered = text.lower()
    now = _utcnow()

    if "next month" in lowered:
        month = now.month + 1
        year = now.year
        if month == 13:
            month = 1
            year += 1
        return month, year

    if "this month" in lowered:
        return now.month, now.year

    for alias, month_number in sorted(MONTHS.items(), key=lambda item: len(item[0]), reverse=True):
        pattern = rf"(?<!\w)[בל]?(?:{re.escape(alias.lower())})(?!\w)"
        month_match = re.search(pattern, lowered)
        if month_match:
            year_match = re.search(r"(20\d{2})", lowered)
            year = int(year_match.group(1)) if year_match else now.year
            return month_number, year

    slash_match = re.search(r"\b(\d{1,2})/(\d{4})\b", lowered)
    if slash_match:
        return int(slash_match.group(1)), int(slash_match.group(2))

    return None, None


def _clean_title(text):
    cleaned = _strip_location_from_title(text)
    cleaned = _strip_duration_from_title(cleaned)
    cleaned = re.sub(r"\b(?:event named|event called|named)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"(?:אירוע בשם|בשם האירוע|בשם)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"^\s*(?:please\s+)?(?:add|schedule|book|set|create|put|insert)\s+(?:me\s+)?(?:an?\s+)?(?:event\s+)?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^\s*(?:תכניס(?:י)?|תוסיף(?:י)?|תשים(?:י)?|תקבע(?:י)?)(?:\s+לי)?(?:\s+ליומן)?(?:\s+ביומן)?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s+(?:ביום|בתאריך|בשעה|משעה|מהשעה|from|on|starting|start|today|tomorrow|היום|מחר|בחמישי|בשישי|ברביעי|בשלישי|בשני|בראשון|בשבת)\b.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b(today|tomorrow|next\s+\w+)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bthis month\b|\bnext month\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(" + "|".join(re.escape(key) for key in MONTHS.keys()) + r")\b(?:\s+20\d{2})?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[בל](?=(" + "|".join(re.escape(key) for key in MONTHS.keys()) + r"))", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bevery\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)(?:\s+and\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday))*\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d{1,2}\s*[-–]\s*\d{1,2}\b", "", cleaned)
    cleaned = re.sub(r"\b\d{1,2}(?:\s*,\s*\d{1,2}){1,}\b", "", cleaned)
    cleaned = re.sub(r"\b\d{1,2}(?::\d{2})?\s*(am|pm)?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(20\d{2})-(\d{2})-(\d{2})\b", "", cleaned)
    cleaned = re.sub(r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", "", cleaned)
    cleaned = re.sub(r"\b\d{1,2}\.\d{1,2}(?:\.\d{2,4})?\b", "", cleaned)
    cleaned = re.sub(r"\b\d{1,2}-\d{1,2}(?:-\d{2,4})?\b", "", cleaned)
    cleaned = re.sub(r"\b(?:at|on|for|with|schedule|book|set|create|add|put|insert)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(appointment|meeting|call)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(פגישה|שיחה|תכניס ליומן|תוסיף ליומן|תשים לי ביומן|תקבע לי|ביומן|ליומן|אירוע|תאריך|בתאריך|בשעה|עד|בין)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
    return cleaned or "Untitled event"


def _get_pending_details_context(session_id):
    return scheduling_drafts_collection.find_one(
        {
            "session_id": session_id,
            "status": "awaiting_details",
        },
        sort=[("updated_at", -1)],
    )


def _save_pending_details_context(session_id, raw_message, parsed_event):
    now = _utcnow()
    existing = _get_pending_details_context(session_id)
    payload = {
        "raw_message": raw_message,
        "parsed_event": parsed_event,
        "action_type": "create",
        "status": "awaiting_details",
        "updated_at": now,
    }
    if existing:
        scheduling_drafts_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$set": payload,
            },
        )
        return existing.get("draft_id")

    draft_id = str(uuid4())
    scheduling_drafts_collection.insert_one(
        {
            "draft_id": draft_id,
            "session_id": session_id,
            "created_at": now,
            **payload,
        }
    )
    return draft_id


def _clear_pending_details_context(session_id):
    scheduling_drafts_collection.update_many(
        {"session_id": session_id, "status": "awaiting_details"},
        {"$set": {"status": "superseded", "updated_at": _utcnow()}},
    )


def _maybe_merge_with_pending_context(session_id, user_message):
    pending = _get_pending_details_context(session_id)
    if not pending:
        return user_message

    pending_message = (pending.get("raw_message") or "").strip()
    if not pending_message:
        return user_message

    merged = f"{pending_message} {user_message.strip()}".strip()
    return merged


def _parse_iso_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def _parse_hhmm(value):
    if not value:
        return None
    match = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(value))
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    return hour, minute


def _build_shift_window_from_times(event_date, start_time, end_time=None, duration_minutes=None):
    start_hour, start_minute = start_time or (8, 0)
    start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=start_hour, minute=start_minute)

    if end_time:
        end_hour, end_minute = end_time
        end_at = datetime.combine(event_date, datetime.min.time()).replace(hour=end_hour, minute=end_minute)
        if end_at <= start_at or ((duration_minutes or 0) >= (24 * 60) and end_at > start_at):
            end_at += timedelta(days=1)
    else:
        end_at = start_at + timedelta(minutes=duration_minutes or (DEFAULT_SHIFT_DURATION_HOURS * 60))

    return start_at, end_at


def _should_use_llm_extraction(extraction):
    if not extraction:
        return False
    if extraction.get("confidence") == "high":
        return True
    if extraction.get("confidence") == "medium" and (
        extraction.get("action")
        or extraction.get("title")
        or extraction.get("date")
        or extraction.get("source_date")
        or extraction.get("target_date")
        or extraction.get("bulk_dates")
    ):
        return True
    return False


def _apply_extraction_defaults(extraction, raw_message):
    if not extraction:
        return extraction
    normalized = _normalize_text(raw_message)
    extraction = dict(extraction)
    template = _match_scheduling_template(normalized)
    if not extraction.get("calendar_type"):
        extraction["calendar_type"] = _infer_calendar_type(normalized)
    if template:
        extraction["title"] = extraction.get("title") or template["title"]
        extraction["location"] = extraction.get("location") or template["location"]
        extraction["start_time"] = extraction.get("start_time") or template["start_time"]
        extraction["end_time"] = extraction.get("end_time") or template["end_time"]
        extraction["duration_minutes"] = extraction.get("duration_minutes") or template["duration_minutes"]
        extraction["is_shift"] = bool(extraction.get("is_shift") or template["is_shift"])
    elif not extraction.get("duration_minutes"):
        extraction["duration_minutes"] = _extract_duration_minutes(normalized) or DEFAULT_EVENT_MINUTES
    return extraction


def _build_event_from_extraction(extraction, raw_message):
    extraction = _apply_extraction_defaults(extraction, raw_message)
    calendar_type = extraction.get("calendar_type") or _infer_calendar_type(raw_message)
    reminders = _infer_reminders(calendar_type)
    event_date = _parse_iso_date(extraction.get("date"))
    event_time = _parse_hhmm(extraction.get("start_time"))
    end_time = _parse_hhmm(extraction.get("end_time"))
    is_shift_template = bool(extraction.get("is_shift"))
    duration_minutes = extraction.get("duration_minutes") or _infer_event_minutes(raw_message, is_shift_template=is_shift_template)
    title = extraction.get("title") or _normalize_event_title(raw_message)
    location = extraction.get("location") or _infer_default_location(raw_message)

    missing = []
    if not title or title == "Untitled event":
        missing.append("title")
    if not event_date:
        missing.append("date")
    if not event_time and not is_shift_template:
        missing.append("time")

    if missing:
        return {
            "status": "needs_details",
            "missing_fields": missing,
            "calendar_type": calendar_type,
            "title": title,
            "location": location,
            "duration_minutes": duration_minutes,
            "raw_message": raw_message,
        }

    start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=event_time[0], minute=event_time[1])
    if is_shift_template:
        start_at, end_at = _build_shift_window_from_times(event_date, event_time, end_time=end_time, duration_minutes=duration_minutes)
        duration_minutes = int((end_at - start_at).total_seconds() / 60)
    elif end_time:
        end_at = datetime.combine(event_date, datetime.min.time()).replace(hour=end_time[0], minute=end_time[1])
        if end_at <= start_at:
            end_at += timedelta(days=1)
        duration_minutes = int((end_at - start_at).total_seconds() / 60)
    else:
        end_at = start_at + timedelta(minutes=duration_minutes)

    return {
        "status": "ready",
        "title": title,
        "calendar_type": calendar_type,
        "reminders": reminders,
        "location": location,
        "duration_minutes": duration_minutes,
        "start_at": start_at,
        "end_at": end_at,
        "raw_message": raw_message,
    }


def _build_bulk_events_from_extraction(extraction, raw_message):
    extraction = _apply_extraction_defaults(extraction, raw_message)
    bulk_dates = [_parse_iso_date(item) for item in extraction.get("bulk_dates") or []]
    bulk_dates = sorted({item for item in bulk_dates if item})
    if len(bulk_dates) < 2:
        return None

    calendar_type = extraction.get("calendar_type") or _infer_calendar_type(raw_message)
    reminders = _infer_reminders(calendar_type)
    is_shift_template = bool(extraction.get("is_shift"))
    start_time = _parse_hhmm(extraction.get("start_time"))
    end_time = _parse_hhmm(extraction.get("end_time"))
    duration_minutes = extraction.get("duration_minutes") or _infer_event_minutes(raw_message, is_shift_template=is_shift_template)
    title = extraction.get("title") or _normalize_event_title(raw_message)
    location = extraction.get("location") or _infer_default_location(raw_message)

    if not start_time and not is_shift_template:
        return {
            "status": "needs_details",
            "missing_fields": ["time"],
            "calendar_type": calendar_type,
            "title": title,
            "raw_message": raw_message,
        }

    events = []
    for event_date in bulk_dates:
        if is_shift_template:
            start_at, end_at = _build_shift_window_from_times(
                event_date,
                start_time,
                end_time=end_time,
                duration_minutes=duration_minutes,
            )
        else:
            start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=start_time[0], minute=start_time[1])
            if end_time:
                end_at = datetime.combine(event_date, datetime.min.time()).replace(hour=end_time[0], minute=end_time[1])
                if end_at <= start_at:
                    end_at += timedelta(days=1)
            else:
                end_at = start_at + timedelta(minutes=duration_minutes)
        events.append(
            {
                "title": title,
                "calendar_type": calendar_type,
                "reminders": reminders,
                "location": location,
                "duration_minutes": int((end_at - start_at).total_seconds() / 60),
                "start_at": start_at,
                "end_at": end_at,
            }
        )

    return {"status": "ready", "events": events, "calendar_type": calendar_type, "title": title}


def _extract_template_clause_days(segment):
    normalized = _normalize_text(segment)
    days = set()

    list_match = re.search(r"\b(\d{1,2}(?:\s*,\s*\d{1,2})+)\b", normalized)
    if list_match:
        for part in list_match.group(1).split(","):
            try:
                days.add(int(part.strip()))
            except Exception:
                continue

    single_matches = re.findall(r"(?:\bב|\bon|\bfor|\bdate\b|\bday\b|\bבתאריך|\bליום|\bביום)\s*(\d{1,2})\b", normalized, flags=re.IGNORECASE)
    for part in single_matches:
        try:
            days.add(int(part))
        except Exception:
            continue

    trailing_match = re.search(r"(\d{1,2})\s*$", normalized)
    if trailing_match:
        try:
            days.add(int(trailing_match.group(1)))
        except Exception:
            pass

    return sorted(day for day in days if 1 <= day <= 31)


def _build_template_events_for_days(template, days, month, year, calendar_type):
    reminders = _infer_reminders(calendar_type)
    start_time = _parse_hhmm(template["start_time"])
    end_time = _parse_hhmm(template["end_time"])
    events = []
    for day in days:
        if day > monthrange(year, month)[1]:
            continue
        event_date = datetime(year, month, day).date()
        if template.get("is_shift"):
            start_at, end_at = _build_shift_window_from_times(
                event_date,
                start_time,
                end_time=end_time,
                duration_minutes=template["duration_minutes"],
            )
        else:
            start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=start_time[0], minute=start_time[1])
            end_at = datetime.combine(event_date, datetime.min.time()).replace(hour=end_time[0], minute=end_time[1])
            if end_at <= start_at:
                end_at += timedelta(days=1)
        events.append(
            {
                "title": template["title"],
                "calendar_type": calendar_type,
                "reminders": reminders,
                "location": template["location"],
                "duration_minutes": int((end_at - start_at).total_seconds() / 60),
                "start_at": start_at,
                "end_at": end_at,
            }
        )
    return events


def _build_mixed_template_events_from_message(message):
    normalized = _normalize_text(message)
    lowered = normalized.lower()
    month, year = _extract_month_year(normalized)
    now = _utcnow()
    if not month:
        month = now.month
        year = now.year

    clauses = []
    pattern_map = [
        (r"(תורנות חצי|חצי תורנות|half shift|half-call|half call|partial call)(.*?)(?=(?:\bתורנות\b|\bתורנויות\b|\bתורניות\b|\bמחלקות\b|$))", _match_scheduling_template("תורנות חצי")),
        (r"(מחלקות|משמרת מחלקות|department shift|ward shift)(.*?)(?=(?:\bתורנות\b|\bתורנויות\b|\bתורניות\b|\bתורנות חצי\b|$))", _match_scheduling_template("מחלקות")),
        (r"(תורנויות|תורניות|תורנות|on-call|on call|call shifts?)(.*?)(?=(?:\bתורנות חצי\b|\bחצי תורנות\b|\bמחלקות\b|$))", _match_scheduling_template("תורנות")),
    ]

    for pattern, template in pattern_map:
        for match in re.finditer(pattern, lowered, flags=re.IGNORECASE):
            full_segment = normalized[match.start():match.end()]
            days = _extract_template_clause_days(full_segment)
            if days:
                clauses.append((match.start(), template, days))

    if len(clauses) < 2:
        return None

    calendar_type = _infer_calendar_type(normalized)
    events = []
    for _, template, days in sorted(clauses, key=lambda item: item[0]):
        events.extend(_build_template_events_for_days(template, days, month, year, calendar_type))

    unique_events = []
    seen = set()
    for event in sorted(events, key=lambda item: (item["start_at"], item["title"])):
        key = (event["title"], event["start_at"], event["end_at"])
        if key in seen:
            continue
        seen.add(key)
        unique_events.append(event)

    if len(unique_events) < 2:
        return None

    return {
        "status": "ready",
        "events": unique_events,
        "calendar_type": calendar_type,
        "title": unique_events[0]["title"] if len({item["title"] for item in unique_events}) == 1 else "Mixed shifts",
    }


def _find_target_event_from_extraction(session_id, extraction, raw_message):
    extraction = _apply_extraction_defaults(extraction, raw_message)
    requested_date = _parse_iso_date(extraction.get("source_date")) or _parse_iso_date(extraction.get("date"))
    title_tokens = set(_tokenize_title(extraction.get("title") or _clean_title(raw_message)))
    is_shift = bool(extraction.get("is_shift"))
    candidates = list(
        scheduled_events_collection.find({"session_id": session_id, "status": "confirmed"}).sort("start_at", 1)
    )

    best_match = None
    best_score = -1
    for candidate in candidates:
        score = 0
        candidate_title = candidate.get("title", "")
        candidate_tokens = set(_tokenize_title(candidate_title))
        overlap = len(title_tokens & candidate_tokens)
        score += overlap * 6

        if requested_date and candidate.get("start_at") and candidate["start_at"].date() == requested_date:
            score += 10

        if is_shift:
            duration_hours = (candidate.get("end_at") - candidate.get("start_at")).total_seconds() / 3600
            if candidate_title == "תורנות" or candidate.get("location") == DEFAULT_SHIFT_LOCATION or abs(duration_hours - DEFAULT_SHIFT_DURATION_HOURS) < 0.1:
                score += 6

        if title_tokens and any(token in candidate_title.lower() for token in title_tokens):
            score += 2

        if not title_tokens and requested_date and candidate.get("start_at") and candidate["start_at"].date() == requested_date:
            score += 3

        if score > best_score:
            best_score = score
            best_match = candidate

    if not best_match or best_score <= 0:
        return None
    return best_match


def _find_target_event_from_last_reference(session_id):
    last_reference = _get_last_scheduling_reference(session_id)
    if not last_reference:
        return None
    event_id = last_reference.get("event_id")
    if not event_id:
        return None
    try:
        candidate = scheduled_events_collection.find_one(
            {"_id": ObjectId(event_id), "session_id": session_id, "status": "confirmed"}
        )
    except Exception:
        return None
    return candidate


def _build_update_from_extraction(extraction, existing_event, raw_message):
    extraction = _apply_extraction_defaults(extraction, raw_message)
    existing_start = existing_event["start_at"]
    existing_end = existing_event["end_at"]
    requested_date = _parse_iso_date(extraction.get("target_date")) or _parse_iso_date(extraction.get("date"))
    requested_time = _parse_hhmm(extraction.get("start_time"))
    requested_end_time = _parse_hhmm(extraction.get("end_time"))

    start_at = existing_start
    if requested_date:
        start_at = datetime.combine(requested_date, start_at.time())
    if requested_time:
        start_at = start_at.replace(hour=requested_time[0], minute=requested_time[1])

    if requested_end_time:
        end_at = datetime.combine(start_at.date(), datetime.min.time()).replace(hour=requested_end_time[0], minute=requested_end_time[1])
        if end_at <= start_at:
            end_at += timedelta(days=1)
    else:
        duration = existing_end - existing_start
        end_at = start_at + duration

    title = extraction.get("title") or existing_event.get("title", "Untitled event")
    calendar_type = extraction.get("calendar_type") or existing_event.get("calendar_type", "personal")
    location = extraction.get("location") if extraction.get("location") is not None else existing_event.get("location")
    duration_minutes = int((end_at - start_at).total_seconds() / 60)

    return {
        "title": title,
        "calendar_type": calendar_type,
        "reminders": existing_event.get("reminders", _infer_reminders(calendar_type)),
        "location": location,
        "duration_minutes": duration_minutes,
        "start_at": start_at,
        "end_at": end_at,
    }


def _serialize_conflict(conflict):
    return {
        "title": conflict.get("title", "Existing event"),
        "calendar_type": conflict.get("calendar_type", "personal"),
        "start_at": conflict["start_at"].isoformat(timespec="minutes"),
        "end_at": conflict["end_at"].isoformat(timespec="minutes"),
    }


def _build_bulk_events_from_message(message):
    normalized = _normalize_text(message)
    event_time = _extract_time(normalized)
    calendar_type = _infer_calendar_type(normalized)
    reminders = _infer_reminders(calendar_type)
    month, year = _extract_month_year(normalized)
    now = _utcnow()
    is_shift_template = _is_shift_template(normalized)
    duration_minutes = _infer_event_minutes(normalized, is_shift_template=is_shift_template)

    if not event_time and not is_shift_template:
        return None

    candidate_dates = []

    weekday_matches = re.findall(r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", normalized, flags=re.IGNORECASE)
    if "every" in normalized.lower() and weekday_matches:
        if not month or not year:
            return {
                "status": "needs_details",
                "missing_fields": ["month"],
                "calendar_type": calendar_type,
                "title": _clean_title(normalized),
            }
        seen = set()
        for weekday_name in weekday_matches:
            weekday_index = WEEKDAYS[weekday_name.lower()]
            days_in_month = monthrange(year, month)[1]
            for day in range(1, days_in_month + 1):
                if datetime(year, month, day).weekday() == weekday_index:
                    date_value = datetime(year, month, day).date()
                    if date_value >= now.date() and date_value not in seen:
                        candidate_dates.append(date_value)
                        seen.add(date_value)
    else:
        if not month:
            month = now.month
            year = now.year

        range_match = re.search(r"\b(\d{1,2})\s*[-–]\s*(\d{1,2})\b", normalized)
        if range_match:
            start_day = int(range_match.group(1))
            end_day = int(range_match.group(2))
            if start_day <= end_day:
                candidate_dates.extend(
                    [
                        datetime(year, month, day).date()
                        for day in range(start_day, end_day + 1)
                        if day <= monthrange(year, month)[1]
                    ]
                )
        else:
            list_match = re.search(r"\b(\d{1,2}(?:\s*,\s*\d{1,2}){1,})\b", normalized)
            if list_match:
                for part in list_match.group(1).split(","):
                    day = int(part.strip())
                    if day <= monthrange(year, month)[1]:
                        candidate_dates.append(datetime(year, month, day).date())

    candidate_dates = sorted({date_value for date_value in candidate_dates if date_value >= now.date()})
    if len(candidate_dates) < 2:
        return None

    title = _normalize_event_title(normalized)
    events = []
    for event_date in candidate_dates:
        if is_shift_template:
            start_at, end_at = _build_shift_window(event_date)
        else:
            start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=event_time[0], minute=event_time[1])
            end_at = start_at + timedelta(minutes=duration_minutes)
        events.append(
            {
                "title": title,
                "calendar_type": calendar_type,
                "reminders": reminders,
                "location": _infer_default_location(normalized),
                "duration_minutes": duration_minutes,
                "start_at": start_at,
                "end_at": end_at,
            }
        )

    return {"status": "ready", "events": events, "calendar_type": calendar_type, "title": title}


def _build_event_from_message(message):
    normalized = _normalize_text(message)
    calendar_type = _infer_calendar_type(normalized)
    reminders = _infer_reminders(calendar_type)
    event_date = _extract_date(normalized)
    time_range = _extract_time_range(normalized)
    event_time = time_range[0] if time_range else _extract_time(normalized)
    is_shift_template = _is_shift_template(normalized)
    duration_minutes = _infer_event_minutes(normalized, is_shift_template=is_shift_template)

    missing = []
    if not event_date:
        missing.append("date")
    if not event_time and not is_shift_template:
        missing.append("time")

    if missing:
        return {
            "status": "needs_details",
            "missing_fields": missing,
            "calendar_type": calendar_type,
            "title": _normalize_event_title(normalized),
            "location": _infer_default_location(normalized),
            "duration_minutes": duration_minutes,
        }

    if is_shift_template and not event_time:
        start_at, end_at = _build_shift_window(event_date)
    elif is_shift_template and event_time:
        start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=event_time[0], minute=event_time[1])
        end_at = start_at + timedelta(hours=DEFAULT_SHIFT_DURATION_HOURS)
    else:
        start_at = datetime.combine(event_date, datetime.min.time()).replace(hour=event_time[0], minute=event_time[1])
        if time_range:
            end_at = datetime.combine(event_date, datetime.min.time()).replace(hour=time_range[1][0], minute=time_range[1][1])
            if end_at <= start_at:
                end_at = end_at + timedelta(days=1)
            duration_minutes = int((end_at - start_at).total_seconds() / 60)
        else:
            end_at = start_at + timedelta(minutes=duration_minutes)

    parsed_event = {
        "status": "ready",
        "title": _normalize_event_title(normalized),
        "calendar_type": calendar_type,
        "reminders": reminders,
        "location": _infer_default_location(normalized),
        "duration_minutes": duration_minutes,
        "start_at": start_at,
        "end_at": end_at,
    }
    log_event(
        "scheduling_parse",
        payload={
            "raw_message": message,
            "title": parsed_event["title"],
            "location": parsed_event.get("location"),
            "duration_minutes": parsed_event.get("duration_minutes"),
            "calendar_type": parsed_event["calendar_type"],
        },
    )
    return parsed_event


def _find_target_event(session_id, message):
    normalized = _normalize_text(message)
    requested_date = _extract_date(normalized)
    title_tokens = _tokenize_title(_clean_title(normalized))
    candidates = list(
        scheduled_events_collection.find({"session_id": session_id, "status": "confirmed"}).sort("start_at", 1)
    )

    best_match = None
    best_score = -1
    for candidate in candidates:
        score = 0
        candidate_title = candidate.get("title", "")
        candidate_tokens = set(_tokenize_title(candidate_title))
        overlap = len(set(title_tokens) & candidate_tokens)
        score += overlap * 5

        if requested_date and candidate.get("start_at") and candidate["start_at"].date() == requested_date:
            score += 4

        if title_tokens and any(token in candidate_title.lower() for token in title_tokens):
            score += 2

        if not title_tokens and requested_date and candidate.get("start_at") and candidate["start_at"].date() == requested_date:
            score += 1

        if score > best_score:
            best_score = score
            best_match = candidate

    if not best_match or best_score <= 0:
        return None
    return best_match


def _is_bulk_shift_delete_request(message):
    normalized = _normalize_text(message)
    lowered = normalized.lower()
    if _detect_action(normalized) != "delete" or not _is_shift_template(normalized):
        return False

    month, year = _extract_month_year(normalized)
    if month and year:
        return True

    return "all" in lowered or "כל" in normalized


def _find_bulk_target_events(session_id, message):
    month, year = _extract_month_year(message)
    if not month or not year:
        return []

    start_of_month = datetime(year, month, 1)
    end_of_month = datetime(year, month, monthrange(year, month)[1], 23, 59, 59)
    candidates = list(
        scheduled_events_collection.find(
            {
                "session_id": session_id,
                "status": "confirmed",
                "start_at": {"$gte": start_of_month, "$lte": end_of_month},
            }
        ).sort("start_at", 1)
    )
    targets = []
    for candidate in candidates:
        title = (candidate.get("title") or "").strip()
        location = (candidate.get("location") or "").strip()
        duration_hours = (candidate.get("end_at") - candidate.get("start_at")).total_seconds() / 3600
        if title == "תורנות" or location == DEFAULT_SHIFT_LOCATION or abs(duration_hours - DEFAULT_SHIFT_DURATION_HOURS) < 0.1:
            targets.append(candidate)
    return targets


def _build_update_from_message(message, existing_event):
    normalized = _normalize_text(message)
    existing_start = existing_event["start_at"]
    existing_end = existing_event["end_at"]
    requested_date = _extract_update_target_date(normalized)
    requested_time = _extract_update_target_time(normalized)

    start_at = existing_start
    if requested_date:
        start_at = datetime.combine(requested_date, start_at.time())
    if requested_time:
        start_at = start_at.replace(hour=requested_time[0], minute=requested_time[1])

    duration = existing_end - existing_start
    end_at = start_at + duration

    return {
        "status": "ready",
        "title": existing_event.get("title", "Untitled event"),
        "calendar_type": existing_event.get("calendar_type", "personal"),
        "reminders": existing_event.get("reminders", _infer_reminders(existing_event.get("calendar_type", "personal"))),
        "start_at": start_at,
        "end_at": end_at,
    }


def _serialize_existing_event(existing_event):
    return {
        "event_id": str(existing_event.get("_id")),
        "title": existing_event.get("title", "Existing event"),
        "calendar_type": existing_event.get("calendar_type", "personal"),
        "provider_event_id": existing_event.get("provider_event_id"),
        "provider_calendar_id": existing_event.get("provider_calendar_id"),
        "start_at": existing_event["start_at"].isoformat(timespec="minutes"),
        "end_at": existing_event["end_at"].isoformat(timespec="minutes"),
        "reminders": existing_event.get("reminders", []),
    }


def _serialize_event(event):
    return {
        "title": event["title"],
        "calendar_type": event["calendar_type"],
        "start_at": event["start_at"].isoformat(timespec="minutes"),
        "end_at": event["end_at"].isoformat(timespec="minutes"),
        "reminders": event["reminders"],
        "location": event.get("location"),
        "duration_minutes": event.get("duration_minutes"),
    }


def _serialize_events(events):
    return [_serialize_event(event) for event in events]


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
    return [_serialize_conflict(conflict) for conflict in conflicts]


def _find_bulk_conflicts(session_id, events):
    all_conflicts = []
    for event in events:
        for conflict in _find_conflicts(session_id, event):
            all_conflicts.append(
                {
                    "event_start_at": event["start_at"].isoformat(timespec="minutes"),
                    "event_title": event["title"],
                    **conflict,
                }
            )
    return all_conflicts


def _save_draft(session_id, raw_message, action_type, parsed_event=None, conflicts=None, target_event=None, target_events=None):
    now = _utcnow()
    draft_id = str(uuid4())
    doc = {
        "draft_id": draft_id,
        "session_id": session_id,
        "raw_message": raw_message,
        "action_type": action_type,
        "parsed_event": parsed_event,
        "conflicts": conflicts or [],
        "target_event": target_event,
        "target_events": target_events or [],
        "status": "pending",
        "created_at": now,
        "updated_at": now,
    }
    scheduling_drafts_collection.insert_one(doc)
    return draft_id


def _format_missing_fields_reply(parsed):
    prefers_hebrew = bool(re.search(r"[\u0590-\u05FF]", (parsed.get("raw_message") or "") + " " + (parsed.get("title") or "")))
    if parsed["missing_fields"] == ["date", "time"]:
        return "בשמחה, חסרים לי גם יום וגם שעה." if prefers_hebrew else "Sure, I still need the day and time."
    if "date" in parsed["missing_fields"]:
        return "באיזה יום לקבוע את זה?" if prefers_hebrew else "What day should I put it on?"
    return "באיזו שעה לקבוע?" if prefers_hebrew else "What time should I set it for?"


def _format_draft_reply(parsed_event, conflicts):
    start_label = parsed_event["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"Here’s the draft for {start_label}. "
            f"I found {len(conflicts)} conflict{'s' if len(conflicts) != 1 else ''} to review before saving."
        )
    return f"Here’s the draft for {start_label}. Review it before saving."


def _format_update_reply(existing_event, updated_event, conflicts):
    start_label = updated_event["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"I prepared an update for {existing_event.get('title', 'this event')} to {start_label}. "
            f"I found {len(conflicts)} conflict{'s' if len(conflicts) != 1 else ''} to review before saving."
        )
    return f"I prepared an update for {existing_event.get('title', 'this event')} to {start_label}. Review it before saving."


def _format_delete_reply(existing_event):
    start_label = existing_event["start_at"].strftime("%a %d %b at %H:%M")
    return f"I found {existing_event.get('title', 'this event')} on {start_label}. Review it before deleting."


def _format_bulk_reply(events, conflicts):
    first_label = events[0]["start_at"].strftime("%a %d %b at %H:%M")
    last_label = events[-1]["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"I prepared {len(events)} events, "
            f"from {first_label} through {last_label}. I found {len(conflicts)} conflict"
            f"{'s' if len(conflicts) != 1 else ''} to review before saving."
        )
    return (
        f"I prepared {len(events)} events, "
        f"from {first_label} through {last_label}. Review them before saving."
    )


def _format_bulk_delete_reply(events):
    if not events:
        return "I couldn’t find matching events to delete."
    first_label = events[0]["start_at"].strftime("%a %d %b at %H:%M")
    last_label = events[-1]["start_at"].strftime("%a %d %b at %H:%M")
    return f"I found {len(events)} events to delete, from {first_label} through {last_label}. Review them before deleting."


def _sync_status_suffix(sync_status, *, plural=False):
    if sync_status == "synced":
        return " Synced to Google Calendar."
    if sync_status == "failed":
        return " Saved here, but Google Calendar sync failed."
    if sync_status == "skipped":
        return " Saved here only. Google Calendar isn't connected on this device yet."
    return ""


def _build_calendar_selector_payload(session_id, calendar_type):
    calendars = get_google_calendars(session_id)
    if not calendars:
        return {"available_calendars": [], "selected_calendar": None}

    preferred_calendar_id = _get_preferred_google_calendar(session_id, calendar_type)
    selected_calendar = None

    if preferred_calendar_id:
        selected_calendar = next(
            (item for item in calendars if item["provider_calendar_id"] == preferred_calendar_id),
            None,
        )

    if not selected_calendar:
        selected_calendar = next((item for item in calendars if item.get("is_primary")), None) or calendars[0]

    return {
        "available_calendars": calendars,
        "selected_calendar": {
            "provider_calendar_id": selected_calendar.get("provider_calendar_id"),
            "name": selected_calendar.get("name", "Primary calendar"),
            "is_primary": bool(selected_calendar.get("is_primary")),
        },
    }


def _resolve_reply_calendar_name(session_id, selected_calendar_id=None, provider_calendar_id=None, fallback_calendar_type=None):
    calendars = get_google_calendars(session_id)
    chosen_id = selected_calendar_id or provider_calendar_id
    if chosen_id:
        matching = next((item for item in calendars if item.get("provider_calendar_id") == chosen_id), None)
        if matching:
            return "Primary" if matching.get("is_primary") else (matching.get("name") or "Calendar")

    calendar_name = None
    if selected_calendar_id:
        calendar_name = get_google_calendar_name(session_id, selected_calendar_id)
    if not calendar_name and provider_calendar_id:
        calendar_name = get_google_calendar_name(session_id, provider_calendar_id)
    if calendar_name:
        return calendar_name
    if fallback_calendar_type:
        return fallback_calendar_type
    return "default"


def _format_event_line(event):
    start_label = event["start_at"].strftime("%H:%M")
    end_label = event["end_at"].strftime("%H:%M")
    return f"- {start_label}-{end_label} · {event.get('title', 'Untitled event')} ({event.get('calendar_type', 'personal')})"


def _build_daily_summary(session_id):
    now = _utcnow()
    start_of_day = datetime.combine(now.date(), datetime.min.time())
    end_of_day = start_of_day + timedelta(days=1)

    events = list(
        scheduled_events_collection.find(
            {
                "session_id": session_id,
                "status": "confirmed",
                "start_at": {"$gte": start_of_day, "$lt": end_of_day},
            }
        ).sort("start_at", 1)
    )

    if not events:
        return {
            "reply": "Today looks clear so far. I don’t see any scheduled events yet.",
            "scheduling_draft": None,
        }

    lines = [f"Today you have {len(events)} event{'s' if len(events) != 1 else ''}:"]
    lines.extend(_format_event_line(event) for event in events)

    total_minutes = int(sum((event["end_at"] - event["start_at"]).total_seconds() for event in events) / 60)
    pressure_points = []
    for current_event, next_event in zip(events, events[1:]):
        gap_minutes = int((next_event["start_at"] - current_event["end_at"]).total_seconds() / 60)
        if gap_minutes < 30:
            pressure_points.append(
                f"{current_event.get('title', 'Event')} -> {next_event.get('title', 'Event')} ({max(gap_minutes, 0)} min gap)"
            )

    upcoming_critical = []
    for event in events:
        minutes_until = int((event["start_at"] - now).total_seconds() / 60)
        if 0 <= minutes_until <= 120 and event.get("calendar_type") in {"work", "kids"}:
            upcoming_critical.append(f"{event['start_at'].strftime('%H:%M')} · {event.get('title', 'Event')}")

    lines.append("")
    lines.append(f"Planned time: {total_minutes // 60}h {total_minutes % 60:02d}m")

    if len(events) >= 5 or total_minutes >= 8 * 60:
        lines.append("Load: heavy")
    elif len(events) >= 3 or total_minutes >= 4 * 60:
        lines.append("Load: moderate")
    else:
        lines.append("Load: light")

    if pressure_points:
        lines.append("Pressure points:")
        lines.extend(f"- {item}" for item in pressure_points[:3])

    if upcoming_critical:
        lines.append("Critical soon:")
        lines.extend(f"- {item}" for item in upcoming_critical[:3])

    return {
        "reply": "\n".join(lines),
        "scheduling_draft": None,
    }


def build_scheduling_welcome():
    return ""


def handle_scheduling_message(session_id, user_message):
    if _is_daily_summary_request(user_message):
        return _build_daily_summary(session_id)

    if _is_bulk_shift_delete_request(user_message):
        target_events = _find_bulk_target_events(session_id, user_message)
        if not target_events:
            return {
                "reply": "I couldn’t find any saved on-call shifts for that month yet.",
                "scheduling_draft": None,
            }
        serialized_targets = [_serialize_existing_event(item) for item in target_events]
        draft_id = _save_draft(
            session_id,
            user_message,
            action_type="bulk_delete",
            target_events=serialized_targets,
        )
        return {
            "reply": _format_bulk_delete_reply(target_events),
            "scheduling_draft": {
                "draft_id": draft_id,
                "action_type": "bulk_delete",
                "title": "תורנות",
                "calendar_type": target_events[0].get("calendar_type", "work").title(),
                "start_at": target_events[0]["start_at"].strftime("%a, %d %b %Y · %H:%M"),
                "end_at": target_events[-1]["start_at"].strftime("%a, %d %b %Y · %H:%M"),
                "reminders": [],
                "location": target_events[0].get("location"),
                "conflicts": [],
                "status": "needs_review",
                "event_count": len(target_events),
                "target_summary": f"{len(target_events)} shift events",
            },
        }

    pending_context = _get_pending_details_context(session_id)
    pending_message = (pending_context or {}).get("raw_message")
    last_reference = _get_last_scheduling_reference(session_id)
    extraction = extract_scheduling_intent(user_message, pending_message=pending_message, last_reference=last_reference)
    use_llm_extraction = _should_use_llm_extraction(extraction)

    log_event(
        "scheduling_parser_selected",
        session_id=session_id,
        payload={
            "user_message": user_message,
            "used_llm_extraction": use_llm_extraction,
            "extraction": extraction,
            "last_reference": last_reference,
        },
    )

    action_type = extraction.get("action") if use_llm_extraction and extraction.get("action") else _detect_action(user_message)
    if action_type in {"update", "delete"}:
        _clear_pending_details_context(session_id)
        target_event = _find_target_event_from_extraction(session_id, extraction, user_message) if use_llm_extraction else _find_target_event(session_id, user_message)
        if not target_event and use_llm_extraction and extraction.get("references_previous"):
            target_event = _find_target_event_from_last_reference(session_id)
        if not target_event:
            return {
                "reply": "I couldn’t find the event yet. Try adding the event name and date.",
                "scheduling_draft": None,
            }

        serialized_target = _serialize_existing_event(target_event)
        if action_type == "delete":
            _save_last_scheduling_reference(session_id, target_event)
            draft_id = _save_draft(
                session_id,
                user_message,
                action_type="delete",
                target_event=serialized_target,
            )
            return {
                "reply": _format_delete_reply(target_event),
                "scheduling_draft": {
                    "draft_id": draft_id,
                    "action_type": "delete",
                    "title": target_event.get("title", "Existing event"),
                "calendar_type": target_event.get("calendar_type", "personal").title(),
                "start_at": target_event["start_at"].strftime("%a, %d %b %Y · %H:%M"),
                "end_at": target_event["end_at"].strftime("%H:%M"),
                "reminders": target_event.get("reminders", []),
                "location": target_event.get("location"),
                "duration_label": _duration_label(int((target_event["end_at"] - target_event["start_at"]).total_seconds() / 60)),
                "conflicts": [],
                "status": "needs_review",
                },
            }

        parsed = _build_update_from_extraction(extraction, target_event, user_message) if use_llm_extraction else _build_update_from_message(user_message, target_event)
        _save_last_scheduling_reference(session_id, target_event)
        conflicts = [
            conflict
            for conflict in _find_conflicts(session_id, parsed)
            if conflict["start_at"] != serialized_target["start_at"] or conflict["title"] != serialized_target["title"]
        ]
        draft_id = _save_draft(
            session_id,
            user_message,
            action_type="update",
            parsed_event=_serialize_event(parsed),
            conflicts=conflicts,
            target_event=serialized_target,
        )
        return {
            "reply": _format_update_reply(target_event, parsed, conflicts),
            "scheduling_draft": {
                "draft_id": draft_id,
                "action_type": "update",
                "title": parsed["title"],
                "calendar_type": parsed["calendar_type"].title(),
                "start_at": parsed["start_at"].strftime("%a, %d %b %Y · %H:%M"),
                "end_at": parsed["end_at"].strftime("%H:%M"),
                "reminders": parsed["reminders"],
                "location": parsed.get("location"),
                "duration_label": _duration_label(parsed.get("duration_minutes")),
                "conflicts": conflicts,
                "status": "needs_review",
                "target_summary": f"{target_event.get('title', 'Existing event')} · {target_event['start_at'].strftime('%a, %d %b %Y · %H:%M')}",
            },
        }

    merged_message = _maybe_merge_with_pending_context(session_id, user_message)

    bulk_parsed = _build_mixed_template_events_from_message(merged_message)
    if not bulk_parsed:
        bulk_parsed = _build_bulk_events_from_extraction(extraction, merged_message) if use_llm_extraction else None
    if not bulk_parsed:
        bulk_parsed = _build_bulk_events_from_message(merged_message)
    if bulk_parsed:
        _clear_pending_details_context(session_id)
        if bulk_parsed["status"] == "needs_details":
            return {
                "reply": "I can prepare those events. I still need the month for the repeated weekdays.",
                "scheduling_draft": None,
            }

        conflicts = _find_bulk_conflicts(session_id, bulk_parsed["events"])
        calendar_selector = _build_calendar_selector_payload(session_id, bulk_parsed["calendar_type"])
        draft_id = _save_draft(
            session_id,
            merged_message,
            action_type="bulk_create",
            parsed_event={"events": _serialize_events(bulk_parsed["events"])},
            conflicts=conflicts,
        )
        return {
            "reply": _format_bulk_reply(bulk_parsed["events"], conflicts),
            "scheduling_draft": {
                "draft_id": draft_id,
                "action_type": "bulk_create",
                "title": bulk_parsed["title"],
                "calendar_type": bulk_parsed["calendar_type"].title(),
                "start_at": bulk_parsed["events"][0]["start_at"].strftime("%a, %d %b %Y · %H:%M"),
                "end_at": bulk_parsed["events"][-1]["start_at"].strftime("%a, %d %b %Y · %H:%M"),
                "reminders": bulk_parsed["events"][0]["reminders"],
                "location": bulk_parsed["events"][0].get("location"),
                "duration_label": _duration_label(bulk_parsed["events"][0].get("duration_minutes")),
                "conflicts": conflicts,
                "status": "needs_review",
                "event_count": len(bulk_parsed["events"]),
                "target_summary": f"{len(bulk_parsed['events'])} events",
                **calendar_selector,
            },
        }

    parsed = _build_event_from_extraction(extraction, merged_message) if use_llm_extraction else _build_event_from_message(merged_message)
    if parsed["status"] == "needs_details":
        _save_pending_details_context(session_id, merged_message, parsed)
        return {
            "reply": _format_missing_fields_reply(parsed),
            "scheduling_draft": None,
        }

    _clear_pending_details_context(session_id)
    conflicts = _find_conflicts(session_id, parsed)
    calendar_selector = _build_calendar_selector_payload(session_id, parsed["calendar_type"])
    draft_id = _save_draft(
        session_id,
        merged_message,
        action_type="create",
        parsed_event=_serialize_event(parsed),
        conflicts=conflicts,
    )

    return {
        "reply": _format_draft_reply(parsed, conflicts),
        "scheduling_draft": {
            "draft_id": draft_id,
            "action_type": "create",
            "title": parsed["title"],
            "calendar_type": parsed["calendar_type"].title(),
            "start_at": parsed["start_at"].strftime("%a, %d %b %Y · %H:%M"),
            "end_at": parsed["end_at"].strftime("%H:%M"),
            "reminders": parsed["reminders"],
            "location": parsed.get("location"),
            "duration_label": _duration_label(parsed.get("duration_minutes")),
            "conflicts": conflicts,
            "status": "needs_review",
            **calendar_selector,
        },
    }


def confirm_scheduling_draft(session_id, draft_id, selected_calendar_id=None):
    draft = scheduling_drafts_collection.find_one({"session_id": session_id, "draft_id": draft_id, "status": "pending"})
    if not draft:
        return {"status": "missing"}

    action_type = draft.get("action_type", "create")
    now = _utcnow()
    if action_type == "create":
        parsed_event = draft["parsed_event"]
        event_doc = {
            "session_id": session_id,
            "draft_id": draft_id,
            "event_id": str(uuid4()),
            "title": parsed_event["title"],
            "calendar_type": parsed_event["calendar_type"],
            "start_at": datetime.fromisoformat(parsed_event["start_at"]),
            "end_at": datetime.fromisoformat(parsed_event["end_at"]),
            "reminders": parsed_event["reminders"],
            "location": parsed_event.get("location"),
            "status": "confirmed",
            "created_at": now,
            "updated_at": now,
        }
        scheduled_events_collection.insert_one(event_doc)
        sync_result = sync_google_create_event(session_id, event_doc, preferred_calendar_id=selected_calendar_id)
        if sync_result.get("status") == "synced":
            scheduled_events_collection.update_one(
                {"draft_id": draft_id, "session_id": session_id},
                {
                    "$set": {
                        "provider": "google",
                        "provider_event_id": sync_result.get("provider_event_id"),
                        "provider_calendar_id": selected_calendar_id or sync_result.get("provider_calendar_id"),
                        "provider_html_link": sync_result.get("html_link"),
                        "updated_at": now,
                    }
                },
            )
            log_event(
                "scheduling_google_create_confirmed",
                session_id=session_id,
                payload={
                    "title": parsed_event["title"],
                    "provider_event_id": sync_result.get("provider_event_id"),
                    "provider_calendar_id": selected_calendar_id or sync_result.get("provider_calendar_id"),
                    "start_at": parsed_event["start_at"],
                    "end_at": parsed_event["end_at"],
                },
            )
        if selected_calendar_id:
            _save_preferred_google_calendar(session_id, parsed_event["calendar_type"], selected_calendar_id)
        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        _save_last_scheduling_reference(session_id, event_doc)
        reply_calendar_name = _resolve_reply_calendar_name(
            session_id,
            selected_calendar_id=selected_calendar_id,
            provider_calendar_id=sync_result.get("provider_calendar_id"),
            fallback_calendar_type=parsed_event["calendar_type"],
        )
        reply = f'Created {parsed_event["title"]} in your "{reply_calendar_name}" calendar.'
        if sync_result.get("status") == "synced":
            reply += f" Synced to Google Calendar"
        else:
            reply += _sync_status_suffix(sync_result.get("status"))
        return {
            "status": "confirmed",
            "reply": reply,
        }

    if action_type == "bulk_create":
        parsed_event = draft.get("parsed_event", {})
        events = parsed_event.get("events", [])
        if not events:
            return {"status": "missing", "reply": "I couldn’t find that draft anymore."}

        inserted_count = 0
        synced_count = 0
        failed_count = 0
        skipped_count = 0
        last_created_event_doc = None
        for event in events:
            event_doc = {
                "session_id": session_id,
                "draft_id": draft_id,
                "event_id": str(uuid4()),
                "title": event["title"],
                "calendar_type": event["calendar_type"],
                "start_at": datetime.fromisoformat(event["start_at"]),
                "end_at": datetime.fromisoformat(event["end_at"]),
                "reminders": event["reminders"],
                "location": event.get("location"),
                "status": "confirmed",
                "created_at": now,
                "updated_at": now,
            }
            insert_result = scheduled_events_collection.insert_one(event_doc)
            event_doc["_id"] = insert_result.inserted_id
            last_created_event_doc = event_doc
            sync_result = sync_google_create_event(session_id, event_doc, preferred_calendar_id=selected_calendar_id)
            if sync_result.get("status") == "synced":
                scheduled_events_collection.update_one(
                    {"_id": insert_result.inserted_id},
                    {
                        "$set": {
                            "provider": "google",
                            "provider_event_id": sync_result.get("provider_event_id"),
                            "provider_calendar_id": selected_calendar_id or sync_result.get("provider_calendar_id"),
                            "provider_html_link": sync_result.get("html_link"),
                            "updated_at": now,
                        }
                    },
                )
                synced_count += 1
                log_event(
                    "scheduling_google_bulk_create_confirmed",
                    session_id=session_id,
                    payload={
                        "title": event["title"],
                        "provider_event_id": sync_result.get("provider_event_id"),
                        "provider_calendar_id": selected_calendar_id or sync_result.get("provider_calendar_id"),
                        "start_at": event["start_at"],
                        "end_at": event["end_at"],
                    },
                )
            elif sync_result.get("status") == "failed":
                failed_count += 1
            else:
                skipped_count += 1
            inserted_count += 1
        if selected_calendar_id:
            _save_preferred_google_calendar(session_id, events[0]["calendar_type"], selected_calendar_id)

        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        if last_created_event_doc:
            _save_last_scheduling_reference(session_id, last_created_event_doc)
        reply_calendar_name = _resolve_reply_calendar_name(
            session_id,
            selected_calendar_id=selected_calendar_id,
            fallback_calendar_type=events[0]["calendar_type"],
        )
        reply = f'Created {inserted_count} events in your "{reply_calendar_name}" calendar.'
        if synced_count == inserted_count:
            reply += " Synced to Google Calendar"
        elif failed_count:
            reply += f" {synced_count} synced to Google Calendar, {failed_count} failed, and {skipped_count} stayed here only."
        elif skipped_count:
            reply += f" {synced_count} synced to Google Calendar, {skipped_count} stayed here only."
        return {"status": "confirmed", "reply": reply}

    if action_type == "bulk_delete":
        target_events = draft.get("target_events", [])
        if not target_events:
            return {"status": "missing", "reply": "I couldn’t find that draft anymore."}

        google_connected = has_google_calendar_connection(session_id)
        deleted_count = 0
        synced_count = 0
        for target in target_events:
            target_filter = {
                "_id": ObjectId(target["event_id"]),
                "session_id": session_id,
                "status": "confirmed",
            }
            existing_doc = scheduled_events_collection.find_one(target_filter)
            if not existing_doc:
                continue

            sync_result = sync_google_delete_event(
                session_id,
                existing_doc.get("provider_event_id"),
                target.get("calendar_type", "personal"),
                preferred_calendar_id=target.get("provider_calendar_id"),
                provider_calendar_id=target.get("provider_calendar_id"),
                event_doc={
                    "title": existing_doc.get("title", target.get("title")),
                    "start_at": existing_doc.get("start_at"),
                    "end_at": existing_doc.get("end_at"),
                },
            )
            if google_connected and sync_result.get("status") != "synced":
                continue

            result = scheduled_events_collection.update_one(
                target_filter,
                {"$set": {"status": "deleted", "updated_at": now}},
            )
            if result.modified_count:
                deleted_count += 1
                if sync_result.get("status") == "synced":
                    synced_count += 1

        if deleted_count == 0:
            return {
                "status": "blocked",
                "reply": "I couldn’t remove those events from Google Calendar, so I left them unchanged.",
            }

        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        if target_events:
            last_deleted = target_events[-1]
            _save_last_scheduling_reference(
                session_id,
                {
                    "event_id": last_deleted.get("event_id"),
                    "title": last_deleted.get("title"),
                    "calendar_type": last_deleted.get("calendar_type"),
                    "start_at": last_deleted.get("start_at"),
                    "end_at": last_deleted.get("end_at"),
                    "location": last_deleted.get("location"),
                    "provider_event_id": last_deleted.get("provider_event_id"),
                    "provider_calendar_id": last_deleted.get("provider_calendar_id"),
                    "status": "deleted",
                },
            )
        reply = f"Deleted {deleted_count} events."
        if google_connected:
            if synced_count == deleted_count:
                reply += " Removed from Google Calendar."
            else:
                reply += f" Removed {synced_count} from Google Calendar."
        else:
            reply += " Removed here only."
        return {"status": "confirmed", "reply": reply}

    target_event = draft.get("target_event")
    if not target_event or not target_event.get("event_id"):
        return {"status": "missing", "reply": "I couldn’t find that draft anymore."}

    target_filter = {"_id": ObjectId(target_event["event_id"]), "session_id": session_id, "status": "confirmed"}

    if action_type == "update":
        parsed_event = draft["parsed_event"]
        result = scheduled_events_collection.update_one(
            target_filter,
            {
                "$set": {
                    "title": parsed_event["title"],
                    "calendar_type": parsed_event["calendar_type"],
                    "start_at": datetime.fromisoformat(parsed_event["start_at"]),
                    "end_at": datetime.fromisoformat(parsed_event["end_at"]),
                    "reminders": parsed_event["reminders"],
                    "location": parsed_event.get("location"),
                    "updated_at": now,
                }
            },
        )
        if not result.modified_count:
            return {"status": "missing", "reply": "I couldn’t update that event because it no longer exists."}

        sync_result = sync_google_update_event(
            session_id,
            target_event.get("provider_event_id"),
            {
                "title": parsed_event["title"],
                "calendar_type": parsed_event["calendar_type"],
                "start_at": datetime.fromisoformat(parsed_event["start_at"]),
                "end_at": datetime.fromisoformat(parsed_event["end_at"]),
                "location": parsed_event.get("location"),
            },
            preferred_calendar_id=selected_calendar_id or target_event.get("provider_calendar_id"),
        )
        if selected_calendar_id:
            scheduling_preferences_collection.update_one(
                {"session_id": session_id},
                {
                    "$set": {
                        "google_calendar_preferences." + parsed_event["calendar_type"]: selected_calendar_id,
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )

        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        updated_doc = scheduled_events_collection.find_one(target_filter)
        if updated_doc:
            _save_last_scheduling_reference(session_id, updated_doc)
        reply = f"Updated {parsed_event['title']} to {datetime.fromisoformat(parsed_event['start_at']).strftime('%a %d %b at %H:%M')}."
        reply += _sync_status_suffix(sync_result.get("status"))
        return {
            "status": "confirmed",
            "reply": reply,
        }

    if action_type == "delete":
        existing_doc = scheduled_events_collection.find_one(target_filter)
        google_connected = has_google_calendar_connection(session_id)
        sync_result = sync_google_delete_event(
            session_id,
            (existing_doc or {}).get("provider_event_id"),
            target_event.get("calendar_type", "personal"),
            preferred_calendar_id=selected_calendar_id or target_event.get("provider_calendar_id"),
            provider_calendar_id=target_event.get("provider_calendar_id"),
            event_doc={
                "title": (existing_doc or {}).get("title", target_event.get("title")),
                "start_at": (existing_doc or {}).get("start_at"),
                "end_at": (existing_doc or {}).get("end_at"),
            },
        )

        if google_connected and sync_result.get("status") != "synced":
            return {
                "status": "blocked",
                "reply": "I couldn’t remove that event from Google Calendar, so I left it unchanged.",
            }

        result = scheduled_events_collection.update_one(
            target_filter,
            {"$set": {"status": "deleted", "updated_at": now}},
        )
        if not result.modified_count:
            return {"status": "missing", "reply": "I couldn’t delete that event because it no longer exists."}

        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        if existing_doc:
            _save_last_scheduling_reference(
                session_id,
                {
                    **existing_doc,
                    "status": "deleted",
                },
            )
        reply = f"Deleted {target_event.get('title', 'the event')}."
        if sync_result.get("status") == "synced":
            reply += " Removed from Google Calendar."
        elif sync_result.get("status") == "failed":
            reply += " Removed here, but Google Calendar deletion failed."
        elif sync_result.get("status") == "skipped":
            reply += " Removed here only."
        return {
            "status": "confirmed",
            "reply": reply,
        }

    return {"status": "missing", "reply": "I couldn’t confirm that draft."}


def dismiss_scheduling_draft(session_id, draft_id):
    now = _utcnow()
    result = scheduling_drafts_collection.update_one(
        {"session_id": session_id, "draft_id": draft_id, "status": "pending"},
        {"$set": {"status": "dismissed", "updated_at": now}},
    )
    if not result.modified_count:
        return {"status": "missing"}
    return {"status": "dismissed", "reply": "Draft dismissed."}
