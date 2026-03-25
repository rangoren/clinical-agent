import re
from calendar import monthrange
from datetime import datetime, timedelta
from uuid import uuid4

from bson import ObjectId

from db import scheduled_events_collection, scheduling_drafts_collection, scheduling_preferences_collection
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
KNOWN_LOCATIONS = {
    "שיבא": ("shiba", "sheba", "tel hashomer", "תל השומר", "שיבא"),
}
CALENDAR_KEYWORDS = {
    "work": ("work", "shift", "clinic", "ward", "call", "hospital", "meeting", "on-call", "on call", "night shift", "night shifts", "call shift", "call shifts", "תורנות", "תורנויות", "כוננות", "משמרת לילה", "תורנית"),
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


def _utcnow():
    return datetime.utcnow()


def _get_scheduling_preferences(session_id):
    return scheduling_preferences_collection.find_one({"session_id": session_id}) or {}


def _get_preferred_google_calendar(session_id, calendar_type):
    preferences = _get_scheduling_preferences(session_id)
    preferred_map = preferences.get("google_calendar_preferences", {})
    return preferred_map.get(calendar_type)


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
    return [
        token
        for token in re.findall(r"[A-Za-z0-9\u0590-\u05FF]+", (text or "").lower())
        if token
        not in {
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
            "ביום",
            "בתאריך",
            "בשעה",
            "משעה",
            "עד",
            "היום",
            "מחר",
        }
    ]


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


def _build_shift_window(event_date):
    start_at = datetime.combine(event_date, datetime.min.time()).replace(
        hour=DEFAULT_SHIFT_START_HOUR,
        minute=DEFAULT_SHIFT_START_MINUTE,
    )
    end_at = start_at + timedelta(hours=DEFAULT_SHIFT_DURATION_HOURS)
    return start_at, end_at


def _infer_default_location(text):
    if _is_shift_template(text):
        return DEFAULT_SHIFT_LOCATION
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
    if participant and any(keyword in lowered for keyword in ("meeting", "appointment", "פגישה", "שיחה", "זום", "zoom")):
        if any(keyword in text for keyword in ("פגישה", "שיחה")):
            return f"פגישה עם {participant}"
        return f"Meeting with {participant}"
    return None


def _normalize_event_title(text):
    if _is_shift_template(text):
        return "תורנות"
    semantic_title = _build_semantic_title(text)
    if semantic_title:
        return semantic_title
    return _clean_title(text)


def _infer_event_minutes(text, is_shift_template=False):
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

    month_names_pattern = "|".join(sorted((re.escape(name) for name in MONTHS.keys() if name.isascii()), key=len, reverse=True))
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

    month_names_pattern = "|".join(sorted((re.escape(name) for name in MONTHS.keys() if name.isascii()), key=len, reverse=True))
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
        r"^\s*(?:תכניס(?:י)?|תוסיף(?:י)?|תשים(?:י)?)(?:\s+לי)?(?:\s+ליומן)?(?:\s+ביומן)?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s+(?:ביום|בתאריך|בשעה|משעה|מהשעה|from|on|starting|start|today|tomorrow|היום|מחר)\b.*$",
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
    cleaned = re.sub(r"\b(פגישה|שיחה|תכניס ליומן|תוסיף ליומן|תשים לי ביומן|ביומן|ליומן|אירוע|תאריך|בתאריך|בשעה|עד|בין)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
    return cleaned or "Untitled event"


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
    details = []
    if parsed.get("title"):
        details.append(f"title: {parsed['title']}")
    if parsed.get("location"):
        details.append(f"location: {parsed['location']}")
    if parsed.get("duration_minutes"):
        details.append(f"duration: {parsed['duration_minutes']} minutes")
    details_suffix = f" I understood {', '.join(details)}." if details else ""
    if parsed["missing_fields"] == ["date", "time"]:
        return "I can draft that, but I still need a date and time before I create anything." + details_suffix
    if "date" in parsed["missing_fields"]:
        return "I can draft that, but I still need the date." + details_suffix
    return "I can draft that, but I still need the time." + details_suffix


def _format_draft_reply(parsed_event, conflicts):
    start_label = parsed_event["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"I drafted this for {start_label}. "
            f"I also found {len(conflicts)} conflict{'s' if len(conflicts) != 1 else ''}. Review before creating."
        )
    return f"I drafted this for {start_label}. Review it before I create it."


def _format_update_reply(existing_event, updated_event, conflicts):
    start_label = updated_event["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"I drafted an update for {existing_event.get('title', 'this event')} to {start_label}. "
            f"I also found {len(conflicts)} conflict{'s' if len(conflicts) != 1 else ''}. Review before I update it."
        )
    return f"I drafted an update for {existing_event.get('title', 'this event')} to {start_label}. Review it before I update it."


def _format_delete_reply(existing_event):
    start_label = existing_event["start_at"].strftime("%a %d %b at %H:%M")
    return f"I found {existing_event.get('title', 'this event')} on {start_label}. Review it before I delete it."


def _format_bulk_reply(events, conflicts):
    first_label = events[0]["start_at"].strftime("%a %d %b at %H:%M")
    last_label = events[-1]["start_at"].strftime("%a %d %b at %H:%M")
    if conflicts:
        return (
            f"I drafted {len(events)} events, "
            f"from {first_label} through {last_label}. I also found {len(conflicts)} conflict"
            f"{'s' if len(conflicts) != 1 else ''}. Review before creating."
        )
    return (
        f"I drafted {len(events)} events, "
        f"from {first_label} through {last_label}. Review them before I create them."
    )


def _format_bulk_delete_reply(events):
    if not events:
        return "I couldn't find any matching events to delete."
    first_label = events[0]["start_at"].strftime("%a %d %b at %H:%M")
    last_label = events[-1]["start_at"].strftime("%a %d %b at %H:%M")
    return f"I found {len(events)} events to delete, from {first_label} through {last_label}. Review them before I delete them."


def _sync_status_suffix(sync_status, *, plural=False):
    if sync_status == "synced":
        return " Synced to Google Calendar."
    if sync_status == "failed":
        return " Saved locally, but Google Calendar sync failed."
    if sync_status == "skipped":
        return " Saved locally only."
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
    return (
        "Scheduling Copilot is active.<br><br>"
        "Try something like:<br>"
        "- Add dinner with Maya tomorrow at 19:30<br>"
        "- Schedule school pickup next Tuesday at 16:00<br>"
        "- Add clinic meeting next Monday at 08:00"
    )


def handle_scheduling_message(session_id, user_message):
    if _is_daily_summary_request(user_message):
        return _build_daily_summary(session_id)

    if _is_bulk_shift_delete_request(user_message):
        target_events = _find_bulk_target_events(session_id, user_message)
        if not target_events:
            return {
                "reply": "I couldn't find any scheduled on-call shifts for that month yet.",
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

    action_type = _detect_action(user_message)
    if action_type in {"update", "delete"}:
        target_event = _find_target_event(session_id, user_message)
        if not target_event:
            return {
                "reply": "I couldn't find a matching scheduled event to update yet. Try including the event name and date.",
                "scheduling_draft": None,
            }

        serialized_target = _serialize_existing_event(target_event)
        if action_type == "delete":
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

        parsed = _build_update_from_message(user_message, target_event)
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

    bulk_parsed = _build_bulk_events_from_message(user_message)
    if bulk_parsed:
        if bulk_parsed["status"] == "needs_details":
            return {
                "reply": "I can draft those events, but I still need the month for the repeated weekdays.",
                "scheduling_draft": None,
            }

        conflicts = _find_bulk_conflicts(session_id, bulk_parsed["events"])
        calendar_selector = _build_calendar_selector_payload(session_id, bulk_parsed["calendar_type"])
        draft_id = _save_draft(
            session_id,
            user_message,
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

    parsed = _build_event_from_message(user_message)
    if parsed["status"] == "needs_details":
        return {
            "reply": _format_missing_fields_reply(parsed),
            "scheduling_draft": None,
        }

    conflicts = _find_conflicts(session_id, parsed)
    calendar_selector = _build_calendar_selector_payload(session_id, parsed["calendar_type"])
    draft_id = _save_draft(
        session_id,
        user_message,
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
        reply_calendar_name = _resolve_reply_calendar_name(
            session_id,
            selected_calendar_id=selected_calendar_id,
            provider_calendar_id=sync_result.get("provider_calendar_id"),
            fallback_calendar_type=parsed_event["calendar_type"],
        )
        reply = f'Created {parsed_event["title"]} in your "{reply_calendar_name}" calendar.'
        if sync_result.get("status") == "synced":
            reply += f" Synced to Google Calendar"
            reply += ". Apple Calendar may take a moment to refresh."
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
            return {"status": "missing", "reply": "I couldn't find those drafted events anymore."}

        inserted_count = 0
        synced_count = 0
        failed_count = 0
        skipped_count = 0
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
        reply_calendar_name = _resolve_reply_calendar_name(
            session_id,
            selected_calendar_id=selected_calendar_id,
            fallback_calendar_type=events[0]["calendar_type"],
        )
        reply = f'Created {inserted_count} events in your "{reply_calendar_name}" calendar.'
        if synced_count == inserted_count:
            reply += " Synced to Google Calendar"
            reply += ". Apple Calendar may take a moment to refresh."
        elif failed_count:
            reply += f" {synced_count} synced to Google Calendar, {failed_count} failed, and {skipped_count} stayed local only."
        elif skipped_count:
            reply += f" {synced_count} synced to Google Calendar, {skipped_count} stayed local only."
        return {"status": "confirmed", "reply": reply}

    if action_type == "bulk_delete":
        target_events = draft.get("target_events", [])
        if not target_events:
            return {"status": "missing", "reply": "I couldn't find those drafted events anymore."}

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
                "reply": "I couldn't remove those events from Google Calendar, so I left them unchanged.",
            }

        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        reply = f"Deleted {deleted_count} events."
        if google_connected:
            if synced_count == deleted_count:
                reply += " Removed from Google Calendar. Apple Calendar may take a moment to refresh."
            else:
                reply += f" Removed {synced_count} from Google Calendar."
        else:
            reply += " Removed locally only."
        return {"status": "confirmed", "reply": reply}

    target_event = draft.get("target_event")
    if not target_event or not target_event.get("event_id"):
        return {"status": "missing", "reply": "I couldn't find that draft target anymore."}

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
            return {"status": "missing", "reply": "I couldn't update that event because it no longer exists."}

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
                "reply": "I couldn't remove that event from Google Calendar, so I left it unchanged.",
            }

        result = scheduled_events_collection.update_one(
            target_filter,
            {"$set": {"status": "deleted", "updated_at": now}},
        )
        if not result.modified_count:
            return {"status": "missing", "reply": "I couldn't delete that event because it no longer exists."}

        scheduling_drafts_collection.update_one(
            {"draft_id": draft_id, "session_id": session_id},
            {"$set": {"status": "confirmed", "updated_at": now}},
        )
        reply = f"Deleted {target_event.get('title', 'the event')}."
        if sync_result.get("status") == "synced":
            reply += " Removed from Google Calendar. Apple Calendar may take a moment to refresh."
        elif sync_result.get("status") == "failed":
            reply += " Removed locally, but Google Calendar deletion failed."
        elif sync_result.get("status") == "skipped":
            reply += " Removed locally only."
        return {
            "status": "confirmed",
            "reply": reply,
        }

    return {"status": "missing", "reply": "I couldn't confirm that draft."}


def dismiss_scheduling_draft(session_id, draft_id):
    now = _utcnow()
    result = scheduling_drafts_collection.update_one(
        {"session_id": session_id, "draft_id": draft_id, "status": "pending"},
        {"$set": {"status": "dismissed", "updated_at": now}},
    )
    if not result.modified_count:
        return {"status": "missing"}
    return {"status": "dismissed", "reply": "Draft dismissed."}
