from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import re
from urllib.parse import urlparse


STRUCTURAL_CHANGE_MESSAGE = (
    "We detected a structural change in the duty schedule file. "
    "Your shifts cannot be reliably identified right now."
)
RELEVANT_TAB_TOKEN = "תורנויות"
DATE_HEADER = "תאריך"
EXPECTED_HEADERS = [
    "תאריך",
    "יום",
    "חדר לידה",
    "קבלה",
    "מיון",
    "ב",
    "תורן חצי",
    "תורן ד",
    "מחלקות",
]
RELEVANT_ROLE_HEADERS = [
    "חדר לידה",
    "קבלה",
    "מיון",
    "ב",
    "תורן חצי",
    "תורן ד",
    "מחלקות",
]
SHORT_DUTY_ROLES = {"תורן חצי", "מחלקות"}
ROLE_TITLE_MAP = {
    "חדר לידה": "תורנות/חדר לידה",
    "קבלה": "תורנות/קבלה",
    "מיון": "תורנות/מיון",
    "ב": "תורנות/ב",
    "תורן חצי": "תורנות/תורן חצי",
    "תורן ד": "תורנות/תורן ד",
    "מחלקות": "תורנות/מחלקות",
}
FRIDAY_MORNING_TOKENS = ("שישי", "בוקר")


class DutySyncStructuralError(Exception):
    def __init__(self, detail=None, context=None):
        super().__init__(STRUCTURAL_CHANGE_MESSAGE)
        self.detail = detail or "Structural parsing check failed."
        self.context = context or {}


@dataclass
class DetectedDuty:
    date: str
    role: str
    title: str
    duty_key: str
    start_datetime: str
    end_datetime: str
    source_tab_name: str
    source_row_index: int
    source_column_name: str
    raw_cell_value: str


def as_iso(dt_value):
    if not dt_value:
        return None
    if isinstance(dt_value, str):
        return dt_value
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_sheet_id(sheet_url_or_id):
    raw = normalize_text(sheet_url_or_id)
    if not raw:
        raise DutySyncStructuralError("Duty sheet URL was empty.")
    parsed = urlparse(raw)
    if not parsed.scheme:
        return raw
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", raw)
    if not match:
        raise DutySyncStructuralError("Could not extract a Google Sheet ID from the provided URL.", {"sheet_url": raw})
    return match.group(1)


def parse_sheet_date(raw_value):
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, (int, float)):
        if raw_value <= 0:
            return None
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=float(raw_value))).date()

    text = normalize_text(raw_value)
    if not text:
        return None
    if text.isdigit():
        numeric = int(text)
        if numeric > 10000:
            return parse_sheet_date(numeric)

    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def detect_header_row(values):
    expected = {normalize_text(item): item for item in EXPECTED_HEADERS}
    for index, row in enumerate(values):
        row_map = {}
        for column_index, cell in enumerate(row):
            normalized = normalize_text(cell)
            if normalized in expected and normalized not in row_map:
                row_map[normalized] = column_index
        if all(key in row_map for key in expected):
            return {
                "row_index": index,
                "columns": {expected[key]: row_map[key] for key in expected},
            }
    raise DutySyncStructuralError(
        "Could not detect a header row with all expected duty columns.",
        {"expected_headers": EXPECTED_HEADERS},
    )


def build_duty_datetimes(duty_date, role):
    start_dt = datetime.combine(duty_date, time(hour=15, minute=0))
    if role in SHORT_DUTY_ROLES:
        end_dt = datetime.combine(duty_date, time(hour=23, minute=0))
    else:
        end_dt = datetime.combine(duty_date + timedelta(days=1), time(hour=8, minute=0))
    return start_dt, end_dt


def is_friday_morning_section_marker(row):
    row_text = " ".join(normalize_text(cell) for cell in (row or []) if normalize_text(cell))
    if not row_text:
        return False
    return all(token in row_text for token in FRIDAY_MORNING_TOKENS)


def analyze_candidate_tab(tab_name, values, full_name, session_id):
    header_info = detect_header_row(values)
    columns = header_info["columns"]
    duties = []
    latest_date = None
    normalized_full_name = normalize_text(full_name)

    for row_index in range(header_info["row_index"] + 1, len(values)):
        row = values[row_index]
        date_cell = row[columns[DATE_HEADER]] if columns[DATE_HEADER] < len(row) else ""
        duty_date = parse_sheet_date(date_cell)

        relevant_values = {}
        for role in RELEVANT_ROLE_HEADERS:
            column_index = columns[role]
            relevant_values[role] = normalize_text(row[column_index] if column_index < len(row) else "")

        row_has_content = any(relevant_values.values()) or normalize_text(date_cell)
        if not row_has_content:
            continue

        if duty_date is None:
            if is_friday_morning_section_marker(row):
                break
            raise DutySyncStructuralError(
                "Found a non-empty duty row with an unreadable date value.",
                {"tab_name": tab_name, "row_index": row_index + 1, "raw_date": date_cell},
            )
        latest_date = max(latest_date, duty_date) if latest_date else duty_date

        matched_roles = []
        for role, cell_value in relevant_values.items():
            if not cell_value:
                continue
            if normalized_full_name in cell_value and cell_value != normalized_full_name:
                raise DutySyncStructuralError(
                    "Found a duty cell that mentions the user name but is not an exact full-name match.",
                    {"tab_name": tab_name, "row_index": row_index + 1, "role": role, "raw_cell_value": cell_value},
                )
            if cell_value != normalized_full_name:
                continue
            matched_roles.append(role)
            start_dt, end_dt = build_duty_datetimes(duty_date, role)
            duty_key = f"{session_id}:{duty_date.isoformat()}:{role}"
            duties.append(
                DetectedDuty(
                    date=duty_date.isoformat(),
                    role=role,
                    title=ROLE_TITLE_MAP[role],
                    duty_key=duty_key,
                    start_datetime=as_iso(start_dt),
                    end_datetime=as_iso(end_dt),
                    source_tab_name=tab_name,
                    source_row_index=row_index + 1,
                    source_column_name=role,
                    raw_cell_value=cell_value,
                )
            )
        if len(matched_roles) > 1:
            raise DutySyncStructuralError(
                "The user appeared in more than one relevant duty role on the same row.",
                {"tab_name": tab_name, "row_index": row_index + 1, "roles": matched_roles},
            )

    if latest_date is None:
        raise DutySyncStructuralError(
            "No readable duty dates were found in the candidate duty tab.",
            {"tab_name": tab_name},
        )

    return {
        "tab_name": tab_name,
        "latest_date": latest_date,
        "source_month": latest_date.strftime("%Y-%m"),
        "duties": duties,
    }


def select_relevant_tab_from_values_map(tab_values, full_name, session_id):
    analyses = []
    for tab_name, values in tab_values.items():
        normalized_tab_name = normalize_text(tab_name)
        if RELEVANT_TAB_TOKEN not in normalized_tab_name:
            continue
        analyses.append(analyze_candidate_tab(normalized_tab_name, values, full_name, session_id))
    if not analyses:
        raise DutySyncStructuralError(
            "No tab name containing the required token was found.",
            {"required_tab_token": RELEVANT_TAB_TOKEN},
        )
    analyses.sort(key=lambda item: item["latest_date"], reverse=True)
    best = analyses[0]
    if len(analyses) > 1 and analyses[1]["latest_date"] == best["latest_date"]:
        raise DutySyncStructuralError(
            "Two duty tabs shared the same latest detected date, so the latest roster could not be selected deterministically.",
            {
                "top_tabs": [
                    {"tab_name": analyses[0]["tab_name"], "latest_date": analyses[0]["latest_date"].isoformat()},
                    {"tab_name": analyses[1]["tab_name"], "latest_date": analyses[1]["latest_date"].isoformat()},
                ]
            },
        )
    return best
