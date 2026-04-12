from dataclasses import asdict
from datetime import datetime
from urllib.parse import quote

import requests

from db import duty_sync_connections_collection
from services.duty_sync_parsing import (
    RELEVANT_TAB_TOKEN,
    STRUCTURAL_CHANGE_MESSAGE,
    DutySyncStructuralError,
    analyze_candidate_tab,
    as_iso,
    normalize_sheet_id,
    normalize_text,
)
from services.google_calendar_service import (
    GOOGLE_HTTP_TIMEOUT_SECONDS,
    GOOGLE_SHEETS_READONLY_SCOPE,
    _auth_headers,
    begin_google_calendar_connect,
    get_google_connection,
    google_calendar_enabled,
    google_connection_has_scopes,
    has_google_calendar_connection,
)
from services.logging_service import log_event


NO_DUTIES_MESSAGE_HE = "לא שובצת החודש בלוח התורנויות"
DEFAULT_TEST_DUTY_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1L50eFprLasbWbu808emdUa5HFQAtAZjzkbyvhGSG9eQ/edit?gid=0#gid=0"
)
DEFAULT_TEST_DUTY_USER_FULL_NAME = "גורן"
GOOGLE_SHEETS_METADATA_URL = "https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
GOOGLE_SHEETS_VALUES_URL = "https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{sheet_range}"


def _utcnow():
    return datetime.utcnow()


def _required_google_sheet_access(session_id):
    if not google_calendar_enabled():
        return {
            "ok": False,
            "status": "unavailable",
            "reply": "Google integration is not configured in this environment.",
        }
    if not has_google_calendar_connection(session_id):
        connect_result = begin_google_calendar_connect(session_id)
        return {
            "ok": False,
            "status": "google_connect_required",
            "reply": "Reconnect Google once to allow Duty Sync to read the test sheet.",
            "auth_url": connect_result.get("auth_url"),
        }
    if not google_connection_has_scopes(session_id, [GOOGLE_SHEETS_READONLY_SCOPE]):
        connect_result = begin_google_calendar_connect(session_id)
        return {
            "ok": False,
            "status": "google_reconnect_required",
            "reply": "Reconnect Google once to allow Duty Sync to read the test sheet.",
            "auth_url": connect_result.get("auth_url"),
        }
    return {"ok": True}


def _google_get(session_id, url, params=None):
    connection = get_google_connection(session_id)
    if not connection:
        raise DutySyncStructuralError(STRUCTURAL_CHANGE_MESSAGE)
    response = requests.get(
        url,
        headers=_auth_headers(connection["access_token"]),
        params=params or {},
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        raise DutySyncStructuralError("Google authorization expired. Reconnect Google and try again.")
    response.raise_for_status()
    return response.json()


def _fetch_sheet_metadata(session_id, sheet_id):
    url = GOOGLE_SHEETS_METADATA_URL.format(sheet_id=sheet_id)
    return _google_get(
        session_id,
        url,
        params={"fields": "sheets.properties(title,sheetId)"},
    )


def _fetch_sheet_values(session_id, sheet_id, tab_name):
    encoded_range = quote(f"'{tab_name}'", safe="")
    url = GOOGLE_SHEETS_VALUES_URL.format(sheet_id=sheet_id, sheet_range=encoded_range)
    data = _google_get(session_id, url)
    return data.get("values") or []


def _select_relevant_tab(session_id, sheet_id, full_name):
    metadata = _fetch_sheet_metadata(session_id, sheet_id)
    tab_names = [
        normalize_text((sheet.get("properties") or {}).get("title"))
        for sheet in metadata.get("sheets") or []
        if RELEVANT_TAB_TOKEN in normalize_text((sheet.get("properties") or {}).get("title"))
    ]
    if not tab_names:
        raise DutySyncStructuralError(STRUCTURAL_CHANGE_MESSAGE)

    analyses = []
    for tab_name in tab_names:
        values = _fetch_sheet_values(session_id, sheet_id, tab_name)
        analyses.append(analyze_candidate_tab(tab_name, values, full_name, session_id))

    analyses.sort(key=lambda item: item["latest_date"], reverse=True)
    best = analyses[0]
    if len(analyses) > 1 and analyses[1]["latest_date"] == best["latest_date"]:
        raise DutySyncStructuralError(STRUCTURAL_CHANGE_MESSAGE)
    return best


def _connection_doc(session_id):
    return duty_sync_connections_collection.find_one({"session_id": session_id})


def _upsert_connection_state(session_id, payload):
    now = _utcnow()
    duty_sync_connections_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {
                **payload,
                "session_id": session_id,
                "user_id": session_id,
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


def get_duty_sync_status(session_id):
    access_state = _required_google_sheet_access(session_id)
    doc = _connection_doc(session_id) or {}
    current_status = doc.get("current_status", "not_connected")
    last_checked_at = as_iso(doc.get("last_checked_at"))
    if not access_state.get("ok"):
        return {
            "available": google_calendar_enabled(),
            "connected": bool(doc.get("is_connected")),
            "google_connected": has_google_calendar_connection(session_id),
            "requires_google_reconnect": access_state.get("status") in {"google_connect_required", "google_reconnect_required"},
            "current_status": access_state.get("status"),
            "label": "Duty schedule: Not available" if access_state.get("status") == "unavailable" else "Duty schedule: Google reconnect required",
            "details": access_state.get("reply"),
            "last_checked_at": last_checked_at,
        }

    if not doc.get("is_connected"):
        return {
            "available": True,
            "connected": False,
            "google_connected": True,
            "requires_google_reconnect": False,
            "current_status": "not_connected",
            "label": "Duty schedule: Not connected",
            "details": "Connect the test duty sheet once from Settings.",
            "last_checked_at": None,
        }

    details = {
        "connected": "Duty schedule connected",
        "no_duties": NO_DUTIES_MESSAGE_HE,
        "error": doc.get("last_error_message") or STRUCTURAL_CHANGE_MESSAGE,
    }.get(current_status, "Duty schedule connected")
    if current_status == "connected" and doc.get("duty_count") is not None:
        details = f"Detected {doc.get('duty_count', 0)} duties in {doc.get('source_tab_name', 'the latest roster')}."

    return {
        "available": True,
        "connected": True,
        "google_connected": True,
        "requires_google_reconnect": False,
        "current_status": current_status,
        "label": "Duty schedule: Error" if current_status == "error" else "Duty schedule connected",
        "details": details,
        "last_checked_at": last_checked_at,
        "last_successful_parse_at": as_iso(doc.get("last_successful_parse_at")),
    }


def connect_duty_sheet(session_id, sheet_url=None, full_name=None):
    access_state = _required_google_sheet_access(session_id)
    if not access_state.get("ok"):
        return access_state

    normalized_sheet_url = normalize_text(sheet_url) or DEFAULT_TEST_DUTY_SHEET_URL
    normalized_full_name = normalize_text(full_name) or DEFAULT_TEST_DUTY_USER_FULL_NAME
    sheet_id = normalize_sheet_id(normalized_sheet_url)
    now = _utcnow()

    try:
        selected_tab = _select_relevant_tab(session_id, sheet_id, normalized_full_name)
        duties = [asdict(item) for item in selected_tab["duties"]]
        current_status = "connected" if duties else "no_duties"
        reply = (
            f"Duty Sync connected. Found {len(duties)} duties in the latest roster."
            if duties
            else NO_DUTIES_MESSAGE_HE
        )
        _upsert_connection_state(
            session_id,
            {
                "sheet_url": normalized_sheet_url,
                "sheet_id": sheet_id,
                "full_name": normalized_full_name,
                "is_connected": True,
                "connected_at": now,
                "last_checked_at": now,
                "last_successful_parse_at": now,
                "current_status": current_status,
                "source_tab_name": selected_tab["tab_name"],
                "source_month": selected_tab["source_month"],
                "duty_count": len(duties),
                "latest_detected_duties": duties,
                "last_error_message": None,
            },
        )
        log_event(
            "duty_sync_connected",
            session_id=session_id,
            payload={
                "sheet_id": sheet_id,
                "source_tab_name": selected_tab["tab_name"],
                "duty_count": len(duties),
                "current_status": current_status,
            },
        )
        return {
            "status": current_status,
            "reply": reply,
            "duty_count": len(duties),
            "source_tab_name": selected_tab["tab_name"],
        }
    except DutySyncStructuralError as exc:
        _upsert_connection_state(
            session_id,
            {
                "sheet_url": normalized_sheet_url,
                "sheet_id": sheet_id,
                "full_name": normalized_full_name,
                "is_connected": True,
                "connected_at": now,
                "last_checked_at": now,
                "current_status": "error",
                "duty_count": 0,
                "latest_detected_duties": [],
                "last_error_message": str(exc) or STRUCTURAL_CHANGE_MESSAGE,
            },
        )
        log_event(
            "duty_sync_parse_failed",
            session_id=session_id,
            payload={"sheet_id": sheet_id, "error": str(exc)},
            level="error",
        )
        return {"status": "error", "reply": str(exc) or STRUCTURAL_CHANGE_MESSAGE}
    except requests.RequestException as exc:
        log_event(
            "duty_sync_request_failed",
            session_id=session_id,
            payload={"sheet_id": sheet_id, "error": str(exc)},
            level="error",
        )
        return {"status": "failed", "reply": "Duty Sync could not read the test sheet right now."}


def disconnect_duty_sheet(session_id):
    duty_sync_connections_collection.delete_many({"session_id": session_id})
    log_event("duty_sync_disconnected", session_id=session_id)
    return {"status": "disconnected", "reply": "Duty Sync disconnected."}
