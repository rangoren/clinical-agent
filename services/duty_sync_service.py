from dataclasses import asdict
from datetime import datetime
from urllib.parse import quote

import requests

from db import (
    duty_sync_connections_collection,
    duty_sync_managed_events_collection,
    duty_sync_pending_reviews_collection,
    duty_sync_snapshots_collection,
)
from services.duty_sync_parsing import (
    RELEVANT_TAB_TOKEN,
    RELEVANT_ROLE_HEADERS,
    ROLE_TITLE_MAP,
    STRUCTURAL_CHANGE_MESSAGE,
    DutySyncStructuralError,
    analyze_candidate_tab,
    as_iso,
    build_duty_datetimes,
    normalize_sheet_id,
    normalize_text,
    parse_sheet_date,
)
from services.duty_sync_diff import build_diff_changes, duty_map_by_key
from services.google_calendar_service import (
    GOOGLE_HTTP_TIMEOUT_SECONDS,
    GOOGLE_SHEETS_READONLY_SCOPE,
    _refresh_google_access_token,
    _auth_headers,
    begin_google_calendar_connect,
    get_google_connection,
    google_calendar_enabled,
    google_connection_has_scopes,
    google_calendar_write_enabled,
    has_google_calendar_connection,
    sync_google_create_event,
    sync_google_delete_event,
    sync_google_update_event,
)
from services.logging_service import log_event
from settings import APP_ENV


NO_DUTIES_MESSAGE_HE = "לא שובצת החודש בלוח התורנויות"
DEFAULT_TEST_DUTY_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1L50eFprLasbWbu808emdUa5HFQAtAZjzkbyvhGSG9eQ/edit?gid=0#gid=0"
)
DEFAULT_TEST_DUTY_USER_FULL_NAME = "גורן"
GOOGLE_SHEETS_METADATA_URL = "https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
GOOGLE_SHEETS_VALUES_URL = "https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{sheet_range}"
DUTY_SYNC_CALENDAR_TYPE = "personal"
DUTY_SYNC_WRITE_DISABLED_MESSAGE = "Duty Sync review is ready, but Google Calendar writes are disabled in this environment."
DEFAULT_DUTY_SYNC_POLLING_MINUTES = 45 if APP_ENV == "production" else 1


def _is_debug_env():
    return APP_ENV != "production"


def _debug_payload(exc):
    if not _is_debug_env():
        return {}
    payload = {}
    if getattr(exc, "detail", None):
        payload["debug_reason"] = exc.detail
    if getattr(exc, "context", None):
        payload["debug_context"] = exc.context
    return payload


def _utcnow():
    return datetime.utcnow()


def _parse_iso_datetime(raw_value):
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalized_review_updated_at(raw_value):
    parsed = _parse_iso_datetime(raw_value)
    if parsed is not None:
        return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return raw_value or None


def _managed_event_doc(session_id, duty_key):
    if not duty_key:
        return None
    return duty_sync_managed_events_collection.find_one({"session_id": session_id, "duty_key": duty_key})


def _build_managed_event_payload(session_id, duty):
    start_at = _parse_iso_datetime(duty.get("start_datetime"))
    end_at = _parse_iso_datetime(duty.get("end_datetime"))
    if not start_at or not end_at:
        raise DutySyncStructuralError(
            "A detected duty could not be converted into calendar event times.",
            {
                "duty_key": duty.get("duty_key"),
                "start_datetime": duty.get("start_datetime"),
                "end_datetime": duty.get("end_datetime"),
            },
        )
    return {
        "session_id": session_id,
        "title": duty.get("title") or duty.get("role"),
        "calendar_type": DUTY_SYNC_CALENDAR_TYPE,
        "start_at": start_at,
        "end_at": end_at,
        "location": None,
    }


def _touch_managed_event_record(session_id, duty, sync_result, existing_doc=None, status="active"):
    now = _utcnow()
    duty_key = duty.get("duty_key")
    update_payload = {
        "session_id": session_id,
        "user_id": session_id,
        "duty_key": duty_key,
        "date": duty.get("date"),
        "role": duty.get("role"),
        "title": duty.get("title"),
        "start_datetime": duty.get("start_datetime"),
        "end_datetime": duty.get("end_datetime"),
        "provider": "google",
        "provider_event_id": sync_result.get("provider_event_id") or (existing_doc or {}).get("provider_event_id"),
        "provider_calendar_id": sync_result.get("provider_calendar_id") or (existing_doc or {}).get("provider_calendar_id"),
        "provider_html_link": sync_result.get("html_link") or (existing_doc or {}).get("provider_html_link"),
        "status": status,
        "last_synced_at": now,
        "updated_at": now,
    }
    duty_sync_managed_events_collection.update_one(
        {"session_id": session_id, "duty_key": duty_key},
        {"$set": update_payload, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )


def _mark_managed_event_deleted(session_id, duty_key):
    if not duty_key:
        return
    duty_sync_managed_events_collection.update_one(
        {"session_id": session_id, "duty_key": duty_key},
        {"$set": {"status": "deleted", "deleted_at": _utcnow(), "updated_at": _utcnow()}},
    )


def _sync_added_duty(session_id, duty):
    managed_doc = _managed_event_doc(session_id, duty.get("duty_key"))
    event_payload = _build_managed_event_payload(session_id, duty)
    if managed_doc and managed_doc.get("status") == "active" and managed_doc.get("provider_event_id"):
        sync_result = sync_google_update_event(
            session_id,
            managed_doc.get("provider_event_id"),
            event_payload,
            preferred_calendar_id=managed_doc.get("provider_calendar_id"),
        )
    else:
        sync_result = sync_google_create_event(session_id, event_payload)
    if sync_result.get("status") != "synced":
        return sync_result
    _touch_managed_event_record(session_id, duty, sync_result, existing_doc=managed_doc, status="active")
    return sync_result


def _sync_removed_duty(session_id, duty):
    managed_doc = _managed_event_doc(session_id, duty.get("duty_key"))
    if not managed_doc or not managed_doc.get("provider_event_id"):
        _mark_managed_event_deleted(session_id, duty.get("duty_key"))
        return {"status": "synced"}
    event_payload = _build_managed_event_payload(session_id, duty)
    sync_result = sync_google_delete_event(
        session_id,
        managed_doc.get("provider_event_id"),
        DUTY_SYNC_CALENDAR_TYPE,
        preferred_calendar_id=managed_doc.get("provider_calendar_id"),
        provider_calendar_id=managed_doc.get("provider_calendar_id"),
        event_doc=event_payload,
    )
    if sync_result.get("status") != "synced":
        return sync_result
    _mark_managed_event_deleted(session_id, duty.get("duty_key"))
    return sync_result


def _sync_changed_duty(session_id, old_duty, new_duty):
    old_key = (old_duty or {}).get("duty_key")
    managed_doc = _managed_event_doc(session_id, old_key)
    event_payload = _build_managed_event_payload(session_id, new_duty)
    if managed_doc and managed_doc.get("status") == "active" and managed_doc.get("provider_event_id"):
        sync_result = sync_google_update_event(
            session_id,
            managed_doc.get("provider_event_id"),
            event_payload,
            preferred_calendar_id=managed_doc.get("provider_calendar_id"),
        )
        if sync_result.get("status") != "synced":
            return sync_result
        now = _utcnow()
        duty_sync_managed_events_collection.delete_many(
            {
                "session_id": session_id,
                "duty_key": new_duty.get("duty_key"),
                "_id": {"$ne": managed_doc.get("_id")},
            }
        )
        duty_sync_managed_events_collection.update_one(
            {"_id": managed_doc["_id"]},
            {
                "$set": {
                    "duty_key": new_duty.get("duty_key"),
                    "date": new_duty.get("date"),
                    "role": new_duty.get("role"),
                    "title": new_duty.get("title"),
                    "start_datetime": new_duty.get("start_datetime"),
                    "end_datetime": new_duty.get("end_datetime"),
                    "status": "active",
                    "updated_at": now,
                    "last_synced_at": now,
                }
            },
        )
        return sync_result
    return _sync_added_duty(session_id, new_duty)


def _apply_review_to_calendar(session_id, review_doc):
    if not google_calendar_write_enabled():
        log_event(
            "duty_sync_calendar_write_blocked",
            session_id=session_id,
            payload={"review_id": review_doc.get("review_id")},
            level="warning",
        )
        return {
            "status": "blocked",
            "reply": DUTY_SYNC_WRITE_DISABLED_MESSAGE,
        }

    applied = {"added": 0, "removed": 0, "changed": 0}
    for change in review_doc.get("detected_changes_json") or []:
        if not change.get("included", True):
            continue
        change_type = change.get("change_type")
        old_duty = change.get("old_duty") or {}
        new_duty = change.get("new_duty") or {}
        if change_type == "added":
            sync_result = _sync_added_duty(session_id, new_duty)
        elif change_type == "removed":
            sync_result = _sync_removed_duty(session_id, old_duty)
        elif change_type == "changed":
            sync_result = _sync_changed_duty(session_id, old_duty, new_duty)
        else:
            continue
        if sync_result.get("status") != "synced":
            log_event(
                "duty_sync_calendar_sync_failed",
                session_id=session_id,
                payload={
                    "review_id": review_doc.get("review_id"),
                    "change_key": change.get("change_key"),
                    "change_type": change_type,
                    "sync_status": sync_result.get("status"),
                },
                level="error",
            )
            return {
                "status": "failed",
                "reply": "Duty Sync could not apply the approved updates to Google Calendar right now.",
                "failed_change_key": change.get("change_key"),
                "failed_change_type": change_type,
            }
        if change_type in applied:
            applied[change_type] += 1
    log_event(
        "duty_sync_calendar_sync_applied",
        session_id=session_id,
        payload={"review_id": review_doc.get("review_id"), "applied": applied},
    )
    return {"status": "synced", "applied": applied}


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
        raise DutySyncStructuralError("No active Google connection was found for the current session.")
    response = requests.get(
        url,
        headers=_auth_headers(connection["access_token"]),
        params=params or {},
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.get(
                url,
                headers=_auth_headers(refreshed_access_token),
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
        available_tabs = [
            normalize_text((sheet.get("properties") or {}).get("title"))
            for sheet in metadata.get("sheets") or []
            if normalize_text((sheet.get("properties") or {}).get("title"))
        ]
        raise DutySyncStructuralError(
            "No tab containing the required Hebrew token was found in the spreadsheet.",
            {"required_tab_token": RELEVANT_TAB_TOKEN, "available_tabs": available_tabs},
        )

    analyses = []
    for tab_name in tab_names:
        values = _fetch_sheet_values(session_id, sheet_id, tab_name)
        analyses.append(analyze_candidate_tab(tab_name, values, full_name, session_id))

    analyses.sort(key=lambda item: item["latest_date"], reverse=True)
    best = analyses[0]
    if len(analyses) > 1 and analyses[1]["latest_date"] == best["latest_date"]:
        raise DutySyncStructuralError(
            "Two candidate duty tabs shared the same latest detected date, so the active roster could not be selected deterministically.",
            {
                "top_tabs": [
                    {"tab_name": analyses[0]["tab_name"], "latest_date": analyses[0]["latest_date"].isoformat()},
                    {"tab_name": analyses[1]["tab_name"], "latest_date": analyses[1]["latest_date"].isoformat()},
                ]
            },
        )
    return best


def _connection_doc(session_id):
    return duty_sync_connections_collection.find_one({"session_id": session_id})


def _latest_approved_snapshot(session_id):
    return duty_sync_snapshots_collection.find_one(
        {"session_id": session_id},
        sort=[("approved_at", -1)],
    )


def _active_pending_review(session_id):
    return duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "status": "pending"},
        sort=[("created_at", -1)],
    )


def _duty_map_by_key(duties):
    return duty_map_by_key(duties)


def _build_diff_changes(approved_duties, detected_duties):
    return build_diff_changes(approved_duties, detected_duties)


def _review_summary(changes):
    summary = {"added": 0, "removed": 0, "changed": 0}
    for item in changes or []:
        change_type = item.get("change_type")
        if change_type in summary:
            summary[change_type] += 1
    return summary


def _serialize_review_doc(review_doc):
    if not review_doc:
        return None
    changes = review_doc.get("detected_changes_json") or []
    included_count = sum(1 for item in changes if item.get("included", True))
    return {
        "review_id": review_doc.get("review_id"),
        "review_type": review_doc.get("review_type") or "incremental",
        "status": review_doc.get("status"),
        "source_month": review_doc.get("source_month"),
        "source_tab_name": review_doc.get("source_tab_name"),
        "summary": review_doc.get("summary") or _review_summary(changes),
        "included_count": included_count,
        "updated_at": as_iso(review_doc.get("updated_at")) if review_doc.get("updated_at") else None,
        "changes": changes,
    }


def _replace_pending_review(session_id, source_tab_name, source_month, changes, review_type="incremental"):
    now = _utcnow()
    existing = _active_pending_review(session_id)
    if existing:
        if (
            existing.get("source_tab_name") == source_tab_name
            and existing.get("source_month") == source_month
            and existing.get("review_type", "incremental") == review_type
            and (existing.get("detected_changes_json") or []) == (changes or [])
        ):
            return existing
        duty_sync_pending_reviews_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$set": {
                    "detected_changes_json": changes,
                    "source_tab_name": source_tab_name,
                    "source_month": source_month,
                    "review_type": review_type,
                    "summary": _review_summary(changes),
                    "updated_at": now,
                }
            },
        )
        return duty_sync_pending_reviews_collection.find_one({"_id": existing["_id"]})

    review_id = f"duty-review-{session_id}-{int(now.timestamp())}"
    doc = {
        "review_id": review_id,
        "session_id": session_id,
        "user_id": session_id,
        "detected_changes_json": changes,
        "source_tab_name": source_tab_name,
        "source_month": source_month,
        "review_type": review_type,
        "created_at": now,
        "updated_at": now,
        "resolved_at": None,
        "status": "pending",
        "summary": _review_summary(changes),
    }
    duty_sync_pending_reviews_collection.insert_one(doc)
    return doc


def _clear_pending_review_if_unchanged(session_id):
    existing = _active_pending_review(session_id)
    if not existing:
        return False
    now = _utcnow()
    duty_sync_pending_reviews_collection.update_one(
        {"_id": existing["_id"]},
        {"$set": {"status": "superseded", "resolved_at": now, "updated_at": now}},
    )
    duty_sync_connections_collection.update_one(
        {"session_id": session_id},
        {"$set": {"current_status": "connected", "updated_at": now}},
    )
    return True


def _build_review_payload(session_id, source_tab_name, source_month, detected_duties):
    snapshot = _latest_approved_snapshot(session_id)
    pending_review = _active_pending_review(session_id)
    approved_duties = (snapshot or {}).get("duties_json") or []
    same_month_pending_review = bool(pending_review and pending_review.get("source_month") == source_month)
    previous_source_month = None
    if pending_review and pending_review.get("source_month"):
        previous_source_month = pending_review.get("source_month")
    elif snapshot and snapshot.get("source_month"):
        previous_source_month = snapshot.get("source_month")
    if pending_review and pending_review.get("review_type") == "monthly_rollover" and pending_review.get("source_month") == source_month:
        approved_duties = [item.get("new_duty") for item in (pending_review.get("detected_changes_json") or []) if item.get("new_duty")]
    review_type = "incremental"
    changes = _build_diff_changes(approved_duties, detected_duties)
    if previous_source_month and previous_source_month != source_month and detected_duties and not same_month_pending_review:
        review_type = "monthly_rollover"
        changes = [
            {
                "change_type": "added",
                "change_key": f"rollover:{item['duty_key']}",
                "date": item.get("date"),
                "included": True,
                "new_duty": item,
            }
            for item in detected_duties
        ]
    elif not snapshot and detected_duties:
        changes = [
            {
                "change_type": "added",
                "change_key": f"initial:{item['duty_key']}",
                "date": item.get("date"),
                "included": True,
                "new_duty": item,
            }
            for item in detected_duties
        ]
    if not changes:
        _clear_pending_review_if_unchanged(session_id)
        return None
    review_doc = _replace_pending_review(session_id, source_tab_name, source_month, changes, review_type=review_type)
    return _serialize_review_doc(review_doc)


def _apply_review_to_snapshot(session_id, review_doc):
    duties_json = _apply_review_changes_to_snapshot(
        session_id=session_id,
        source_tab_name=review_doc.get("source_tab_name"),
        source_month=review_doc.get("source_month"),
        selected_changes=review_doc.get("detected_changes_json") or [],
    )
    now = _utcnow()
    duty_sync_pending_reviews_collection.update_one(
        {"_id": review_doc["_id"]},
        {"$set": {"status": "approved", "resolved_at": now, "updated_at": now}},
    )
    duty_sync_connections_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "current_status": "connected",
                "last_error_message": None,
                "last_debug_reason": None,
                "last_debug_context": None,
                "last_pushed_review_signature": None,
                "last_pushed_review_payload": None,
                "last_push_review_scope": None,
            }
        },
    )
    return duties_json


def _apply_review_changes_to_snapshot(session_id, source_tab_name, source_month, selected_changes):
    now = _utcnow()
    latest_connection = _connection_doc(session_id) or {}
    latest_detected_duties = _duty_map_by_key(latest_connection.get("latest_detected_duties") or [])
    latest_snapshot = _latest_approved_snapshot(session_id)
    approved_duties = _duty_map_by_key((latest_snapshot or {}).get("duties_json") or [])

    for change in selected_changes or []:
        if not change.get("included", True):
            continue
        change_type = change.get("change_type")
        old_duty = (change.get("old_duty") or {})
        new_duty = (change.get("new_duty") or {})
        old_key = old_duty.get("duty_key")
        new_key = new_duty.get("duty_key")
        if change_type == "added" and new_key and new_key in latest_detected_duties:
            approved_duties[new_key] = latest_detected_duties[new_key]
        elif change_type == "removed" and old_key:
            approved_duties.pop(old_key, None)
        elif change_type == "changed":
            if old_key:
                approved_duties.pop(old_key, None)
            if new_key and new_key in latest_detected_duties:
                approved_duties[new_key] = latest_detected_duties[new_key]

    duties_json = sorted(approved_duties.values(), key=lambda item: ((item.get("date") or ""), (item.get("role") or "")))
    snapshot_doc = {
        "session_id": session_id,
        "user_id": session_id,
        "source_tab_name": source_tab_name,
        "source_month": source_month,
        "duties_json": duties_json,
        "approved_at": now,
    }
    duty_sync_snapshots_collection.insert_one(snapshot_doc)
    return duties_json


def _change_keys_set(change_keys):
    return {key for key in (change_keys or []) if key}


def _format_shift_phrase(count):
    return f"{count} shift was" if count == 1 else f"{count} shifts were"


def _build_calendar_apply_reply(applied):
    applied = applied or {}
    parts = []
    if applied.get("added"):
        parts.append(f"{_format_shift_phrase(applied['added'])} added to your calendar")
    if applied.get("changed"):
        parts.append(f"{_format_shift_phrase(applied['changed'])} updated in your calendar")
    if applied.get("removed"):
        parts.append(f"{_format_shift_phrase(applied['removed'])} removed from your calendar")
    if not parts:
        return "Your calendar is already up to date."
    if len(parts) == 1:
        return parts[0][0].upper() + parts[0][1:]
    return ", ".join(parts[:-1]) + " and " + parts[-1]


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


def _resolve_duty_sync_identity(session_id, sheet_url=None, full_name=None):
    existing = _connection_doc(session_id) or {}
    normalized_sheet_url = normalize_text(sheet_url) or normalize_text(existing.get("sheet_url")) or DEFAULT_TEST_DUTY_SHEET_URL
    normalized_full_name = normalize_text(full_name) or normalize_text(existing.get("full_name")) or DEFAULT_TEST_DUTY_USER_FULL_NAME
    sheet_id = normalize_sheet_id(normalized_sheet_url)
    return normalized_sheet_url, normalized_full_name, sheet_id, existing


def _build_sync_reply(current_status, duty_count, source_tab_name, review_payload=None, is_connect=False, is_poll=False):
    if review_payload:
        included_count = review_payload.get("included_count", 0)
        if is_poll:
            return f"Duty Sync found {included_count} updates that need review."
        return f"Duty Sync found {included_count} updates pending review."
    if current_status == "no_duties":
        return NO_DUTIES_MESSAGE_HE
    if is_connect:
        return f"Duty Sync connected. Found {duty_count} duties in the latest roster."
    if is_poll:
        return "Duty Sync checked the latest roster. No personal changes were found."
    return f"Duty Sync checked the latest roster. Found {duty_count} duties with no new personal changes."


def _sync_duty_sheet(session_id, sheet_url=None, full_name=None, *, is_connect=False, is_poll=False):
    access_state = _required_google_sheet_access(session_id)
    if not access_state.get("ok"):
        return access_state

    normalized_sheet_url, normalized_full_name, sheet_id, existing = _resolve_duty_sync_identity(
        session_id,
        sheet_url=sheet_url,
        full_name=full_name,
    )
    now = _utcnow()
    previous_pending_payload = _serialize_review_doc(_active_pending_review(session_id))

    try:
        selected_tab = _select_relevant_tab(session_id, sheet_id, normalized_full_name)
        duties = [asdict(item) for item in selected_tab["duties"]]
        current_status = "connected" if duties else "no_duties"
        connected_at = existing.get("connected_at") or now
        _upsert_connection_state(
            session_id,
            {
                "sheet_url": normalized_sheet_url,
                "sheet_id": sheet_id,
                "full_name": normalized_full_name,
                "is_connected": True,
                "connected_at": connected_at,
                "last_checked_at": now,
                "last_successful_parse_at": now,
                "current_status": current_status,
                "source_tab_name": selected_tab["tab_name"],
                "source_month": selected_tab["source_month"],
                "duty_count": len(duties),
                "latest_detected_duties": duties,
                "last_error_message": None,
                "last_debug_reason": None,
                "last_debug_context": None,
                "last_poll_checked_at": now if is_poll else existing.get("last_poll_checked_at"),
            },
        )
        review_payload = _build_review_payload(
            session_id,
            selected_tab["tab_name"],
            selected_tab["source_month"],
            duties,
        )
        if review_payload:
            duty_sync_connections_collection.update_one(
                {"session_id": session_id},
                {"$set": {"current_status": "pending_review"}},
            )
        reply = _build_sync_reply(
            current_status,
            len(duties),
            selected_tab["tab_name"],
            review_payload=review_payload,
            is_connect=is_connect,
            is_poll=is_poll,
        )
        log_event(
            "duty_sync_sheet_checked",
            session_id=session_id,
            payload={
                "sheet_id": sheet_id,
                "source_tab_name": selected_tab["tab_name"],
                "duty_count": len(duties),
                "current_status": "pending_review" if review_payload else current_status,
                "is_connect": is_connect,
                "is_poll": is_poll,
            },
        )
        result = {
            "status": "pending_review" if review_payload else current_status,
            "reply": reply,
            "duty_count": len(duties),
            "source_tab_name": selected_tab["tab_name"],
            "pending_review": review_payload,
        }
        if is_poll:
            has_new_pending = bool(review_payload) and review_payload != previous_pending_payload
            result["polling_minutes"] = DEFAULT_DUTY_SYNC_POLLING_MINUTES
            result["has_new_pending_review"] = has_new_pending
            result["deep_link_path"] = "/?app_mode=scheduling&duty_sync_review=1"
        return result
    except DutySyncStructuralError as exc:
        debug_payload = _debug_payload(exc)
        _upsert_connection_state(
            session_id,
            {
                "sheet_url": normalized_sheet_url,
                "sheet_id": sheet_id,
                "full_name": normalized_full_name,
                "is_connected": True,
                "connected_at": existing.get("connected_at") or now,
                "last_checked_at": now,
                "current_status": "error",
                "duty_count": 0,
                "latest_detected_duties": [],
                "last_error_message": str(exc) or STRUCTURAL_CHANGE_MESSAGE,
                "last_debug_reason": debug_payload.get("debug_reason"),
                "last_debug_context": debug_payload.get("debug_context"),
                "last_poll_checked_at": now if is_poll else existing.get("last_poll_checked_at"),
            },
        )
        log_event(
            "duty_sync_parse_failed",
            session_id=session_id,
            payload={"sheet_id": sheet_id, "error": str(exc), "is_poll": is_poll, **debug_payload},
            level="error",
        )
        return {"status": "error", "reply": str(exc) or STRUCTURAL_CHANGE_MESSAGE, **debug_payload}
    except requests.RequestException as exc:
        log_event(
            "duty_sync_request_failed",
            session_id=session_id,
            payload={"sheet_id": sheet_id, "error": str(exc), "is_poll": is_poll},
            level="error",
        )
        return {"status": "failed", "reply": "Duty Sync could not read the test sheet right now."}


def get_duty_sync_status(session_id):
    access_state = _required_google_sheet_access(session_id)
    doc = _connection_doc(session_id) or {}
    current_status = doc.get("current_status", "not_connected")
    last_checked_at = as_iso(doc.get("last_checked_at"))
    if not access_state.get("ok"):
        result = {
            "available": google_calendar_enabled(),
            "connected": bool(doc.get("is_connected")),
            "google_connected": has_google_calendar_connection(session_id),
            "requires_google_reconnect": access_state.get("status") in {"google_connect_required", "google_reconnect_required"},
            "current_status": access_state.get("status"),
            "label": "Duty schedule: Not available" if access_state.get("status") == "unavailable" else "Duty schedule: Google reconnect required",
            "details": access_state.get("reply"),
            "last_checked_at": last_checked_at,
        }
        if _is_debug_env():
            result["debug_reason"] = access_state.get("debug_reason")
        return result

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

    pending_review = _active_pending_review(session_id)
    if pending_review:
        pending_payload = _serialize_review_doc(pending_review)
        result = {
            "available": True,
            "connected": True,
            "google_connected": True,
            "requires_google_reconnect": False,
            "current_status": "pending_review",
            "label": "Duty schedule connected",
            "details": f"{pending_payload.get('included_count', 0)} updates pending review.",
            "last_checked_at": last_checked_at,
            "pending_review": pending_payload,
            "push_review_scope": doc.get("last_push_review_scope"),
        }
        return result

    details = {
        "connected": "Duty schedule connected",
        "no_duties": NO_DUTIES_MESSAGE_HE,
        "error": doc.get("last_error_message") or STRUCTURAL_CHANGE_MESSAGE,
    }.get(current_status, "Duty schedule connected")
    if current_status == "connected" and doc.get("duty_count") is not None:
        details = f"Detected {doc.get('duty_count', 0)} duties in {doc.get('source_tab_name', 'the latest roster')}."

    result = {
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
    if _is_debug_env() and current_status == "error":
        if doc.get("last_debug_reason"):
            result["debug_reason"] = doc.get("last_debug_reason")
        if doc.get("last_debug_context"):
            result["debug_context"] = doc.get("last_debug_context")
    return result


def load_pending_duty_review(session_id, review_id, updated_at=None):
    if not review_id:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    pending_review = _active_pending_review(session_id)
    if not pending_review or pending_review.get("review_id") != review_id:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    pending_payload = _serialize_review_doc(pending_review)
    connection = _connection_doc(session_id) or {}
    scoped_review = connection.get("last_push_review_scope")
    if (
        scoped_review
        and scoped_review.get("review_id") == review_id
        and (
            not updated_at
            or _normalized_review_updated_at(scoped_review.get("updated_at")) == _normalized_review_updated_at(updated_at)
        )
    ):
        return {"status": "loaded", "review": pending_payload, "source": "push_scoped"}

    if updated_at and _normalized_review_updated_at(pending_payload.get("updated_at")) != _normalized_review_updated_at(updated_at):
        return {"status": "stale", "reply": "Pending duty review changed before it could be opened."}

    return {"status": "loaded", "review": pending_payload, "source": "pending_review"}


def connect_duty_sheet(session_id, sheet_url=None, full_name=None):
    return _sync_duty_sheet(session_id, sheet_url=sheet_url, full_name=full_name, is_connect=True, is_poll=False)


def check_duty_sheet(session_id):
    return _sync_duty_sheet(session_id, is_connect=False, is_poll=False)


def poll_duty_sheet(session_id):
    return _sync_duty_sheet(session_id, is_connect=False, is_poll=True)


def disconnect_duty_sheet(session_id):
    duty_sync_connections_collection.delete_many({"session_id": session_id})
    log_event("duty_sync_disconnected", session_id=session_id)
    return {"status": "disconnected", "reply": "Duty Sync disconnected."}


def approve_pending_duty_review(session_id, review_id):
    review_doc = duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "review_id": review_id, "status": "pending"}
    )
    if not review_doc:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    calendar_sync_result = _apply_review_to_calendar(session_id, review_doc)
    if calendar_sync_result.get("status") != "synced":
        return calendar_sync_result
    duties_json = _apply_review_to_snapshot(session_id, review_doc)
    applied = calendar_sync_result.get("applied") or {}
    return {
        "status": "approved",
        "reply": _build_calendar_apply_reply(applied),
        "approved_duty_count": len(duties_json),
    }


def approve_pending_duty_review_scope(session_id, review_id, change_keys):
    review_doc = duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "review_id": review_id, "status": "pending"}
    )
    if not review_doc:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    selected_keys = _change_keys_set(change_keys)
    selected_changes = [item for item in (review_doc.get("detected_changes_json") or []) if item.get("change_key") in selected_keys]
    if not selected_changes:
        return {"status": "not_found", "reply": "No matching duty review items were found."}
    calendar_sync_result = _apply_review_to_calendar(session_id, {**review_doc, "detected_changes_json": selected_changes})
    if calendar_sync_result.get("status") != "synced":
        return calendar_sync_result
    duties_json = _apply_review_changes_to_snapshot(
        session_id=session_id,
        source_tab_name=review_doc.get("source_tab_name"),
        source_month=review_doc.get("source_month"),
        selected_changes=selected_changes,
    )
    remaining_changes = [item for item in (review_doc.get("detected_changes_json") or []) if item.get("change_key") not in selected_keys]
    now = _utcnow()
    if remaining_changes:
        duty_sync_pending_reviews_collection.update_one(
            {"_id": review_doc["_id"]},
            {"$set": {"detected_changes_json": remaining_changes, "summary": _review_summary(remaining_changes), "updated_at": now}},
        )
        duty_sync_connections_collection.update_one(
            {"session_id": session_id},
            {"$set": {"current_status": "pending_review", "last_pushed_review_signature": None, "last_pushed_review_payload": None, "last_push_review_scope": None}},
        )
    else:
        duty_sync_pending_reviews_collection.update_one(
            {"_id": review_doc["_id"]},
            {"$set": {"status": "approved", "resolved_at": now, "updated_at": now}},
        )
        duty_sync_connections_collection.update_one(
            {"session_id": session_id},
            {"$set": {"current_status": "connected", "last_pushed_review_signature": None, "last_pushed_review_payload": None, "last_push_review_scope": None}},
        )
    return {
        "status": "approved",
        "reply": _build_calendar_apply_reply(calendar_sync_result.get("applied") or {}),
        "approved_duty_count": len(duties_json),
        "pending_review": _serialize_review_doc(duty_sync_pending_reviews_collection.find_one({"_id": review_doc["_id"]})) if remaining_changes else None,
    }


def ignore_pending_duty_review(session_id, review_id):
    review_doc = duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "review_id": review_id, "status": "pending"}
    )
    if not review_doc:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    now = _utcnow()
    duty_sync_pending_reviews_collection.update_one(
        {"_id": review_doc["_id"]},
        {"$set": {"status": "ignored", "resolved_at": now, "updated_at": now}},
    )
    duty_sync_connections_collection.update_one(
        {"session_id": session_id},
        {"$set": {"current_status": "connected", "last_pushed_review_signature": None, "last_pushed_review_payload": None, "last_push_review_scope": None}},
    )
    return {"status": "ignored", "reply": "Duty review ignored for now."}


def ignore_pending_duty_review_scope(session_id, review_id, change_keys):
    review_doc = duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "review_id": review_id, "status": "pending"}
    )
    if not review_doc:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    selected_keys = _change_keys_set(change_keys)
    remaining_changes = [item for item in (review_doc.get("detected_changes_json") or []) if item.get("change_key") not in selected_keys]
    now = _utcnow()
    if remaining_changes:
        duty_sync_pending_reviews_collection.update_one(
            {"_id": review_doc["_id"]},
            {"$set": {"detected_changes_json": remaining_changes, "summary": _review_summary(remaining_changes), "updated_at": now}},
        )
        return {
            "status": "ignored",
            "reply": "Duty review items ignored for now.",
            "pending_review": _serialize_review_doc(duty_sync_pending_reviews_collection.find_one({"_id": review_doc["_id"]})),
        }
    duty_sync_pending_reviews_collection.update_one(
        {"_id": review_doc["_id"]},
        {"$set": {"status": "ignored", "resolved_at": now, "updated_at": now}},
    )
    duty_sync_connections_collection.update_one(
        {"session_id": session_id},
        {"$set": {"current_status": "connected", "last_pushed_review_signature": None, "last_pushed_review_payload": None, "last_push_review_scope": None}},
    )
    return {"status": "ignored", "reply": "Duty review ignored for now.", "pending_review": None}


def toggle_pending_review_change(session_id, review_id, change_key):
    review_doc = duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "review_id": review_id, "status": "pending"}
    )
    if not review_doc:
        return {"status": "not_found", "reply": "No pending duty review was found."}
    changes = review_doc.get("detected_changes_json") or []
    updated = False
    for item in changes:
        if item.get("change_key") == change_key:
            item["included"] = not item.get("included", True)
            updated = True
            break
    if not updated:
        return {"status": "not_found", "reply": "That duty review item was not found."}
    duty_sync_pending_reviews_collection.update_one(
        {"_id": review_doc["_id"]},
        {
            "$set": {
                "detected_changes_json": changes,
                "summary": _review_summary(changes),
                "updated_at": _utcnow(),
            }
        },
    )
    fresh = duty_sync_pending_reviews_collection.find_one({"_id": review_doc["_id"]})
    return {
        "status": "updated",
        "reply": "Duty review updated.",
        "pending_review": _serialize_review_doc(fresh),
    }


def _resolve_review_edit_target(change):
    if change.get("new_duty"):
        return "new_duty"
    if change.get("old_duty"):
        return "old_duty"
    return None


def _resolve_role_and_title_for_review_edit(raw_title, current_duty):
    normalized_title = normalize_text(raw_title)
    if not normalized_title:
        raise DutySyncStructuralError("Duty review edit needs a non-empty event title.")
    for role in RELEVANT_ROLE_HEADERS:
        if normalized_title in {normalize_text(role), normalize_text(ROLE_TITLE_MAP.get(role))}:
            return role, ROLE_TITLE_MAP[role]
    current_role = normalize_text((current_duty or {}).get("role"))
    current_title = normalized_title
    if current_role in ROLE_TITLE_MAP:
        return current_role, current_title
    raise DutySyncStructuralError("Duty review edit must keep a known duty role.", {"title": raw_title})


def _apply_review_item_edit(session_id, duty, edited_date, edited_title):
    parsed_date = parse_sheet_date(edited_date)
    if not parsed_date:
        raise DutySyncStructuralError("Duty review edit needs a readable date.", {"date": edited_date})
    role, title = _resolve_role_and_title_for_review_edit(edited_title, duty)
    start_dt, end_dt = build_duty_datetimes(parsed_date, role)
    updated = dict(duty or {})
    updated["date"] = parsed_date.isoformat()
    updated["role"] = role
    updated["title"] = title
    updated["duty_key"] = f"{session_id}:{parsed_date.isoformat()}:{role}"
    updated["start_datetime"] = as_iso(start_dt)
    updated["end_datetime"] = as_iso(end_dt)
    return updated


def edit_pending_review_change(session_id, review_id, change_key, edited_date, edited_title):
    review_doc = duty_sync_pending_reviews_collection.find_one(
        {"session_id": session_id, "review_id": review_id, "status": "pending"}
    )
    if not review_doc:
        return {"status": "not_found", "reply": "No pending duty review was found."}

    changes = review_doc.get("detected_changes_json") or []
    updated = False
    for item in changes:
        if item.get("change_key") != change_key:
            continue
        target_key = _resolve_review_edit_target(item)
        if not target_key:
            return {"status": "not_found", "reply": "That duty review item could not be edited."}
        try:
            item[target_key] = _apply_review_item_edit(session_id, item.get(target_key) or {}, edited_date, edited_title)
        except DutySyncStructuralError as exc:
            return {"status": "invalid", "reply": exc.detail}
        updated = True
        break

    if not updated:
        return {"status": "not_found", "reply": "That duty review item was not found."}

    duty_sync_pending_reviews_collection.update_one(
        {"_id": review_doc["_id"]},
        {
            "$set": {
                "detected_changes_json": changes,
                "updated_at": _utcnow(),
            }
        },
    )
    fresh = duty_sync_pending_reviews_collection.find_one({"_id": review_doc["_id"]})
    return {
        "status": "updated",
        "reply": "Duty review item updated.",
        "pending_review": _serialize_review_doc(fresh),
    }
