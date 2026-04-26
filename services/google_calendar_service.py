from datetime import datetime, timedelta, timezone
import re
from uuid import uuid4
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import requests

from db import calendar_connections_collection, oauth_states_collection, user_calendars_collection
from services.logging_service import log_event
from settings import (
    APP_BASE_URL,
    APP_ENV,
    APP_TIMEZONE,
    ENABLE_EXTERNAL_SIDE_EFFECTS,
    ENABLE_GOOGLE_CALENDAR_INTEGRATION,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI,
)


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_CALENDAR_LIST_URL = "https://www.googleapis.com/calendar/v3/users/me/calendarList"
GOOGLE_EVENTS_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
GOOGLE_CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"
GOOGLE_SHEETS_READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
GOOGLE_SCOPES = [GOOGLE_CALENDAR_SCOPE, GOOGLE_SHEETS_READONLY_SCOPE]
GOOGLE_SCOPE = " ".join(GOOGLE_SCOPES)
GOOGLE_HTTP_TIMEOUT_SECONDS = 8
APP_ZONEINFO = ZoneInfo(APP_TIMEZONE)
GOOGLE_HISTORY_CACHE_TTL_SECONDS = 300
_google_history_cache = {}


def _utcnow():
    return datetime.utcnow()


def _as_google_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=APP_ZONEINFO)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _google_events_url(calendar_id, event_id=None):
    encoded_calendar_id = quote(calendar_id or "", safe="")
    base_url = GOOGLE_EVENTS_URL_TEMPLATE.format(calendar_id=encoded_calendar_id)
    if not event_id:
        return base_url
    return f"{base_url}/{quote(event_id, safe='')}"


def _history_cache_key(session_id, start_at, end_at):
    return (
        session_id,
        _as_google_utc(start_at),
        _as_google_utc(end_at),
    )


def _get_cached_google_history(session_id, start_at, end_at):
    cache_key = _history_cache_key(session_id, start_at, end_at)
    cached = _google_history_cache.get(cache_key)
    if not cached:
        return None
    age_seconds = (_utcnow() - cached["cached_at"]).total_seconds()
    if age_seconds < 0 or age_seconds > GOOGLE_HISTORY_CACHE_TTL_SECONDS:
        _google_history_cache.pop(cache_key, None)
        return None
    return cached["events"]


def _store_cached_google_history(session_id, start_at, end_at, events):
    _google_history_cache[_history_cache_key(session_id, start_at, end_at)] = {
        "cached_at": _utcnow(),
        "events": events,
    }


def _normalize_google_datetime(raw_value):
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(APP_ZONEINFO).replace(tzinfo=None, second=0, microsecond=0)


def google_calendar_enabled():
    return bool(
        ENABLE_GOOGLE_CALENDAR_INTEGRATION
        and GOOGLE_CLIENT_ID
        and GOOGLE_CLIENT_SECRET
        and GOOGLE_REDIRECT_URI
    )


def google_calendar_write_enabled():
    return google_calendar_enabled() and ENABLE_EXTERNAL_SIDE_EFFECTS


def _auth_headers(access_token):
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}


def _scope_set(scope_value):
    raw = (scope_value or "").strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split() if item.strip()}


def _refresh_google_access_token(session_id, connection):
    refresh_token = (connection or {}).get("refresh_token")
    if not refresh_token:
        return None

    response = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    token_payload = response.json()
    new_access_token = token_payload.get("access_token")
    if not new_access_token:
        return None

    expires_at = _utcnow() + timedelta(seconds=int(token_payload.get("expires_in", 3600)))
    calendar_connections_collection.update_one(
        {"session_id": session_id, "provider": "google"},
        {
            "$set": {
                "access_token": new_access_token,
                "scope": token_payload.get("scope") or connection.get("scope"),
                "expires_at": expires_at,
                "updated_at": _utcnow(),
            }
        },
    )
    return new_access_token


def _guess_calendar_type(name):
    lowered = (name or "").lower()
    if any(token in lowered for token in ("work", "clinic", "hospital", "call", "shift")):
        return "work"
    if any(token in lowered for token in ("kid", "school", "children", "family")):
        return "kids" if "kid" in lowered or "school" in lowered or "children" in lowered else "family"
    if any(token in lowered for token in ("shared", "wife", "husband", "partner")):
        return "shared"
    if any(token in lowered for token in ("personal", "home")):
        return "personal"
    return "personal"


def _upsert_connection(session_id, token_payload, calendars):
    now = _utcnow()
    expiry = now + timedelta(seconds=int(token_payload.get("expires_in", 3600)))
    connection_doc = {
        "session_id": session_id,
        "provider": "google",
        "access_token": token_payload.get("access_token"),
        "refresh_token": token_payload.get("refresh_token"),
        "scope": token_payload.get("scope"),
        "token_type": token_payload.get("token_type", "Bearer"),
        "expires_at": expiry,
        "updated_at": now,
        "is_active": True,
    }
    calendar_connections_collection.update_one(
        {"session_id": session_id, "provider": "google"},
        {"$set": connection_doc, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )

    _store_google_calendars(session_id, calendars)


def _store_google_calendars(session_id, calendars):
    now = _utcnow()

    user_calendars_collection.delete_many({"session_id": session_id, "provider": "google"})
    for calendar in calendars:
        user_calendars_collection.insert_one(
            {
                "session_id": session_id,
                "provider": "google",
                "provider_calendar_id": calendar.get("id"),
                "name": calendar.get("summary", "Google Calendar"),
                "calendar_type": _guess_calendar_type(calendar.get("summary")),
                "is_primary": bool(calendar.get("primary")),
                "is_selected": bool(calendar.get("primary")),
                "created_at": now,
                "updated_at": now,
            }
        )


def _fetch_google_calendar_list(session_id, connection=None):
    connection = connection or _get_connection(session_id)
    if not connection:
        return []

    response = requests.get(
        GOOGLE_CALENDAR_LIST_URL,
        headers=_auth_headers(connection["access_token"]),
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.get(
                GOOGLE_CALENDAR_LIST_URL,
                headers=_auth_headers(refreshed_access_token),
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
    response.raise_for_status()
    calendars = response.json().get("items", [])
    _store_google_calendars(session_id, calendars)
    log_event(
        "google_calendar_list_refreshed",
        session_id=session_id,
        payload={"calendar_count": len(calendars)},
    )
    return calendars


def get_google_calendar_status(session_id):
    if not google_calendar_enabled():
        return {"connected": False, "available": False, "provider": "google"}

    connection = calendar_connections_collection.find_one(
        {"session_id": session_id, "provider": "google", "is_active": True}
    )
    selected = list(
        user_calendars_collection.find(
            {"session_id": session_id, "provider": "google", "is_selected": True}
        ).sort("is_primary", -1)
    )
    return {
        "connected": bool(connection),
        "available": True,
        "provider": "google",
        "calendar_count": len(selected),
        "calendars": [
            {
                "name": item.get("name", "Google Calendar"),
                "calendar_type": item.get("calendar_type", "personal"),
                "is_primary": bool(item.get("is_primary")),
            }
            for item in selected
        ],
    }


def get_google_calendars(session_id):
    calendars = list(
        user_calendars_collection.find({"session_id": session_id, "provider": "google"}).sort(
            [("is_primary", -1), ("name", 1)]
        )
    )
    if not calendars and _get_connection(session_id):
        try:
            _fetch_google_calendar_list(session_id)
            calendars = list(
                user_calendars_collection.find({"session_id": session_id, "provider": "google"}).sort(
                    [("is_primary", -1), ("name", 1)]
                )
            )
        except Exception as exc:
            log_event(
                "google_calendar_list_refresh_failed",
                session_id=session_id,
                payload={"error": str(exc)},
                level="error",
            )
    return [
        {
            "provider_calendar_id": item.get("provider_calendar_id"),
            "name": item.get("name", "Google Calendar"),
            "calendar_type": item.get("calendar_type", "personal"),
            "is_primary": bool(item.get("is_primary")),
            "is_selected": bool(item.get("is_selected")),
        }
        for item in calendars
    ]


def has_google_calendar_connection(session_id):
    return bool(
        calendar_connections_collection.find_one(
            {"session_id": session_id, "provider": "google", "is_active": True}
        )
    )


def get_google_connection(session_id):
    return _get_connection(session_id)


def google_connection_has_scopes(session_id, required_scopes):
    connection = _get_connection(session_id)
    if not connection:
        return False
    available_scopes = _scope_set(connection.get("scope"))
    return all(scope in available_scopes for scope in (required_scopes or []))


def get_google_calendar_name(session_id, provider_calendar_id):
    if not provider_calendar_id:
        return None
    calendar_doc = user_calendars_collection.find_one(
        {
            "session_id": session_id,
            "provider": "google",
            "provider_calendar_id": provider_calendar_id,
        }
    )
    if not calendar_doc:
        return None
    return calendar_doc.get("name")


def begin_google_calendar_connect(session_id):
    if not google_calendar_enabled():
        if APP_ENV != "production":
            return {
                "status": "unavailable",
                "reply": "Google Calendar is disabled in this environment until DEV integration is explicitly enabled.",
            }
        return {"status": "unavailable", "reply": "Google Calendar is not configured yet."}

    now = _utcnow()
    state = str(uuid4())
    oauth_states_collection.insert_one(
        {
            "state": state,
            "session_id": session_id,
            "provider": "google",
            "created_at": now,
            "expires_at": now + timedelta(minutes=15),
        }
    )
    query = urlencode(
        {
            "client_id": GOOGLE_CLIENT_ID,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "response_type": "code",
            "scope": GOOGLE_SCOPE,
            "access_type": "offline",
            "prompt": "consent select_account",
            "state": state,
        }
    )
    return {"status": "ok", "auth_url": f"{GOOGLE_AUTH_URL}?{query}"}


def complete_google_calendar_connect(code, state):
    state_doc = oauth_states_collection.find_one({"state": state, "provider": "google"})
    if not state_doc:
        log_event("google_calendar_connect_failed", payload={"reason": "invalid_state"}, level="error")
        return {"status": "invalid_state", "reply": "Google Calendar connect session expired."}

    try:
        token_response = requests.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": GOOGLE_REDIRECT_URI,
                "grant_type": "authorization_code",
            },
            timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
        )
        token_response.raise_for_status()
        token_payload = token_response.json()

        calendar_response = requests.get(
            GOOGLE_CALENDAR_LIST_URL,
            headers=_auth_headers(token_payload["access_token"]),
            timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
        )
        calendar_response.raise_for_status()
        calendars = calendar_response.json().get("items", [])

        _upsert_connection(state_doc["session_id"], token_payload, calendars)
        oauth_states_collection.delete_one({"state": state})
        log_event(
            "google_calendar_connected",
            session_id=state_doc["session_id"],
            payload={"calendar_count": len(calendars)},
        )
        return {"status": "connected", "session_id": state_doc["session_id"]}
    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        body = ""
        try:
            body = response.text[:500] if response is not None and response.text else ""
        except Exception:
            body = ""
        log_event(
            "google_calendar_connect_failed",
            session_id=state_doc["session_id"],
            payload={
                "reason": "http_error",
                "status_code": getattr(response, "status_code", None),
                "response": body,
            },
            level="error",
        )
        return {"status": "failed", "reply": "Google Calendar token exchange failed."}
    except requests.RequestException as exc:
        log_event(
            "google_calendar_connect_failed",
            session_id=state_doc["session_id"],
            payload={"reason": "request_exception", "error": str(exc)},
            level="error",
        )
        return {"status": "failed", "reply": "Google Calendar request failed."}
    except Exception as exc:
        log_event(
            "google_calendar_connect_failed",
            session_id=state_doc["session_id"],
            payload={"reason": "unexpected_exception", "error": str(exc)},
            level="error",
        )
        return {"status": "failed", "reply": "Google Calendar connection failed."}


def disconnect_google_calendar(session_id):
    calendar_connections_collection.delete_many({"session_id": session_id, "provider": "google"})
    user_calendars_collection.delete_many({"session_id": session_id, "provider": "google"})
    return {"status": "disconnected", "reply": "Google Calendar disconnected."}


def _get_connection(session_id):
    return calendar_connections_collection.find_one({"session_id": session_id, "provider": "google", "is_active": True})


def _get_selected_google_calendar_id(session_id, calendar_type, preferred_calendar_id=None):
    if preferred_calendar_id:
        explicit = user_calendars_collection.find_one(
            {
                "session_id": session_id,
                "provider": "google",
                "provider_calendar_id": preferred_calendar_id,
            }
        )
        if explicit:
            return explicit["provider_calendar_id"]

    preferred = user_calendars_collection.find_one(
        {
            "session_id": session_id,
            "provider": "google",
            "calendar_type": calendar_type,
            "is_selected": True,
        }
    )
    if preferred:
        return preferred["provider_calendar_id"]

    primary = user_calendars_collection.find_one(
        {
            "session_id": session_id,
            "provider": "google",
            "is_primary": True,
        }
    )
    if primary:
        return primary["provider_calendar_id"]

    calendars = get_google_calendars(session_id)
    if preferred_calendar_id:
        explicit = next(
            (item for item in calendars if item.get("provider_calendar_id") == preferred_calendar_id),
            None,
        )
        if explicit:
            return explicit["provider_calendar_id"]
    typed_match = next((item for item in calendars if item.get("calendar_type") == calendar_type), None)
    if typed_match:
        return typed_match["provider_calendar_id"]
    primary_match = next((item for item in calendars if item.get("is_primary")), None)
    if primary_match:
        return primary_match["provider_calendar_id"]
    if calendars:
        return calendars[0]["provider_calendar_id"]
    return None


def _get_all_google_calendar_ids(session_id):
    calendars = get_google_calendars(session_id)
    return [calendar.get("provider_calendar_id") for calendar in calendars if calendar.get("provider_calendar_id")]


def _google_get_json_with_refresh(session_id, connection, url, params):
    response = requests.get(
        url,
        headers=_auth_headers(connection["access_token"]),
        params=params,
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.get(
                url,
                headers=_auth_headers(refreshed_access_token),
                params=params,
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
    response.raise_for_status()
    return response.json()


def _list_google_calendar_events(session_id, connection, calendar_id, start_at, end_at):
    if not calendar_id:
        return []

    items = []
    page_token = None
    while True:
        payload = _google_get_json_with_refresh(
            session_id,
            connection,
            _google_events_url(calendar_id),
            {
                "timeMin": _as_google_utc(start_at),
                "timeMax": _as_google_utc(end_at),
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": 2500,
                "pageToken": page_token,
                "fields": "items(id,summary,status,start/dateTime,end/dateTime),nextPageToken",
            },
        )
        items.extend(payload.get("items", []))
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
    return items


def list_google_calendar_events_across_calendars(session_id, start_at, end_at):
    cached_events = _get_cached_google_history(session_id, start_at, end_at)
    if cached_events is not None:
        return cached_events

    connection = _get_connection(session_id)
    if not connection:
        return []

    calendars = get_google_calendars(session_id)
    all_events = []
    for calendar in calendars:
        calendar_id = calendar.get("provider_calendar_id")
        if not calendar_id:
            continue
        for item in _list_google_calendar_events(session_id, connection, calendar_id, start_at, end_at):
            start_value = item.get("start", {}).get("dateTime")
            end_value = item.get("end", {}).get("dateTime")
            start_dt = _normalize_google_datetime(start_value)
            end_dt = _normalize_google_datetime(end_value)
            if not start_dt or not end_dt:
                continue
            all_events.append(
                {
                    "provider_calendar_id": calendar_id,
                    "calendar_name": calendar.get("name", "Google Calendar"),
                    "provider_event_id": item.get("id"),
                    "title": item.get("summary") or "",
                    "status": item.get("status") or "confirmed",
                    "start_at": start_dt,
                    "end_at": end_dt,
                }
            )

    _store_cached_google_history(session_id, start_at, end_at, all_events)
    return all_events


def _post_event(session_id, event_payload, calendar_type, preferred_calendar_id=None):
    connection = _get_connection(session_id)
    if not connection:
        return None

    calendar_id = _get_selected_google_calendar_id(session_id, calendar_type, preferred_calendar_id=preferred_calendar_id)
    if not calendar_id:
        return None

    headers = {
        **_auth_headers(connection["access_token"]),
        "Content-Type": "application/json",
    }
    response = requests.post(
        _google_events_url(calendar_id),
        headers=headers,
        json=event_payload,
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.post(
                _google_events_url(calendar_id),
                headers={
                    **_auth_headers(refreshed_access_token),
                    "Content-Type": "application/json",
                },
                json=event_payload,
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
    response.raise_for_status()
    return response.json(), calendar_id


def _find_matching_google_event_id(session_id, calendar_id, title, start_at, end_at):
    matches = _find_matching_google_events(session_id, calendar_id, title, start_at, end_at)
    if not matches:
        return None
    return matches[0].get("id")


def _find_matching_google_events(session_id, calendar_id, title, start_at, end_at):
    connection = _get_connection(session_id)
    if not connection or not calendar_id:
        return []

    response = requests.get(
        _google_events_url(calendar_id),
        headers=_auth_headers(connection["access_token"]),
        params={
            "timeMin": _as_google_utc(start_at - timedelta(hours=2)),
            "timeMax": _as_google_utc(end_at + timedelta(hours=2)),
            "singleEvents": "true",
            "orderBy": "startTime",
        },
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.get(
                _google_events_url(calendar_id),
                headers=_auth_headers(refreshed_access_token),
                params={
                    "timeMin": _as_google_utc(start_at - timedelta(hours=2)),
                    "timeMax": _as_google_utc(end_at + timedelta(hours=2)),
                    "singleEvents": "true",
                    "orderBy": "startTime",
                },
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
    response.raise_for_status()
    items = response.json().get("items", [])
    normalized_title = (title or "").strip().lower()
    target_start = start_at.replace(second=0, microsecond=0)
    target_end = end_at.replace(second=0, microsecond=0)
    target_tokens = {token for token in re.findall(r"[a-zA-Z0-9\u0590-\u05FF]+", normalized_title) if token}
    exact_matches = []
    fuzzy_matches = []
    matches = []

    for item in items:
        item_title = (item.get("summary") or "").strip().lower()
        item_start_raw = item.get("start", {}).get("dateTime")
        item_end_raw = item.get("end", {}).get("dateTime")
        if not item_start_raw or not item_end_raw:
            continue
        item_start = _normalize_google_datetime(item_start_raw)
        item_end = _normalize_google_datetime(item_end_raw)
        if not item_start or not item_end:
            continue

        if item_start != target_start or item_end != target_end:
            continue

        item_tokens = {token for token in re.findall(r"[a-zA-Z0-9\u0590-\u05FF]+", item_title) if token}
        if item_title == normalized_title:
            exact_matches.append(item)
            continue

        overlap = len(target_tokens & item_tokens)
        if overlap >= max(1, min(2, len(target_tokens))):
            fuzzy_matches.append(item)

    matches.extend(exact_matches)
    matches.extend(fuzzy_matches)
    return matches


def _find_matching_google_event(session_id, title, start_at, end_at, preferred_calendar_id=None, calendar_type=None):
    candidate_calendar_ids = []
    if preferred_calendar_id:
        candidate_calendar_ids.append(preferred_calendar_id)
    fallback_calendar_id = _get_selected_google_calendar_id(
        session_id,
        calendar_type or "personal",
        preferred_calendar_id=preferred_calendar_id,
    )
    if fallback_calendar_id and fallback_calendar_id not in candidate_calendar_ids:
        candidate_calendar_ids.append(fallback_calendar_id)
    for calendar_id in _get_all_google_calendar_ids(session_id):
        if calendar_id not in candidate_calendar_ids:
            candidate_calendar_ids.append(calendar_id)

    for calendar_id in candidate_calendar_ids:
        provider_event_id = _find_matching_google_event_id(session_id, calendar_id, title, start_at, end_at)
        if provider_event_id:
            return {"provider_event_id": provider_event_id, "provider_calendar_id": calendar_id}
    return None


def _find_all_matching_google_event_pairs(session_id, title, start_at, end_at, preferred_calendar_id=None, calendar_type=None):
    candidate_calendar_ids = []
    if preferred_calendar_id:
        candidate_calendar_ids.append(preferred_calendar_id)
    fallback_calendar_id = _get_selected_google_calendar_id(
        session_id,
        calendar_type or "personal",
        preferred_calendar_id=preferred_calendar_id,
    )
    if fallback_calendar_id and fallback_calendar_id not in candidate_calendar_ids:
        candidate_calendar_ids.append(fallback_calendar_id)
    for calendar_id in _get_all_google_calendar_ids(session_id):
        if calendar_id not in candidate_calendar_ids:
            candidate_calendar_ids.append(calendar_id)

    seen = set()
    pairs = []
    for calendar_id in candidate_calendar_ids:
        for item in _find_matching_google_events(session_id, calendar_id, title, start_at, end_at):
            event_id = item.get("id")
            candidate = (calendar_id, event_id)
            if event_id and candidate not in seen:
                seen.add(candidate)
                pairs.append(candidate)
    return pairs


def _google_event_is_gone(session_id, calendar_id, provider_event_id):
    connection = _get_connection(session_id)
    if not connection or not calendar_id or not provider_event_id:
        return False

    response = requests.get(
        _google_events_url(calendar_id, provider_event_id),
        headers=_auth_headers(connection["access_token"]),
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 404:
        return True
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.get(
                _google_events_url(calendar_id, provider_event_id),
                headers=_auth_headers(refreshed_access_token),
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
            if response.status_code == 404:
                return True
    if response.status_code != 200:
        response.raise_for_status()
    payload = response.json()
    return payload.get("status") == "cancelled"


def _get_google_event_by_id(session_id, calendar_id, provider_event_id):
    connection = _get_connection(session_id)
    if not connection or not calendar_id or not provider_event_id:
        return None

    response = requests.get(
        _google_events_url(calendar_id, provider_event_id),
        headers=_auth_headers(connection["access_token"]),
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.get(
                _google_events_url(calendar_id, provider_event_id),
                headers=_auth_headers(refreshed_access_token),
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _delete_google_event_by_id(session_id, calendar_id, provider_event_id):
    connection = _get_connection(session_id)
    if not connection or not calendar_id or not provider_event_id:
        return False

    event_url = _google_events_url(calendar_id, provider_event_id)
    response = requests.delete(
        event_url,
        headers=_auth_headers(connection["access_token"]),
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        refreshed_access_token = _refresh_google_access_token(session_id, connection)
        if refreshed_access_token:
            response = requests.delete(
                event_url,
                headers=_auth_headers(refreshed_access_token),
                timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
            )
    if response.status_code not in (200, 204, 404):
        patch_response = requests.patch(
            event_url,
            headers={
                **_auth_headers(connection["access_token"]),
                "Content-Type": "application/json",
            },
            json={"status": "cancelled"},
            timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
        )
        if patch_response.status_code == 401:
            refreshed_access_token = _refresh_google_access_token(session_id, connection)
            if refreshed_access_token:
                patch_response = requests.patch(
                    event_url,
                    headers={
                        **_auth_headers(refreshed_access_token),
                        "Content-Type": "application/json",
                    },
                    json={"status": "cancelled"},
                    timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
                )
        if patch_response.status_code not in (200, 204, 404):
            patch_response.raise_for_status()
    elif response.status_code == 404:
        return True
    return _google_event_is_gone(session_id, calendar_id, provider_event_id)


def sync_google_create_event(session_id, event_doc, preferred_calendar_id=None):
    if not google_calendar_write_enabled():
        return {"status": "skipped"}
    try:
        payload = {
            "summary": event_doc["title"],
            "start": {"dateTime": event_doc["start_at"].isoformat(), "timeZone": APP_TIMEZONE},
            "end": {"dateTime": event_doc["end_at"].isoformat(), "timeZone": APP_TIMEZONE},
            "location": event_doc.get("location"),
            "reminders": {"useDefault": True},
        }
        post_result = _post_event(
            session_id,
            payload,
            event_doc["calendar_type"],
            preferred_calendar_id=preferred_calendar_id,
        )
        if not post_result:
            return {"status": "skipped"}
        created, calendar_id = post_result
        if not created:
            return {"status": "skipped"}
        created_id = created.get("id")
        verified_event = _get_google_event_by_id(session_id, calendar_id, created_id)
        if not verified_event:
            log_event(
                "google_calendar_sync_failed",
                session_id=session_id,
                payload={
                    "action": "create_verify",
                    "title": event_doc.get("title"),
                    "provider_event_id": created_id,
                    "provider_calendar_id": calendar_id,
                    "reason": "event_not_found_after_create",
                },
                level="error",
            )
            return {"status": "failed"}
        return {
            "status": "synced",
            "provider_event_id": created_id,
            "provider_calendar_id": calendar_id,
            "provider_calendar_name": get_google_calendar_name(session_id, calendar_id),
            "html_link": verified_event.get("htmlLink") or created.get("htmlLink"),
        }
    except Exception as exc:
        response_body = ""
        if getattr(exc, "response", None) is not None:
            try:
                response_body = exc.response.text[:500]
            except Exception:
                response_body = ""
        log_event(
            "google_calendar_sync_failed",
            session_id=session_id,
            payload={"action": "create", "title": event_doc.get("title"), "error": str(exc), "response": response_body},
            level="error",
        )
        return {"status": "failed"}


def sync_google_update_event(session_id, provider_event_id, event_doc, preferred_calendar_id=None):
    if not google_calendar_write_enabled():
        return {"status": "skipped"}
    connection = _get_connection(session_id)
    calendar_id = _get_selected_google_calendar_id(
        session_id,
        event_doc["calendar_type"],
        preferred_calendar_id=preferred_calendar_id,
    )
    if not connection or not calendar_id or not provider_event_id:
        return {"status": "skipped"}
    try:
        headers = {
            **_auth_headers(connection["access_token"]),
            "Content-Type": "application/json",
        }
        response = requests.patch(
            _google_events_url(calendar_id, provider_event_id),
            headers=headers,
            json={
                "summary": event_doc["title"],
                "start": {"dateTime": event_doc["start_at"].isoformat(), "timeZone": APP_TIMEZONE},
                "end": {"dateTime": event_doc["end_at"].isoformat(), "timeZone": APP_TIMEZONE},
                "location": event_doc.get("location"),
            },
            timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
        )
        if response.status_code == 401:
            refreshed_access_token = _refresh_google_access_token(session_id, connection)
            if refreshed_access_token:
                response = requests.patch(
                    _google_events_url(calendar_id, provider_event_id),
                    headers={
                        **_auth_headers(refreshed_access_token),
                        "Content-Type": "application/json",
                    },
                    json={
                        "summary": event_doc["title"],
                        "start": {"dateTime": event_doc["start_at"].isoformat(), "timeZone": APP_TIMEZONE},
                        "end": {"dateTime": event_doc["end_at"].isoformat(), "timeZone": APP_TIMEZONE},
                        "location": event_doc.get("location"),
                    },
                    timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
                )
        response.raise_for_status()
        return {"status": "synced"}
    except Exception as exc:
        response_body = ""
        if getattr(exc, "response", None) is not None:
            try:
                response_body = exc.response.text[:500]
            except Exception:
                response_body = ""
        log_event(
            "google_calendar_sync_failed",
            session_id=session_id,
            payload={"action": "update", "title": event_doc.get("title"), "error": str(exc), "response": response_body},
            level="error",
        )
        return {"status": "failed"}


def sync_google_delete_event(
    session_id,
    provider_event_id,
    calendar_type,
    preferred_calendar_id=None,
    event_doc=None,
    provider_calendar_id=None,
):
    if not google_calendar_write_enabled():
        return {"status": "skipped"}
    connection = _get_connection(session_id)
    if not connection:
        return {"status": "skipped"}
    try:
        candidate_pairs = []
        if provider_event_id and provider_calendar_id:
            candidate_pairs.append((provider_calendar_id, provider_event_id))

        if event_doc:
            matched_pairs = _find_all_matching_google_event_pairs(
                session_id,
                event_doc.get("title"),
                event_doc.get("start_at"),
                event_doc.get("end_at"),
                preferred_calendar_id=provider_calendar_id or preferred_calendar_id,
                calendar_type=calendar_type,
            )
            for candidate in matched_pairs:
                if candidate not in candidate_pairs:
                    candidate_pairs.append(candidate)

        if provider_event_id:
            selected_calendar_id = _get_selected_google_calendar_id(
                session_id,
                calendar_type,
                preferred_calendar_id=provider_calendar_id or preferred_calendar_id,
            )
            if selected_calendar_id:
                candidate = (selected_calendar_id, provider_event_id)
                if candidate not in candidate_pairs:
                    candidate_pairs.append(candidate)
            for calendar_id in _get_all_google_calendar_ids(session_id):
                candidate = (calendar_id, provider_event_id)
                if candidate not in candidate_pairs:
                    candidate_pairs.append(candidate)

        if not candidate_pairs:
            return {"status": "skipped"}

        log_event(
            "google_calendar_delete_candidates",
            session_id=session_id,
            payload={
                "provider_event_id": provider_event_id,
                "provider_calendar_id": provider_calendar_id,
                "candidate_pairs": candidate_pairs,
                "title": (event_doc or {}).get("title"),
            },
        )

        deleted_any = False
        deleted_pairs = []
        for calendar_id, event_id in candidate_pairs:
            try:
                if _delete_google_event_by_id(session_id, calendar_id, event_id):
                    deleted_any = True
                    deleted_pairs.append((calendar_id, event_id))
            except requests.HTTPError as exc:
                response = getattr(exc, "response", None)
                if response is not None and response.status_code == 404:
                    continue
                raise

        if event_doc:
            remaining_matches = _find_all_matching_google_event_pairs(
                session_id,
                event_doc.get("title"),
                event_doc.get("start_at"),
                event_doc.get("end_at"),
                preferred_calendar_id=provider_calendar_id or preferred_calendar_id,
                calendar_type=calendar_type,
            )
            if remaining_matches:
                return {"status": "failed"}

        if deleted_any:
            first_calendar_id = deleted_pairs[0][0]
            first_event_id = deleted_pairs[0][1]
            return {
                "status": "synced",
                "provider_calendar_id": first_calendar_id,
                "provider_event_id": first_event_id,
                "provider_calendar_name": get_google_calendar_name(session_id, first_calendar_id),
                "deleted_count": len(deleted_pairs),
            }

        return {"status": "failed"}
    except Exception as exc:
        response_body = ""
        if getattr(exc, "response", None) is not None:
            try:
                response_body = exc.response.text[:500]
            except Exception:
                response_body = ""
        log_event(
            "google_calendar_sync_failed",
            session_id=session_id,
            payload={"action": "delete", "provider_event_id": provider_event_id, "error": str(exc), "response": response_body},
            level="error",
        )
        return {"status": "failed"}
