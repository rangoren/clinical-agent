from datetime import datetime, timedelta
from uuid import uuid4
from urllib.parse import urlencode

import requests

from db import calendar_connections_collection, oauth_states_collection, user_calendars_collection
from services.logging_service import log_event
from settings import APP_BASE_URL, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REDIRECT_URI


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_CALENDAR_LIST_URL = "https://www.googleapis.com/calendar/v3/users/me/calendarList"
GOOGLE_EVENTS_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
GOOGLE_SCOPE = "https://www.googleapis.com/auth/calendar"
GOOGLE_HTTP_TIMEOUT_SECONDS = 8


def _utcnow():
    return datetime.utcnow()


def google_calendar_enabled():
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI)


def _auth_headers(access_token):
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}


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


def begin_google_calendar_connect(session_id):
    if not google_calendar_enabled():
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


def _get_selected_google_calendar_id(session_id, calendar_type):
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
    return None


def _post_event(session_id, event_payload, calendar_type):
    connection = _get_connection(session_id)
    if not connection:
        return None

    calendar_id = _get_selected_google_calendar_id(session_id, calendar_type)
    if not calendar_id:
        return None

    response = requests.post(
        GOOGLE_EVENTS_URL_TEMPLATE.format(calendar_id=calendar_id),
        headers={
            **_auth_headers(connection["access_token"]),
            "Content-Type": "application/json",
        },
        json=event_payload,
        timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def sync_google_create_event(session_id, event_doc):
    if not google_calendar_enabled():
        return {"status": "skipped"}
    try:
        payload = {
            "summary": event_doc["title"],
            "start": {"dateTime": event_doc["start_at"].isoformat()},
            "end": {"dateTime": event_doc["end_at"].isoformat()},
            "reminders": {"useDefault": True},
        }
        created = _post_event(session_id, payload, event_doc["calendar_type"])
        if not created:
            return {"status": "skipped"}
        return {"status": "synced", "provider_event_id": created.get("id"), "provider_calendar_id": created.get("organizer", {}).get("email")}
    except Exception:
        return {"status": "failed"}


def sync_google_update_event(session_id, provider_event_id, event_doc):
    connection = _get_connection(session_id)
    calendar_id = _get_selected_google_calendar_id(session_id, event_doc["calendar_type"])
    if not connection or not calendar_id or not provider_event_id:
        return {"status": "skipped"}
    try:
        response = requests.patch(
            f"{GOOGLE_EVENTS_URL_TEMPLATE.format(calendar_id=calendar_id)}/{provider_event_id}",
            headers={
                **_auth_headers(connection["access_token"]),
                "Content-Type": "application/json",
            },
            json={
                "summary": event_doc["title"],
                "start": {"dateTime": event_doc["start_at"].isoformat()},
                "end": {"dateTime": event_doc["end_at"].isoformat()},
            },
            timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return {"status": "synced"}
    except Exception:
        return {"status": "failed"}


def sync_google_delete_event(session_id, provider_event_id, calendar_type):
    connection = _get_connection(session_id)
    calendar_id = _get_selected_google_calendar_id(session_id, calendar_type)
    if not connection or not calendar_id or not provider_event_id:
        return {"status": "skipped"}
    try:
        response = requests.delete(
            f"{GOOGLE_EVENTS_URL_TEMPLATE.format(calendar_id=calendar_id)}/{provider_event_id}",
            headers=_auth_headers(connection["access_token"]),
            timeout=GOOGLE_HTTP_TIMEOUT_SECONDS,
        )
        if response.status_code not in (200, 204):
            response.raise_for_status()
        return {"status": "synced"}
    except Exception:
        return {"status": "failed"}
