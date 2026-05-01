from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime, time as dt_time, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from db import (
    duty_reminder_push_logs_collection,
    duty_reminder_states_collection,
    duty_sync_connections_collection,
    duty_sync_managed_events_collection,
    push_subscriptions_collection,
)
from services.logging_service import log_event
from settings import APP_BASE_URL, APP_ENV, WEB_PUSH_PRIVATE_KEY, WEB_PUSH_PUBLIC_KEY, WEB_PUSH_SUBJECT


_push_poller_started = False
_push_poller_lock = threading.Lock()
APP_TIMEZONE = ZoneInfo("Asia/Jerusalem")
QUIET_HOURS_START = 22
QUIET_HOURS_END = 8
QUIET_HOURS_RELEASE = dt_time(hour=8, minute=30)
MAX_DUTY_FEATURE_PUSHES_PER_DAY = 2
TAXI_STATUS_PENDING = "pending"
TAXI_REMINDER_KIND = "taxi"
TOMORROW_REMINDER_KIND = "tomorrow_duty"


def web_push_configured():
    return bool(WEB_PUSH_PUBLIC_KEY and WEB_PUSH_PRIVATE_KEY and WEB_PUSH_SUBJECT)


def _utcnow():
    return datetime.utcnow()


def _local_now():
    return datetime.now(APP_TIMEZONE)


def _parse_iso_datetime(raw_value):
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=APP_TIMEZONE)
    return parsed.astimezone(APP_TIMEZONE)


def _is_quiet_hours(local_dt):
    if not local_dt:
        return False
    current_time = local_dt.timetz().replace(tzinfo=None)
    return current_time >= dt_time(hour=QUIET_HOURS_START) or current_time < dt_time(hour=QUIET_HOURS_END)


def _effective_send_time(local_dt):
    if not local_dt:
        return None
    if not _is_quiet_hours(local_dt):
        return local_dt
    target_day = local_dt.date()
    if local_dt.timetz().replace(tzinfo=None) >= dt_time(hour=QUIET_HOURS_START):
        target_day = target_day + timedelta(days=1)
    return datetime.combine(target_day, QUIET_HOURS_RELEASE, tzinfo=APP_TIMEZONE)


def _notification_is_due(send_at_local, now_local):
    effective_send_time = _effective_send_time(send_at_local)
    if not effective_send_time or now_local < effective_send_time:
        return False
    if _is_quiet_hours(now_local):
        return False
    if now_local.date() > effective_send_time.date() and now_local.timetz().replace(tzinfo=None) < QUIET_HOURS_RELEASE:
        return False
    return True


def _feature_push_count_for_day(session_id, local_day):
    return duty_reminder_push_logs_collection.count_documents({"session_id": session_id, "local_day": local_day.isoformat()})


def _feature_push_sent(session_id, notification_key):
    return duty_reminder_push_logs_collection.find_one({"session_id": session_id, "notification_key": notification_key}) is not None


def _record_feature_push_log(session_id, duty_key, notification_key, reminder_kind, title, body, sent_at_local):
    now = _utcnow()
    duty_reminder_push_logs_collection.update_one(
        {"session_id": session_id, "notification_key": notification_key},
        {
            "$set": {
                "session_id": session_id,
                "duty_key": duty_key,
                "notification_key": notification_key,
                "reminder_kind": reminder_kind,
                "title": title,
                "body": body,
                "local_day": sent_at_local.date().isoformat(),
                "sent_at_local": sent_at_local.isoformat(),
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


def _default_taxi_state():
    return {"taxi_reminder_enabled": True, "taxi_status": TAXI_STATUS_PENDING}


def _duty_open_url(reminder_kind, duty_key):
    query = urlencode(
        {
            "app_mode": "scheduling",
            "duty_reminder": "1",
            "reminder_kind": reminder_kind,
            "duty_key": duty_key or "",
        }
    )
    return f"{APP_BASE_URL}/?{query}" if APP_BASE_URL else f"/?{query}"


def _build_taxi_candidate(managed_doc, state_doc, days_before, send_hour, body_text, now_local, priority, reminder_kind_suffix):
    start_local = _parse_iso_datetime(managed_doc.get("start_datetime"))
    if not start_local or start_local <= now_local:
        return None
    state_doc = state_doc or _default_taxi_state()
    if state_doc.get("duty_deleted"):
        return None
    if state_doc.get("taxi_reminder_enabled") is False:
        return None
    if state_doc.get("taxi_status") and state_doc.get("taxi_status") != TAXI_STATUS_PENDING:
        return None
    send_day = start_local.date() - timedelta(days=days_before)
    notification_key = f"taxi:{reminder_kind_suffix}:{managed_doc.get('duty_key')}"
    return {
        "notification_key": notification_key,
        "duty_key": managed_doc.get("duty_key"),
        "reminder_kind": TAXI_REMINDER_KIND,
        "priority": priority,
        "send_at_local": datetime.combine(send_day, dt_time(hour=send_hour, minute=0), tzinfo=APP_TIMEZONE),
        "title": "תזכורת מונית",
        "body": body_text,
        "url": _duty_open_url(f"taxi_{reminder_kind_suffix}", managed_doc.get("duty_key")),
        "relevant": lambda current_local: _parse_iso_datetime(managed_doc.get("start_datetime")) and _parse_iso_datetime(managed_doc.get("start_datetime")) > current_local,
    }


def _build_snooze_candidate(managed_doc, state_doc, now_local):
    start_local = _parse_iso_datetime(managed_doc.get("start_datetime"))
    if not start_local or start_local <= now_local:
        return None
    state_doc = state_doc or {}
    snooze_for = state_doc.get("last_snoozed_for_date")
    if not snooze_for:
        return None
    try:
        snooze_day = date.fromisoformat(str(snooze_for))
    except ValueError:
        return None
    if state_doc.get("duty_deleted"):
        return None
    if state_doc.get("taxi_reminder_enabled") is False:
        return None
    if state_doc.get("taxi_status") and state_doc.get("taxi_status") != TAXI_STATUS_PENDING:
        return None
    return {
        "notification_key": f"taxi:snooze:{managed_doc.get('duty_key')}:{snooze_day.isoformat()}",
        "duty_key": managed_doc.get("duty_key"),
        "reminder_kind": TAXI_REMINDER_KIND,
        "priority": 1,
        "send_at_local": datetime.combine(snooze_day, dt_time(hour=18, minute=0), tzinfo=APP_TIMEZONE),
        "title": "תזכורת מונית",
        "body": "תזכורת להזמין מונית לתורנות הקרובה.",
        "url": _duty_open_url("taxi_snooze", managed_doc.get("duty_key")),
        "relevant": lambda current_local: _parse_iso_datetime(managed_doc.get("start_datetime")) and _parse_iso_datetime(managed_doc.get("start_datetime")) > current_local,
    }


def _build_tomorrow_candidate(managed_doc, now_local):
    start_local = _parse_iso_datetime(managed_doc.get("start_datetime"))
    if not start_local or start_local <= now_local:
        return None
    duty_key = managed_doc.get("duty_key")
    role_text = managed_doc.get("title") or managed_doc.get("role") or "תורנות"
    send_day = start_local.date() - timedelta(days=1)
    return {
        "notification_key": f"tomorrow:{duty_key}:{start_local.date().isoformat()}",
        "duty_key": duty_key,
        "reminder_kind": TOMORROW_REMINDER_KIND,
        "priority": 0,
        "send_at_local": datetime.combine(send_day, dt_time(hour=20, minute=0), tzinfo=APP_TIMEZONE),
        "title": "תזכורת תורנות",
        "body": f"מחר יש לך תורנות. התפקיד: {role_text}.",
        "url": _duty_open_url("tomorrow_duty", duty_key),
        "relevant": lambda current_local: _parse_iso_datetime(managed_doc.get("start_datetime")) and _parse_iso_datetime(managed_doc.get("start_datetime")) > current_local,
    }


def _build_duty_feature_candidates(session_id, now_local):
    state_docs = {
        doc.get("duty_key"): doc
        for doc in duty_reminder_states_collection.find({"session_id": session_id})
        if doc.get("duty_key")
    }
    candidates = []
    for managed_doc in duty_sync_managed_events_collection.find({"session_id": session_id, "status": "active"}):
        duty_key = managed_doc.get("duty_key")
        if not duty_key:
            continue
        state_doc = state_docs.get(duty_key) or _default_taxi_state()
        for candidate in (
            _build_taxi_candidate(
                managed_doc,
                state_doc,
                days_before=7,
                send_hour=18,
                body_text="יש לך תורנות בעוד שבוע. זה הזמן להזמין מונית.",
                now_local=now_local,
                priority=3,
                reminder_kind_suffix="7d",
            ),
            _build_taxi_candidate(
                managed_doc,
                state_doc,
                days_before=3,
                send_hour=18,
                body_text="תזכורת אחרונה להזמין מונית לתורנות הקרובה.",
                now_local=now_local,
                priority=2,
                reminder_kind_suffix="3d",
            ),
            _build_snooze_candidate(managed_doc, state_doc, now_local),
            _build_tomorrow_candidate(managed_doc, now_local),
        ):
            if candidate:
                candidates.append(candidate)
    candidates.sort(key=lambda item: (item.get("send_at_local"), item.get("priority"), item.get("duty_key") or ""))
    return candidates


def _send_due_duty_feature_pushes(session_id):
    if not web_push_configured():
        return 0
    now_local = _local_now()
    sent_count = 0
    day_count = _feature_push_count_for_day(session_id, now_local.date())
    if day_count >= MAX_DUTY_FEATURE_PUSHES_PER_DAY:
        return 0
    for candidate in _build_duty_feature_candidates(session_id, now_local):
        if day_count >= MAX_DUTY_FEATURE_PUSHES_PER_DAY:
            break
        notification_key = candidate.get("notification_key")
        if not notification_key or _feature_push_sent(session_id, notification_key):
            continue
        if not _notification_is_due(candidate.get("send_at_local"), now_local):
            continue
        relevant_check = candidate.get("relevant")
        if callable(relevant_check) and not relevant_check(now_local):
            continue
        delivered = send_web_push_message(
            session_id=session_id,
            title=candidate.get("title"),
            body=candidate.get("body"),
            tag=notification_key,
            url=candidate.get("url"),
        )
        if delivered:
            _record_feature_push_log(
                session_id=session_id,
                duty_key=candidate.get("duty_key"),
                notification_key=notification_key,
                reminder_kind=candidate.get("reminder_kind"),
                title=candidate.get("title"),
                body=candidate.get("body"),
                sent_at_local=now_local,
            )
            sent_count += 1
            day_count += 1
    return sent_count


def get_web_push_status(session_id):
    subscription_count = push_subscriptions_collection.count_documents({"session_id": session_id})
    return {
        "available": web_push_configured(),
        "subscribed": subscription_count > 0,
        "subscription_count": subscription_count,
        "public_key": WEB_PUSH_PUBLIC_KEY if web_push_configured() else "",
    }


def save_web_push_subscription(session_id, subscription):
    if not web_push_configured():
        return {"status": "unavailable", "reply": "Web push is not configured in this environment."}
    endpoint = str((subscription or {}).get("endpoint") or "").strip()
    keys = (subscription or {}).get("keys") or {}
    p256dh = str(keys.get("p256dh") or "").strip()
    auth = str(keys.get("auth") or "").strip()
    if not endpoint or not p256dh or not auth:
        return {"status": "invalid", "reply": "Push subscription payload was incomplete."}
    now = datetime.utcnow()
    push_subscriptions_collection.delete_many({"endpoint": endpoint, "session_id": {"$ne": session_id}})
    result = push_subscriptions_collection.update_one(
        {"endpoint": endpoint},
        {
            "$set": {
                "session_id": session_id,
                "endpoint": endpoint,
                "subscription": {"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth}},
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )
    subscription_count = push_subscriptions_collection.count_documents({"session_id": session_id})
    debug_payload = {
        "session_id": session_id,
        "endpoint_saved": True,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
        "upserted": bool(result.upserted_id),
        "subscription_count": subscription_count,
    }
    log_event("duty_sync_push_subscription_saved", session_id=session_id, payload=debug_payload)
    response = {"status": "subscribed", "reply": "Duty Sync push alerts are enabled."}
    if APP_ENV != "production":
        response["debug"] = debug_payload
    return response


def delete_web_push_subscription(session_id, endpoint=None):
    query = {"session_id": session_id}
    if endpoint:
        query["endpoint"] = endpoint
    push_subscriptions_collection.delete_many(query)
    return {"status": "unsubscribed", "reply": "Duty Sync push alerts are disabled."}


def _send_notification_to_subscription(subscription, payload):
    from pywebpush import WebPushException, webpush

    try:
        webpush(
            subscription_info=subscription,
            data=json.dumps(payload),
            vapid_private_key=WEB_PUSH_PRIVATE_KEY,
            vapid_claims={"sub": WEB_PUSH_SUBJECT},
        )
        return True
    except WebPushException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code in {404, 410}:
            push_subscriptions_collection.delete_one({"endpoint": subscription.get("endpoint")})
        log_event(
            "duty_sync_push_send_failed",
            payload={"endpoint": subscription.get("endpoint"), "status_code": status_code, "error": str(exc)},
            level="warning",
        )
        return False


def send_duty_sync_push(session_id, review, reply_text):
    if not web_push_configured() or not review:
        return 0
    query = urlencode(
        {
            "app_mode": "scheduling",
            "duty_sync_review": "1",
            "duty_sync_review_id": review.get("review_id") or "",
            "duty_sync_review_updated_at": review.get("updated_at") or "",
        }
    )
    return send_web_push_message(
        session_id=session_id,
        title="Duty Sync update",
        body=reply_text or "Duty Sync found personal schedule changes.",
        tag=review.get("review_id") or "duty-sync-review",
        url=f"{APP_BASE_URL}/?{query}" if APP_BASE_URL else f"/?{query}",
        review=review,
    )


def send_web_push_message(session_id, title, body, tag="duty-sync-review", url=None, review=None):
    if not web_push_configured():
        return 0
    payload = {
        "title": title,
        "body": body,
        "tag": tag,
        "url": url or (f"{APP_BASE_URL}/?app_mode=scheduling&duty_sync_review=1" if APP_BASE_URL else "/?app_mode=scheduling&duty_sync_review=1"),
    }
    if review:
        payload["review_id"] = review.get("review_id") or ""
        payload["updated_at"] = review.get("updated_at") or ""
    sent_count = 0
    for doc in push_subscriptions_collection.find({"session_id": session_id}):
        if _send_notification_to_subscription(doc.get("subscription") or {}, payload):
            sent_count += 1
    return sent_count


def _review_signature(review):
    if not review:
        return ""
    stable_review = {
        "review_id": review.get("review_id"),
        "review_type": review.get("review_type"),
        "source_month": review.get("source_month"),
        "source_tab_name": review.get("source_tab_name"),
        "summary": review.get("summary"),
        "included_count": review.get("included_count"),
        "changes": review.get("changes") or [],
    }
    return json.dumps(stable_review, sort_keys=True, ensure_ascii=False)


def _build_push_review_scope(current_review, previous_review):
    if not current_review:
        return None
    if current_review.get("review_type") == "monthly_rollover":
        return current_review
    previous_map = {}
    for item in (previous_review or {}).get("changes") or []:
        previous_map[item.get("change_key")] = json.dumps(item, sort_keys=True, ensure_ascii=False)
    scoped_changes = []
    for item in current_review.get("changes") or []:
        change_key = item.get("change_key")
        serialized = json.dumps(item, sort_keys=True, ensure_ascii=False)
        if previous_map.get(change_key) != serialized:
            scoped_changes.append(item)
    if not scoped_changes:
        scoped_changes = current_review.get("changes") or []
    scoped_review = dict(current_review)
    scoped_review["changes"] = scoped_changes
    scoped_review["scope_change_keys"] = [item.get("change_key") for item in scoped_changes if item.get("change_key")]
    summary = {"added": 0, "changed": 0, "removed": 0}
    for item in scoped_changes:
        if item.get("change_type") in summary:
            summary[item.get("change_type")] += 1
    scoped_review["summary"] = summary
    scoped_review["included_count"] = sum(1 for item in scoped_changes if item.get("included", True))
    return scoped_review


def _poll_once():
    from services.duty_sync_service import DEFAULT_DUTY_SYNC_POLLING_MINUTES, poll_duty_sheet

    session_ids = []
    for doc in duty_sync_connections_collection.find({"is_connected": True}, {"session_id": 1}):
        session_id = doc.get("session_id")
        if session_id:
            session_ids.append(session_id)
    for session_id in session_ids:
        try:
            result = poll_duty_sheet(session_id)
            _send_due_duty_feature_pushes(session_id)
            review = result.get("pending_review")
            if review:
                connection = duty_sync_connections_collection.find_one(
                    {"session_id": session_id},
                    {"last_pushed_review_signature": 1, "last_pushed_review_payload": 1},
                )
                current_signature = _review_signature(review)
                if current_signature and current_signature != (connection or {}).get("last_pushed_review_signature"):
                    push_scope_review = _build_push_review_scope(review, (connection or {}).get("last_pushed_review_payload") or {})
                    claim_result = duty_sync_connections_collection.update_one(
                        {
                            "session_id": session_id,
                            "$or": [
                                {"last_pushed_review_signature": {"$exists": False}},
                                {"last_pushed_review_signature": {"$ne": current_signature}},
                            ],
                        },
                        {
                            "$set": {
                                "last_pushed_review_signature": current_signature,
                                "last_pushed_review_payload": review,
                                "last_push_review_scope": push_scope_review,
                                "last_push_open_context": {
                                    "review_id": review.get("review_id"),
                                    "updated_at": review.get("updated_at"),
                                    "pushed_at": datetime.utcnow(),
                                },
                                "last_pushed_at": datetime.utcnow(),
                            }
                        },
                    )
                    if claim_result.modified_count:
                        send_duty_sync_push(session_id, push_scope_review, result.get("reply"))
            else:
                duty_sync_connections_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"last_pushed_review_signature": None, "last_pushed_review_payload": None, "last_push_review_scope": None, "last_push_open_context": None}},
                )
        except Exception as exc:
            log_event(
                "duty_sync_push_poll_failed",
                session_id=session_id,
                payload={"error": str(exc)},
                level="error",
            )
    return DEFAULT_DUTY_SYNC_POLLING_MINUTES


def _poll_loop():
    while True:
        interval_minutes = _poll_once()
        time.sleep(max(60, int(interval_minutes) * 60))


def start_duty_sync_push_poller():
    global _push_poller_started
    if not web_push_configured():
        return False
    with _push_poller_lock:
        if _push_poller_started:
            return True
        thread = threading.Thread(target=_poll_loop, name="duty-sync-push-poller", daemon=True)
        thread.start()
        _push_poller_started = True
        return True
