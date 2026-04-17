from __future__ import annotations

import json
import threading
import time
from datetime import datetime

from db import duty_sync_connections_collection, push_subscriptions_collection
from services.logging_service import log_event
from settings import APP_BASE_URL, WEB_PUSH_PRIVATE_KEY, WEB_PUSH_PUBLIC_KEY, WEB_PUSH_SUBJECT


_push_poller_started = False
_push_poller_lock = threading.Lock()


def web_push_configured():
    return bool(WEB_PUSH_PUBLIC_KEY and WEB_PUSH_PRIVATE_KEY and WEB_PUSH_SUBJECT)


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
    push_subscriptions_collection.update_one(
        {"session_id": session_id, "endpoint": endpoint},
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
    return {"status": "subscribed", "reply": "Duty Sync push alerts are enabled."}


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
    return send_web_push_message(
        session_id=session_id,
        title="Duty Sync update",
        body=reply_text or "Duty Sync found personal schedule changes.",
        tag=review.get("review_id") or "duty-sync-review",
        url=f"{APP_BASE_URL}/?app_mode=scheduling&duty_sync_review=1" if APP_BASE_URL else "/?app_mode=scheduling&duty_sync_review=1",
    )


def send_web_push_message(session_id, title, body, tag="duty-sync-review", url=None):
    if not web_push_configured():
        return 0
    payload = {
        "title": title,
        "body": body,
        "tag": tag,
        "url": url or (f"{APP_BASE_URL}/?app_mode=scheduling&duty_sync_review=1" if APP_BASE_URL else "/?app_mode=scheduling&duty_sync_review=1"),
    }
    sent_count = 0
    for doc in push_subscriptions_collection.find({"session_id": session_id}):
        if _send_notification_to_subscription(doc.get("subscription") or {}, payload):
            sent_count += 1
    return sent_count


def _review_signature(review):
    if not review:
        return ""
    return json.dumps(review, sort_keys=True, ensure_ascii=False)


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
            review = result.get("pending_review")
            if review:
                connection = duty_sync_connections_collection.find_one(
                    {"session_id": session_id},
                    {"last_pushed_review_signature": 1, "last_pushed_review_payload": 1},
                )
                current_signature = _review_signature(review)
                if current_signature and current_signature != (connection or {}).get("last_pushed_review_signature"):
                    push_scope_review = _build_push_review_scope(review, (connection or {}).get("last_pushed_review_payload") or {})
                    sent_count = send_duty_sync_push(session_id, push_scope_review, result.get("reply"))
                    if sent_count:
                        duty_sync_connections_collection.update_one(
                            {"session_id": session_id},
                            {
                                "$set": {
                                    "last_pushed_review_signature": current_signature,
                                    "last_pushed_review_payload": review,
                                    "last_push_review_scope": push_scope_review,
                                    "last_pushed_at": datetime.utcnow(),
                                }
                            },
                        )
            else:
                duty_sync_connections_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"last_pushed_review_signature": None, "last_pushed_review_payload": None, "last_push_review_scope": None}},
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
