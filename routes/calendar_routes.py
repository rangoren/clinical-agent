from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from settings import APP_BASE_URL, APP_ENV
from services.google_calendar_service import (
    GoogleCalendarReconnectRequiredError,
    begin_google_calendar_connect,
    complete_google_calendar_connect,
    disconnect_google_calendar,
    get_google_calendar_status,
)
from services.duty_sync_service import (
    apply_duty_taxi_action,
    approve_pending_duty_review,
    approve_pending_duty_review_scope,
    check_duty_sheet,
    connect_duty_sheet,
    disconnect_duty_sheet,
    edit_pending_review_change,
    get_duty_sync_status,
    list_duty_reminder_qa_duties,
    load_duty_reminder_card,
    load_pending_duty_review,
    ignore_pending_duty_review,
    ignore_pending_duty_review_scope,
    poll_duty_sheet,
    set_duty_taxi_state_for_qa,
    toggle_pending_review_change,
    update_duty_taxi_toggle,
)
from services.logging_service import log_event
from services.scheduling_service import run_scheduling_quick_card
from services.web_push_service import (
    delete_web_push_subscription,
    get_web_push_status,
    save_web_push_subscription,
    send_web_push_message,
)


router = APIRouter()


def _qa_disabled_response():
    return JSONResponse({"status": "disabled", "reply": "QA tools are available in development only."}, status_code=404)


@router.post("/calendar/quick-card/run")
async def handle_calendar_quick_card_run(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            run_scheduling_quick_card(
                session_id=data.get("session_id"),
                intent_type=data.get("intent_type"),
                quick_key=data.get("quick_key"),
            )
        )
    except GoogleCalendarReconnectRequiredError:
        return JSONResponse(
            {
                "status": "google_reconnect_required",
                "reply": "Reconnect Google Calendar to keep using calendar cards.",
            }
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/quick-card/run", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/status")
async def handle_calendar_status(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        provider = data.get("provider", "google")
        if provider != "google":
            return JSONResponse({"connected": False, "available": False, "provider": provider})
        return JSONResponse(get_google_calendar_status(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/status", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/connect/google")
async def handle_google_calendar_connect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(begin_google_calendar_connect(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/connect/google", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.get("/calendar/google/callback")
async def handle_google_calendar_callback(code: str = "", state: str = "", error: str = ""):
    base_redirect = APP_BASE_URL or "/"
    try:
        if error:
            log_event("google_calendar_callback_error", payload={"reason": "provider_error", "error": error}, level="error")
            return RedirectResponse(url=f"{base_redirect}?app_mode=scheduling&calendar_status=error")
        result = complete_google_calendar_connect(code=code, state=state)
        if result.get("status") == "connected":
            return RedirectResponse(url=f"{base_redirect}?app_mode=scheduling&calendar_status=connected&provider=google")
        log_event(
            "google_calendar_callback_error",
            session_id=result.get("session_id"),
            payload={"reason": result.get("status"), "reply": result.get("reply")},
            level="error",
        )
        return RedirectResponse(url=f"{base_redirect}?app_mode=scheduling&calendar_status=error")
    except Exception as exc:
        log_event("google_calendar_callback_error", payload={"reason": "exception", "error": str(exc)}, level="error")
        return RedirectResponse(url=f"{base_redirect}?app_mode=scheduling&calendar_status=error")


@router.post("/calendar/disconnect/google")
async def handle_google_calendar_disconnect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(disconnect_google_calendar(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/disconnect/google", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/status")
async def handle_duty_sync_status(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(get_duty_sync_status(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/status", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/connect")
async def handle_duty_sync_connect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        sheet_url = data.get("sheet_url")
        full_name = data.get("full_name")
        return JSONResponse(connect_duty_sheet(session_id, sheet_url=sheet_url, full_name=full_name))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/connect", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/check")
async def handle_duty_sync_check(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(check_duty_sheet(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/check", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/poll")
async def handle_duty_sync_poll(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(poll_duty_sheet(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/poll", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/disconnect")
async def handle_duty_sync_disconnect(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        return JSONResponse(disconnect_duty_sheet(session_id))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/disconnect", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/approve")
async def handle_duty_sync_review_approve(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            approve_pending_duty_review(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                selected_calendar_id=data.get("selected_calendar_id"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/approve", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/approve-scope")
async def handle_duty_sync_review_approve_scope(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            approve_pending_duty_review_scope(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                change_keys=data.get("change_keys") or [],
                selected_calendar_id=data.get("selected_calendar_id"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/approve-scope", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/ignore")
async def handle_duty_sync_review_ignore(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            ignore_pending_duty_review(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/ignore", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/ignore-scope")
async def handle_duty_sync_review_ignore_scope(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            ignore_pending_duty_review_scope(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                change_keys=data.get("change_keys") or [],
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/ignore-scope", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/toggle-item")
async def handle_duty_sync_review_toggle_item(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            toggle_pending_review_change(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                change_key=data.get("change_key"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/toggle-item", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/edit-item")
async def handle_duty_sync_review_edit_item(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            edit_pending_review_change(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                change_key=data.get("change_key"),
                edited_date=data.get("edited_date"),
                edited_title=data.get("edited_title"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/edit-item", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/taxi-toggle")
async def handle_duty_sync_review_taxi_toggle(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            update_duty_taxi_toggle(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                duty_key=data.get("duty_key"),
                enabled=bool(data.get("enabled")),
                duty=data.get("duty") or {},
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/taxi-toggle", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/reminder/load")
async def handle_duty_sync_reminder_load(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            load_duty_reminder_card(
                session_id=data.get("session_id"),
                duty_key=data.get("duty_key"),
                reminder_kind=data.get("reminder_kind"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/reminder/load", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/reminder/action")
async def handle_duty_sync_reminder_action(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            apply_duty_taxi_action(
                session_id=data.get("session_id"),
                duty_key=data.get("duty_key"),
                action=data.get("action"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/reminder/action", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/qa/duties")
async def handle_duty_sync_qa_duties(request: Request):
    if APP_ENV == "production":
        return _qa_disabled_response()
    try:
        data = await request.json()
        return JSONResponse(list_duty_reminder_qa_duties(data.get("session_id")))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/qa/duties", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/qa/state")
async def handle_duty_sync_qa_state(request: Request):
    if APP_ENV == "production":
        return _qa_disabled_response()
    try:
        data = await request.json()
        return JSONResponse(
            set_duty_taxi_state_for_qa(
                session_id=data.get("session_id"),
                duty_key=data.get("duty_key"),
                state=data.get("state"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/qa/state", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/review/load")
async def handle_duty_sync_review_load(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            load_pending_duty_review(
                session_id=data.get("session_id"),
                review_id=data.get("review_id"),
                updated_at=data.get("updated_at"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/review/load", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/push/status")
async def handle_duty_sync_push_status(request: Request):
    try:
        data = await request.json()
        return JSONResponse(get_web_push_status(data.get("session_id")))
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/push/status", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/push/subscribe")
async def handle_duty_sync_push_subscribe(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            save_web_push_subscription(
                session_id=data.get("session_id"),
                subscription=data.get("subscription") or {},
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/push/subscribe", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/push/unsubscribe")
async def handle_duty_sync_push_unsubscribe(request: Request):
    try:
        data = await request.json()
        return JSONResponse(
            delete_web_push_subscription(
                session_id=data.get("session_id"),
                endpoint=data.get("endpoint"),
            )
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/push/unsubscribe", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})


@router.post("/calendar/duty-sync/push/test")
async def handle_duty_sync_push_test(request: Request):
    try:
        data = await request.json()
        session_id = data.get("session_id")
        sent_count = send_web_push_message(
            session_id=session_id,
            title="Duty Sync test",
            body="Test push sent from Duty Sync settings.",
            tag="duty-sync-test",
        )
        return JSONResponse(
            {
                "status": "sent" if sent_count else "not_sent",
                "reply": "Test push sent." if sent_count else "No push subscription was found for this session.",
                "sent_count": sent_count,
            }
        )
    except Exception as exc:
        log_event("route_error", payload={"route": "/calendar/duty-sync/push/test", "error": str(exc)}, level="error")
        return JSONResponse({"reply": f"ERROR: {str(exc)}"})
