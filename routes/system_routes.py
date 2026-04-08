from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from routes.home_routes import APP_VERSION
from services.textbook_catalog_service import build_textbook_catalog, get_gabbe_mvp_topic_map, search_gabbe_topic
from services.textbook_audit_service import audit_textbook_objects
from settings import (
    APP_BASE_URL,
    APP_ENV,
    ENABLE_EXTERNAL_SIDE_EFFECTS,
    ENABLE_GOOGLE_CALENDAR_INTEGRATION,
    MONGODB_DB_NAME,
)


router = APIRouter()


@router.get("/health/config")
def health_config():
    payload = {
        "status": "ok",
        "app_version": APP_VERSION,
        "app_env": APP_ENV,
        "external_side_effects_enabled": ENABLE_EXTERNAL_SIDE_EFFECTS,
        "google_calendar_integration_enabled": ENABLE_GOOGLE_CALENDAR_INTEGRATION,
    }

    if APP_ENV != "production":
        payload.update(
            {
                "app_base_url": APP_BASE_URL or None,
                "mongodb_db_name": MONGODB_DB_NAME,
            }
        )

    return JSONResponse(payload)


@router.get("/health/textbooks")
def health_textbooks():
    if APP_ENV == "production":
        return JSONResponse({"status": "forbidden"}, status_code=403)
    return JSONResponse(audit_textbook_objects())


@router.get("/health/textbooks/gabbe")
def health_textbooks_gabbe():
    if APP_ENV == "production":
        return JSONResponse({"status": "forbidden"}, status_code=403)

    catalog_payload = build_textbook_catalog("gabbe_9")
    preview = catalog_payload["catalog"][:40]
    return JSONResponse(
        {
            "status": "ok",
            "book_id": catalog_payload["book_id"],
            "title": catalog_payload["title"],
            "edition": catalog_payload["edition"],
            "domain": catalog_payload["domain"],
            "page_count": catalog_payload["page_count"],
            "catalog_entry_count": catalog_payload["catalog_entry_count"],
            "scan_window": catalog_payload["scan_window"],
            "catalog_preview": preview,
            "gabbe_mvp_topic_map": get_gabbe_mvp_topic_map(),
        }
    )


@router.get("/health/textbooks/gabbe/search")
def health_textbooks_gabbe_search(topic: str = Query(..., min_length=2)):
    if APP_ENV == "production":
        return JSONResponse({"status": "forbidden"}, status_code=403)

    payload = search_gabbe_topic(topic)
    return JSONResponse(
        {
            "status": "ok",
            "book_id": "gabbe_9",
            **payload,
        }
    )
