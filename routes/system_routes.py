from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from routes.home_routes import APP_VERSION
from services.logging_service import log_event
from services.textbook_cache_service import get_textbook_cache, merge_textbook_cache_topics
from services.textbook_catalog_service import (
    build_gabbe_topic_mapping_batch,
    cache_gabbe_page_text_batch,
    build_gabbe_topic_mapping,
    build_gabbe_topic_mapping_summary,
    build_textbook_catalog,
    get_gabbe_mvp_topic_map,
    search_gabbe_topic,
)
from services.textbook_audit_service import audit_textbook_objects, to_isoformat
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


@router.get("/health/textbooks/gabbe/mapping")
def health_textbooks_gabbe_mapping():
    if APP_ENV == "production":
        return JSONResponse({"status": "forbidden"}, status_code=403)

    cached = get_textbook_cache("gabbe_topic_mapping")
    if not cached:
        return JSONResponse(
            {
                "status": "not_ready",
                "message": "Gabbe topic mapping has not been precomputed yet.",
            },
            status_code=202,
        )

    payload = cached.get("payload") or {}
    return JSONResponse(
        {
            "status": "ok",
            "cache_updated_at": to_isoformat(cached.get("updated_at")),
            **build_gabbe_topic_mapping_summary(payload),
        }
    )


@router.post("/health/textbooks/gabbe/page-cache/rebuild")
def health_textbooks_gabbe_page_cache_rebuild(
    start_page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=50),
):
    if APP_ENV == "production":
        return JSONResponse({"status": "forbidden"}, status_code=403)

    payload = cache_gabbe_page_text_batch(start_page=start_page, limit=limit)
    return JSONResponse(
        {
            "status": "ok",
            **{**payload, "cache_updated_at": to_isoformat(payload.get("cache_updated_at"))},
        }
    )


@router.post("/health/textbooks/gabbe/mapping/rebuild")
def health_textbooks_gabbe_mapping_rebuild(
    offset: int = Query(0, ge=0),
    limit: int = Query(5, ge=1, le=10),
    tier: str | None = Query(None),
):
    if APP_ENV == "production":
        return JSONResponse({"status": "forbidden"}, status_code=403)

    payload = build_gabbe_topic_mapping_batch(offset=offset, limit=limit, tier=tier)
    cached = merge_textbook_cache_topics(
        "gabbe_topic_mapping",
        payload.get("topics") or [],
        metadata={"book_id": payload.get("book_id", "gabbe_9")},
    )
    cached_payload = cached.get("payload") or {}
    log_event(
        "gabbe_topic_mapping_rebuilt",
        payload={
            "offset": offset,
            "limit": limit,
            "tier": tier,
            "batch_count": payload.get("batch_count"),
            "cached_topic_count": cached_payload.get("topic_count"),
        },
    )
    return JSONResponse(
        {
            "status": "ok",
            "message": "Gabbe topic mapping batch rebuilt and cached.",
            "offset": offset,
            "limit": limit,
            "tier": tier,
            "batch_count": payload.get("batch_count"),
            "total_available_topics": payload.get("total_available_topics"),
            "cache_updated_at": to_isoformat(cached.get("updated_at")),
            **build_gabbe_topic_mapping_summary(cached_payload),
        }
    )
