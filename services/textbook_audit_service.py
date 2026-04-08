import io
from datetime import datetime, timezone

from services.book_storage_service import build_r2_object_url, get_book_objects, get_r2_client, is_r2_configured
from services.logging_service import log_event
from settings import R2_BUCKET_NAME


LARGE_FILE_AUDIT_THRESHOLD_BYTES = 250 * 1024 * 1024
RANGE_SAMPLE_BYTES = 64 * 1024


def _isoformat(value):
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def to_isoformat(value):
    return _isoformat(value)


def _download_range(client, key, byte_range):
    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key, Range=byte_range)
    try:
        return response["Body"].read()
    finally:
        response["Body"].close()


def _basic_pdf_probe(client, key):
    sample = _download_range(client, key, f"bytes=0-{RANGE_SAMPLE_BYTES - 1}")
    header = sample[:16]
    decoded_sample = sample.decode("latin-1", errors="ignore")
    return {
        "is_pdf_header": header.startswith(b"%PDF-"),
        "header_preview": header.decode("latin-1", errors="ignore"),
        "contains_obj_markers": " obj" in decoded_sample,
        "contains_stream_markers": "stream" in decoded_sample,
        "contains_toc_hints": any(marker in decoded_sample.lower() for marker in ("outlines", "contents", "bookmark")),
    }


def _extract_full_pdf_audit(client, key):
    from pypdf import PdfReader

    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
    try:
        pdf_bytes = response["Body"].read()
    finally:
        response["Body"].close()

    reader = PdfReader(io.BytesIO(pdf_bytes))
    metadata = reader.metadata or {}
    first_page_text = ""
    text_extractable = False
    if reader.pages:
        first_page_text = (reader.pages[0].extract_text() or "").strip()
        text_extractable = bool(first_page_text)

    outline_count = 0
    try:
        outlines = reader.outline or []
        outline_count = len(outlines)
    except Exception:
        outline_count = 0

    return {
        "page_count": len(reader.pages),
        "metadata": {
            "title": getattr(metadata, "title", None) if hasattr(metadata, "title") else metadata.get("/Title"),
            "author": getattr(metadata, "author", None) if hasattr(metadata, "author") else metadata.get("/Author"),
            "producer": getattr(metadata, "producer", None) if hasattr(metadata, "producer") else metadata.get("/Producer"),
        },
        "has_outlines": outline_count > 0,
        "outline_count": outline_count,
        "first_page_text_preview": first_page_text[:500] or None,
        "text_extractable": text_extractable,
    }


def audit_textbook_objects():
    if not is_r2_configured():
        return {
            "status": "not_configured",
            "bucket": R2_BUCKET_NAME or None,
            "books": [],
            "message": "R2 credentials are not fully configured.",
        }

    client = get_r2_client()
    books = []

    for book in get_book_objects():
        key = book["key"]
        entry = {
            "book_id": book["book_id"],
            "title": book["title"],
            "edition": book["edition"],
            "domain": book["domain"],
            "object_key": key,
            "bucket": R2_BUCKET_NAME,
            "object_url": build_r2_object_url(key),
        }

        try:
            head = client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
        except Exception as exc:
            entry.update(
                {
                    "status": "missing_or_inaccessible",
                    "error": str(exc),
                }
            )
            books.append(entry)
            continue

        size_bytes = int(head.get("ContentLength") or 0)
        entry.update(
            {
                "status": "available",
                "size_bytes": size_bytes,
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "etag": str(head.get("ETag") or "").strip('"'),
                "content_type": head.get("ContentType"),
                "last_modified": _isoformat(head.get("LastModified")),
            }
        )

        try:
            entry["pdf_probe"] = _basic_pdf_probe(client, key)
        except Exception as exc:
            entry["pdf_probe_error"] = str(exc)

        if size_bytes > LARGE_FILE_AUDIT_THRESHOLD_BYTES:
            entry["full_text_audit"] = {
                "skipped": True,
                "reason": "file_exceeds_full_audit_threshold",
                "threshold_mb": round(LARGE_FILE_AUDIT_THRESHOLD_BYTES / (1024 * 1024), 2),
            }
            books.append(entry)
            continue

        try:
            entry["full_text_audit"] = _extract_full_pdf_audit(client, key)
        except Exception as exc:
            entry["full_text_audit"] = {
                "skipped": False,
                "error": str(exc),
            }

        books.append(entry)

    payload = {
        "status": "ok",
        "bucket": R2_BUCKET_NAME,
        "book_count": len(books),
        "books": books,
    }
    log_event(
        "textbook_audit_run",
        payload={
            "bucket": R2_BUCKET_NAME,
            "book_count": len(books),
            "available_books": [book["book_id"] for book in books if book.get("status") == "available"],
        },
    )
    return payload
