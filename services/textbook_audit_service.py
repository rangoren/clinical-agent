import io
import os
import tempfile
from datetime import datetime, timezone

from services.book_storage_service import build_r2_object_url, get_book_object, get_book_objects, get_r2_client, is_r2_configured
from services.textbook_cache_service import get_textbook_cache, save_textbook_cache
from services.logging_service import log_event
from settings import R2_BUCKET_NAME


LARGE_FILE_AUDIT_THRESHOLD_BYTES = 250 * 1024 * 1024
RANGE_SAMPLE_BYTES = 64 * 1024
STREAM_CHUNK_BYTES = 8 * 1024 * 1024


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


def _stream_book_to_tempfile(client, key):
    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_path = temp_file.name
            while True:
                chunk = response["Body"].read(STREAM_CHUNK_BYTES)
                if not chunk:
                    break
                temp_file.write(chunk)
    finally:
        response["Body"].close()
    return temp_path


def _sample_page_numbers(page_count, sample_pages=5):
    if page_count <= 0:
        return []
    if page_count <= sample_pages:
        return list(range(1, page_count + 1))

    anchors = [1, 2, 3, page_count]
    for fraction in (0.1, 0.25, 0.5, 0.75, 0.9):
        anchors.append(max(1, min(page_count, int(page_count * fraction))))

    sampled = []
    seen = set()
    for page in anchors:
        if page not in seen:
            sampled.append(page)
            seen.add(page)
        if len(sampled) >= sample_pages:
            break

    if len(sampled) < sample_pages:
        step = max(1, page_count // sample_pages)
        for page in range(1, page_count + 1, step):
            if page not in seen:
                sampled.append(page)
                seen.add(page)
            if len(sampled) >= sample_pages:
                break

    return sorted(sampled[:sample_pages])


def _extract_streamed_pdf_audit(temp_path, sample_pages=5):
    from pypdf import PdfReader

    with open(temp_path, "rb") as pdf_file:
        reader = PdfReader(pdf_file)
        metadata = reader.metadata or {}
        page_count = len(reader.pages)
        probe_pages = _sample_page_numbers(page_count, sample_pages=sample_pages)
        sampled_pages = []
        extractable_pages = 0

        for page_number in probe_pages:
            text = (reader.pages[page_number - 1].extract_text() or "").strip()
            if text:
                extractable_pages += 1
            sampled_pages.append(
                {
                    "page": page_number,
                    "text_preview": text[:500] or None,
                    "text_extractable": bool(text),
                }
            )

        outline_count = 0
        try:
            outlines = reader.outline or []
            outline_count = len(outlines)
        except Exception:
            outline_count = 0

    return {
        "page_count": page_count,
        "sampled_page_count": len(sampled_pages),
        "probe_pages": probe_pages,
        "extractable_sample_pages": extractable_pages,
        "sample_extractable_ratio": round(extractable_pages / max(1, len(sampled_pages)), 2),
        "sample_pages": sampled_pages,
        "metadata": {
            "title": getattr(metadata, "title", None) if hasattr(metadata, "title") else metadata.get("/Title"),
            "author": getattr(metadata, "author", None) if hasattr(metadata, "author") else metadata.get("/Author"),
            "producer": getattr(metadata, "producer", None) if hasattr(metadata, "producer") else metadata.get("/Producer"),
        },
        "has_outlines": outline_count > 0,
        "outline_count": outline_count,
    }


def run_deep_textbook_audit(book_id, sample_pages=5):
    if not is_r2_configured():
        raise RuntimeError("R2 credentials are not fully configured.")

    book = get_book_object(book_id)
    if not book:
        raise ValueError(f"Unknown book_id: {book_id}")

    client = get_r2_client()
    temp_path = _stream_book_to_tempfile(client, book["key"])
    try:
        audit = _extract_streamed_pdf_audit(temp_path, sample_pages=sample_pages)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)

    payload = {
        "book_id": book["book_id"],
        "title": book["title"],
        "edition": book["edition"],
        "domain": book["domain"],
        "object_key": book["key"],
        "bucket": R2_BUCKET_NAME,
        "object_url": build_r2_object_url(book["key"]),
        "sample_pages_requested": sample_pages,
        "deep_audit": audit,
    }

    cached = save_textbook_cache(f"{book_id}_deep_audit", payload)
    log_event(
        "textbook_deep_audit_run",
        payload={
            "book_id": book_id,
            "sample_pages": sample_pages,
            "page_count": audit.get("page_count"),
            "sample_extractable_ratio": audit.get("sample_extractable_ratio"),
        },
    )
    return {
        "cache_updated_at": _isoformat(cached.get("updated_at")),
        **payload,
    }


def get_cached_deep_textbook_audit(book_id):
    cached = get_textbook_cache(f"{book_id}_deep_audit")
    if not cached:
        return None
    return {
        "cache_updated_at": _isoformat(cached.get("updated_at")),
        **(cached.get("payload") or {}),
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
