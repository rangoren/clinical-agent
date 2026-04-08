import io
import re
from functools import lru_cache

from services.book_storage_service import get_book_object, get_r2_client
from settings import R2_BUCKET_NAME


GABBE_TOPIC_MAP = [
    {"topic": "preeclampsia", "domain": "obstetrics", "priority": "high", "why": "core board topic and management pivot"},
    {"topic": "gestational hypertension", "domain": "obstetrics", "priority": "high", "why": "common overlap with severe-feature triage"},
    {"topic": "preterm labor", "domain": "obstetrics", "priority": "high", "why": "tocolysis, steroids, magnesium, transfer decisions"},
    {"topic": "pprom", "domain": "obstetrics", "priority": "high", "why": "high-yield board management and admission logic"},
    {"topic": "fetal growth restriction", "domain": "obstetrics", "priority": "high", "why": "surveillance and timing of delivery"},
    {"topic": "placenta previa", "domain": "obstetrics", "priority": "high", "why": "bleeding triage and delivery planning"},
    {"topic": "placenta accreta spectrum", "domain": "obstetrics", "priority": "high", "why": "critical obstetric planning topic"},
    {"topic": "multiple gestation", "domain": "obstetrics", "priority": "medium", "why": "timing, surveillance, and complications"},
    {"topic": "gestational diabetes", "domain": "obstetrics", "priority": "high", "why": "common exam and clinic topic"},
    {"topic": "postpartum hemorrhage", "domain": "obstetrics", "priority": "high", "why": "acute management sequence"},
    {"topic": "operative vaginal delivery", "domain": "obstetrics", "priority": "medium", "why": "indications, contraindications, and traps"},
    {"topic": "shoulder dystocia", "domain": "obstetrics", "priority": "high", "why": "classic emergency sequence topic"},
    {"topic": "fetal surveillance", "domain": "obstetrics", "priority": "high", "why": "NST/BPP/CTG interpretation and action"},
    {"topic": "labor induction", "domain": "obstetrics", "priority": "medium", "why": "Bishop score and ripening decisions"},
    {"topic": "trial of labor after cesarean", "domain": "obstetrics", "priority": "high", "why": "counseling and contraindications"},
]


def _clean_title(value):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"[^\x20-\x7E]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace(" ?", "")
    return text or "Untitled"


def _destination_page_number(reader, destination):
    try:
        return reader.get_destination_page_number(destination) + 1
    except Exception:
        return None


def _flatten_outline(reader, outline, depth=0, entries=None):
    entries = entries or []
    for item in outline or []:
        if isinstance(item, list):
            _flatten_outline(reader, item, depth=depth + 1, entries=entries)
            continue

        title = _clean_title(getattr(item, "title", None) or getattr(item, "/Title", None) or str(item))
        page = _destination_page_number(reader, item)
        entry = {
            "title": title,
            "page": page,
            "depth": depth,
        }
        entries.append(entry)
    return entries


def _dedupe_and_sort_entries(flat_entries):
    deduped = []
    seen = set()
    for entry in flat_entries:
        title = _clean_title(entry.get("title"))
        page = entry.get("page")
        depth = int(entry.get("depth") or 0)
        if not page:
            continue
        key = (title.lower(), page, depth)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"title": title, "page": page, "depth": depth})

    deduped.sort(key=lambda entry: (entry["page"], entry["depth"], entry["title"].lower()))
    return deduped


def _to_catalog(flat_entries, page_count, max_depth=2):
    filtered_entries = [entry for entry in _dedupe_and_sort_entries(flat_entries) if entry["depth"] <= max_depth]
    catalog = []
    for index, entry in enumerate(filtered_entries):
        current_page = entry.get("page")
        next_page = None
        for later in filtered_entries[index + 1:]:
            later_page = later.get("page")
            if later_page and current_page and later_page >= current_page:
                next_page = later_page
                break

        page_end = page_count
        if current_page and next_page and next_page > current_page:
            page_end = next_page - 1
        elif current_page:
            page_end = page_count

        catalog.append(
            {
                "title": entry["title"],
                "depth": entry["depth"],
                "page_start": current_page,
                "page_end": page_end,
            }
        )
    return catalog


@lru_cache(maxsize=4)
def build_textbook_catalog(book_id):
    book = get_book_object(book_id)
    if not book:
        raise ValueError(f"Unknown book_id '{book_id}'.")

    client = get_r2_client()
    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=book["key"])
    try:
        pdf_bytes = response["Body"].read()
    finally:
        response["Body"].close()

    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    outline = []
    try:
        outline = reader.outline or []
    except Exception:
        outline = []

    flat_entries = _flatten_outline(reader, outline, depth=0, entries=[])
    catalog = _to_catalog(flat_entries, len(reader.pages), max_depth=2)
    chapter_catalog = [entry for entry in catalog if entry["depth"] <= 1]

    return {
        "book_id": book["book_id"],
        "title": book["title"],
        "edition": book["edition"],
        "domain": book["domain"],
        "page_count": len(reader.pages),
        "outline_entry_count": len(flat_entries),
        "catalog_entry_count": len(catalog),
        "chapter_entry_count": len(chapter_catalog),
        "catalog": catalog,
        "chapter_catalog": chapter_catalog,
    }


def get_gabbe_mvp_topic_map():
    return list(GABBE_TOPIC_MAP)
