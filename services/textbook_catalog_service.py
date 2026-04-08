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

CHAPTER_TITLE_RE = re.compile(r"^\s*(\d{1,3})\s*[.\-]?\s+([A-Z][A-Za-z0-9,\-:;/()' ]{6,120})\s*$")
SHORT_ALL_CAPS_RE = re.compile(r"^[A-Z][A-Z0-9,\-:;/()' ]{6,90}$")

TEXT_SCAN_START_PAGE = 1
TEXT_SCAN_END_PAGE = 250
TOPIC_SEARCH_SCAN_END_PAGE = 900

GABBE_TOPIC_QUERIES = {
    "preeclampsia": ["preeclampsia", "pre-eclampsia", "severe features"],
    "gestational hypertension": ["gestational hypertension", "hypertension in pregnancy"],
    "preterm labor": ["preterm labor", "preterm labour", "tocolysis"],
    "pprom": ["pprom", "prelabor rupture of membranes", "preterm premature rupture"],
    "fetal growth restriction": ["fetal growth restriction", "growth restriction", "fgr"],
    "placenta previa": ["placenta previa"],
    "placenta accreta spectrum": ["placenta accreta", "accreta spectrum"],
    "multiple gestation": ["multiple gestation", "twin pregnancy", "triplet pregnancy"],
    "gestational diabetes": ["gestational diabetes", "gdm"],
    "postpartum hemorrhage": ["postpartum hemorrhage", "pph", "uterine atony"],
    "operative vaginal delivery": ["operative vaginal delivery", "vacuum extraction", "forceps delivery"],
    "shoulder dystocia": ["shoulder dystocia"],
    "fetal surveillance": ["fetal surveillance", "biophysical profile", "nonstress test", "ctg"],
    "labor induction": ["labor induction", "labour induction", "cervical ripening"],
    "trial of labor after cesarean": ["trial of labor after cesarean", "tOLAC", "VBAC", "vaginal birth after cesarean"],
}


def _clean_text(value):
    text = re.sub(r"[^\x20-\x7E]+", " ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _page_text_lines(page):
    raw_text = page.extract_text() or ""
    cleaned_lines = []
    for line in raw_text.splitlines():
        cleaned = _clean_text(line)
        if cleaned:
            cleaned_lines.append(cleaned)
    return cleaned_lines


def _page_text(page):
    return "\n".join(_page_text_lines(page))


def _candidate_heading(line):
    if not line:
        return None

    chapter_match = CHAPTER_TITLE_RE.match(line)
    if chapter_match:
        return {
            "title": f"{chapter_match.group(1)}. {chapter_match.group(2).strip()}",
            "level": "chapter",
        }

    if SHORT_ALL_CAPS_RE.match(line):
        if any(token in line.lower() for token in ("copyright", "elsevier", "isbn", "printed")):
            return None
        return {
            "title": line.title(),
            "level": "section",
        }

    return None


def _dedupe_preserve_order(entries):
    seen = set()
    deduped = []
    for entry in entries:
        key = (entry["title"].lower(), entry["page_start"], entry["level"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def _scan_gabbe_text_catalog(reader):
    page_limit = min(TEXT_SCAN_END_PAGE, len(reader.pages))
    candidates = []

    for page_number in range(TEXT_SCAN_START_PAGE, page_limit + 1):
        page = reader.pages[page_number - 1]
        for line in _page_text_lines(page)[:18]:
            heading = _candidate_heading(line)
            if not heading:
                continue
            candidates.append(
                {
                    "title": heading["title"],
                    "level": heading["level"],
                    "page_start": page_number,
                }
            )

    deduped = _dedupe_preserve_order(candidates)

    catalog = []
    for index, entry in enumerate(deduped):
        next_page = page_limit
        for later in deduped[index + 1:]:
            if later["page_start"] > entry["page_start"]:
                next_page = later["page_start"] - 1
                break
        catalog.append(
            {
                "title": entry["title"],
                "level": entry["level"],
                "page_start": entry["page_start"],
                "page_end": next_page,
            }
        )

    return catalog


def _snippet_around_match(text, match_start, radius=180):
    start = max(0, match_start - radius)
    end = min(len(text), match_start + radius)
    snippet = text[start:end]
    snippet = re.sub(r"\s+", " ", snippet).strip()
    return snippet


def _topic_queries(topic):
    queries = GABBE_TOPIC_QUERIES.get(topic, [])
    if queries:
        return queries
    return [topic]


@lru_cache(maxsize=16)
def search_gabbe_topic(topic):
    book = get_book_object("gabbe_9")
    client = get_r2_client()
    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=book["key"])
    try:
        pdf_bytes = response["Body"].read()
    finally:
        response["Body"].close()

    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    page_limit = min(TOPIC_SEARCH_SCAN_END_PAGE, len(reader.pages))
    normalized_queries = [query.lower() for query in _topic_queries(topic)]
    results = []

    for page_number in range(1, page_limit + 1):
        text = _page_text(reader.pages[page_number - 1])
        if not text:
            continue
        lower_text = text.lower()
        for query in normalized_queries:
            match_index = lower_text.find(query)
            if match_index == -1:
                continue
            results.append(
                {
                    "query": query,
                    "page": page_number,
                    "snippet": _snippet_around_match(text, match_index),
                }
            )
            break

    return {
        "topic": topic,
        "queries": normalized_queries,
        "scan_window": {"start_page": 1, "end_page": page_limit},
        "match_count": len(results),
        "matches": results[:12],
    }


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

    if book_id != "gabbe_9":
        return {
            "book_id": book["book_id"],
            "title": book["title"],
            "edition": book["edition"],
            "domain": book["domain"],
            "page_count": len(reader.pages),
            "catalog": [],
            "catalog_entry_count": 0,
            "scan_window": {"start_page": TEXT_SCAN_START_PAGE, "end_page": min(TEXT_SCAN_END_PAGE, len(reader.pages))},
        }

    catalog = _scan_gabbe_text_catalog(reader)

    return {
        "book_id": book["book_id"],
        "title": book["title"],
        "edition": book["edition"],
        "domain": book["domain"],
        "page_count": len(reader.pages),
        "catalog_entry_count": len(catalog),
        "catalog": catalog,
        "scan_window": {"start_page": TEXT_SCAN_START_PAGE, "end_page": min(TEXT_SCAN_END_PAGE, len(reader.pages))},
    }


def get_gabbe_mvp_topic_map():
    return list(GABBE_TOPIC_MAP)
