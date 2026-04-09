import re

from services.book_storage_service import get_book_object
from services.textbook_cache_service import get_textbook_cache


TEXTBOOK_REQUEST_HINTS = (
    "what does",
    "according to",
    "based on",
    "from the book",
    "what is written",
    "what does the book say",
    "say about",
    "what is written in the book",
    "what does the textbook say",
    "what does this book say",
    "what does the book say about",
    "what is written in",
    "לפי",
    "מה כתוב",
    "מה כתוב בספר",
    "מה הספר אומר",
    "מה הספר כותב",
    "מה כתוב בספר על",
    "מה הספר אומר על",
    "מה כתוב בגאבי",
    "מה גאבי אומר",
    "מה כתוב בברק",
    "מה ברק אומר",
    "מה כתוב בספרוף",
    "מה ספרוף אומר",
    "מה אומר",
    "כתוב ב",
)

BOOK_ALIASES = {
    "gabbe_9": ("gabbe", "gabbe's", "gabbe obstetrics"),
    "berek_17": ("berek", "berek & novak", "berek and novak"),
    "speroff_10": ("speroff", "speroff's", "clinical gynecologic endocrinology"),
}


def _normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _trim_excerpt(text, limit=1800):
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def detect_textbook_request(user_message):
    normalized = _normalize_text(user_message)
    if not normalized:
        return None

    explicit_request = any(marker in normalized for marker in TEXTBOOK_REQUEST_HINTS)
    matched_book_id = None
    matched_alias = None

    for book_id, aliases in BOOK_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                matched_book_id = book_id
                matched_alias = alias
                break
        if matched_book_id:
            break

    if not matched_book_id:
        return None

    if not explicit_request and not normalized.startswith(matched_alias):
        return None

    book = get_book_object(matched_book_id)
    if not book:
        return None

    return {
        "book_id": matched_book_id,
        "book_title": book["title"],
        "edition": book["edition"],
        "supported": matched_book_id == "gabbe_9",
    }


def _score_topic_match(normalized_message, topic_entry):
    score = 0
    topic = _normalize_text(topic_entry.get("topic"))
    if topic and topic in normalized_message:
        score += 8

    for query in topic_entry.get("queries") or []:
        query_normalized = _normalize_text(query)
        if query_normalized and query_normalized in normalized_message:
            score += max(3, min(6, len(query_normalized.split()) + 2))

    return score


def _find_best_gabbe_topic(user_message):
    mapping_doc = get_textbook_cache("gabbe_topic_mapping") or {}
    mapping_payload = mapping_doc.get("payload") or {}
    topics = mapping_payload.get("topics") or []
    if not topics:
        return None

    normalized_message = _normalize_text(user_message)
    best_entry = None
    best_score = 0

    for topic_entry in topics:
        score = _score_topic_match(normalized_message, topic_entry)
        if score > best_score:
            best_score = score
            best_entry = topic_entry

    if best_score <= 0:
        return None
    return best_entry


def _build_range_excerpt(page_map, page_start, page_end):
    pages = []
    for page_number in range(page_start, page_end + 1):
        text = page_map.get(page_number)
        if text:
            pages.append(text)
    return _trim_excerpt(" ".join(pages))


def build_gabbe_textbook_context(user_message, max_ranges=3):
    topic_entry = _find_best_gabbe_topic(user_message)
    if not topic_entry:
        return {
            "status": "topic_not_found",
            "message": "I couldn't confidently map this request to one of the indexed Gabbe topics yet.",
        }

    page_cache_doc = get_textbook_cache("gabbe_page_text") or {}
    page_payload = page_cache_doc.get("payload") or {}
    cached_pages = page_payload.get("pages") or []
    if not cached_pages:
        return {
            "status": "page_cache_missing",
            "message": "Gabbe page cache is not available yet.",
        }

    page_map = {entry.get("page"): entry.get("text") for entry in cached_pages if entry.get("page")}
    candidate_ranges = topic_entry.get("candidate_ranges") or []
    sources = []
    excerpts = []

    for index, candidate_range in enumerate(candidate_ranges[:max_ranges], start=1):
        page_start = candidate_range.get("page_start")
        page_end = candidate_range.get("page_end")
        if not page_start or not page_end:
            continue
        excerpt_text = _build_range_excerpt(page_map, page_start, page_end)
        if not excerpt_text:
            continue

        source_id = f"T{index}"
        sources.append(
            {
                "source_id": source_id,
                "title": "Gabbe's Obstetrics: Normal and Problem Pregnancies, 9th edition",
                "url": None,
                "source_type": "Textbook excerpt",
                "source_detail": f"Topic: {topic_entry['topic']} | pp. {page_start}-{page_end}",
                "page_start": page_start,
                "page_end": page_end,
                "book_id": "gabbe_9",
                "topic": topic_entry["topic"],
                "is_textbook": True,
            }
        )
        excerpts.append(
            {
                "source_id": source_id,
                "topic": topic_entry["topic"],
                "page_start": page_start,
                "page_end": page_end,
                "text": excerpt_text,
            }
        )

    if not excerpts:
        return {
            "status": "no_excerpts",
            "message": "I found the topic, but I couldn't assemble textbook excerpts for it.",
            "topic_entry": topic_entry,
        }

    return {
        "status": "ok",
        "book_id": "gabbe_9",
        "book_title": "Gabbe's Obstetrics: Normal and Problem Pregnancies",
        "edition": "9",
        "matched_topic": topic_entry["topic"],
        "topic_entry": topic_entry,
        "sources": sources,
        "excerpts": excerpts,
    }
