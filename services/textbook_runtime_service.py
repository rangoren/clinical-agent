import re

from services.book_storage_service import get_book_object
from services.textbook_cache_service import get_textbook_cache
from services.textbook_catalog_service import GABBE_TOPIC_QUERIES, TOPIC_SIGNAL_MARKERS, _search_gabbe_topic_matches


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


MANAGEMENT_MARKERS = (
    "management",
    "treatment",
    "deliver",
    "delivery",
    "expectant",
    "antibiotic",
    "corticosteroid",
    "magnesium",
    "tocolysis",
    "monitor",
    "surveillance",
    "induction",
)


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


def _split_into_sentences(text):
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized:
        return []
    parts = re.split(r"(?<=[\.\?\!;:])\s+", normalized)
    return [part.strip() for part in parts if len(part.strip()) >= 40]


def _matches_within_range(matches, page_start, page_end):
    return [match for match in matches if page_start <= (match.get("page") or 0) <= page_end]


def _format_match_snippets(topic, matches, limit=2):
    snippets = []
    seen = set()
    topic_words = set(_normalize_text(topic).split())

    ranked_matches = []
    for match in matches:
        snippet = _normalize_text(match.get("snippet"))
        if not snippet:
            continue
        score = 0
        query = _normalize_text(match.get("query"))
        if query in topic_words or query == _normalize_text(topic):
            score += 3
        if any(word in snippet for word in topic_words):
            score += 2
        if any(marker in snippet for marker in ("management", "treatment", "delivery", "antibiotic", "magnesium", "tocolysis", "rupture")):
            score += 2
        ranked_matches.append((score, match))

    for _, match in sorted(ranked_matches, key=lambda item: item[0], reverse=True):
        snippet = re.sub(r"\s+", " ", str(match.get("snippet") or "")).strip()
        if not snippet:
            continue
        if snippet in seen:
            continue
        seen.add(snippet)
        snippets.append(f"Page {match['page']}: {snippet}")
        if len(snippets) >= limit:
            break

    return _trim_excerpt(" ".join(snippets), limit=1400)


def _score_sentence(topic, sentence, topic_queries):
    normalized_sentence = _normalize_text(sentence)
    score = 0

    for query in topic_queries:
        normalized_query = _normalize_text(query)
        if normalized_query and normalized_query in normalized_sentence:
            score += 4

    for marker in TOPIC_SIGNAL_MARKERS.get(topic, ()):
        if marker in normalized_sentence:
            score += 3

    for marker in MANAGEMENT_MARKERS:
        if marker in normalized_sentence:
            score += 2

    if any(token in normalized_sentence for token in ("trial", "review", "consortium", "doi.org", "downloaded for")):
        score -= 3

    return score


def _build_curated_range_excerpt(topic, topic_queries, page_map, page_start, page_end, sentence_limit=3):
    ranked_sentences = []

    for page_number in range(page_start, page_end + 1):
        text = page_map.get(page_number)
        if not text:
            continue
        for sentence in _split_into_sentences(text):
            score = _score_sentence(topic, sentence, topic_queries)
            if score <= 0:
                continue
            ranked_sentences.append((score, page_number, sentence))

    if not ranked_sentences:
        return ""

    seen_sentences = set()
    selected = []
    for _, page_number, sentence in sorted(ranked_sentences, key=lambda item: (item[0], -item[1]), reverse=True):
        normalized_sentence = _normalize_text(sentence)
        if normalized_sentence in seen_sentences:
            continue
        seen_sentences.add(normalized_sentence)
        selected.append(f"Page {page_number}: {sentence}")
        if len(selected) >= sentence_limit:
            break

    return _trim_excerpt(" ".join(selected), limit=1400)


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
    _, topic_queries, topic_matches = _search_gabbe_topic_matches(topic_entry["topic"])
    candidate_ranges = topic_entry.get("candidate_ranges") or []
    sources = []
    excerpts = []

    for index, candidate_range in enumerate(candidate_ranges[:max_ranges], start=1):
        page_start = candidate_range.get("page_start")
        page_end = candidate_range.get("page_end")
        if not page_start or not page_end:
            continue
        range_matches = _matches_within_range(topic_matches, page_start, page_end)
        excerpt_text = _build_curated_range_excerpt(
            topic_entry["topic"],
            topic_queries or GABBE_TOPIC_QUERIES.get(topic_entry["topic"], []),
            page_map,
            page_start,
            page_end,
            sentence_limit=3,
        )
        if not excerpt_text:
            excerpt_text = _format_match_snippets(topic_entry["topic"], range_matches, limit=2)
        if not excerpt_text:
            excerpt_text = _build_range_excerpt(page_map, page_start, min(page_end, page_start + 2))
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
                "queries": topic_queries,
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


def build_textbook_overload_fallback_reply(textbook_context):
    excerpts = textbook_context.get("excerpts") or []
    lines = [
        f"According to {textbook_context['book_title']}, I couldn't generate a full synthesized textbook answer right now because the model is temporarily overloaded.",
    ]

    if excerpts:
        lines.append("What the indexed textbook excerpts most clearly support:")
        for excerpt in excerpts[:3]:
            lines.append(
                f"- [{excerpt['source_id']}] Pages {excerpt['page_start']}-{excerpt['page_end']}: {excerpt['text']}"
            )

    lines.append("This is a direct excerpt-based fallback rather than a polished textbook synthesis.")
    return "\n".join(lines)
