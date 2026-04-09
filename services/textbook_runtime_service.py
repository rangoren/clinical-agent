import re

from services.book_storage_service import get_book_object
from services.textbook_cache_service import get_textbook_cache
from services.textbook_catalog_service import (
    BOOK_TOPIC_QUERIES,
    TOPIC_SIGNAL_MARKERS,
    _search_book_topic_matches,
)


TEXTBOOK_REQUEST_HINTS = (
    "what does",
    "what do",
    "what dose",
    "what dos",
    "waht does",
    "waht dose",
    "waht do",
    "wht does",
    "what is says",
    "what it says",
    "according to",
    "acording to",
    "accoring to",
    "accordng to",
    "according too",
    "based on",
    "base on",
    "based of",
    "from the book",
    "frm the book",
    "from book",
    "from textbook",
    "what is written",
    "what is writen",
    "what is writeen",
    "what does the book say",
    "what does book say",
    "what dose the book say",
    "what dos the book say",
    "waht does the book say",
    "say about",
    "says about",
    "say abut",
    "says abot",
    "what is written in the book",
    "what is writen in the book",
    "what does the textbook say",
    "what dose the textbook say",
    "what does this book say",
    "what does the book say about",
    "what dose the book say about",
    "what do the book say about",
    "what the book says about",
    "what textbook says about",
    "what is written in",
    "what is writen in",
    "what written in",
    "what gabbe says about",
    "what speroff says about",
    "what berek says about",
    "what gabbe say about",
    "what speroff say about",
    "what berek say about",
    "gabbe says about",
    "speroff says about",
    "berek says about",
    "what gabbe says",
    "what speroff says",
    "what berek says",
    "what gabbe say",
    "what speroff say",
    "what berek say",
    "gabbe say",
    "speroff say",
    "berek say",
    "according gabbe",
    "according speroff",
    "according berek",
    "לפי",
    "עפ",
    "ע\"פ",
    "על פי",
    "מה כתוב",
    "מה כותב",
    "מה רשום",
    "מה רשום בספר",
    "מה רשום בספר על",
    "מה כתוב בספר",
    "מה כתוב בסיפר",
    "מה כתוב בספרר",
    "מה הספר אומר",
    "מה הספ ר אומר",
    "מה הסיפר אומר",
    "מה הספר רושם",
    "מה הספר כותב",
    "מה הסיפר כותב",
    "מה כתוב בספר על",
    "מה כתוב בסיפר על",
    "מה הספר אומר על",
    "מה הסיפר אומר על",
    "מה הספר כותב על",
    "מה רשום על",
    "מה כתוב בגאבי",
    "מה גאבי אומר",
    "מה גאבי כותב",
    "מה גבה אומר",
    "מה גבי אומר",
    "מה כתוב בברק",
    "מה ברק אומר",
    "מה ברק כותב",
    "מה כתוב בבירק",
    "מה בירק אומר",
    "מה כתוב בספרוף",
    "מה ספרוף אומר",
    "מה ספרוף כותב",
    "מה ספרופ אומר",
    "מה ספרוב אומר",
    "מה אומר",
    "כתוב ב",
)

BOOK_ALIASES = {
    "gabbe_9": ("gabbe", "gabbe's", "gabe", "gabb", "gabbe obstetrics", "גאבי", "גבי", "גבה"),
    "berek_17": ("berek", "berek & novak", "berek and novak", "berk", "bereck", "novak gynecology", "ברק", "בירק"),
    "speroff_10": ("speroff", "speroff's", "sperof", "sperrof", "clinical gynecologic endocrinology", "ספרוף", "ספרופ", "ספרוב"),
}

TEXTBOOK_ACTION_HINTS = (
    "say",
    "says",
    "said",
    "dose",
    "does",
    "do",
    "according",
    "acording",
    "accoring",
    "written",
    "writen",
    "write",
    "book",
    "textbook",
    "לפי",
    "כתוב",
    "כותב",
    "אומר",
    "רשום",
)


def _normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _contains_textbook_hint(normalized):
    if any(marker in normalized for marker in TEXTBOOK_REQUEST_HINTS):
        return True

    compact = re.sub(r"[^a-z0-9\u0590-\u05ff\s]", " ", normalized)
    compact = re.sub(r"\s+", " ", compact).strip()
    if not compact:
        return False

    has_book_alias = any(
        alias in compact
        for aliases in BOOK_ALIASES.values()
        for alias in aliases
    )
    has_action = any(marker in compact for marker in TEXTBOOK_ACTION_HINTS)
    if has_book_alias and has_action:
        return True

    typo_patterns = (
        r"\b(wha?t|waht|wht)\s+(does|dose|do|dos)?\s*(the\s+)?(book|textbook|gabbe|gabe|speroff|sperof|berek|berk)\s+(say|says|sey|sez)\b",
        r"\b(according|acording|accoring|accordng)\s+(to\s+)?(gabbe|gabe|speroff|sperof|berek|berk|book|textbook)\b",
        r"\b(what|waht)\s+(gabbe|gabe|speroff|sperof|berek|berk)\s+(say|says|sey|sez)\b",
        r"(מה|מה ש|לפי)\s+(הספר|הסיפר|גאבי|גבי|גבה|ברק|בירק|ספרוף|ספרופ|ספרוב).{0,16}(אומר|כותב|כתוב|רשום)",
    )
    return any(re.search(pattern, compact) for pattern in typo_patterns)


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
    "recommended",
    "indicated",
    "should",
    "administer",
    "admission",
    "inpatient",
    "outpatient",
    "prophylaxis",
)

LOW_SIGNAL_SENTENCE_MARKERS = (
    "doi.org",
    "downloaded for",
    "et al",
    "fig.",
    "figure ",
    "table ",
    "committee opinion",
    "systematic review",
    "trial group",
)

TOPIC_PRIORITY_MARKERS = {
    "preterm labor": ("tocolysis", "antenatal corticosteroids", "magnesium sulfate", "delivery", "transfer"),
    "preeclampsia": ("severe features", "magnesium sulfate", "delivery", "antihypertensive", "expectant management", "aspirin", "timing of delivery", "late preterm"),
    "postpartum hemorrhage": ("uterotonic", "tranexamic", "balloon tamponade", "massive transfusion", "hysterectomy"),
    "labor induction": ("bishop", "cervical ripening", "oxytocin", "prostaglandin", "balloon catheter"),
    "cesarean delivery": ("prophylaxis", "skin incision", "uterine incision", "hemorrhage"),
    "trial of labor after cesarean": ("candidate", "contraindication", "uterine rupture", "counseling"),
    "shoulder dystocia": ("mcroberts", "suprapubic pressure", "posterior arm", "woods"),
    "operative vaginal delivery": ("forceps", "vacuum", "prerequisite", "contraindication"),
    "placenta previa": ("digital examination", "bleeding", "cesarean delivery", "placental edge"),
    "placenta accreta spectrum": ("multidisciplinary", "cesarean hysterectomy", "left in situ"),
    "gestational diabetes": ("diet", "insulin", "metformin", "postpartum screening"),
    "chronic hypertension in pregnancy": ("labetalol", "nifedipine", "surveillance", "delivery timing"),
    "superimposed preeclampsia": ("severe features", "magnesium sulfate", "delivery", "chronic hypertension"),
    "cervical insufficiency": ("painless dilation", "second trimester", "cerclage", "short cervix"),
    "cerclage": ("history-indicated", "ultrasound-indicated", "rescue cerclage", "short cervix"),
    "preterm birth prevention": ("progesterone", "short cervix", "prior spontaneous preterm birth", "cerclage"),
    "periviable birth": ("corticosteroids", "magnesium sulfate", "counseling", "resuscitation"),
    "labor dystocia": ("active phase", "arrest", "adequate contractions", "cesarean", "first-stage arrest", "failed induction", "6 cm", "second stage"),
    "abnormal fetal heart rate tracing": ("late decelerations", "variable decelerations", "resuscitative measures", "category iii"),
    "breech presentation": ("frank breech", "external cephalic version", "planned cesarean"),
    "external cephalic version": ("tocolysis", "success", "contraindication", "breech"),
    "oligohydramnios": ("single deepest pocket", "delivery timing", "surveillance"),
    "polyhydramnios": ("amnioreduction", "fetal anomaly", "preterm labor"),
    "fetal macrosomia": ("estimated fetal weight", "shoulder dystocia", "cesarean delivery"),
    "rh alloimmunization": ("anti-d", "middle cerebral artery", "doppler", "intrauterine transfusion"),
    "postpartum endometritis": ("clindamycin", "gentamicin", "fever", "postpartum infection"),
    "amenorrhea": ("pregnancy test", "prolactin", "tsh", "fsh", "estradiol", "primary amenorrhea", "secondary amenorrhea", "breast development", "evaluation"),
}


def detect_textbook_request(user_message):
    normalized = _normalize_text(user_message)
    if not normalized:
        return None

    explicit_request = _contains_textbook_hint(normalized)
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
        "supported": matched_book_id in {"gabbe_9", "speroff_10"},
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


def _mapping_cache_key(book_id):
    if book_id == "gabbe_9":
        return "gabbe_topic_mapping"
    return f"{book_id}_topic_mapping"


def _page_cache_key(book_id):
    if book_id == "gabbe_9":
        return "gabbe_page_text"
    return f"{book_id}_page_text"


def _find_best_topic(book_id, user_message):
    mapping_doc = get_textbook_cache(_mapping_cache_key(book_id)) or {}
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

    for marker in TOPIC_PRIORITY_MARKERS.get(topic, ()):
        if marker in normalized_sentence:
            score += 4

    if any(token in normalized_sentence for token in LOW_SIGNAL_SENTENCE_MARKERS):
        score -= 5

    if re.search(r"\b\d{1,3}\.\s", normalized_sentence):
        score -= 2

    if normalized_sentence.count("(") >= 2 or normalized_sentence.count(";") >= 3:
        score -= 2

    if len(normalized_sentence) < 80:
        score -= 1

    return score


def _build_curated_range_excerpt(topic, topic_queries, topic_matches, page_map, page_start, page_end, sentence_limit=3):
    ranked_sentences = []
    matched_pages = {
        match.get("page")
        for match in _matches_within_range(topic_matches, page_start, page_end)
        if match.get("page")
    }

    for page_number in range(page_start, page_end + 1):
        text = page_map.get(page_number)
        if not text:
            continue
        for sentence in _split_into_sentences(text):
            score = _score_sentence(topic, sentence, topic_queries)
            if page_number in matched_pages:
                score += 3
            if score <= 0:
                continue
            ranked_sentences.append((score, page_number, sentence))

    if not ranked_sentences:
        return ""

    seen_sentences = set()
    page_usage = {}
    selected = []
    for _, page_number, sentence in sorted(ranked_sentences, key=lambda item: (item[0], -item[1]), reverse=True):
        normalized_sentence = _normalize_text(sentence)
        if normalized_sentence in seen_sentences:
            continue
        if page_usage.get(page_number, 0) >= 2:
            continue
        seen_sentences.add(normalized_sentence)
        page_usage[page_number] = page_usage.get(page_number, 0) + 1
        selected.append(f"Page {page_number}: {sentence}")
        if len(selected) >= sentence_limit:
            break

    return _trim_excerpt(" ".join(selected), limit=1400)


def build_textbook_context(book_id, user_message, max_ranges=3):
    topic_entry = _find_best_topic(book_id, user_message)
    if not topic_entry:
        return {
            "status": "topic_not_found",
            "message": "I couldn't confidently map this textbook request to one of the indexed topics yet.",
        }

    page_cache_doc = get_textbook_cache(_page_cache_key(book_id)) or {}
    page_payload = page_cache_doc.get("payload") or {}
    cached_pages = page_payload.get("pages") or []
    if not cached_pages:
        book = get_book_object(book_id) or {}
        return {
            "status": "page_cache_missing",
            "message": f"{book.get('title') or 'This textbook'} page cache is not available yet.",
        }

    page_map = {entry.get("page"): entry.get("text") for entry in cached_pages if entry.get("page")}
    _, topic_queries, topic_matches = _search_book_topic_matches(book_id, topic_entry["topic"])
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
            topic_queries or (BOOK_TOPIC_QUERIES.get(book_id) or {}).get(topic_entry["topic"], []),
            topic_matches,
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
                "title": get_book_object(book_id)["title"] + f", {get_book_object(book_id)['edition']}th edition",
                "url": None,
                "source_type": "Textbook excerpt",
                "source_detail": f"Topic: {topic_entry['topic']} | pp. {page_start}-{page_end}",
                "page_start": page_start,
                "page_end": page_end,
                "book_id": book_id,
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
        "book_id": book_id,
        "book_title": get_book_object(book_id)["title"],
        "edition": get_book_object(book_id)["edition"],
        "matched_topic": topic_entry["topic"],
        "topic_entry": topic_entry,
        "sources": sources,
        "excerpts": excerpts,
    }


def build_gabbe_textbook_context(user_message, max_ranges=3):
    return build_textbook_context("gabbe_9", user_message, max_ranges=max_ranges)


def build_speroff_textbook_context(user_message, max_ranges=3):
    return build_textbook_context("speroff_10", user_message, max_ranges=max_ranges)


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
