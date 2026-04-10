import re


BOOK_ALIASES = {
    "gabbe_9": ("gabbe", "gabbe's", "gabe", "gabb", "gabbe obstetrics", "גאבי", "גבי", "גבה"),
    "berek_17": ("berek", "berek & novak", "berek and novak", "berk", "bereck", "novak gynecology", "ברק", "בירק"),
    "speroff_10": ("speroff", "speroff's", "sperof", "sperrof", "clinical gynecologic endocrinology", "ספרוף", "ספרופ", "ספרוב"),
}

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
    "by gabbe",
    "by speroff",
    "by berek",
    "in gabbe",
    "in speroff",
    "in berek",
    "from gabbe",
    "from speroff",
    "from berek",
    "per gabbe",
    "per speroff",
    "per berek",
    "gabbe on",
    "speroff on",
    "berek on",
    "gabbe about",
    "speroff about",
    "berek about",
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
    "from",
    "per",
    "on",
    "about",
    "לפי",
    "כתוב",
    "כותב",
    "אומר",
    "רשום",
)

CLINICAL_SHORTHAND_ALIASES = {
    "preeclampsia": ("pet", "severe pet", "pec", "severe pec", "pre e", "pre-e"),
    "postpartum hemorrhage": ("pph",),
    "gestational diabetes": ("gdm",),
    "preterm prelabor rupture of membranes": ("pprom",),
}

TOPIC_REQUEST_ALIASES = {
    "preeclampsia": ("רעלת", "רעלת הריון", "פרה אקלמפסיה", "פרה-אקלמפסיה", "pet", "severe pet", "pec", "severe pec"),
    "pprom": ("ירידת מים מוקדמת", "ירידת מים מוקדמת מוקדמת", "פקיעת קרומים מוקדמת", "פקיעת קרומים מוקדמת לפני לידה"),
    "cervical insufficiency": ("אי ספיקת צוואר הרחם", "אי ספיקה צווארית", "צוואר רחם קצר", "צוואר קצר"),
    "gestational diabetes": ("סוכרת הריון", "סכרת הריון", "gdm"),
    "postpartum hemorrhage": ("דימום לאחר לידה", "דמם לאחר לידה", "pph"),
    "labor dystocia": ("דיסטוציה", "עיכוב בלידה", "חוסר התקדמות בלידה", "ארסט בלידה"),
}


def normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def tokenize_text(value):
    normalized = normalize_text(value)
    if not normalized:
        return []
    return re.findall(r"[a-z0-9\u0590-\u05ff]+", normalized)


def normalize_textbook_prompt(user_message):
    normalized = normalize_text(user_message)
    if not normalized:
        return normalized

    resolved = f" {normalized} "
    replacements = []
    for canonical, aliases in CLINICAL_SHORTHAND_ALIASES.items():
        for alias in aliases:
            alias_normalized = normalize_text(alias)
            pattern = rf"(?<![a-z0-9\u0590-\u05ff]){re.escape(alias_normalized)}(?![a-z0-9\u0590-\u05ff])"
            if re.search(pattern, resolved):
                resolved = re.sub(pattern, canonical, resolved)
                replacements.append((alias_normalized, canonical))

    resolved = re.sub(r"\s+", " ", resolved).strip()
    return resolved


def contains_textbook_hint(normalized):
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


def detect_textbook_request_components(user_message):
    normalized = normalize_textbook_prompt(user_message)
    if not normalized:
        return None

    explicit_request = contains_textbook_hint(normalized)
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
        by_book_pattern = rf"\b(by|from|in|per)\s+{re.escape(matched_alias)}\b"
        if not re.search(by_book_pattern, normalized):
            if not re.search(rf"\b{re.escape(matched_alias)}\s+(on|about)\b", normalized):
                compact_tokens = tokenize_text(normalized)
                alias_tokens = tokenize_text(matched_alias)
                remaining_tokens = [token for token in compact_tokens if token not in alias_tokens]
                telegraphic_request = (
                    len(compact_tokens) <= 6
                    and len(remaining_tokens) >= 1
                    and any(len(token) >= 3 for token in remaining_tokens)
                )
                if not telegraphic_request:
                    return None

    return {
        "normalized_message": normalized,
        "book_id": matched_book_id,
        "matched_alias": matched_alias,
        "explicit_request": explicit_request,
    }
