from html import unescape
from datetime import datetime, timezone
import hashlib
import re
import time
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup
from bson import ObjectId

from db import feedback_logs_collection, messages_collection, search_cache_collection
from services.logging_service import log_event
from services.trusted_source_registry import build_search_stages, get_domain_tier, get_source_domain


SEARCH_URL = "https://html.duckduckgo.com/html/"
REQUEST_TIMEOUT = 1.8
SEARCH_BUDGET_SECONDS = 4.0
MAX_DOMAIN_ATTEMPTS = 8
MAX_RESULTS_PER_DOMAIN = 3
MAX_DOMAIN_ATTEMPTS_PER_STAGE = 5
MAX_EXCERPT_CHARS = 360
USER_AGENT = "Mozilla/5.0 (compatible; ClinicalAssistant/1.0; +https://example.local)"
QUERY_CACHE_TTL_SECONDS = 12 * 60 * 60
PAGE_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
FEEDBACK_BONUS_CACHE_TTL_SECONDS = 6 * 60 * 60
CURRENT_YEAR = datetime.now(timezone.utc).year
DATE_PATTERNS = (
    r"\b(20\d{2})[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])\b",
    r"\b(0[1-9]|[12]\d|3[01])[-/](0[1-9]|1[0-2])[-/](20\d{2})\b",
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+([0-3]?\d),\s*(20\d{2})\b",
    r"\b([0-3]?\d)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b",
)
RECENCY_HINT_TERMS = (
    "guideline",
    "guidelines",
    "position statement",
    "position paper",
    "practice advisory",
    "practice bulletin",
    "updated",
    "last updated",
    "screening",
    "recommendation",
)

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _utcnow():
    return datetime.now(timezone.utc)


def _cache_key(*parts):
    joined = "||".join(str(part or "") for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _load_cache(cache_type, key):
    now = _utcnow()
    try:
        doc = search_cache_collection.find_one({"cache_type": cache_type, "cache_key": key})
    except Exception:
        return None

    if not doc:
        return None

    expires_at = doc.get("expires_at")
    if expires_at and expires_at < now:
        return None

    try:
        search_cache_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"last_used_at": now}, "$inc": {"hit_count": 1}},
        )
    except Exception:
        pass
    return doc


def _save_cache(cache_type, key, payload, ttl_seconds, metadata=None):
    now = _utcnow()
    doc = {
        "cache_type": cache_type,
        "cache_key": key,
        "payload": payload,
        "metadata": metadata or {},
        "fetched_at": now,
        "last_used_at": now,
        "expires_at": datetime.fromtimestamp(now.timestamp() + ttl_seconds, tz=timezone.utc),
    }
    try:
        search_cache_collection.update_one(
            {"cache_type": cache_type, "cache_key": key},
            {"$set": doc, "$setOnInsert": {"created_at": now}, "$inc": {"write_count": 1}},
            upsert=True,
        )
    except Exception:
        pass


def _query_cache_lookup(query, domains, limit):
    key = _cache_key("query", query, ",".join(domains), limit)
    doc = _load_cache("query_results", key)
    if not doc:
        return None
    payload = doc.get("payload") or {}
    return payload.get("results")


def _query_cache_store(query, domains, limit, results):
    key = _cache_key("query", query, ",".join(domains), limit)
    _save_cache(
        "query_results",
        key,
        {"results": results},
        QUERY_CACHE_TTL_SECONDS,
        metadata={"query": query, "domains": domains, "limit": limit},
    )


def _page_cache_lookup(url, query):
    key = _cache_key("page", url)
    doc = _load_cache("page_preview", key)
    if not doc:
        return None
    payload = doc.get("payload") or {}
    cached = {
        "title": payload.get("title"),
        "excerpt": payload.get("excerpt"),
        "updated_at": _extract_result_date_from_text(payload.get("updated_at")) if payload.get("updated_at") else None,
    }
    if not cached["excerpt"]:
        return None
    return cached


def _page_cache_store(url, query, preview):
    key = _cache_key("page", url)
    payload = {
        "title": preview.get("title"),
        "excerpt": preview.get("excerpt"),
        "updated_at": _format_updated_label(preview.get("updated_at")),
    }
    _save_cache(
        "page_preview",
        key,
        payload,
        PAGE_CACHE_TTL_SECONDS,
        metadata={"url": url, "query": query},
    )


def _feedback_cache_lookup(domain):
    key = _cache_key("feedback", domain)
    doc = _load_cache("feedback_bonus", key)
    if not doc:
        return None
    return (doc.get("payload") or {}).get("bonus")


def _feedback_cache_store(domain, bonus):
    key = _cache_key("feedback", domain)
    _save_cache(
        "feedback_bonus",
        key,
        {"bonus": bonus},
        FEEDBACK_BONUS_CACHE_TTL_SECONDS,
        metadata={"domain": domain},
    )


def _domain_feedback_bonus(domain):
    cached = _feedback_cache_lookup(domain)
    if cached is not None:
        return cached

    score = 0
    try:
        feedback_docs = list(feedback_logs_collection.find().sort("created_at", -1).limit(200))
    except Exception:
        return 0

    for feedback in feedback_docs:
        used_sources = feedback.get("used_sources") or []
        if not used_sources:
            message_id = feedback.get("message_id")
            if message_id:
                try:
                    message_doc = messages_collection.find_one({"_id": ObjectId(message_id)})
                except Exception:
                    message_doc = None
                if message_doc:
                    used_sources = (message_doc.get("metadata") or {}).get("used_sources") or []
        matched = False
        for source in used_sources:
            source_domain = get_source_domain(source.get("url"))
            if source_domain == domain or source_domain.endswith(f".{domain}") or domain.endswith(f".{source_domain}"):
                matched = True
                break
        if not matched:
            continue
        score += 2 if feedback.get("direction") == "up" else -2

    bounded = max(-6, min(6, score))
    _feedback_cache_store(domain, bounded)
    return bounded

def _request(url, params=None):
    return requests.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )


def _query_terms(text):
    return [term for term in re.findall(r"\w+", (text or "").lower()) if len(term) >= 4]


def _unwrap_duckduckgo_url(url):
    if not url:
        return None

    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        target = parse_qs(parsed.query).get("uddg", [None])[0]
        return target or url
    return url


def _clean_text(text):
    return re.sub(r"\s+", " ", unescape(text or "")).strip()


def _extract_search_results(html, domain):
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for anchor in soup.select("a.result__a"):
        href = _unwrap_duckduckgo_url(anchor.get("href"))
        if not href:
            continue

        hostname = urlparse(href).netloc.lower().replace("www.", "")
        if domain not in hostname:
            continue

        title = _clean_text(anchor.get_text(" ", strip=True))
        if not title:
            continue

        snippet_node = anchor.find_parent().select_one(".result__snippet")
        snippet = _clean_text(snippet_node.get_text(" ", strip=True)) if snippet_node else ""
        results.append({"title": title, "url": href, "snippet": snippet})

    return results


def _safe_datetime(year, month=1, day=1):
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_date_match(match_text):
    text = (match_text or "").strip()
    if not text:
        return None

    iso_match = re.fullmatch(r"(20\d{2})[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])", text)
    if iso_match:
        return _safe_datetime(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))

    dmy_match = re.fullmatch(r"(0[1-9]|[12]\d|3[01])[-/](0[1-9]|1[0-2])[-/](20\d{2})", text)
    if dmy_match:
        return _safe_datetime(int(dmy_match.group(3)), int(dmy_match.group(2)), int(dmy_match.group(1)))

    mdy_match = re.fullmatch(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+([0-3]?\d),\s*(20\d{2})",
        text,
        flags=re.IGNORECASE,
    )
    if mdy_match:
        return _safe_datetime(int(mdy_match.group(3)), MONTHS[mdy_match.group(1).lower()], int(mdy_match.group(2)))

    dmy_word_match = re.fullmatch(
        r"([0-3]?\d)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})",
        text,
        flags=re.IGNORECASE,
    )
    if dmy_word_match:
        return _safe_datetime(int(dmy_word_match.group(3)), MONTHS[dmy_word_match.group(2).lower()], int(dmy_word_match.group(1)))

    return None


def _extract_result_date_from_text(text):
    normalized = _clean_text(text)
    if not normalized:
        return None

    for pattern in DATE_PATTERNS:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            parsed = _parse_date_match(match.group(0))
            if parsed:
                return parsed
    return None


def _extract_page_date(soup, text):
    selectors = [
        "meta[property='article:modified_time']",
        "meta[property='article:published_time']",
        "meta[name='last-modified']",
        "meta[name='pubdate']",
        "meta[name='publish-date']",
        "meta[name='date']",
        "time[datetime]",
    ]

    for selector in selectors:
        for node in soup.select(selector):
            value = node.get("content") or node.get("datetime") or ""
            parsed = _extract_result_date_from_text(value)
            if parsed:
                return parsed

    return _extract_result_date_from_text(text[:4000])


def _recency_bonus(updated_at, title="", snippet=""):
    if not updated_at:
        bonus = 0
    else:
        age_days = max((datetime.now(timezone.utc) - updated_at).days, 0)
        if age_days <= 365:
            bonus = 35
        elif age_days <= 2 * 365:
            bonus = 24
        elif age_days <= 4 * 365:
            bonus = 12
        else:
            bonus = 0

    combined = f"{title} {snippet}".lower()
    if any(term in combined for term in RECENCY_HINT_TERMS):
        bonus += 8
    if any(str(year) in combined for year in range(CURRENT_YEAR - 1, CURRENT_YEAR + 1)):
        bonus += 4
    return bonus


def _format_updated_label(updated_at):
    if not updated_at:
        return None
    return updated_at.strftime("%Y-%m-%d")


def _extract_page_excerpt(html, query):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    blocks = []
    for node in soup.select("main p, article p, div p, li"):
        text = _clean_text(node.get_text(" ", strip=True))
        if len(text) < 80:
            continue
        blocks.append(text)

    if not blocks:
        text = _clean_text(soup.get_text(" ", strip=True))
        return text[:MAX_EXCERPT_CHARS]

    terms = _query_terms(query)
    scored = []
    for block in blocks[:80]:
        overlap = sum(1 for term in terms if term in block.lower())
        scored.append((overlap, len(block), block))

    scored.sort(key=lambda item: (item[0], -abs(item[1] - 220)), reverse=True)
    excerpt = scored[0][2] if scored else blocks[0]
    return excerpt[:MAX_EXCERPT_CHARS]


def _fetch_result_preview(url, query):
    cached = _page_cache_lookup(url, query)
    if cached:
        return cached

    response = _request(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    title = None
    try:
        if soup.title:
            title = _clean_text(soup.title.get_text(" ", strip=True))
    except Exception:
        title = None

    updated_at = _extract_page_date(soup, response.text)
    preview = {
        "title": title,
        "excerpt": _extract_page_excerpt(response.text, query),
        "updated_at": updated_at,
    }
    _page_cache_store(url, query, preview)
    return preview


def _search_domain(query, domain):
    response = _request(SEARCH_URL, params={"q": f"site:{domain} {query}"})
    response.raise_for_status()
    results = _extract_search_results(response.text, domain)
    if not results:
        return None

    candidates = []
    for result in results[:MAX_RESULTS_PER_DOMAIN]:
        try:
            preview = _fetch_result_preview(result["url"], query)
        except Exception:
            continue

        updated_at = preview.get("updated_at") or _extract_result_date_from_text(result.get("snippet", ""))
        score = _recency_bonus(updated_at, title=preview.get("title") or result["title"], snippet=result.get("snippet", ""))
        score += len(set(_query_terms(query)) & set(_query_terms((preview.get("excerpt") or "") + " " + result.get("snippet", "")))) * 3
        tier = get_domain_tier(domain)
        if tier == "tier1":
            score += 18
        elif tier == "tier2":
            score += 10
        elif tier == "tier3":
            score += 22
        elif tier == "tier4":
            score -= 2
        elif tier == "tier45":
            score -= 6
        elif tier == "operational":
            score += 8
        score += _domain_feedback_bonus(domain)
        candidates.append(
            {
                "title": preview["title"] or result["title"],
                "url": result["url"],
                "source_type": f"trusted web result · {domain}",
                "excerpt": preview["excerpt"],
                "updated_at": _format_updated_label(updated_at),
                "score": score,
            }
        )

    if not candidates:
        return None

    candidates.sort(key=lambda item: item["score"], reverse=True)
    selected = candidates[0]
    selected.pop("score", None)
    return selected


def get_live_trusted_sources(user_message, user_profile=None, limit=3):
    stages = build_search_stages(user_message, user_profile=user_profile)
    domains = [domain for stage in stages for domain in stage["domains"]]
    cached_results = _query_cache_lookup(user_message, domains, limit)
    if cached_results:
        return cached_results[:limit]

    collected = []
    started_at = time.monotonic()
    attempted_domains = 0
    stage_results = []

    for stage in stages:
        local_results = []
        stage_attempts = 0
        for domain in stage["domains"]:
            if len(collected) >= limit:
                break
            if attempted_domains >= MAX_DOMAIN_ATTEMPTS:
                break
            if stage_attempts >= MAX_DOMAIN_ATTEMPTS_PER_STAGE:
                break
            if time.monotonic() - started_at >= SEARCH_BUDGET_SECONDS:
                break
            try:
                attempted_domains += 1
                stage_attempts += 1
                result = _search_domain(user_message, domain)
            except Exception:
                continue
            if result:
                result["tier"] = stage["tier"]
                local_results.append(result)
                collected.append(result)

        stage_results.append({"stage": stage["name"], "tier": stage["tier"], "result_count": len(local_results)})
        if local_results and stage.get("stop_if_found", True):
            break

    deduped = []
    seen_urls = set()
    for source in collected:
        if source["url"] in seen_urls:
            continue
        seen_urls.add(source["url"])
        deduped.append(source)

    final_results = deduped[:limit]
    log_event(
        "trusted_source_search_plan",
        payload={
            "query": user_message,
            "stages": stage_results,
            "result_count": len(final_results),
        },
    )
    _query_cache_store(user_message, domains, limit, final_results)
    return final_results
