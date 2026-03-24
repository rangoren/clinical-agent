from html import unescape
import re
import time
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from services.trusted_source_registry import get_candidate_domains


SEARCH_URL = "https://html.duckduckgo.com/html/"
REQUEST_TIMEOUT = 1.8
SEARCH_BUDGET_SECONDS = 4.0
MAX_DOMAIN_ATTEMPTS = 3
MAX_EXCERPT_CHARS = 360
USER_AGENT = "Mozilla/5.0 (compatible; ClinicalAssistant/1.0; +https://example.local)"

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

        results.append({"title": title, "url": href})

    return results


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
    response = _request(url)
    response.raise_for_status()
    title = None
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        if soup.title:
            title = _clean_text(soup.title.get_text(" ", strip=True))
    except Exception:
        title = None

    return {
        "title": title,
        "excerpt": _extract_page_excerpt(response.text, query),
    }


def _search_domain(query, domain):
    response = _request(SEARCH_URL, params={"q": f"site:{domain} {query}"})
    response.raise_for_status()
    results = _extract_search_results(response.text, domain)
    if not results:
        return None

    selected = results[0]
    preview = _fetch_result_preview(selected["url"], query)

    return {
        "title": preview["title"] or selected["title"],
        "url": selected["url"],
        "source_type": f"trusted web result · {domain}",
        "excerpt": preview["excerpt"],
    }


def get_live_trusted_sources(user_message, user_profile=None, limit=3):
    collected = []
    started_at = time.monotonic()
    attempted_domains = 0

    for domain in get_candidate_domains(user_message, user_profile=user_profile):
        if len(collected) >= limit:
            break
        if attempted_domains >= MAX_DOMAIN_ATTEMPTS:
            break
        if time.monotonic() - started_at >= SEARCH_BUDGET_SECONDS:
            break

        try:
            attempted_domains += 1
            result = _search_domain(user_message, domain)
        except Exception:
            continue

        if result:
            collected.append(result)

    deduped = []
    seen_urls = set()
    for source in collected:
        if source["url"] in seen_urls:
            continue
        seen_urls.add(source["url"])
        deduped.append(source)

    return deduped[:limit]
