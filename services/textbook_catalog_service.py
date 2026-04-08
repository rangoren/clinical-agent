import io
import re
from functools import lru_cache

from services.book_storage_service import get_book_object, get_r2_client
from settings import R2_BUCKET_NAME


GABBE_TOPIC_MAP = [
    {"topic": "preeclampsia", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "core board topic and management pivot"},
    {"topic": "gestational hypertension", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "common overlap with severe-feature triage"},
    {"topic": "eclampsia", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "classic emergency topic linked to seizure and magnesium"},
    {"topic": "preterm labor", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "tocolysis, steroids, magnesium, transfer decisions"},
    {"topic": "pprom", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "high-yield board management and admission logic"},
    {"topic": "antenatal corticosteroids", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "frequently paired with preterm labor decisions"},
    {"topic": "magnesium neuroprotection", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "common overlap with preterm labor and delivery timing"},
    {"topic": "chorioamnionitis", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "important trigger that changes management now"},
    {"topic": "group b strep", "domain": "obstetrics", "tier": "A", "priority": "medium", "why": "common intrapartum antibiotic pathway"},
    {"topic": "fetal growth restriction", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "surveillance and timing of delivery"},
    {"topic": "fetal surveillance", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "NST/BPP/CTG interpretation and action"},
    {"topic": "labor induction", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "Bishop score and ripening decisions"},
    {"topic": "cesarean delivery", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "core intrapartum decision and complication source"},
    {"topic": "trial of labor after cesarean", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "counseling and contraindications"},
    {"topic": "shoulder dystocia", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "classic emergency sequence topic"},
    {"topic": "operative vaginal delivery", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "indications, contraindications, and traps"},
    {"topic": "postpartum hemorrhage", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "acute management sequence"},
    {"topic": "placenta previa", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "bleeding triage and delivery planning"},
    {"topic": "placenta accreta spectrum", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "critical obstetric planning topic"},
    {"topic": "postpartum hypertension", "domain": "obstetrics", "tier": "A", "priority": "medium", "why": "real-world overlap and follow-up issue"},
    {"topic": "gestational diabetes", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "common exam and clinic topic"},
    {"topic": "multiple gestation", "domain": "obstetrics", "tier": "A", "priority": "medium", "why": "timing, surveillance, and complications"},
    {"topic": "twin pregnancy", "domain": "obstetrics", "tier": "A", "priority": "medium", "why": "high-yield subtopic of multiple gestation"},
    {"topic": "postterm pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "timing and surveillance decisions"},
    {"topic": "amniotic fluid abnormalities", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "oligohydramnios and polyhydramnios pathways"},
    {"topic": "placental abruption", "domain": "obstetrics", "tier": "B", "priority": "high", "why": "important acute bleeding differential"},
    {"topic": "ectopic pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "core emergency pregnancy topic"},
    {"topic": "early pregnancy loss", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "common triage and counseling topic"},
    {"topic": "nausea and vomiting of pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "common practical management topic"},
    {"topic": "anemia in pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "common overlap and outpatient issue"},
    {"topic": "thyroid disease in pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "frequent medical overlap"},
    {"topic": "asthma in pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "medical overlap with pregnancy-specific management"},
    {"topic": "venous thromboembolism in pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "important overlap with anticoagulation decisions"},
    {"topic": "cardiac disease in pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "high-stakes maternal medicine overlap"},
    {"topic": "intrahepatic cholestasis of pregnancy", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "common board-style timing question"},
    {"topic": "vaccination in pregnancy", "domain": "obstetrics", "tier": "B", "priority": "low", "why": "high-yield preventive counseling topic"},
    {"topic": "stillbirth evaluation", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "important workup and counseling topic"},
    {"topic": "breastfeeding and lactation", "domain": "obstetrics", "tier": "B", "priority": "low", "why": "postpartum counseling and overlap topic"},
]

CHAPTER_TITLE_RE = re.compile(r"^\s*(\d{1,3})\s*[.\-]?\s+([A-Z][A-Za-z0-9,\-:;/()' ]{6,120})\s*$")
SHORT_ALL_CAPS_RE = re.compile(r"^[A-Z][A-Z0-9,\-:;/()' ]{6,90}$")

TEXT_SCAN_START_PAGE = 1
TEXT_SCAN_END_PAGE = 250
TOPIC_SEARCH_SCAN_END_PAGE = 900

GABBE_TOPIC_QUERIES = {
    "preeclampsia": ["preeclampsia", "pre-eclampsia", "severe features"],
    "gestational hypertension": ["gestational hypertension", "hypertension in pregnancy"],
    "eclampsia": ["eclampsia", "seizure in pregnancy"],
    "preterm labor": ["preterm labor", "preterm labour", "tocolysis"],
    "pprom": ["pprom", "prelabor rupture of membranes", "preterm premature rupture"],
    "antenatal corticosteroids": ["antenatal corticosteroids", "betamethasone", "dexamethasone"],
    "magnesium neuroprotection": ["magnesium sulfate for neuroprotection", "fetal neuroprotection", "magnesium sulfate"],
    "chorioamnionitis": ["chorioamnionitis", "intraamniotic infection"],
    "group b strep": ["group b streptococcus", "group b strep", "gbs"],
    "fetal growth restriction": ["fetal growth restriction", "growth restriction", "fgr"],
    "fetal surveillance": ["fetal surveillance", "biophysical profile", "nonstress test", "ctg"],
    "labor induction": ["labor induction", "labour induction", "cervical ripening"],
    "cesarean delivery": ["cesarean delivery", "caesarean delivery", "cesarean section"],
    "trial of labor after cesarean": ["trial of labor after cesarean", "tOLAC", "VBAC", "vaginal birth after cesarean"],
    "shoulder dystocia": ["shoulder dystocia"],
    "operative vaginal delivery": ["operative vaginal delivery", "vacuum extraction", "forceps delivery"],
    "postpartum hemorrhage": ["postpartum hemorrhage", "pph", "uterine atony"],
    "placenta previa": ["placenta previa"],
    "placenta accreta spectrum": ["placenta accreta", "accreta spectrum"],
    "postpartum hypertension": ["postpartum hypertension", "postpartum preeclampsia", "postpartum gestational hypertension"],
    "multiple gestation": ["multiple gestation", "twin pregnancy", "triplet pregnancy"],
    "twin pregnancy": ["twin pregnancy", "twin gestation", "monochorionic twins"],
    "gestational diabetes": ["gestational diabetes", "gdm"],
    "postterm pregnancy": ["postterm pregnancy", "post-term pregnancy", "late-term pregnancy"],
    "amniotic fluid abnormalities": ["oligohydramnios", "polyhydramnios", "amniotic fluid"],
    "placental abruption": ["placental abruption", "abruption placentae"],
    "ectopic pregnancy": ["ectopic pregnancy", "tubal pregnancy"],
    "early pregnancy loss": ["early pregnancy loss", "miscarriage"],
    "nausea and vomiting of pregnancy": ["nausea and vomiting of pregnancy", "hyperemesis gravidarum"],
    "anemia in pregnancy": ["anemia in pregnancy", "iron deficiency anemia"],
    "thyroid disease in pregnancy": ["thyroid disease in pregnancy", "hypothyroidism", "hyperthyroidism"],
    "asthma in pregnancy": ["asthma in pregnancy", "asthma exacerbation"],
    "venous thromboembolism in pregnancy": ["venous thromboembolism", "vte", "deep vein thrombosis", "pulmonary embolism"],
    "cardiac disease in pregnancy": ["cardiac disease in pregnancy", "heart disease in pregnancy"],
    "intrahepatic cholestasis of pregnancy": ["intrahepatic cholestasis of pregnancy", "cholestasis of pregnancy"],
    "vaccination in pregnancy": ["vaccination in pregnancy", "influenza vaccine", "tdap"],
    "stillbirth evaluation": ["stillbirth evaluation", "intrauterine fetal demise", "fetal death"],
    "breastfeeding and lactation": ["breastfeeding", "lactation", "mastitis"],
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


def _cluster_match_pages(matches, gap=3):
    pages = sorted({match["page"] for match in matches})
    if not pages:
        return []

    clusters = []
    current_start = pages[0]
    current_end = pages[0]

    for page in pages[1:]:
        if page - current_end <= gap:
            current_end = page
            continue
        clusters.append({"page_start": current_start, "page_end": current_end})
        current_start = page
        current_end = page

    clusters.append({"page_start": current_start, "page_end": current_end})
    return clusters


def _range_for_cluster(cluster, padding=2):
    return {
        "page_start": max(1, cluster["page_start"] - padding),
        "page_end": cluster["page_end"] + padding,
    }


def _map_single_gabbe_topic(topic_entry):
    topic = topic_entry["topic"]
    search_payload = search_gabbe_topic(topic)
    matches = search_payload["matches"]
    clusters = _cluster_match_pages(matches, gap=3)
    candidate_ranges = [_range_for_cluster(cluster, padding=2) for cluster in clusters[:3]]

    return {
        **topic_entry,
        "queries": search_payload["queries"],
        "match_count": search_payload["match_count"],
        "candidate_ranges": candidate_ranges,
        "sample_matches": matches[:3],
        "status": "mapped" if candidate_ranges else "unmapped",
    }


@lru_cache(maxsize=8)
def build_gabbe_topic_mapping_batch(offset=0, limit=5, tier=None):
    topic_entries = list(GABBE_TOPIC_MAP)
    if tier:
        topic_entries = [entry for entry in topic_entries if entry.get("tier") == tier]

    selected_topics = topic_entries[offset : offset + limit]
    mappings = []
    for topic_entry in selected_topics:
        mappings.append(_map_single_gabbe_topic(topic_entry))

    return {
        "book_id": "gabbe_9",
        "offset": offset,
        "limit": limit,
        "tier": tier,
        "batch_count": len(mappings),
        "total_available_topics": len(topic_entries),
        "topics": mappings,
    }


@lru_cache(maxsize=1)
def build_gabbe_topic_mapping():
    mappings = []
    for topic_entry in GABBE_TOPIC_MAP:
        mappings.append(_map_single_gabbe_topic(topic_entry))

    return {
        "book_id": "gabbe_9",
        "topic_count": len(mappings),
        "mapped_count": sum(1 for item in mappings if item["status"] == "mapped"),
        "unmapped_count": sum(1 for item in mappings if item["status"] != "mapped"),
        "topics": mappings,
    }


def build_gabbe_topic_mapping_summary(payload):
    topics = payload.get("topics") or []
    return {
        "book_id": payload.get("book_id", "gabbe_9"),
        "topic_count": payload.get("topic_count", len(topics)),
        "mapped_count": payload.get("mapped_count", sum(1 for item in topics if item.get("status") == "mapped")),
        "unmapped_count": payload.get("unmapped_count", sum(1 for item in topics if item.get("status") != "mapped")),
        "tier_a_count": sum(1 for item in topics if item.get("tier") == "A"),
        "tier_b_count": sum(1 for item in topics if item.get("tier") == "B"),
        "preview": topics[:12],
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
