import io
import re
from functools import lru_cache

from services.book_storage_service import get_book_object, get_r2_client
from services.textbook_cache_service import append_textbook_page_cache, get_textbook_cache
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
    {"topic": "chronic hypertension in pregnancy", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "common overlap with medication choice and surveillance"},
    {"topic": "superimposed preeclampsia", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "high-risk overlap that changes management quickly"},
    {"topic": "cervical insufficiency", "domain": "obstetrics", "tier": "A", "priority": "medium", "why": "important pathway in preterm birth prevention"},
    {"topic": "cerclage", "domain": "obstetrics", "tier": "A", "priority": "medium", "why": "procedural decision tightly linked to preterm prevention"},
    {"topic": "preterm birth prevention", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "bridges history, cervical length, progesterone, and cerclage"},
    {"topic": "periviable birth", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "important counseling and steroid/magnesium threshold topic"},
    {"topic": "labor dystocia", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "common labor floor management problem"},
    {"topic": "abnormal fetal heart rate tracing", "domain": "obstetrics", "tier": "A", "priority": "high", "why": "frequent intrapartum interpretation and action topic"},
    {"topic": "breech presentation", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "delivery planning and counseling topic"},
    {"topic": "external cephalic version", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "procedural alternative to breech cesarean delivery"},
    {"topic": "oligohydramnios", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "common surveillance and timing question"},
    {"topic": "polyhydramnios", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "differential and monitoring topic"},
    {"topic": "fetal macrosomia", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "delivery planning and shoulder dystocia overlap"},
    {"topic": "rh alloimmunization", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "classic testing and monitoring topic"},
    {"topic": "postpartum endometritis", "domain": "obstetrics", "tier": "B", "priority": "medium", "why": "common postpartum infection management topic"},
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
    "chronic hypertension in pregnancy": ["chronic hypertension in pregnancy", "chronic hypertension", "antihypertensive therapy"],
    "superimposed preeclampsia": ["superimposed preeclampsia", "chronic hypertension with preeclampsia"],
    "cervical insufficiency": ["cervical insufficiency", "cervical incompetence", "short cervix"],
    "cerclage": ["cerclage", "history-indicated cerclage", "ultrasound-indicated cerclage"],
    "preterm birth prevention": ["preterm birth prevention", "progesterone", "short cervix", "prior spontaneous preterm birth"],
    "periviable birth": ["periviable birth", "periviable", "threshold of viability"],
    "labor dystocia": ["labor dystocia", "arrest of labor", "protracted labor"],
    "abnormal fetal heart rate tracing": ["category ii tracing", "category iii tracing", "fetal heart rate tracing", "late decelerations"],
    "breech presentation": ["breech presentation", "frank breech", "complete breech"],
    "external cephalic version": ["external cephalic version", "ecv"],
    "oligohydramnios": ["oligohydramnios", "amniotic fluid index", "single deepest pocket"],
    "polyhydramnios": ["polyhydramnios", "excess amniotic fluid"],
    "fetal macrosomia": ["fetal macrosomia", "large for gestational age", "estimated fetal weight"],
    "rh alloimmunization": ["rh alloimmunization", "anti-d", "middle cerebral artery doppler"],
    "postpartum endometritis": ["postpartum endometritis", "endometritis postpartum", "clindamycin gentamicin"],
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

LOW_SIGNAL_SNIPPET_MARKERS = (
    "doi.org",
    "downloaded for",
    "references",
    "summary",
    "outline",
    "abbreviations",
    "consortium",
    "systematic review",
    "trial",
    "microbiome",
)

TOPIC_SIGNAL_MARKERS = {
    "pprom": ("latency", "antibiotic", "delivery", "expectant", "rupture of membranes", "infection", "corticosteroids", "gbs prophylaxis"),
    "preterm labor": ("tocolysis", "corticosteroids", "magnesium sulfate", "delivery", "cervical change", "contractions", "cerclage", "latency"),
    "preeclampsia": ("severe features", "magnesium sulfate", "delivery", "blood pressure", "hypertensive", "proteinuria", "antihypertensive", "seizure prophylaxis"),
    "eclampsia": ("seizure", "magnesium sulfate", "delivery", "severe features"),
    "postpartum hemorrhage": ("uterine atony", "tranexamic", "uterotonic", "massive transfusion", "hemorrhage", "bleeding", "bakri", "balloon tamponade"),
    "fetal surveillance": ("biophysical profile", "nonstress test", "doppler", "monitoring"),
    "antenatal corticosteroids": ("betamethasone", "dexamethasone", "rescue course", "lung maturity", "preterm birth"),
    "magnesium neuroprotection": ("magnesium sulfate", "neuroprotection", "cerebral palsy", "imminent preterm birth"),
    "chorioamnionitis": ("intraamniotic infection", "ampicillin", "gentamicin", "delivery", "fever"),
    "group b strep": ("prophylaxis", "penicillin", "ampicillin", "colonization", "intrapartum"),
    "labor induction": ("bishop", "ripening", "oxytocin", "cervix", "induction", "prostaglandin", "balloon catheter"),
    "cesarean delivery": ("skin incision", "uterine incision", "antibiotic prophylaxis", "hemorrhage", "complication"),
    "trial of labor after cesarean": ("vbac", "uterine rupture", "candidate", "contraindication", "counseling"),
    "shoulder dystocia": ("mcroberts", "suprapubic pressure", "posterior arm", "delivery of posterior arm"),
    "operative vaginal delivery": ("vacuum", "forceps", "prerequisite", "contraindication", "station"),
    "placenta previa": ("bleeding", "ultrasound", "cesarean", "placental edge", "digital examination"),
    "placenta accreta spectrum": ("placenta accreta spectrum", "cesarean hysterectomy", "multidisciplinary", "placenta left in situ"),
    "gestational diabetes": ("screening", "glucose", "diet", "fasting", "insulin", "glyburide", "metformin", "postpartum screening"),
    "chronic hypertension in pregnancy": ("antihypertensive", "labetalol", "nifedipine", "surveillance", "delivery"),
    "superimposed preeclampsia": ("severe features", "chronic hypertension", "magnesium sulfate", "delivery"),
    "cervical insufficiency": ("short cervix", "painless dilation", "second trimester loss", "cerclage"),
    "cerclage": ("history-indicated", "ultrasound-indicated", "rescue cerclage", "short cervix"),
    "preterm birth prevention": ("progesterone", "short cervix", "prior spontaneous preterm birth", "cerclage"),
    "periviable birth": ("corticosteroids", "magnesium sulfate", "resuscitation", "counseling"),
    "labor dystocia": ("active phase", "arrest", "adequate contractions", "cesarean delivery"),
    "abnormal fetal heart rate tracing": ("late decelerations", "variable decelerations", "resuscitative measures", "category iii"),
    "breech presentation": ("external cephalic version", "frank breech", "planned cesarean"),
    "external cephalic version": ("tocolysis", "success rate", "contraindication", "breech"),
    "oligohydramnios": ("single deepest pocket", "surveillance", "delivery timing", "rupture of membranes"),
    "polyhydramnios": ("amnioreduction", "diabetes", "fetal anomaly", "preterm labor"),
    "fetal macrosomia": ("shoulder dystocia", "estimated fetal weight", "cesarean delivery", "diabetes"),
    "rh alloimmunization": ("anti-d", "middle cerebral artery", "doppler", "intrauterine transfusion"),
    "postpartum endometritis": ("fever", "clindamycin", "gentamicin", "postpartum infection"),
}

GABBE_MANUAL_TOPIC_RANGES = {
    "pprom": [
        {"page_start": 823, "page_end": 850},
        {"page_start": 800, "page_end": 813},
    ],
    "preterm labor": [
        {"page_start": 791, "page_end": 846},
        {"page_start": 720, "page_end": 735},
    ],
    "preeclampsia": [
        {"page_start": 854, "page_end": 876},
        {"page_start": 1024, "page_end": 1039},
    ],
    "postpartum hemorrhage": [
        {"page_start": 476, "page_end": 514},
        {"page_start": 567, "page_end": 579},
    ],
    "antenatal corticosteroids": [
        {"page_start": 36, "page_end": 48},
        {"page_start": 148, "page_end": 152},
    ],
    "magnesium neuroprotection": [
        {"page_start": 147, "page_end": 158},
        {"page_start": 280, "page_end": 284},
    ],
    "chorioamnionitis": [
        {"page_start": 72, "page_end": 76},
        {"page_start": 79, "page_end": 85},
        {"page_start": 90, "page_end": 98},
    ],
    "group b strep": [
        {"page_start": 90, "page_end": 100},
        {"page_start": 107, "page_end": 114},
    ],
    "fetal surveillance": [
        {"page_start": 174, "page_end": 180},
        {"page_start": 329, "page_end": 333},
        {"page_start": 416, "page_end": 421},
    ],
    "labor induction": [
        {"page_start": 71, "page_end": 75},
        {"page_start": 316, "page_end": 321},
        {"page_start": 334, "page_end": 339},
    ],
    "cesarean delivery": [
        {"page_start": 1017, "page_end": 1033},
        {"page_start": 1048, "page_end": 1060},
    ],
    "trial of labor after cesarean": [
        {"page_start": 1060, "page_end": 1078},
        {"page_start": 1086, "page_end": 1094},
    ],
    "shoulder dystocia": [
        {"page_start": 1141, "page_end": 1152},
        {"page_start": 1153, "page_end": 1159},
    ],
    "operative vaginal delivery": [
        {"page_start": 1107, "page_end": 1125},
        {"page_start": 1126, "page_end": 1137},
    ],
    "placenta previa": [
        {"page_start": 960, "page_end": 973},
        {"page_start": 974, "page_end": 983},
    ],
    "placenta accreta spectrum": [
        {"page_start": 984, "page_end": 1000},
        {"page_start": 1001, "page_end": 1016},
    ],
    "gestational diabetes": [
        {"page_start": 601, "page_end": 619},
        {"page_start": 620, "page_end": 632},
    ],
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


def _load_gabbe_reader():
    book = get_book_object("gabbe_9")
    client = get_r2_client()
    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=book["key"])
    try:
        pdf_bytes = response["Body"].read()
    finally:
        response["Body"].close()

    from pypdf import PdfReader

    return PdfReader(io.BytesIO(pdf_bytes))


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


def _search_gabbe_topic_matches(topic):
    page_cache = get_textbook_cache("gabbe_page_text") or {}
    page_payload = page_cache.get("payload") or {}
    cached_pages = page_payload.get("pages") or []
    normalized_queries = [query.lower() for query in _topic_queries(topic)]
    results = []

    for page_entry in cached_pages:
        page_number = page_entry.get("page")
        text = page_entry.get("text") or ""
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

    return page_payload, normalized_queries, results


@lru_cache(maxsize=16)
def search_gabbe_topic(topic):
    page_payload, normalized_queries, results = _search_gabbe_topic_matches(topic)

    return {
        "topic": topic,
        "queries": normalized_queries,
        "scan_window": {
            "start_page": page_payload.get("start_page", 1),
            "end_page": page_payload.get("end_page", 0),
        },
        "match_count": len(results),
        "matches": results[:12],
    }


def build_gabbe_page_text_batch(start_page=1, limit=25):
    reader = _load_gabbe_reader()
    total_pages = len(reader.pages)
    end_page = min(total_pages, start_page + limit - 1)
    extracted_pages = []

    for page_number in range(start_page, end_page + 1):
        text = _page_text(reader.pages[page_number - 1])
        extracted_pages.append(
            {
                "page": page_number,
                "text": text,
            }
        )

    return {
        "book_id": "gabbe_9",
        "start_page": start_page,
        "end_page": end_page,
        "batch_count": len(extracted_pages),
        "total_pages": total_pages,
        "pages": extracted_pages,
    }


def cache_gabbe_page_text_batch(start_page=1, limit=25):
    payload = build_gabbe_page_text_batch(start_page=start_page, limit=limit)
    cached = append_textbook_page_cache(
        "gabbe_page_text",
        payload["pages"],
        metadata={"book_id": "gabbe_9", "start_page": 1, "end_page": payload["end_page"], "total_pages": payload["total_pages"]},
    )
    return {
        "cache_updated_at": cached.get("updated_at"),
        "book_id": payload["book_id"],
        "start_page": payload["start_page"],
        "end_page": payload["end_page"],
        "batch_count": payload["batch_count"],
        "total_pages": payload["total_pages"],
        "sample_pages": [page["page"] for page in payload["pages"][:5]],
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


def _match_quality_score(topic, match):
    snippet = (match.get("snippet") or "").lower()
    score = 1

    if any(marker in snippet for marker in LOW_SIGNAL_SNIPPET_MARKERS):
        score -= 2

    query = (match.get("query") or "").lower()
    topic_markers = TOPIC_SIGNAL_MARKERS.get(topic, ())

    if query == topic.lower():
        score += 3
    elif topic.lower() in query:
        score += 2

    for marker in topic_markers:
        if marker in snippet:
            score += 2

    if "management" in snippet or "treatment" in snippet:
        score += 2

    return score


def _cluster_topic_matches(topic, matches, gap=3):
    if not matches:
        return []

    sorted_matches = sorted(matches, key=lambda item: item["page"])
    clusters = []
    current_matches = [sorted_matches[0]]
    current_start = sorted_matches[0]["page"]
    current_end = sorted_matches[0]["page"]

    for match in sorted_matches[1:]:
        page = match["page"]
        if page - current_end <= gap:
            current_matches.append(match)
            current_end = page
            continue
        clusters.append(
            {
                "page_start": current_start,
                "page_end": current_end,
                "score": sum(_match_quality_score(topic, item) for item in current_matches),
                "match_count": len(current_matches),
            }
        )
        current_matches = [match]
        current_start = page
        current_end = page

    clusters.append(
        {
            "page_start": current_start,
            "page_end": current_end,
            "score": sum(_match_quality_score(topic, item) for item in current_matches),
            "match_count": len(current_matches),
        }
    )
    return clusters


def _range_for_cluster(cluster, padding=2):
    return {
        "page_start": max(1, cluster["page_start"] - padding),
        "page_end": cluster["page_end"] + padding,
    }


def _map_single_gabbe_topic(topic_entry):
    topic = topic_entry["topic"]
    _, queries, all_matches = _search_gabbe_topic_matches(topic)
    preview_matches = all_matches[:12]
    manual_ranges = GABBE_MANUAL_TOPIC_RANGES.get(topic)
    if manual_ranges:
        return {
            **topic_entry,
            "queries": queries,
            "match_count": len(all_matches),
            "candidate_ranges": manual_ranges,
            "sample_matches": preview_matches[:3],
            "status": "mapped",
            "mapping_mode": "manual_override",
        }

    clusters = _cluster_topic_matches(topic, all_matches, gap=3)
    ranked_clusters = sorted(
        clusters,
        key=lambda cluster: (cluster["score"], cluster["match_count"], -cluster["page_start"]),
        reverse=True,
    )
    candidate_ranges = [_range_for_cluster(cluster, padding=2) for cluster in ranked_clusters[:3] if cluster["score"] > 0]

    return {
        **topic_entry,
        "queries": queries,
        "match_count": len(all_matches),
        "candidate_ranges": candidate_ranges,
        "sample_matches": preview_matches[:3],
        "status": "mapped" if candidate_ranges else "unmapped",
        "mapping_mode": "ranked_auto",
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
