import re
from urllib.parse import urlparse


COUNTRY_ALIASES = {
    "israel": "Israel",
    "ישראל": "Israel",
    "usa": "USA",
    "us": "USA",
    "united states": "USA",
    "america": "USA",
    "uk": "UK",
    "united kingdom": "UK",
    "england": "UK",
    "britain": "UK",
    "canada": "Canada",
    "australia": "Australia",
    "new zealand": "New Zealand",
    "germany": "Germany",
    "france": "France",
    "italy": "Italy",
    "spain": "Spain",
    "india": "India",
}

COUNTRY_SOURCE_DOMAINS = {
    "Israel": [
        "obgyn.org.il",
        "ima.org.il",
        "health.gov.il",
        "me.health.gov.il",
        "iscpc.org.il",
        "isuog.ima.org.il",
    ],
    "USA": [
        "acog.org",
        "asccp.org",
        "smfm.org",
        "publications.smfm.org",
        "cdc.gov",
        "uspreventiveservicestaskforce.org",
        "nih.gov",
    ],
    "UK": [
        "rcog.org.uk",
        "nice.org.uk",
        "nhs.uk",
        "gov.uk",
    ],
    "Canada": [
        "sogc.org",
        "canada.ca",
    ],
    "Australia": [
        "ranzcog.edu.au",
        "health.gov.au",
    ],
    "New Zealand": [
        "ranzcog.edu.au",
        "health.govt.nz",
        "tewhatuora.govt.nz",
    ],
    "Germany": [
        "dggg.de",
        "awmf.org",
    ],
    "France": [
        "cngof.fr",
        "has-sante.fr",
    ],
    "Italy": [
        "sigo.it",
        "salute.gov.it",
    ],
    "Spain": [
        "sego.es",
        "sanidad.gob.es",
    ],
    "India": [
        "fogsi.org",
        "mohfw.gov.in",
    ],
}

GLOBAL_CORE_DOMAINS = [
    "who.int",
    "figo.org",
    "isuog.org",
]

TIER1_REGULATOR_DOMAINS = {
    "obgyn.org.il",
    "ima.org.il",
    "health.gov.il",
    "me.health.gov.il",
    "iscpc.org.il",
    "isuog.ima.org.il",
    "acog.org",
    "asccp.org",
    "smfm.org",
    "publications.smfm.org",
    "cdc.gov",
    "uspreventiveservicestaskforce.org",
    "rcog.org.uk",
    "nice.org.uk",
    "nhs.uk",
    "gov.uk",
    "sogc.org",
    "ranzcog.edu.au",
    "dggg.de",
    "awmf.org",
    "cngof.fr",
    "has-sante.fr",
    "sigo.it",
    "salute.gov.it",
    "sego.es",
    "sanidad.gob.es",
    "fogsi.org",
    "mohfw.gov.in",
    "who.int",
    "figo.org",
    "isuog.org",
    "eshre.eu",
    "asrm.org",
    "esgo.org",
    "sgo.org",
    "imsociety.org",
    "fsrh.org",
}

TIER2_SYNTHESIS_DOMAINS = {
    "wikirefua.org.il",
}

TIER3_DRUG_DOMAINS = {
    "ncbi.nlm.nih.gov",
    "dailymed.nlm.nih.gov",
    "fda.gov",
}

TIER4_LITERATURE_DOMAINS = {
    "pubmed.ncbi.nlm.nih.gov",
    "cochrane.org",
    "cochranelibrary.com",
}

TIER45_CONTROLLED_EXPANSION_DOMAINS = {
    "societyfp.org",
}

LOCAL_OPERATIONAL_DOMAINS = {
    "clalit.co.il",
    "maccabi4u.co.il",
    "leumit.co.il",
    "meuhedet.co.il",
}

SPECIALTY_DOMAIN_GROUPS = {
    "general_obgyn": [
        "acog.org",
        "rcog.org.uk",
        "sogc.org",
        "ranzcog.edu.au",
        "figo.org",
    ],
    "cervical_screening": [
        "asccp.org",
        "iscpc.org.il",
        "cdc.gov",
        "acog.org",
    ],
    "maternal_fetal_medicine": [
        "smfm.org",
        "isuog.org",
        "isuog.ima.org.il",
        "acog.org",
    ],
    "ultrasound": [
        "isuog.org",
        "isuog.ima.org.il",
        "rcog.org.uk",
    ],
    "fertility": [
        "eshre.eu",
        "asrm.org",
        "acog.org",
    ],
    "gynecologic_oncology": [
        "esgo.org",
        "sgo.org",
        "acog.org",
    ],
    "menopause": [
        "imsociety.org",
        "acog.org",
        "rcog.org.uk",
    ],
    "contraception": [
        "cdc.gov",
        "who.int",
        "acog.org",
        "fsrh.org",
    ],
    "infectious_disease": [
        "cdc.gov",
        "who.int",
        "nice.org.uk",
    ],
}

SPECIALTY_KEYWORDS = {
    "cervical_screening": [
        "pap",
        "hpv",
        "ascus",
        "lsil",
        "hsil",
        "colposcopy",
        "cervical",
        "cin",
    ],
    "maternal_fetal_medicine": [
        "preeclampsia",
        "pregnancy",
        "fetal",
        "placenta",
        "preterm",
        "growth restriction",
        "gdm",
        "gestational",
    ],
    "ultrasound": [
        "ultrasound",
        "scan",
        "tvus",
        "nt",
        "anomaly",
        "doppler",
        "echocardiography",
    ],
    "fertility": [
        "ivf",
        "fertility",
        "infertility",
        "ovulation",
        "egg reserve",
        "amh",
        "iui",
    ],
    "gynecologic_oncology": [
        "ovarian cancer",
        "endometrial",
        "cervical cancer",
        "cancer",
        "oncology",
        "brca",
    ],
    "menopause": [
        "menopause",
        "perimenopause",
        "hrt",
        "hot flashes",
    ],
    "contraception": [
        "iud",
        "contraception",
        "pill",
        "cocp",
        "implant",
        "emergency contraception",
    ],
    "infectious_disease": [
        "sti",
        "pid",
        "bv",
        "trich",
        "candida",
        "infection",
    ],
}

DRUG_SAFETY_KEYWORDS = [
    "lactation",
    "breastfeeding",
    "breast feeding",
    "breast milk",
    "drug",
    "medication",
    "medicine",
    "dose",
    "dosing",
    "interaction",
    "interactions",
    "interact",
    "teratogenic",
    "teratogen",
    "safe in pregnancy",
    "safe while breastfeeding",
    "can i take",
    "is it safe to take",
    "הנקה",
    "מניקה",
    "תרופה",
    "תרופות",
    "אינטראקציה",
    "אינטראקציות",
    "מינון",
    "בטוח בהריון",
    "בטוח בהנקה",
    "מותר לקחת",
]

LOCAL_OPERATIONAL_KEYWORDS = [
    "entitlement",
    "eligibility",
    "covered",
    "coverage",
    "available through",
    "how do i get",
    "where can i book",
    "where do i book",
    "how to access",
    "service workflow",
    "patient guidance",
    "test access",
    "booking",
    "appointment",
    "referral",
    "how do i do the test",
    "where do i do the test",
    "זכאות",
    "הפניה",
    "איפה עושים",
    "איך קובעים",
    "איך מזמינים",
    "איך ניגשים",
    "איך מקבלים",
    "איך עושים את הבדיקה",
    "קופת חולים",
]

HIGH_RISK_EXPANSION_KEYWORDS = [
    "medication in pregnancy",
    "medication in lactation",
    "lactation",
    "breastfeeding",
    "bleeding in pregnancy",
    "ectopic",
    "pregnancy of unknown location",
    "hypertensive disorders",
    "preeclampsia",
    "labor",
    "rupture of membranes",
    "prom",
    "pprom",
    "abnormal cervical screening",
    "ascus",
    "lsil",
    "hsil",
    "gynecologic oncology",
    "ovarian cancer",
    "fertility treatment",
    "ivf protocol",
    "הנקה",
    "תרופה",
    "דימום בהריון",
    "הריון חוץ רחמי",
    "רעלת",
    "ירידת מים",
    "לידה",
    "קולפוסקופיה",
    "אונקולוגיה גינקולוגית",
    "טיפולי פוריות",
]

PROFILE_SUBSPECIALTY_MAP = {
    "Maternal-Fetal Medicine": "maternal_fetal_medicine",
    "Obstetric and Gynecologic Ultrasound": "ultrasound",
    "Reproductive Endocrinology and Infertility": "fertility",
    "Gynecologic Oncology": "gynecologic_oncology",
    "General OB-GYN": "general_obgyn",
    "Urogynecology": "general_obgyn",
}


def normalize_text(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def normalize_country_name(country):
    normalized = normalize_text(country)
    return COUNTRY_ALIASES.get(normalized) or country or None


def infer_country_from_message(user_message):
    normalized = normalize_text(user_message)
    for alias, country in COUNTRY_ALIASES.items():
        if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", normalized):
            return country
    return None


def get_active_country(user_message, user_profile=None):
    message_country = infer_country_from_message(user_message)
    if message_country:
        return message_country
    profile_country = normalize_country_name((user_profile or {}).get("country"))
    return profile_country


def infer_specialty_tags(user_message, user_profile=None):
    normalized = normalize_text(user_message)
    tags = []

    for specialty, keywords in SPECIALTY_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            tags.append(specialty)

    profile_subspecialty = (user_profile or {}).get("subspecialty")
    mapped = PROFILE_SUBSPECIALTY_MAP.get(profile_subspecialty)
    if mapped and mapped not in tags:
        tags.append(mapped)

    if not tags:
        tags.append("general_obgyn")

    return tags


def infer_question_route(user_message):
    normalized = normalize_text(user_message)
    if any(keyword in normalized for keyword in DRUG_SAFETY_KEYWORDS):
        return "drug_safety"
    if any(keyword in normalized for keyword in LOCAL_OPERATIONAL_KEYWORDS):
        return "local_operational"
    return "general"


def is_high_risk_expansion_topic(user_message):
    normalized = normalize_text(user_message)
    return any(keyword in normalized for keyword in HIGH_RISK_EXPANSION_KEYWORDS)


def _dedupe_domains(domains):
    ordered = []
    seen = set()
    for domain in domains:
        if domain in seen:
            continue
        seen.add(domain)
        ordered.append(domain)
    return ordered


def get_domain_tier(domain):
    normalized = (domain or "").lower().replace("www.", "")
    if normalized in TIER3_DRUG_DOMAINS:
        return "tier3"
    if normalized in TIER2_SYNTHESIS_DOMAINS:
        return "tier2"
    if normalized in TIER4_LITERATURE_DOMAINS:
        return "tier4"
    if normalized in TIER45_CONTROLLED_EXPANSION_DOMAINS:
        return "tier45"
    if normalized in LOCAL_OPERATIONAL_DOMAINS:
        return "operational"
    if normalized in TIER1_REGULATOR_DOMAINS:
        return "tier1"
    return "tier1"


def _domains_for_country(country, tier=None):
    domains = COUNTRY_SOURCE_DOMAINS.get(country, [])
    if not tier:
        return domains
    return [domain for domain in domains if get_domain_tier(domain) == tier]


def _specialty_domains_for_tier(tags, tier):
    domains = []
    for specialty in tags:
        domains.extend(SPECIALTY_DOMAIN_GROUPS.get(specialty, []))
    return [domain for domain in _dedupe_domains(domains) if get_domain_tier(domain) == tier]


def _global_domains_for_tier(tags, tier):
    domains = _specialty_domains_for_tier(tags, tier)
    if tier == "tier1":
        domains.extend(domain for domain in GLOBAL_CORE_DOMAINS if get_domain_tier(domain) == "tier1")
    if tier == "tier3":
        domains.extend(sorted(TIER3_DRUG_DOMAINS))
    if tier == "tier4":
        domains.extend(sorted(TIER4_LITERATURE_DOMAINS))
    if tier == "tier45":
        domains.extend(sorted(TIER45_CONTROLLED_EXPANSION_DOMAINS))
    return _dedupe_domains(domains)


def build_search_stages(user_message, user_profile=None):
    country = get_active_country(user_message, user_profile=user_profile)
    specialty_tags = infer_specialty_tags(user_message, user_profile=user_profile)
    route = infer_question_route(user_message)
    high_risk = is_high_risk_expansion_topic(user_message)
    stages = []

    if route == "local_operational" and country == "Israel":
        stages.append(
            {
                "name": "israel_operational",
                "tier": "operational",
                "domains": sorted(LOCAL_OPERATIONAL_DOMAINS),
                "stop_if_found": True,
            }
        )
        return stages

    if country == "Israel":
        local_tier1 = _domains_for_country("Israel", "tier1")
        local_tier2 = _domains_for_country("Israel", "tier2")
        if local_tier1:
            stages.append({"name": "israel_tier1", "tier": "tier1", "domains": local_tier1, "stop_if_found": True})
        if local_tier2:
            stages.append({"name": "israel_tier2", "tier": "tier2", "domains": local_tier2, "stop_if_found": True})
    elif country:
        local_tier1 = _domains_for_country(country, "tier1")
        local_tier2 = _domains_for_country(country, "tier2")
        if local_tier1:
            stages.append({"name": "country_tier1", "tier": "tier1", "domains": local_tier1, "stop_if_found": True})
        if local_tier2:
            stages.append({"name": "country_tier2", "tier": "tier2", "domains": local_tier2, "stop_if_found": True})

    if route == "drug_safety":
        local_tier3 = _domains_for_country(country, "tier3") if country else []
        global_tier3 = _global_domains_for_tier(specialty_tags, "tier3")
        if local_tier3:
            stages.append({"name": "local_drug", "tier": "tier3", "domains": local_tier3, "stop_if_found": True})
        if global_tier3:
            stages.append({"name": "global_drug", "tier": "tier3", "domains": global_tier3, "stop_if_found": True})

    global_tier1 = _global_domains_for_tier(specialty_tags, "tier1")
    global_tier2 = _global_domains_for_tier(specialty_tags, "tier2")
    global_tier4 = _global_domains_for_tier(specialty_tags, "tier4")

    if global_tier1:
        stages.append({"name": "global_tier1", "tier": "tier1", "domains": global_tier1, "stop_if_found": True})
    if global_tier2:
        stages.append({"name": "global_tier2", "tier": "tier2", "domains": global_tier2, "stop_if_found": True})
    if global_tier4:
        stages.append({"name": "global_tier4", "tier": "tier4", "domains": global_tier4, "stop_if_found": True})
    if not high_risk:
        global_tier45 = _global_domains_for_tier(specialty_tags, "tier45")
        if global_tier45:
            stages.append({"name": "global_tier45", "tier": "tier45", "domains": global_tier45, "stop_if_found": True})

    deduped_stages = []
    seen_stage_domains = set()
    for stage in stages:
        filtered = [domain for domain in _dedupe_domains(stage["domains"]) if domain not in seen_stage_domains]
        if not filtered:
            continue
        seen_stage_domains.update(filtered)
        deduped_stages.append({**stage, "domains": filtered})
    return deduped_stages


def get_candidate_domains(user_message, user_profile=None):
    domains = []
    for stage in build_search_stages(user_message, user_profile=user_profile):
        domains.extend(stage["domains"])
    return _dedupe_domains(domains)


def get_source_domain(url):
    return urlparse(url).netloc.lower().replace("www.", "")


def get_country_domains(country):
    return COUNTRY_SOURCE_DOMAINS.get(normalize_country_name(country), [])
