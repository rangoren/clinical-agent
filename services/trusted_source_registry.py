import re


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
        "wikirefua.org.il",
        "health.gov.il",
        "me.health.gov.il",
        "iscpc.org.il",
        "isuog.ima.org.il",
    ],
    "USA": [
        "acog.org",
        "asccp.org",
        "smfm.org",
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


def get_candidate_domains(user_message, user_profile=None):
    country = get_active_country(user_message, user_profile=user_profile)
    specialty_tags = infer_specialty_tags(user_message, user_profile=user_profile)

    domains = []
    if country in COUNTRY_SOURCE_DOMAINS:
        domains.extend(COUNTRY_SOURCE_DOMAINS[country])

    for specialty in specialty_tags:
        domains.extend(SPECIALTY_DOMAIN_GROUPS.get(specialty, []))

    domains.extend(GLOBAL_CORE_DOMAINS)

    ordered = []
    seen = set()
    for domain in domains:
        if domain in seen:
            continue
        seen.add(domain)
        ordered.append(domain)
    return ordered


def get_country_domains(country):
    return COUNTRY_SOURCE_DOMAINS.get(normalize_country_name(country), [])
