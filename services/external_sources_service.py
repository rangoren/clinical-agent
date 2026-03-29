import re

from services.live_search_service import get_live_trusted_sources
from services.source_preference_service import is_israel_relevant
from services.trusted_source_registry import build_search_stages, get_domain_tier, get_source_domain, infer_question_route


SOURCE_ROUTING_RULES = {
    "obstetric_acute": {
        "query_keywords": [
            "pprom", "prom", "rupture of membranes", "prelabor rupture", "preterm labor",
            "preeclampsia", "pre-eclampsia", "gestational hypertension", "severe features",
            "pph", "postpartum hemorrhage", "uterine atony", "ctg", "late decelerations",
            "variable decelerations", "fetal monitoring", "labor", "labour", "chorioamnionitis",
            "ירידת מים", "לידה מוקדמת", "רעלת", "דימום אחרי לידה", "מוניטור", "צירים",
        ],
        "source_keywords": [
            "pprom", "prom", "rupture of membranes", "preeclampsia", "severe features",
            "postpartum hemorrhage", "pph", "uterine atony", "ctg", "fetal monitoring",
            "labor", "labour", "group b strep", "gestational hypertension", "preterm",
        ],
        "title_keywords": [
            "preeclampsia", "rupture of membranes", "postpartum hemorrhage",
            "fetal monitoring", "labor", "group b strep",
        ],
    },
    "cervical_screening": {
        "query_keywords": [
            "pap", "hpv", "ascus", "lsil", "hsil", "colposcopy", "cin", "cervical screening",
            "בדיקת פאפ", "hpv", "קולפוסקופיה", "דיספלזיה", "צוואר הרחם",
        ],
        "source_keywords": [
            "pap", "hpv", "ascus", "lsil", "hsil", "colposcopy", "cin", "cervical",
        ],
        "title_keywords": [
            "cervical", "asccp", "hpv", "pap",
        ],
    },
    "contraception": {
        "query_keywords": [
            "contraception", "contraceptive", "ocp", "cocp", "combined pill", "combined hormonal",
            "migraine with aura", "iud", "iucd", "implant", "larc", "emergency contraception",
            "plan b", "ella", "levonorgestrel", "ulipristal", "גלולות", "מניעה", "התקן",
        ],
        "source_keywords": [
            "contraception", "contraceptive eligibility", "combined ocp", "pill", "iud", "implant",
            "larc", "emergency contraception", "levonorgestrel", "ulipristal",
        ],
        "title_keywords": [
            "contraception", "medical eligibility", "emergency contraception", "long-acting reversible",
        ],
    },
    "benign_gynecology": {
        "query_keywords": [
            "aub", "abnormal uterine bleeding", "heavy menstrual bleeding", "intermenstrual bleeding",
            "endometriosis", "dysmenorrhea", "chronic pelvic pain", "menopause", "perimenopause",
            "hot flashes", "hrt", "fibroid", "leiomyoma", "postmenopausal bleeding", "pmb",
            "דימום רחמי חריג", "אנדומטריוזיס", "מנופאוזה", "גלי חום", "דימום אחרי מנופאוזה",
        ],
        "source_keywords": [
            "abnormal uterine bleeding", "aub", "heavy periods", "endometriosis", "dysmenorrhea",
            "chronic pelvic pain", "menopause", "perimenopause", "hrt", "hot flashes",
        ],
        "title_keywords": [
            "abnormal uterine bleeding", "endometriosis", "menopause",
        ],
    },
    "infectious_gynecology": {
        "query_keywords": [
            "pid", "pelvic inflammatory disease", "cervical motion tenderness", "adnexal tenderness",
            "tubo-ovarian abscess", "bv", "bacterial vaginosis", "trich", "trichomoniasis",
            "candida", "yeast infection", "vaginal discharge", "pelvic pain", "אגן דלקתי",
            "הפרשה נרתיקית", "טריכומונס", "קנדידה",
        ],
        "source_keywords": [
            "pid", "pelvic inflammatory disease", "infection", "bv", "trichomoniasis", "candida",
            "vaginal discharge",
        ],
        "title_keywords": [
            "pid", "bacterial vaginosis", "trichomoniasis", "candidiasis",
        ],
    },
    "fertility": {
        "query_keywords": [
            "fertility", "infertility", "trying to conceive", "ttc", "ivf", "iui", "hsg",
            "amh", "ovulation induction", "semen analysis", "egg reserve", "ovarian reserve",
            "טיפולי פוריות", "אי פוריות", "פוריות",
        ],
        "source_keywords": [
            "fertility", "infertility", "ivf", "iui", "hsg", "amh", "ovarian reserve",
            "semen analysis", "ovulation induction",
        ],
        "title_keywords": [
            "fertility", "infertility", "ivf", "ovarian stimulation",
        ],
    },
    "early_pregnancy": {
        "query_keywords": [
            "ectopic", "pregnancy of unknown location", "pul", "positive pregnancy test",
            "first trimester bleeding", "spotting", "beta hcg", "hcg", "no intrauterine pregnancy",
            "no iup", "early pregnancy loss", "miscarriage", "threatened abortion",
            "הריון חוץ רחמי", "דימום בתחילת הריון", "בטא", "שק הריון",
        ],
        "source_keywords": [
            "ectopic", "pregnancy of unknown location", "pul", "beta hcg", "hcg",
            "miscarriage", "early pregnancy loss", "first trimester bleeding",
        ],
        "title_keywords": [
            "ectopic", "miscarriage", "early pregnancy loss",
        ],
    },
}

FOCUS_SUBSIGNATURES = {
    "obstetric_acute": {
        "pprom_prom": ["pprom", "prom", "rupture of membranes", "prelabor rupture", "amniotic fluid leak", "ירידת מים"],
        "preeclampsia": ["preeclampsia", "pre-eclampsia", "gestational hypertension", "severe features", "רעלת"],
        "pph": ["pph", "postpartum hemorrhage", "uterine atony", "דימום אחרי לידה"],
        "ctg": ["ctg", "late decelerations", "variable decelerations", "fetal monitoring", "מוניטור"],
        "preterm_labor": ["preterm labor", "לידה מוקדמת", "צירים"],
    },
    "cervical_screening": {
        "hsil_pathway": ["hsil", "colposcopy", "cin", "דיספלזיה", "קולפוסקופיה"],
        "hpv_screening": ["pap", "hpv", "ascus", "lsil", "cervical screening", "בדיקת פאפ", "צוואר הרחם"],
    },
    "fertility": {
        "infertility_eval": ["infertility", "trying to conceive", "ttc", "semen analysis", "hsg", "ovarian reserve", "amh", "אי פוריות", "פוריות"],
        "ivf_art": ["ivf", "iui", "ovulation induction", "ovarian stimulation", "art", "טיפולי פוריות"],
    },
    "early_pregnancy": {
        "pul_ectopic": ["pregnancy of unknown location", "pul", "ectopic", "no intrauterine pregnancy", "no iup", "beta hcg", "hcg", "positive pregnancy test", "הריון חוץ רחמי", "בטא"],
        "early_loss": ["early pregnancy loss", "miscarriage", "first trimester bleeding", "spotting", "threatened abortion", "דימום בתחילת הריון"],
    },
}


EXTERNAL_SOURCE_CATALOG = [
    {
        "title": "Clalit: Cervical Screening (HPV) in Israel",
        "url": "https://www.clalit.co.il/he/myrights/women/Pages/hpv-test.aspx",
        "source_type": "Israeli screening program",
        "keywords": ["pap smear", "pap", "pap test", "hpv", "hpv typing", "cervical screening", "cervical cancer screening"],
        "regions": ["israel"],
        "priority": 3,
    },
    {
        "title": "Maccabi: HPV Test (Formerly Pap Smear)",
        "url": "https://www.maccabi4u.co.il/eligibilites/22446/",
        "source_type": "Israeli screening program",
        "keywords": ["pap smear", "pap", "pap test", "hpv", "hpv typing", "ascus", "lsil", "hsil", "colposcopy", "cervical dysplasia", "cin"],
        "regions": ["israel"],
        "priority": 3,
    },
    {
        "title": "Leumit: Cervical Surface (HPV) Test",
        "url": "https://www.leumit.co.il/ar/womans-health/everything-you-wanted-to-know-about-the-cervical-surface-hpv-test/",
        "source_type": "Israeli screening program",
        "keywords": ["hpv", "pap smear", "pap test", "hpv typing", "cervical screening", "cervical dysplasia"],
        "regions": ["israel"],
        "priority": 2,
    },
    {
        "title": "LactMed Database",
        "url": "https://www.ncbi.nlm.nih.gov/books/NBK501922/",
        "source_type": "drug safety reference",
        "keywords": ["lactation", "breastfeeding", "breast milk", "medication", "drug", "safe in breastfeeding", "הנקה", "מניקה", "תרופה"],
        "priority": 2,
    },
    {
        "title": "FDA Pregnancy and Lactation Labeling",
        "url": "https://www.fda.gov/drugs/labeling-information-drug-products/pregnancy-and-lactation-labeling-drugs-final-rule",
        "source_type": "drug safety reference",
        "keywords": ["pregnancy medication", "lactation", "drug safety", "teratogenic", "תרופה", "בטוח בהריון", "בטוח בהנקה"],
        "priority": 1,
    },
    {
        "title": "DailyMed Drug Labeling",
        "url": "https://dailymed.nlm.nih.gov/dailymed/",
        "source_type": "drug safety reference",
        "keywords": ["drug label", "dose", "dosing", "interaction", "medication", "מינון", "אינטראקציה", "תרופה"],
        "priority": 1,
    },
    {
        "title": "USPSTF: Cervical Cancer Screening",
        "url": "https://www.uspreventiveservicestaskforce.org/uspstf/recommendation/cervical-cancer-screening",
        "source_type": "external guideline",
        "keywords": ["pap smear", "pap", "cervical screening", "hpv screening", "cervical cancer screening"],
        "priority": 0,
    },
    {
        "title": "ACOG: Cervical Cancer Screening",
        "url": "https://www.acog.org/womens-health/infographics/cervical-cancer-screening",
        "source_type": "external guideline",
        "keywords": ["pap smear", "pap", "cervical screening", "hpv screening"],
        "priority": 0,
    },
    {
        "title": "ASCCP Risk-Based Management Guidelines",
        "url": "https://www.asccp.org/guidelines",
        "source_type": "external guideline",
        "keywords": ["asccp", "pap smear", "hpv", "cervical dysplasia", "cin"],
        "priority": 0,
    },
    {
        "title": "ACOG: Preeclampsia and High Blood Pressure During Pregnancy",
        "url": "https://www.acog.org/womens-health/faqs/preeclampsia-and-high-blood-pressure-during-pregnancy",
        "source_type": "external guideline",
        "keywords": ["preeclampsia", "pre-eclampsia", "blood pressure", "magnesium", "severe features"],
        "excerpt": "Preeclampsia with severe features requires stabilization and often delivery rather than routine expectant management, depending on gestational age and maternal-fetal status.",
    },
    {
        "title": "ACOG Practice Advisory: Low-Dose Aspirin Use for the Prevention of Preeclampsia",
        "url": "https://www.acog.org/clinical/clinical-guidance/practice-advisory/articles/2021/12/low-dose-aspirin-use-for-the-prevention-of-preeclampsia-and-related-morbidity-and-mortality",
        "source_type": "external guideline",
        "keywords": ["preeclampsia prevention", "aspirin", "low-dose aspirin"],
    },
    {
        "title": "CDC STI Treatment Guidelines: PID",
        "url": "https://www.cdc.gov/std/treatment-guidelines/pid.htm",
        "source_type": "external guideline",
        "keywords": ["pid", "pelvic inflammatory disease", "infection", "fever"],
    },
    {
        "title": "NICE Guideline: Ectopic Pregnancy and Miscarriage",
        "url": "https://www.nice.org.uk/guidance/ng126",
        "source_type": "external guideline",
        "keywords": [
            "ectopic", "pregnancy of unknown location", "pul", "beta hcg", "hcg", "ultrasound", "tvus",
            "positive pregnancy test", "no intrauterine pregnancy", "no iup", "serial hcg", "repeat hcg",
            "48 hours", "spotting", "first trimester bleeding",
        ],
        "excerpt": "In a stable patient with pregnancy of unknown location, serial hCG testing and repeat ultrasound are used to distinguish early intrauterine pregnancy, failed pregnancy, and ectopic pregnancy.",
    },
    {
        "title": "ACOG: Early Pregnancy Loss",
        "url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2018/11/early-pregnancy-loss",
        "source_type": "external guideline",
        "keywords": ["miscarriage", "early pregnancy loss", "bleeding", "first trimester bleeding"],
    },
    {
        "title": "SMFM Consult Series",
        "url": "https://publications.smfm.org/publications/consult-series/",
        "source_type": "external guideline",
        "keywords": ["smfm", "maternal fetal medicine", "consult"],
    },
    {
        "title": "ASRM: Fertility Evaluation of Infertile Women",
        "url": "https://www.asrm.org/practice-guidance/practice-committee-documents/fertility-evaluation-of-infertile-women-a-committee-opinion-2021/",
        "source_type": "external guideline",
        "keywords": [
            "fertility", "infertility", "ivf", "iui", "ovulation induction", "ovarian reserve",
            "amh", "hsg", "semen analysis", "reproductive endocrinology",
        ],
        "excerpt": "Initial infertility evaluation depends on duration of infertility, age, ovulatory status, tubal assessment, and semen analysis rather than immediate IVF for every couple.",
    },
    {
        "title": "ASRM: Definition of Infertility",
        "url": "https://www.asrm.org/practice-guidance/practice-committee-documents/definition-of-infertility/",
        "source_type": "external guideline",
        "keywords": [
            "infertility", "fertility workup", "trying to conceive", "evaluation timing", "when to evaluate infertility",
        ],
        "excerpt": "Infertility evaluation is usually initiated after 12 months of unprotected intercourse, or earlier when age or clinical factors increase concern.",
    },
    {
        "title": "ESHRE Guideline: Ovarian Stimulation for IVF/ICSI",
        "url": "https://www.eshre.eu/guidelines-and-legal/guidelines/ovarian-stimulation-guideline",
        "source_type": "external guideline",
        "keywords": [
            "ivf", "icsi", "ovarian stimulation", "art", "egg retrieval", "fertility treatment",
        ],
        "excerpt": "IVF management questions should be grounded in patient age, ovarian response, and ART-specific context rather than general gynecology heuristics.",
    },
    {
        "title": "ACOG: Gestational Diabetes",
        "url": "https://www.acog.org/womens-health/faqs/gestational-diabetes",
        "source_type": "external guideline",
        "keywords": ["gestational diabetes", "gdm", "glucose tolerance", "pregnancy diabetes"],
    },
    {
        "title": "ACOG Practice Bulletin: Gestational Hypertension and Preeclampsia",
        "url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2020/06/gestational-hypertension-and-preeclampsia",
        "source_type": "external guideline",
        "keywords": ["gestational hypertension", "preeclampsia", "severe features", "proteinuria"],
    },
    {
        "title": "ACOG: Labor Induction",
        "url": "https://www.acog.org/womens-health/faqs/labor-induction",
        "source_type": "external guideline",
        "keywords": ["induction", "labor induction", "labour induction", "bishop score", "ripening"],
    },
    {
        "title": "ACOG: Prelabor Rupture of Membranes",
        "url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2020/03/prelabor-rupture-of-membranes",
        "source_type": "external guideline",
        "keywords": ["prom", "pprom", "rupture of membranes", "amniotic fluid leak"],
        "excerpt": "In PPROM before 34 weeks without infection, labor, or fetal compromise, expectant management with corticosteroids and latency antibiotics is generally recommended while monitoring for chorioamnionitis or fetal deterioration.",
    },
    {
        "title": "CDC: Group B Strep Prevention in Newborns",
        "url": "https://www.cdc.gov/group-b-strep/about/prevention.html",
        "source_type": "external guideline",
        "keywords": ["gbs", "group b strep", "group b streptococcus", "intrapartum antibiotics"],
    },
    {
        "title": "ACOG: Postpartum Hemorrhage",
        "url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2017/10/postpartum-hemorrhage",
        "source_type": "external guideline",
        "keywords": ["postpartum hemorrhage", "pph", "uterine atony", "massive bleeding postpartum"],
    },
    {
        "title": "ACOG: Long-Acting Reversible Contraception",
        "url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2017/11/long-acting-reversible-contraception-implants-and-intrauterine-devices",
        "source_type": "external guideline",
        "keywords": ["iud", "iucd", "mirena", "copper iud", "implant", "larc", "contraception"],
    },
    {
        "title": "CDC U.S. Medical Eligibility Criteria for Contraceptive Use",
        "url": "https://www.cdc.gov/contraception/hcp/usmec/index.html",
        "source_type": "external guideline",
        "keywords": ["combined ocp", "cocp", "ocp", "pill", "contraception", "contraceptive eligibility"],
    },
    {
        "title": "ACOG: Emergency Contraception",
        "url": "https://www.acog.org/womens-health/faqs/emergency-contraception",
        "source_type": "external guideline",
        "keywords": ["emergency contraception", "plan b", "ella", "levonorgestrel", "ulipristal"],
    },
    {
        "title": "ACOG: Abnormal Uterine Bleeding",
        "url": "https://www.acog.org/womens-health/faqs/abnormal-uterine-bleeding",
        "source_type": "external guideline",
        "keywords": ["abnormal uterine bleeding", "aub", "heavy periods", "menorrhagia", "intermenstrual bleeding"],
    },
    {
        "title": "ACOG: Endometriosis",
        "url": "https://www.acog.org/womens-health/faqs/endometriosis",
        "source_type": "external guideline",
        "keywords": ["endometriosis", "dysmenorrhea", "chronic pelvic pain"],
    },
    {
        "title": "ACOG: Polycystic Ovary Syndrome",
        "url": "https://www.acog.org/womens-health/faqs/polycystic-ovary-syndrome-pcos",
        "source_type": "external guideline",
        "keywords": ["pcos", "polycystic ovary syndrome", "anovulation", "irregular periods"],
    },
    {
        "title": "CDC STI Treatment Guidelines: Vulvovaginal Candidiasis",
        "url": "https://www.cdc.gov/std/treatment-guidelines/candidiasis.htm",
        "source_type": "external guideline",
        "keywords": ["candida", "yeast infection", "vaginal itching", "vaginal discharge"],
    },
    {
        "title": "CDC STI Treatment Guidelines: Bacterial Vaginosis",
        "url": "https://www.cdc.gov/std/treatment-guidelines/bv.htm",
        "source_type": "external guideline",
        "keywords": ["bacterial vaginosis", "bv", "fishy discharge", "clue cells"],
    },
    {
        "title": "CDC STI Treatment Guidelines: Trichomoniasis",
        "url": "https://www.cdc.gov/std/treatment-guidelines/trichomoniasis.htm",
        "source_type": "external guideline",
        "keywords": ["trichomonas", "trichomoniasis", "frothy discharge"],
    },
    {
        "title": "ACOG: Urinary Tract Infections in Pregnancy",
        "url": "https://www.acog.org/clinical/clinical-guidance/clinical-consensus/articles/2023/08/urinary-tract-infections-in-pregnant-individuals",
        "source_type": "external guideline",
        "keywords": ["uti", "asymptomatic bacteriuria", "pyelonephritis", "urinary infection", "pregnancy uti"],
    },
    {
        "title": "ACOG: Breastfeeding Your Baby",
        "url": "https://www.acog.org/womens-health/faqs/breastfeeding-your-baby",
        "source_type": "external guideline",
        "keywords": ["breastfeeding", "lactation", "mastitis", "engorgement"],
    },
    {
        "title": "ACOG: Menopause",
        "url": "https://www.acog.org/womens-health/faqs/the-menopause-years",
        "source_type": "external guideline",
        "keywords": ["menopause", "perimenopause", "hot flashes", "hrt", "hormone therapy"],
    },
    {
        "title": "ACOG: Osteoporosis",
        "url": "https://www.acog.org/womens-health/faqs/osteoporosis",
        "source_type": "external guideline",
        "keywords": ["osteoporosis", "bone density", "dexa", "fracture prevention"],
    },
    {
        "title": "ASCCP Clinical Practice",
        "url": "https://www.asccp.org/clinical-practice",
        "source_type": "external guideline",
        "keywords": ["hsil", "lsil", "ascus", "cin", "colposcopy", "cervical dysplasia"],
    },
    {
        "title": "ACOG: Prenatal Genetic Screening Tests",
        "url": "https://www.acog.org/womens-health/faqs/prenatal-genetic-screening-tests",
        "source_type": "external guideline",
        "keywords": ["nipt", "cfdna", "aneuploidy screening", "genetic screening", "trisomy"],
    },
    {
        "title": "ACOG: Carrier Screening",
        "url": "https://www.acog.org/womens-health/faqs/carrier-screening",
        "source_type": "external guideline",
        "keywords": ["carrier screening", "cf carrier", "sma screening", "genetic carrier"],
    },
    {
        "title": "AIUM Practice Parameter: Ultrasound in Pregnancy",
        "url": "https://www.aium.org/resources/practice-parameters/obstetric-%28standard%29",
        "source_type": "external guideline",
        "keywords": [
            "ultrasound", "obstetric ultrasound", "dating scan", "anatomy scan", "biophysical profile",
            "fetal biometry", "amniotic fluid", "doppler",
        ],
        "excerpt": "Ultrasound findings should be linked to the clinical question being answered, including dating, anatomy, growth, fluid assessment, and next-step management.",
    },
    {
        "title": "AIUM Practice Topic: Gynecologic Ultrasound",
        "url": "https://www.aium.org/practice-topics/gynecologic-ultrasound",
        "source_type": "external guideline",
        "keywords": [
            "gynecologic ultrasound", "pelvic ultrasound", "tvus", "transvaginal ultrasound",
            "adnexal mass", "ovarian cyst", "endometrial thickness", "sonohysterography",
        ],
        "excerpt": "Gynecologic ultrasound questions should be framed around the imaging finding and the next clinical action it supports.",
    },
]


def _normalize_text(text):
    return re.sub(r"\s+", " ", text.strip().lower())


def _dedupe_sources(sources):
    deduped = []
    seen_urls = set()
    for source in sources:
        if source["url"] in seen_urls:
            continue
        seen_urls.add(source["url"])
        deduped.append(source)
    return deduped


def _detect_source_routing_focus(user_message):
    normalized = _normalize_text(user_message)
    for focus, rule in SOURCE_ROUTING_RULES.items():
        if any(keyword in normalized for keyword in rule["query_keywords"]):
            return focus
    return None


def _matched_focus_query_terms(user_message, focus):
    if not focus:
        return []
    normalized = _normalize_text(user_message)
    rule = SOURCE_ROUTING_RULES.get(focus) or {}
    return [keyword for keyword in rule.get("query_keywords", []) if keyword in normalized]


def _matched_focus_subsignature_terms(user_message, focus):
    if not focus:
        return []
    normalized = _normalize_text(user_message)
    groups = FOCUS_SUBSIGNATURES.get(focus) or {}
    for _, terms in groups.items():
        matched = [term for term in terms if term in normalized]
        if matched:
            return matched
    return []


def _source_matches_focus(source, focus, query_terms=None):
    if not focus:
        return False
    rule = SOURCE_ROUTING_RULES.get(focus) or {}
    combined_keywords = " ".join(source.get("keywords") or []).lower()
    title = str(source.get("title") or "").lower()
    excerpt = str(source.get("excerpt") or "").lower()
    url = str(source.get("url") or "").lower()
    combined_text = " ".join(part for part in [combined_keywords, title, excerpt, url] if part).strip()

    narrowed_terms = [term for term in (query_terms or []) if len(term) >= 4]
    if narrowed_terms:
        return any(term in combined_text for term in narrowed_terms)

    if any(keyword in combined_text for keyword in rule.get("source_keywords", [])):
        return True
    if any(keyword in title for keyword in rule.get("title_keywords", [])):
        return True
    return False


def _assign_source_ids(sources):
    assigned = []
    for index, source in enumerate(sources, start=1):
        assigned.append(
            {
                "source_id": f"E{index}",
                "title": source["title"],
                "url": source["url"],
                "source_type": source["source_type"],
                "excerpt": source.get("excerpt"),
                "updated_at": source.get("updated_at"),
                "domain": source.get("domain") or get_source_domain(source["url"]),
                "tier": source.get("tier") or get_domain_tier(source.get("domain") or get_source_domain(source["url"])),
            }
        )
    return assigned


def _catalog_source_by_title(title):
    for source in EXTERNAL_SOURCE_CATALOG:
        if source.get("title") == title:
            return source
    return None


def get_external_sources(user_message, user_profile=None, limit=4, include_live=True):
    normalized_message = _normalize_text(user_message)
    scored = []
    israel_relevant = is_israel_relevant(user_message, user_profile=user_profile)
    stages = build_search_stages(user_message, user_profile=user_profile)
    routing_focus = _detect_source_routing_focus(user_message)
    focus_query_terms = _matched_focus_subsignature_terms(user_message, routing_focus) or _matched_focus_query_terms(user_message, routing_focus)
    stage_rank = {}
    for index, stage in enumerate(stages):
        for domain in stage["domains"]:
            stage_rank[domain] = index
    question_route = infer_question_route(user_message)

    for source in EXTERNAL_SOURCE_CATALOG:
        overlap = sum(1 for keyword in source["keywords"] if keyword in normalized_message)
        if not overlap:
            continue

        domain = get_source_domain(source["url"])
        if domain not in stage_rank:
            continue

        score = overlap * 10
        if israel_relevant and "israel" in source.get("regions", []):
            score += 100 + source.get("priority", 0)
        elif israel_relevant and source.get("regions"):
            score -= 10
        elif source.get("regions"):
            score -= 5

        tier = get_domain_tier(domain)
        if tier == "tier1":
            score += 20
        elif tier == "tier2":
            score += 10
        elif tier == "tier3":
            score += 24 if question_route == "drug_safety" else 4
        elif tier == "tier4":
            score -= 4
        elif tier == "tier45":
            score -= 8
        elif tier == "operational":
            score += 14 if question_route in {"local_operational", "local_israel_policy"} else -20

        score += max(0, 40 - (stage_rank[domain] * 10))

        if routing_focus:
            matches_focus = _source_matches_focus(source, routing_focus, focus_query_terms)
            if matches_focus:
                score += 45
            else:
                score -= 18
                if focus_query_terms:
                    continue

        scored.append((score, source, domain))

    scored.sort(key=lambda item: item[0], reverse=True)

    selected = []
    best_stage_rank = None
    for _, source, domain in scored:
        current_stage_rank = stage_rank.get(domain, 999)
        if best_stage_rank is None:
            best_stage_rank = current_stage_rank
        if best_stage_rank is not None and current_stage_rank > best_stage_rank and selected:
            break
        selected_tier = get_domain_tier(domain)
        selected.append(
            {
                "title": source["title"],
                "url": source["url"],
                "source_type": source["source_type"],
                "domain": domain,
                "tier": selected_tier,
                "excerpt": source.get("excerpt"),
                "updated_at": source.get("updated_at"),
            }
        )
        if len(selected) >= limit:
            break

    if routing_focus == "early_pregnancy" and any(
        term in (focus_query_terms or [])
        for term in ["pregnancy of unknown location", "pul", "ectopic", "no intrauterine pregnancy", "no iup"]
    ):
        nice_source = _catalog_source_by_title("NICE Guideline: Ectopic Pregnancy and Miscarriage")
        if nice_source:
            nice_domain = get_source_domain(nice_source["url"])
            selected = [
                {
                    "title": nice_source["title"],
                    "url": nice_source["url"],
                    "source_type": nice_source["source_type"],
                    "domain": nice_domain,
                    "tier": get_domain_tier(nice_domain),
                    "excerpt": nice_source.get("excerpt"),
                    "updated_at": nice_source.get("updated_at"),
                }
            ] + [source for source in selected if source["url"] != nice_source["url"]]

    if include_live:
        live_sources = get_live_trusted_sources(user_message, user_profile=user_profile, limit=limit)
        if routing_focus:
            focused_live_sources = [source for source in live_sources if _source_matches_focus(source, routing_focus, focus_query_terms)]
            if focused_live_sources:
                live_sources = focused_live_sources
            elif any(_source_matches_focus(source, routing_focus, focus_query_terms) for source in selected):
                live_sources = []
        selected = live_sources + selected

    return _assign_source_ids(_dedupe_sources(selected)[:limit])
