import re

from services.live_search_service import get_live_trusted_sources
from services.source_preference_service import is_israel_relevant
from services.trusted_source_registry import build_search_stages, get_domain_tier, get_source_domain, infer_question_route


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
        "keywords": ["ectopic", "pregnancy of unknown location", "pul", "beta hcg", "ultrasound", "tvus"],
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
            }
        )
    return assigned


def get_external_sources(user_message, user_profile=None, limit=4, include_live=True):
    normalized_message = _normalize_text(user_message)
    scored = []
    israel_relevant = is_israel_relevant(user_message, user_profile=user_profile)
    stages = build_search_stages(user_message, user_profile=user_profile)
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
            score += 14 if question_route == "local_operational" else -20

        score += max(0, 40 - (stage_rank[domain] * 10))

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
        selected.append(
            {
                "title": source["title"],
                "url": source["url"],
                "source_type": source["source_type"],
            }
        )
        if len(selected) >= limit:
            break

    if include_live:
        selected = get_live_trusted_sources(user_message, user_profile=user_profile, limit=limit) + selected

    return _assign_source_ids(_dedupe_sources(selected)[:limit])
