from datetime import datetime
from random import Random
import re

from db import study_content_collection, study_user_state_collection
from services.logging_service import log_event
from services.profile_service import get_user_profile


STAGE_B_AUTHORING_RUBRIC = {
    "warmup": [
        "Short stem with one clear clue",
        "One clear clinical decision point",
        "At least one tempting management distractor",
        "One best answer with one short why-not",
    ],
    "standard": [
        "Short-to-medium stem with one clear, not overly exposed clue",
        "One clear board-relevant clinical decision point",
        "At least one plausible management mistake as distractor",
        "Explanation must stay compressed: best answer, why, why-not, takeaway",
    ],
    "pearl": [
        "One key fact, one clinical meaning, one board takeaway or board focus",
        "Short, clinically meaningful, and not just a flashcard fact",
    ],
}

DIFFICULTY_LEVEL_DISTRIBUTIONS = {
    "R1": {1: 70, 2: 20, 3: 10},
    "R2": {2: 50, 3: 30, 1: 20},
    "R3": {3: 50, 4: 30, 2: 20},
    "R4": {4: 50, 5: 30, 3: 20},
    "R5": {5: 60, 6: 30, 4: 10},
    "R6": {6: 70, 5: 20, 4: 10},
}

DIFFICULTY_LEVEL_BOUNDS = {
    "R1": (1, 3),
    "R2": (1, 3),
    "R3": (2, 4),
    "R4": (3, 5),
    "R5": (4, 6),
    "R6": (4, 6),
}

QUESTION_STYLE_DISTRIBUTIONS = {
    1: {
        "clinical_decision": 50,
        "diagnosis_refinement": 20,
        "overlap": 10,
        "pearl": 10,
        "trap": 10,
    },
    2: {
        "clinical_decision": 50,
        "diagnosis_refinement": 20,
        "overlap": 10,
        "pearl": 10,
        "trap": 10,
    },
    3: {
        "clinical_decision": 60,
        "diagnosis_refinement": 15,
        "overlap": 10,
        "trap": 10,
        "pearl": 5,
    },
    4: {
        "clinical_decision": 60,
        "diagnosis_refinement": 15,
        "overlap": 10,
        "trap": 10,
        "pearl": 5,
    },
    5: {
        "clinical_decision": 65,
        "trap": 15,
        "overlap": 10,
        "diagnosis_refinement": 5,
        "pearl": 5,
    },
    6: {
        "clinical_decision": 65,
        "trap": 15,
        "overlap": 10,
        "diagnosis_refinement": 5,
        "pearl": 5,
    },
}

SEED_ITEM_METADATA_OVERRIDES = {
    "mcq_preeclampsia_delivery": {"difficulty_level": 5, "question_style": "clinical_decision"},
    "mcq_pph_first_step": {"difficulty_level": 1, "question_style": "clinical_decision"},
    "mcq_ctg_late_decels": {"difficulty_level": 4, "question_style": "diagnosis_refinement"},
    "mcq_pprom_antibiotics": {"difficulty_level": 5, "question_style": "clinical_decision"},
    "pearl_pph_atony": {"difficulty_level": 2, "question_style": "pearl"},
    "pearl_ctg_core": {"difficulty_level": 3, "question_style": "pearl"},
    "pearl_cervix_hsil": {"difficulty_level": 4, "question_style": "pearl"},
    "mcq_contraception_migraine_aura": {"difficulty_level": 4, "question_style": "overlap"},
    "mcq_pid_inpatient_toa": {"difficulty_level": 4, "question_style": "clinical_decision"},
    "pearl_aub_palm_coein": {"difficulty_level": 3, "question_style": "pearl"},
    "pearl_emergency_contraception": {"difficulty_level": 3, "question_style": "pearl"},
    "pearl_endometriosis_first_line": {"difficulty_level": 4, "question_style": "pearl"},
    "pearl_menopause_bleeding": {"difficulty_level": 4, "question_style": "pearl"},
    "mcq_unexplained_infertility_escalation": {"difficulty_level": 5, "question_style": "clinical_decision"},
    "mcq_adnexal_mass_referral": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_postmenopausal_bleeding_biopsy": {"difficulty_level": 5, "question_style": "trap"},
    "mcq_aub_age45_sampling": {"difficulty_level": 5, "question_style": "trap"},
    "pearl_adnexal_mass_referral": {"difficulty_level": 5, "question_style": "pearl"},
    "pearl_postmenopausal_bleeding_workup": {"difficulty_level": 5, "question_style": "pearl"},
    "mcq_uti_nonpregnant_first_line": {"difficulty_level": 5, "question_style": "overlap"},
    "mcq_postpartum_endometritis_antibiotics": {"difficulty_level": 5, "question_style": "clinical_decision"},
    "mcq_postpartum_pe_headache": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_vte_postpartum_estrogen": {"difficulty_level": 5, "question_style": "overlap"},
    "mcq_pregnancy_pyelo_admit": {"difficulty_level": 5, "question_style": "overlap"},
    "mcq_platelets_neuraxial_preeclampsia": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_hypoosmolar_hyponatremia_labor": {"difficulty_level": 6, "question_style": "overlap"},
    "pearl_postpartum_hypertension": {"difficulty_level": 5, "question_style": "pearl"},
    "pearl_overlap_uti_vs_pyelo": {"difficulty_level": 4, "question_style": "pearl"},
}

PROMOTION_WINDOW = 4
DEMOTION_WINDOW = 6
PROMOTION_THRESHOLD = 4
DEMOTION_THRESHOLD = 4
MAX_CONSECUTIVE_TOPIC_REPEATS = 2
STYLE_HISTORY_WINDOW = 12
LEVEL_HISTORY_WINDOW = 12


STUDY_SEED_ITEMS = [
    {
        "id": "mcq_preeclampsia_delivery",
        "item_type": "mcq",
        "topic": "Preeclampsia",
        "subtopic": "Timing of delivery",
        "question_stem": "A 35-week pregnant patient is diagnosed with preeclampsia with severe features. What is the most appropriate next step in management?",
        "options": [
            {"key": "A", "text": "Expectant management with close inpatient observation until 37 weeks"},
            {"key": "B", "text": "Delivery after maternal stabilization"},
            {"key": "C", "text": "Complete a full steroid course before making a delivery plan"},
            {"key": "D", "text": "Outpatient management with repeat labs in 48 hours"},
        ],
        "correct_answer_key": "B",
        "explanation": "At 35 weeks, preeclampsia with severe features is generally managed with maternal stabilization followed by delivery rather than routine expectant management.",
        "exam_clue": "Severe features at 35 weeks",
        "board_takeaway": "Severe features at 34 weeks or more: stabilize the mother, then deliver.",
        "decision_point": "Timing of delivery in preeclampsia with severe features",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Expectant management is generally reserved for selected earlier gestations, not routine management at 35 weeks with severe features.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_nice_hypertension",
        "source_name": "NICE Guideline: Hypertension in Pregnancy",
        "source_type": "Guideline",
        "source_url": "https://www.nice.org.uk/guidance/ng133",
        "source_excerpt": "Severe maternal disease changes timing and usually favors delivery after stabilization rather than routine expectant care.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_pph_first_step",
        "item_type": "mcq",
        "topic": "PPH",
        "subtopic": "Initial management",
        "question_stem": "Immediately after vaginal birth, heavy bleeding is attributed to uterine atony. Which medication is the best first-line uterotonic?",
        "options": [
            {"key": "A", "text": "Oxytocin"},
            {"key": "B", "text": "Methylergonovine before initial oxytocin"},
            {"key": "C", "text": "Magnesium sulfate"},
            {"key": "D", "text": "Tranexamic acid as the only first medication"},
        ],
        "correct_answer_key": "A",
        "explanation": "For uterine atony, the standard first medication is oxytocin, given along with immediate uterine massage and hemorrhage resuscitation.",
        "exam_clue": "Immediate postpartum hemorrhage from uterine atony",
        "board_takeaway": "Immediate PPH from uterine atony: uterine massage plus oxytocin first.",
        "decision_point": "First-line management of uterine atony postpartum hemorrhage",
        "difficulty_band": "warmup",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Methylergonovine can be used later in selected patients, but oxytocin is still the first-line uterotonic for initial atony management.",
        "estimated_time_seconds": 45,
        "source_id": "study_src_acog_pph",
        "source_name": "ACOG Practice Bulletin: Postpartum Hemorrhage",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2017/10/postpartum-hemorrhage",
        "source_excerpt": "Initial treatment of uterine atony includes uterine massage and oxytocin as first-line uterotonic therapy.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_ctg_late_decels",
        "item_type": "mcq",
        "topic": "CTG",
        "subtopic": "Interpretation",
        "question_stem": "On intrapartum CTG, recurrent late decelerations are present. What is the most likely underlying problem?",
        "options": [
            {"key": "A", "text": "Cord compression"},
            {"key": "B", "text": "Uteroplacental insufficiency"},
            {"key": "C", "text": "Benign fetal sleep cycle"},
            {"key": "D", "text": "Head compression during contractions"},
        ],
        "correct_answer_key": "B",
        "explanation": "Recurrent late decelerations classically suggest uteroplacental insufficiency rather than cord compression.",
        "exam_clue": "Recurrent late decelerations",
        "board_takeaway": "Late decelerations point to uteroplacental insufficiency; variable decelerations point to cord compression.",
        "decision_point": "Interpret the most likely cause of a CTG deceleration pattern",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Cord compression more typically causes variable decelerations, not recurrent late decelerations.",
        "estimated_time_seconds": 50,
        "source_id": "study_src_nice_ctg",
        "source_name": "NICE Guideline: Fetal Monitoring in Labour",
        "source_type": "Guideline",
        "source_url": "https://www.nice.org.uk/guidance/ng229",
        "source_excerpt": "Late decelerations are associated with uteroplacental insufficiency and possible fetal hypoxia.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_pprom_antibiotics",
        "item_type": "mcq",
        "topic": "PPROM",
        "subtopic": "Latency management",
        "question_stem": "A patient has PPROM at 30 weeks with no signs of chorioamnionitis or fetal compromise. Which management step best improves latency and outcomes?",
        "options": [
            {"key": "A", "text": "Latency antibiotics"},
            {"key": "B", "text": "Immediate delivery because membranes are ruptured"},
            {"key": "C", "text": "Prolonged routine tocolysis alone"},
            {"key": "D", "text": "Observation only if fetal tracing is normal"},
        ],
        "correct_answer_key": "A",
        "explanation": "In appropriate PPROM cases before term and without infection, latency antibiotics help prolong pregnancy and reduce infectious morbidity.",
        "exam_clue": "PPROM at 30 weeks without infection",
        "board_takeaway": "PPROM before term without infection: give latency antibiotics as part of expectant management.",
        "decision_point": "Choose a supportive management step in preterm PPROM",
        "difficulty_band": "standard",
        "tempting_wrong_option": "D",
        "tempting_wrong_reason": "Normal fetal tracing does not remove the benefit of latency antibiotics in eligible preterm PPROM.",
        "estimated_time_seconds": 55,
        "source_id": "study_src_acog_prom",
        "source_name": "ACOG Practice Bulletin: Prelabor Rupture of Membranes",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2020/03/prelabor-rupture-of-membranes",
        "source_excerpt": "Latency antibiotics are recommended in eligible PPROM cases to prolong pregnancy and lower infectious morbidity.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_pph_atony",
        "item_type": "pearl",
        "topic": "PPH",
        "title": "Quick Pearl: PPH",
        "key_fact": "Uterine atony is the most common cause of primary postpartum hemorrhage.",
        "clinical_consequence": "Initial management includes simultaneous resuscitation, uterine massage, and oxytocin rather than delayed stepwise treatment.",
        "board_focus": "If bleeding persists, use the 4 Ts framework: Tone, Trauma, Tissue, Thrombin, to rapidly identify the cause and direct targeted treatment.",
        "board_rule": "Primary PPH: initial management includes resuscitation, massage, and oxytocin; persistent bleeding should trigger rapid 4 Ts assessment and targeted treatment.",
        "board_relevance": "First-line treatment plus escalation framework",
        "estimated_time_seconds": 35,
        "source_id": "study_src_acog_pph",
        "source_name": "ACOG Practice Bulletin: Postpartum Hemorrhage",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2017/10/postpartum-hemorrhage",
        "source_excerpt": "Uterine atony is the most common cause of postpartum hemorrhage and should be treated promptly with uterotonics.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "pearl_ctg_core",
        "item_type": "pearl",
        "topic": "CTG",
        "title": "Quick Pearl: CTG",
        "key_fact": "Early decelerations usually reflect head compression, variable decelerations suggest cord compression, and late decelerations suggest uteroplacental insufficiency.",
        "clinical_consequence": "The board question is not just pattern recognition; recurrent late decelerations or deep variables with slow recovery increase concern for fetal compromise.",
        "board_focus": "CTG type should change management thinking: recurrent concerning variables or late decelerations should prompt escalation beyond simple labeling.",
        "board_rule": "On boards, CTG pattern recognition must lead to action: recurrent concerning variables or late decelerations raise concern and should prompt escalation.",
        "board_relevance": "Pattern recognition plus clinical significance",
        "estimated_time_seconds": 30,
        "source_id": "study_src_nice_ctg",
        "source_name": "NICE Guideline: Fetal Monitoring in Labour",
        "source_type": "Guideline",
        "source_url": "https://www.nice.org.uk/guidance/ng229",
        "source_excerpt": "Different deceleration patterns on CTG suggest different pathophysiologic causes and different levels of concern.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "pearl_cervix_hsil",
        "item_type": "pearl",
        "topic": "Cervical screening",
        "title": "Quick Pearl: HSIL",
        "key_fact": "HSIL cytology is a high-risk screening result and is not managed with routine repeat testing alone.",
        "clinical_consequence": "Management usually turns on the distinction between colposcopy and expedited treatment in nonpregnant patients aged 25 or older.",
        "board_focus": "In pregnancy, HSIL still requires colposcopic evaluation, but expedited treatment is generally not the correct board answer.",
        "board_rule": "HSIL: think colposcopy versus expedited treatment; pregnancy shifts management toward evaluation rather than immediate treatment.",
        "board_relevance": "Risk-based management distinction",
        "estimated_time_seconds": 35,
        "source_id": "study_src_asccp_hsil",
        "source_name": "ASCCP Clinical Practice",
        "source_type": "Guideline",
        "source_url": "https://www.asccp.org/clinical-practice",
        "source_excerpt": "High-grade screening abnormalities require prompt risk-based evaluation and management.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_contraception_migraine_aura",
        "item_type": "mcq",
        "topic": "Contraception",
        "subtopic": "Estrogen contraindication",
        "question_stem": "A 29-year-old asks to start combined hormonal contraception. She has migraine with aura. What is the best next step?",
        "options": [
            {"key": "A", "text": "Start a combined oral contraceptive because she is younger than 35"},
            {"key": "B", "text": "Avoid estrogen-containing contraception and offer a non-estrogen method"},
            {"key": "C", "text": "Use combined contraception if her blood pressure is normal"},
            {"key": "D", "text": "Delay contraception until neurology clears her migraines"},
        ],
        "correct_answer_key": "B",
        "explanation": "Migraine with aura is a major contraindication to estrogen-containing contraception because stroke risk outweighs the benefit of combined hormonal methods.",
        "exam_clue": "Migraine with aura",
        "board_takeaway": "Migraine with aura: avoid estrogen-containing contraception and choose a non-estrogen option.",
        "decision_point": "Choose a safe contraceptive class when estrogen is contraindicated",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Young age does not override the contraindication; aura is the board clue that pushes you away from combined hormonal contraception.",
        "estimated_time_seconds": 50,
        "source_id": "study_src_cdc_usmec",
        "source_name": "CDC U.S. Medical Eligibility Criteria for Contraceptive Use",
        "source_type": "Guideline",
        "source_url": "https://www.cdc.gov/contraception/hcp/usmec/index.html",
        "source_excerpt": "Migraine with aura is a contraindication to combined hormonal contraception, so a non-estrogen method is preferred.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_pid_inpatient_toa",
        "item_type": "mcq",
        "topic": "PID",
        "subtopic": "Need for hospitalization",
        "question_stem": "A patient with PID is found to have a tubo-ovarian abscess on ultrasound. What is the most appropriate management now?",
        "options": [
            {"key": "A", "text": "Outpatient oral antibiotics with review in one week"},
            {"key": "B", "text": "Inpatient treatment with parenteral broad-spectrum antibiotics"},
            {"key": "C", "text": "Immediate hysterectomy"},
            {"key": "D", "text": "Observation only if she is afebrile"},
        ],
        "correct_answer_key": "B",
        "explanation": "Tubo-ovarian abscess is a board-level severity clue in PID and generally requires inpatient management with parenteral broad-spectrum antibiotics.",
        "exam_clue": "PID plus tubo-ovarian abscess",
        "board_takeaway": "PID with tubo-ovarian abscess: admit for parenteral broad-spectrum therapy.",
        "decision_point": "Recognize when PID requires inpatient rather than outpatient management",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "A tubo-ovarian abscess raises severity and complication risk, so routine outpatient oral therapy is not the best board answer.",
        "estimated_time_seconds": 55,
        "source_id": "study_src_cdc_pid",
        "source_name": "CDC STI Treatment Guidelines: PID",
        "source_type": "Guideline",
        "source_url": "https://www.cdc.gov/std/treatment-guidelines/pid.htm",
        "source_excerpt": "Hospitalization should be considered in PID when a tubo-ovarian abscess is present.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_aub_palm_coein",
        "item_type": "pearl",
        "topic": "AUB",
        "title": "Quick Pearl: AUB",
        "key_fact": "Abnormal uterine bleeding is organized by the PALM-COEIN framework.",
        "clinical_consequence": "The board move is not just naming AUB; it is deciding whether the pattern points toward a structural cause or a nonstructural cause that changes evaluation.",
        "board_focus": "PALM-COEIN should structure your next step: think structural causes when imaging or biopsy matters, and nonstructural causes when endocrine or coagulopathic causes are more likely.",
        "board_rule": "AUB on boards: organize the differential with PALM-COEIN, then let that framework guide evaluation rather than treating all bleeding as one entity.",
        "board_relevance": "Framework that changes workup",
        "estimated_time_seconds": 35,
        "source_id": "study_src_acog_aub",
        "source_name": "ACOG: Abnormal Uterine Bleeding",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/abnormal-uterine-bleeding",
        "source_excerpt": "AUB should be evaluated with attention to structural and nonstructural causes, often summarized by PALM-COEIN.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_emergency_contraception",
        "item_type": "pearl",
        "topic": "Contraception",
        "title": "Quick Pearl: Emergency Contraception",
        "key_fact": "A copper IUD is the most effective form of emergency contraception when feasible.",
        "clinical_consequence": "Board questions often hinge on the distinction between the best overall option and the best oral option when an IUD is not practical.",
        "board_focus": "If timing and access allow, think copper IUD first; if not, choose the best oral method for the scenario rather than treating all emergency contraception as equivalent.",
        "board_rule": "Emergency contraception: copper IUD is most effective overall; oral options are alternatives when insertion is not feasible.",
        "board_relevance": "Best option versus best oral option",
        "estimated_time_seconds": 30,
        "source_id": "study_src_acog_ec",
        "source_name": "ACOG: Emergency Contraception",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/emergency-contraception",
        "source_excerpt": "A copper IUD is the most effective emergency contraceptive method and oral options remain useful alternatives.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_endometriosis_first_line",
        "item_type": "pearl",
        "topic": "Endometriosis",
        "title": "Quick Pearl: Endometriosis",
        "key_fact": "Empiric treatment for suspected endometriosis can start without immediate diagnostic laparoscopy in many board-style scenarios.",
        "clinical_consequence": "In a stable patient with classic symptoms, first-line management often begins with NSAIDs and hormonal suppression rather than rushing to surgery.",
        "board_focus": "The board distinction is initial symptom control versus escalation to laparoscopy when symptoms persist, fertility is a priority, or the diagnosis remains unclear.",
        "board_rule": "Suspected endometriosis: start with symptom control and hormonal therapy in typical cases; reserve laparoscopy for selected scenarios.",
        "board_relevance": "Initial management versus escalation",
        "estimated_time_seconds": 35,
        "source_id": "study_src_acog_endometriosis",
        "source_name": "ACOG: Endometriosis",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/endometriosis",
        "source_excerpt": "Treatment of endometriosis often begins with pain control and hormonal therapy, with surgery reserved for selected cases.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_menopause_bleeding",
        "item_type": "pearl",
        "topic": "Menopause",
        "title": "Quick Pearl: Postmenopausal Bleeding",
        "key_fact": "Postmenopausal bleeding is endometrial cancer until proved otherwise on boards.",
        "clinical_consequence": "The key consequence is that bleeding after menopause is not managed like routine irregular bleeding and requires directed evaluation.",
        "board_focus": "When the stem says postmenopausal bleeding, think malignancy exclusion first rather than symptom treatment alone.",
        "board_rule": "Postmenopausal bleeding: prioritize endometrial evaluation and malignancy exclusion.",
        "board_relevance": "Red-flag framing",
        "estimated_time_seconds": 30,
        "source_id": "study_src_acog_menopause",
        "source_name": "ACOG: Menopause",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/the-menopause-years",
        "source_excerpt": "Bleeding after menopause is abnormal and warrants evaluation.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_unexplained_infertility_escalation",
        "item_type": "mcq",
        "topic": "Fertility",
        "subtopic": "Escalation after normal initial workup",
        "question_stem": "A 36-year-old has 2 years of infertility. HSG, semen analysis, and ovarian reserve testing are normal. What is the best next step in management?",
        "options": [
            {"key": "A", "text": "Expectant management for another 12 months"},
            {"key": "B", "text": "Controlled ovarian stimulation with IUI"},
            {"key": "C", "text": "Diagnostic laparoscopy before any fertility treatment"},
            {"key": "D", "text": "Immediate hysterectomy because the workup is unrevealing"},
        ],
        "correct_answer_key": "B",
        "explanation": "With unexplained infertility after a normal initial workup, the usual next step is treatment escalation rather than prolonged observation; COH-IUI is a common board-style next move before IVF.",
        "exam_clue": "Normal infertility workup in a 36-year-old with persistent infertility",
        "board_takeaway": "Unexplained infertility after normal initial evaluation usually escalates to COH-IUI rather than more observation.",
        "decision_point": "Choose the next fertility escalation step after a normal first-line workup",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "At this age and duration of infertility, more expectant management wastes time and is not the best next step.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_asrm_unexplained_infertility",
        "source_name": "ASRM: Evidence-based Treatments for Couples With Unexplained Infertility",
        "source_type": "Guideline",
        "source_url": "https://www.asrm.org/practice-guidance/practice-committee-documents/evidence-based-treatments-for-couples-with-unexplained-infertility-a-guideline-2020/",
        "source_excerpt": "After normal evaluation, unexplained infertility usually moves toward ovarian stimulation with IUI before IVF, especially when time matters.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_adnexal_mass_referral",
        "item_type": "mcq",
        "topic": "Gynecologic oncology",
        "subtopic": "Referral threshold",
        "question_stem": "A 52-year-old has bloating, early satiety, and a new adnexal mass on ultrasound. What is the best next step?",
        "options": [
            {"key": "A", "text": "Repeat ultrasound in 6 weeks before taking any other action"},
            {"key": "B", "text": "Order CA-125 and refer to gynecologic oncology"},
            {"key": "C", "text": "Start empiric antibiotics for presumed pelvic infection"},
            {"key": "D", "text": "Reassure her that most adnexal masses are benign"},
        ],
        "correct_answer_key": "B",
        "explanation": "In a symptomatic peri- or postmenopausal patient with a new adnexal mass, malignancy risk is high enough that CA-125 and gynecologic oncology referral are appropriate rather than short-interval observation alone.",
        "exam_clue": "Early satiety plus a new adnexal mass in a 52-year-old",
        "board_takeaway": "Adnexal mass plus concerning symptoms in this age group should trigger oncologic risk stratification and specialist referral.",
        "decision_point": "Recognize when an adnexal mass should be escalated rather than observed",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Short-interval repeat imaging alone is not enough when the symptom profile raises concern for malignancy.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_acog_adnexal_mass",
        "source_name": "ACOG Practice Bulletin: Evaluation and Management of Adnexal Masses",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2016/11/evaluation-and-management-of-adnexal-masses",
        "source_excerpt": "Adnexal masses with concerning features or symptom profiles should prompt malignancy assessment and gynecologic oncology involvement when appropriate.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_postmenopausal_bleeding_biopsy",
        "item_type": "mcq",
        "topic": "Gynecologic oncology",
        "subtopic": "Postmenopausal bleeding workup",
        "question_stem": "A 58-year-old presents with new postmenopausal bleeding. What is the best next step in evaluation?",
        "options": [
            {"key": "A", "text": "Reassure her because atrophy is the most common cause"},
            {"key": "B", "text": "Endometrial biopsy with transvaginal ultrasound as complementary evaluation"},
            {"key": "C", "text": "Empiric progesterone therapy before any workup"},
            {"key": "D", "text": "Repeat evaluation only if bleeding recurs again"},
        ],
        "correct_answer_key": "B",
        "explanation": "Postmenopausal bleeding requires malignancy-focused evaluation. Endometrial biopsy is central, and transvaginal ultrasound can complement risk stratification rather than replace tissue assessment.",
        "exam_clue": "New postmenopausal bleeding",
        "board_takeaway": "Postmenopausal bleeding means endometrial pathology must be excluded first, not treated empirically.",
        "decision_point": "Recognize malignancy-first evaluation in postmenopausal bleeding",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Atrophy may be common, but it cannot be assumed before malignancy exclusion.",
        "estimated_time_seconds": 55,
        "source_id": "study_src_acog_pmb",
        "source_name": "ACOG: Perimenopausal Bleeding and Bleeding After Menopause",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/perimenopausal-bleeding-and-bleeding-after-menopause",
        "source_excerpt": "Bleeding after menopause is abnormal and requires evaluation, with tissue diagnosis and ultrasound used to exclude malignant causes.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_aub_age45_sampling",
        "item_type": "mcq",
        "topic": "General gynecology",
        "subtopic": "Endometrial sampling threshold",
        "question_stem": "A 46-year-old presents with abnormal uterine bleeding and a known fibroid uterus. What is the best next step in evaluation?",
        "options": [
            {"key": "A", "text": "Treat empirically for fibroids only and defer tissue evaluation"},
            {"key": "B", "text": "Endometrial sampling as part of the initial evaluation"},
            {"key": "C", "text": "Observe for 6 months because fibroids explain the bleeding"},
            {"key": "D", "text": "Hysterectomy before office evaluation"},
        ],
        "correct_answer_key": "B",
        "explanation": "In abnormal uterine bleeding at age 45 or older, endometrial sampling is part of the initial evaluation even when fibroids seem like an obvious structural explanation.",
        "exam_clue": "Age 46 with AUB despite a known structural explanation",
        "board_takeaway": "Age 45 or older with AUB should trigger endometrial sampling rather than anchoring on fibroids alone.",
        "decision_point": "Do not let a plausible structural cause replace indicated malignancy-focused evaluation",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Fibroids can coexist with endometrial pathology, so they do not remove the need for sampling in this age group.",
        "estimated_time_seconds": 55,
        "source_id": "study_src_acog_aub",
        "source_name": "ACOG: Abnormal Uterine Bleeding",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/abnormal-uterine-bleeding",
        "source_excerpt": "Abnormal uterine bleeding evaluation should identify structural causes while also recognizing when biopsy is needed, especially in older patients.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_adnexal_mass_referral",
        "item_type": "pearl",
        "topic": "Gynecologic oncology",
        "title": "Quick Pearl: Adnexal Mass",
        "key_fact": "A symptomatic adnexal mass in a peri- or postmenopausal patient should trigger malignancy-focused evaluation rather than short-interval reassurance alone.",
        "clinical_consequence": "Symptoms like bloating and early satiety increase concern that the mass is not just an incidental benign cyst.",
        "board_focus": "Board-style next steps usually center on CA-125, risk stratification, and gynecologic oncology referral rather than repeat ultrasound alone.",
        "board_rule": "Adnexal mass plus concerning symptom profile: think malignancy exclusion and specialist referral.",
        "board_relevance": "Referral threshold and escalation logic",
        "estimated_time_seconds": 35,
        "source_id": "study_src_acog_adnexal_mass",
        "source_name": "ACOG Practice Bulletin: Evaluation and Management of Adnexal Masses",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2016/11/evaluation-and-management-of-adnexal-masses",
        "source_excerpt": "Adnexal masses with concerning features or symptoms warrant malignancy assessment and referral when appropriate.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "pearl_postmenopausal_bleeding_workup",
        "item_type": "pearl",
        "topic": "Gynecologic oncology",
        "title": "Quick Pearl: Postmenopausal Bleeding",
        "key_fact": "Postmenopausal bleeding is abnormal until a malignant cause has been excluded.",
        "clinical_consequence": "The mistake is assuming atrophy is enough explanation before biopsy or directed endometrial evaluation.",
        "board_focus": "The management pivot is malignancy exclusion first; ultrasound can help, but it does not make symptom-driven evaluation disappear.",
        "board_rule": "Postmenopausal bleeding: prioritize endometrial evaluation before symptom treatment.",
        "board_relevance": "Cancer-first framing",
        "estimated_time_seconds": 30,
        "source_id": "study_src_acog_pmb",
        "source_name": "ACOG: Perimenopausal Bleeding and Bleeding After Menopause",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/perimenopausal-bleeding-and-bleeding-after-menopause",
        "source_excerpt": "Bleeding after menopause requires evaluation and should not be treated like routine irregular bleeding.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "mcq_uti_nonpregnant_first_line",
        "item_type": "mcq",
        "topic": "Infectious gynecology",
        "subtopic": "Adjacent medicine overlap",
        "question_stem": "A 27-year-old nonpregnant patient has dysuria, urinary frequency, no fever, no flank pain, and no vaginal discharge. What is the best first-line treatment approach?",
        "options": [
            {"key": "A", "text": "Treat as uncomplicated cystitis with a first-line oral regimen guided by local resistance patterns"},
            {"key": "B", "text": "Use pyelonephritis-level intravenous therapy because any urinary symptoms may ascend quickly"},
            {"key": "C", "text": "Avoid treatment until urine culture returns because empiric therapy is never appropriate"},
            {"key": "D", "text": "Start antifungal therapy because dysuria without pregnancy is most likely candidiasis"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is a classic uncomplicated cystitis presentation in a nonpregnant patient, so empiric first-line oral therapy is appropriate, guided by local susceptibility patterns and antibiotic stewardship.",
        "exam_clue": "Dysuria and frequency without fever, flank pain, or vaginal symptoms",
        "board_takeaway": "Lower urinary tract symptoms without systemic features point to uncomplicated cystitis, not pyelonephritis.",
        "decision_point": "Distinguish uncomplicated cystitis from pyelonephritis or vaginitis in an OB-GYN overlap scenario",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Escalation to pyelonephritis-level therapy is for systemic illness or upper tract features, not isolated lower urinary symptoms.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_nice_uti",
        "source_name": "NICE Guideline: Lower UTI (Women)",
        "source_type": "Guideline",
        "source_url": "https://www.nice.org.uk/guidance/ng109",
        "source_excerpt": "Uncomplicated lower UTI in nonpregnant women is managed with first-line oral antibiotics while reserving escalation for features suggesting upper tract infection.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_postpartum_endometritis_antibiotics",
        "item_type": "mcq",
        "topic": "Obstetrics",
        "subtopic": "Postpartum infection overlap",
        "question_stem": "A day-2 post-cesarean patient has fever, uterine tenderness, and foul-smelling lochia. What is the most appropriate initial antibiotic approach?",
        "options": [
            {"key": "A", "text": "Broad-spectrum intravenous therapy covering polymicrobial postpartum endometritis"},
            {"key": "B", "text": "Outpatient nitrofurantoin because postpartum fever is usually urinary only"},
            {"key": "C", "text": "Immediate hysterectomy before antibiotics"},
            {"key": "D", "text": "Observation until blood cultures identify the organism"},
        ],
        "correct_answer_key": "A",
        "explanation": "Postpartum endometritis is typically polymicrobial and should be treated promptly with broad-spectrum intravenous antibiotics rather than delayed pending culture data.",
        "exam_clue": "Post-cesarean fever with uterine tenderness and foul lochia",
        "board_takeaway": "Postpartum fever with uterine tenderness is endometritis until proved otherwise and needs broad-spectrum IV therapy.",
        "decision_point": "Recognize postpartum endometritis and start appropriate empiric treatment",
        "difficulty_band": "standard",
        "tempting_wrong_option": "D",
        "tempting_wrong_reason": "Cultures can help later, but board management is to start empiric broad-spectrum therapy immediately.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_acog_postpartum_infection",
        "source_name": "ACOG: Postpartum Care and Infection Principles",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/postpartum-birth-control",
        "source_excerpt": "Suspected postpartum uterine infection requires prompt treatment and should not wait for confirmatory testing before therapy begins.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_postpartum_pe_headache",
        "item_type": "mcq",
        "topic": "Obstetrics",
        "subtopic": "Postpartum severe hypertension",
        "question_stem": "Five days postpartum, a patient presents with severe headache, blood pressure 170/112, and visual symptoms. What is the most appropriate next step?",
        "options": [
            {"key": "A", "text": "Treat as postpartum preeclampsia with severe features, begin acute blood pressure control, and give magnesium sulfate"},
            {"key": "B", "text": "Reassure her because preeclampsia only occurs before delivery"},
            {"key": "C", "text": "Delay treatment until urine protein results return"},
            {"key": "D", "text": "Manage as simple postpartum headache with oral analgesics only"},
        ],
        "correct_answer_key": "A",
        "explanation": "Postpartum preeclampsia can present after discharge. Severe hypertension with neurologic symptoms requires urgent treatment and seizure prophylaxis rather than waiting for proteinuria confirmation.",
        "exam_clue": "Postpartum severe-range blood pressure with headache and visual symptoms",
        "board_takeaway": "Severe postpartum hypertension is still preeclampsia territory and needs urgent treatment now.",
        "decision_point": "Recognize and treat postpartum preeclampsia with severe features",
        "difficulty_band": "standard",
        "tempting_wrong_option": "C",
        "tempting_wrong_reason": "Proteinuria is not required before treating severe postpartum hypertension with neurologic symptoms.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_acog_preeclampsia",
        "source_name": "ACOG Practice Bulletin: Gestational Hypertension and Preeclampsia",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2020/06/gestational-hypertension-and-preeclampsia",
        "source_excerpt": "Severe-range blood pressure with severe symptoms requires urgent treatment and magnesium prophylaxis even in the postpartum setting.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_vte_postpartum_estrogen",
        "item_type": "mcq",
        "topic": "Contraception",
        "subtopic": "VTE risk overlap",
        "question_stem": "A 3-week postpartum patient wants combined hormonal contraception. She is breastfeeding and had a postpartum DVT. What is the best next step?",
        "options": [
            {"key": "A", "text": "Avoid estrogen-containing contraception and offer a non-estrogen method"},
            {"key": "B", "text": "Start combined pills because breastfeeding lowers thrombosis risk"},
            {"key": "C", "text": "Use combined contraception if her leg symptoms have improved"},
            {"key": "D", "text": "Delay all contraception until six months postpartum"},
        ],
        "correct_answer_key": "A",
        "explanation": "Recent postpartum VTE is a strong reason to avoid estrogen-containing contraception. The board move is to offer an effective non-estrogen option instead of withholding contraception entirely.",
        "exam_clue": "Recent postpartum DVT plus request for combined hormonal contraception",
        "board_takeaway": "Postpartum thrombosis history pushes you away from estrogen, not away from contraception.",
        "decision_point": "Choose contraception safely in the setting of recent postpartum VTE",
        "difficulty_band": "standard",
        "tempting_wrong_option": "D",
        "tempting_wrong_reason": "The safer move is to choose a non-estrogen method now, not to leave the patient without contraception.",
        "estimated_time_seconds": 55,
        "source_id": "study_src_cdc_usmec",
        "source_name": "CDC U.S. Medical Eligibility Criteria for Contraceptive Use",
        "source_type": "Guideline",
        "source_url": "https://www.cdc.gov/contraception/hcp/usmec/index.html",
        "source_excerpt": "Recent postpartum thrombosis and breastfeeding status are key reasons to avoid estrogen-containing contraceptive methods.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_pregnancy_pyelo_admit",
        "item_type": "mcq",
        "topic": "Obstetrics",
        "subtopic": "Pregnancy UTI overlap",
        "question_stem": "A 24-week pregnant patient has fever, flank pain, tachycardia, and CVA tenderness. What is the most appropriate management now?",
        "options": [
            {"key": "A", "text": "Hospitalize for pyelonephritis treatment with parenteral antibiotics and maternal-fetal monitoring"},
            {"key": "B", "text": "Treat as uncomplicated cystitis with outpatient nitrofurantoin"},
            {"key": "C", "text": "Wait for culture results before starting antibiotics"},
            {"key": "D", "text": "Manage with oral hydration only if fetal heart rate is normal"},
        ],
        "correct_answer_key": "A",
        "explanation": "Pregnancy pyelonephritis is a maternal-fetal risk condition and should be managed inpatient with prompt parenteral therapy, not like simple cystitis.",
        "exam_clue": "Pregnancy plus fever, flank pain, and CVA tenderness",
        "board_takeaway": "Pyelonephritis in pregnancy is an admit-and-treat problem, not an outpatient cystitis problem.",
        "decision_point": "Differentiate pyelonephritis from lower UTI in pregnancy and escalate appropriately",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Systemic features and flank pain move this out of uncomplicated cystitis and into inpatient pyelonephritis management.",
        "estimated_time_seconds": 60,
        "source_id": "study_src_acog_uti_pregnancy",
        "source_name": "ACOG: Urinary Tract Infections in Pregnancy",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/clinical-consensus/articles/2023/08/urinary-tract-infections-in-pregnant-individuals",
        "source_excerpt": "Pyelonephritis in pregnancy requires prompt inpatient treatment with parenteral antibiotics because of maternal and obstetric risk.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_platelets_neuraxial_preeclampsia",
        "item_type": "mcq",
        "topic": "Obstetrics",
        "subtopic": "Procedural overlap",
        "question_stem": "A patient with severe preeclampsia in labor has platelets of 68,000 and requests epidural analgesia. What is the best next step?",
        "options": [
            {"key": "A", "text": "Avoid routine neuraxial placement at this platelet count and discuss alternative analgesia while the obstetric plan proceeds"},
            {"key": "B", "text": "Place the epidural because thrombocytopenia from preeclampsia never changes neuraxial decisions"},
            {"key": "C", "text": "Delay all obstetric management until platelets normalize"},
            {"key": "D", "text": "Give aspirin and then proceed with epidural placement"},
        ],
        "correct_answer_key": "A",
        "explanation": "Significant thrombocytopenia changes neuraxial risk assessment. The board point is not to freeze the entire obstetric plan, but to recognize that epidural placement may be unsafe at this range.",
        "exam_clue": "Severe preeclampsia with platelets 68,000",
        "board_takeaway": "Low platelets can change neuraxial decisions without changing the need to keep the delivery plan moving.",
        "decision_point": "Recognize when thrombocytopenia changes neuraxial analgesia decisions in obstetrics",
        "difficulty_band": "standard",
        "tempting_wrong_option": "C",
        "tempting_wrong_reason": "The mistake is stopping the obstetric plan entirely; the real pivot is the neuraxial decision, not whether preeclampsia still needs management.",
        "estimated_time_seconds": 65,
        "source_id": "study_src_society_ob_anesthesia",
        "source_name": "SOAP Consensus Statement on Thrombocytopenia and Neuraxial Procedures",
        "source_type": "Consensus statement",
        "source_url": "https://soap.memberclicks.net/assets/docs/SOAP%20Consensus%20Statement%20Thrombocytopenia%202021.pdf",
        "source_excerpt": "Thrombocytopenia changes neuraxial decision-making and requires weighing hematoma risk against obstetric context rather than treating all counts as interchangeable.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "mcq_hypoosmolar_hyponatremia_labor",
        "item_type": "mcq",
        "topic": "Obstetrics",
        "subtopic": "Fluid overlap",
        "question_stem": "During a prolonged induction, a laboring patient becomes confused and nauseated after receiving large volumes of hypotonic fluid with oxytocin. What is the most likely problem?",
        "options": [
            {"key": "A", "text": "Water intoxication with hyponatremia"},
            {"key": "B", "text": "Amniotic fluid embolism"},
            {"key": "C", "text": "Occult placental abruption"},
            {"key": "D", "text": "Normal oxytocin side effects that do not change management"},
        ],
        "correct_answer_key": "A",
        "explanation": "Oxytocin can contribute to water retention, and excessive hypotonic fluids can lead to symptomatic hyponatremia. The clue is neurologic change during prolonged labor management rather than sudden cardiopulmonary collapse.",
        "exam_clue": "Confusion after prolonged oxytocin exposure and hypotonic fluids",
        "board_takeaway": "Not every deterioration on oxytocin is obstetric catastrophe; fluid-related hyponatremia is a real overlap problem.",
        "decision_point": "Recognize oxytocin-associated water intoxication and separate it from obstetric emergencies",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Amniotic fluid embolism presents dramatically with cardiopulmonary collapse, not isolated progressive neurologic symptoms after excess hypotonic fluid exposure.",
        "estimated_time_seconds": 70,
        "source_id": "study_src_nhs_hyponatremia_labor",
        "source_name": "NHS Guideline: Hyponatremia in Labour",
        "source_type": "Guideline",
        "source_url": "https://wisdom.nhs.wales/health-board-guidelines/",
        "source_excerpt": "Excess hypotonic fluids during labor can lead to symptomatic hyponatremia, especially with oxytocin exposure.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_postpartum_hypertension",
        "item_type": "pearl",
        "topic": "Obstetrics",
        "title": "Quick Pearl: Postpartum Hypertension",
        "key_fact": "Severe postpartum headache with severe-range blood pressure is postpartum preeclampsia until proved otherwise.",
        "clinical_consequence": "The trap is waiting for proteinuria or assuming delivery has already removed the risk.",
        "board_focus": "Postpartum severe hypertension should trigger urgent treatment thinking, not reassurance or delayed workup.",
        "board_rule": "Delivery does not end preeclampsia risk; postpartum severe symptoms still demand urgent management.",
        "board_relevance": "High-risk postpartum trap",
        "estimated_time_seconds": 30,
        "source_id": "study_src_acog_preeclampsia",
        "source_name": "ACOG Practice Bulletin: Gestational Hypertension and Preeclampsia",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2020/06/gestational-hypertension-and-preeclampsia",
        "source_excerpt": "Postpartum preeclampsia can present after discharge and severe symptoms require urgent evaluation and treatment.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
    {
        "id": "pearl_overlap_uti_vs_pyelo",
        "item_type": "pearl",
        "topic": "Infectious gynecology",
        "title": "Quick Pearl: Cystitis vs Pyelonephritis",
        "key_fact": "Dysuria and frequency alone suggest cystitis; fever, flank pain, and CVA tenderness move the question to pyelonephritis.",
        "clinical_consequence": "That distinction changes outpatient versus inpatient thinking, especially in pregnancy.",
        "board_focus": "On boards, the pivot is not just naming the infection but recognizing when systemic features demand escalation.",
        "board_rule": "Urinary symptoms plus systemic or upper tract features should change the site and intensity of management.",
        "board_relevance": "Adjacent-medicine escalation logic",
        "estimated_time_seconds": 30,
        "source_id": "study_src_acog_uti_pregnancy",
        "source_name": "ACOG: Urinary Tract Infections in Pregnancy",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/clinical-consensus/articles/2023/08/urinary-tract-infections-in-pregnant-individuals",
        "source_excerpt": "Upper tract features and systemic illness change UTI management intensity and often require inpatient care in pregnancy.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "review_status": "source_grounded",
        "enabled": True,
    },
]


def _utc_now():
    return datetime.utcnow()


def _difficulty_band_for_level(level):
    try:
        normalized = int(level)
    except (TypeError, ValueError):
        normalized = 3
    if normalized <= 2:
        return "warmup"
    if normalized >= 5:
        return "challenge"
    return "standard"


def _level_for_legacy_band(band):
    normalized = (band or "").strip().lower()
    if normalized == "warmup":
        return 2
    if normalized == "challenge":
        return 5
    return 4


def _infer_question_style(item):
    if not item:
        return "clinical_decision"
    if item.get("item_type") == "pearl":
        return "pearl"

    stem = (item.get("question_stem") or "").lower()
    decision_point = (item.get("decision_point") or "").lower()
    topic = (item.get("topic") or "").lower()

    overlap_markers = (
        "migraine",
        "antibiotic",
        "abscess",
        "urology",
        "hematology",
        "infection",
    )
    diagnosis_markers = (
        "most likely",
        "underlying problem",
        "interpret",
        "diagnosis",
    )
    trap_markers = (
        "known fibroid",
        "postmenopausal bleeding",
        "adnexal mass",
        "despite",
        "normal workup",
    )

    combined = " ".join([stem, decision_point, topic])
    if any(marker in combined for marker in overlap_markers):
        return "overlap"
    if any(marker in combined for marker in diagnosis_markers):
        return "diagnosis_refinement"
    if any(marker in combined for marker in trap_markers):
        return "trap"
    return "clinical_decision"


def _residency_year_from_profile(user_profile):
    if not user_profile:
        return None
    residency_year = (user_profile.get("residency_year") or "").strip().upper()
    if residency_year in DIFFICULTY_LEVEL_DISTRIBUTIONS:
        return residency_year
    training_stage = (user_profile.get("training_stage") or "").strip().lower()
    if training_stage in {"specialist", "fellowship"}:
        return "R6"
    return None


def _difficulty_policy_for_profile(user_profile):
    residency_year = _residency_year_from_profile(user_profile) or "R4"
    distribution = dict(DIFFICULTY_LEVEL_DISTRIBUTIONS.get(residency_year, DIFFICULTY_LEVEL_DISTRIBUTIONS["R4"]))
    min_level, max_level = DIFFICULTY_LEVEL_BOUNDS.get(residency_year, (3, 5))
    baseline_level = max(distribution, key=distribution.get)
    return {
        "residency_year": residency_year,
        "distribution": distribution,
        "baseline_level": baseline_level,
        "min_level": min_level,
        "max_level": max_level,
    }


def _distribution_for_level(level):
    try:
        normalized = int(level)
    except (TypeError, ValueError):
        normalized = 4
    normalized = max(1, min(6, normalized))
    return QUESTION_STYLE_DISTRIBUTIONS[normalized]


def _recent_answer_topic_counts(state, limit=6):
    topics = state.get("recent_answer_topics") or []
    counts = {}
    for topic in topics[-limit:]:
        counts[topic] = counts.get(topic, 0) + 1
    return counts


def _recent_topic_streak(state, topic):
    if not topic:
        return 0
    recent_topics = state.get("recent_answer_topics") or []
    streak = 0
    for recent_topic in reversed(recent_topics):
        if recent_topic != topic:
            break
        streak += 1
    return streak


def _desired_bucket(candidates, distribution, recent_values):
    if not candidates:
        return None

    total_recent = len(recent_values)
    counts = {}
    for value in recent_values:
        counts[value] = counts.get(value, 0) + 1

    scored = []
    for candidate in candidates:
        desired = distribution.get(candidate, 0) / 100.0
        actual = (counts.get(candidate, 0) / total_recent) if total_recent else 0
        scored.append((desired - actual, candidate))

    scored.sort(key=lambda entry: (entry[0], distribution.get(entry[1], 0)), reverse=True)
    return scored[0][1]


def _current_difficulty_level(state, policy):
    current_level = state.get("current_difficulty_level")
    if current_level is None:
        current_level = policy["baseline_level"]
    try:
        current_level = int(current_level)
    except (TypeError, ValueError):
        current_level = policy["baseline_level"]
    return max(policy["min_level"], min(policy["max_level"], current_level))


def _target_difficulty_level(state, policy, available_levels):
    if not available_levels:
        return policy["baseline_level"]

    clamped_levels = sorted({level for level in available_levels if policy["min_level"] <= level <= policy["max_level"]})
    if not clamped_levels:
        clamped_levels = sorted(set(available_levels))

    current_level = _current_difficulty_level(state, policy)
    recent_levels = [
        level for level in (state.get("recent_answer_levels") or [])[-LEVEL_HISTORY_WINDOW:]
        if level in clamped_levels
    ]
    desired_level = _desired_bucket(clamped_levels, policy["distribution"], recent_levels) or current_level

    if desired_level > current_level:
        return min(current_level + 1, max(clamped_levels))
    if desired_level < current_level:
        return max(current_level - 1, min(clamped_levels))
    return current_level


def _target_question_style(state, level, available_styles):
    if not available_styles:
        return None

    distribution = _distribution_for_level(level)
    recent_styles = [
        style for style in (state.get("recent_answer_styles") or [])[-STYLE_HISTORY_WINDOW:]
        if style in available_styles
    ]
    return _desired_bucket(sorted(set(available_styles)), distribution, recent_styles)


def _prune_distribution(distribution, allowed_values):
    allowed = set(allowed_values or [])
    if not allowed:
        return {}
    return {key: value for key, value in distribution.items() if key in allowed}


def _pending_reinforcement(state):
    topic = state.get("pending_reinforcement_topic")
    if not topic:
        return None
    return {
        "topic": topic,
        "decision_point": state.get("pending_reinforcement_decision_point"),
        "remaining": int(state.get("pending_reinforcement_remaining") or 0),
    }


def _next_difficulty_level_after_answer(state, policy, answered_levels, answered_results):
    current_level = _current_difficulty_level(state, policy)
    last_promotion_window = answered_results[-PROMOTION_WINDOW:]
    last_demotion_window = answered_results[-DEMOTION_WINDOW:]

    if len(last_promotion_window) == PROMOTION_WINDOW and sum(1 for result in last_promotion_window if result) >= PROMOTION_THRESHOLD:
        return min(policy["max_level"], current_level + 1)

    if len(last_demotion_window) == DEMOTION_WINDOW and sum(1 for result in last_demotion_window if not result) >= DEMOTION_THRESHOLD:
        return max(policy["min_level"], current_level - 1)

    return current_level


def ensure_study_content_seed():
    now = _utc_now()
    for item in STUDY_SEED_ITEMS:
        payload = dict(item)
        payload["updated_at"] = now
        study_content_collection.update_one(
            {"id": item["id"]},
            {
                "$set": payload,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )


def _normalize_study_item(item):
    if not item:
        return None

    normalized = dict(item)
    normalized.update(SEED_ITEM_METADATA_OVERRIDES.get(normalized.get("id"), {}))
    normalized["review_status"] = normalized.get("review_status") or "source_grounded"
    normalized["difficulty_level"] = int(
        normalized.get("difficulty_level")
        or _level_for_legacy_band(normalized.get("difficulty_band"))
    )
    normalized["question_style"] = normalized.get("question_style") or _infer_question_style(normalized)
    normalized["difficulty_band"] = _difficulty_band_for_level(normalized["difficulty_level"])

    if normalized.get("item_type") == "mcq":
        normalized["correct_answer_key"] = (
            normalized.get("correct_answer_key")
            or normalized.get("correct_option")
        )
        explanation = (normalized.get("explanation") or normalized.get("short_explanation") or "").strip()
        explanation = re.sub(r"^(Correct|Not quite)\.\s*", "", explanation).strip()
        normalized["explanation"] = explanation
        normalized["exam_clue"] = (
            normalized.get("exam_clue")
            or normalized.get("key_clue")
            or normalized.get("key_takeaway")
            or normalized.get("subtopic")
            or normalized.get("topic")
        )
        normalized["board_takeaway"] = (
            normalized.get("board_takeaway")
            or normalized.get("board_rule")
            or normalized.get("key_takeaway")
            or explanation
        )
        normalized["decision_point"] = (
            normalized.get("decision_point")
            or normalized.get("subtopic")
            or normalized.get("topic")
        )
        normalized["tempting_wrong_option"] = normalized.get("tempting_wrong_option")
        normalized["tempting_wrong_reason"] = normalized.get("tempting_wrong_reason")

    if normalized.get("item_type") == "pearl":
        existing_bullets = [bullet for bullet in (normalized.get("bullets") or []) if bullet]
        key_fact = (normalized.get("key_fact") or "").strip()
        clinical_consequence = (
            normalized.get("clinical_consequence")
            or normalized.get("clinical_implication")
            or normalized.get("clinical_meaning")
            or ""
        ).strip()
        board_focus = (
            normalized.get("board_focus")
            or normalized.get("board_takeaway")
            or normalized.get("board_rule")
            or ""
        ).strip()

        if not key_fact and existing_bullets:
            key_fact = existing_bullets[0]
        if not clinical_consequence and len(existing_bullets) > 1:
            clinical_consequence = existing_bullets[1]
        if not board_focus and len(existing_bullets) > 2:
            board_focus = existing_bullets[2]

        normalized["key_fact"] = key_fact
        normalized["clinical_consequence"] = clinical_consequence
        normalized["clinical_meaning"] = clinical_consequence
        normalized["board_focus"] = board_focus
        normalized["board_takeaway"] = normalized.get("board_takeaway") or board_focus
        normalized["bullets"] = [line for line in [key_fact, clinical_consequence, board_focus] if line]
    return normalized


def _default_state(session_id):
    now = _utc_now()
    return {
        "session_id": session_id,
        "last_opened_at": None,
        "last_studied_topic": None,
        "last_card_clicked": None,
        "last_interaction_type": None,
        "last_incomplete_item_id": None,
        "last_incomplete_item_type": None,
        "last_active_item_id": None,
        "last_active_item_type": None,
        "topics_seen": [],
        "topics_correct_count": {},
        "topics_incorrect_count": {},
        "recent_mistake_topics": [],
        "recent_topic_history": [],
        "recent_answer_topics": [],
        "recent_answer_results": [],
        "recent_answer_levels": [],
        "recent_answer_styles": [],
        "cards_shown_history": [],
        "cards_clicked_history": [],
        "recent_study_item_history": [],
        "current_difficulty_level": None,
        "pending_reinforcement_topic": None,
        "pending_reinforcement_decision_point": None,
        "pending_reinforcement_remaining": 0,
        "created_at": now,
        "updated_at": now,
    }


def _load_state(session_id):
    state = study_user_state_collection.find_one({"session_id": session_id})
    if state:
        return state
    state = _default_state(session_id)
    study_user_state_collection.insert_one(state)
    return state


def _trim_history(items, max_items=12):
    return items[-max_items:]


def _save_state(session_id, updates):
    updates["updated_at"] = _utc_now()
    insert_defaults = _default_state(session_id)
    for key in list(updates.keys()):
        insert_defaults.pop(key, None)
    study_user_state_collection.update_one(
        {"session_id": session_id},
        {"$set": updates, "$setOnInsert": insert_defaults},
        upsert=True,
    )


def _recent_study_exclude_ids(state, max_items=12):
    return set((state.get("recent_study_item_history") or [])[-max_items:])


def _record_studied_item(state, item_id):
    history = list(state.get("recent_study_item_history") or [])
    history.append(item_id)
    return _trim_history(history, 24)


def _get_items(item_type=None, topic=None, exclude_ids=None):
    query = {"approved_for_stage_b": True, "enabled": True}
    if item_type:
        query["item_type"] = item_type
    if topic:
        query["topic"] = topic
    items = [_normalize_study_item(item) for item in study_content_collection.find(query, {"_id": 0})]
    if exclude_ids:
        items = [item for item in items if item["id"] not in exclude_ids]
    return items


def _rng_for_session(session_id, salt):
    seed = f"{session_id}:{salt}:{_utc_now().strftime('%Y-%m-%d')}"
    return Random(seed)


def _pick_item(session_id, candidates, salt):
    if not candidates:
        return None
    rng = _rng_for_session(session_id, salt)
    return candidates[rng.randrange(len(candidates))]


def _difficulty_rank(item):
    return int(item.get("difficulty_level") or _level_for_legacy_band(item.get("difficulty_band")))


def _topic_seen_count(state, topic):
    if not topic:
        return 0
    topics_seen = state.get("topics_seen") or []
    return sum(1 for seen_topic in topics_seen if seen_topic == topic)


def _recent_unique_topics(state, limit=4):
    recent_topics = state.get("recent_topic_history") or []
    deduped = []
    seen = set()
    for topic in reversed(recent_topics):
        if not topic or topic in seen:
            continue
        seen.add(topic)
        deduped.append(topic)
        if len(deduped) >= limit:
            break
    return set(deduped)


OBSTETRIC_ENTRY_TOPICS = {
    "Preeclampsia",
    "PPH",
    "CTG",
    "PPROM",
}


def _is_gyne_topic(topic):
    return bool(topic) and topic not in OBSTETRIC_ENTRY_TOPICS


def _topic_family(topic):
    if topic in OBSTETRIC_ENTRY_TOPICS:
        return "obstetrics"
    if topic == "Fertility":
        return "fertility"
    if topic in {"Gynecologic oncology", "Cervical screening"}:
        return "gynecologic_oncology"
    return "gynecology"


def _recent_unique_families(state, limit=4):
    recent_topics = state.get("recent_topic_history") or []
    deduped = []
    seen = set()
    for topic in reversed(recent_topics):
        family = _topic_family(topic)
        if not family or family in seen:
            continue
        seen.add(family)
        deduped.append(family)
        if len(deduped) >= limit:
            break
    return set(deduped)


def _coverage_first_candidates(candidates, state, used_topics=None):
    if not candidates:
        return []

    used_topics = set(used_topics or set())
    recent_topics = _recent_unique_topics(state, limit=4)

    fresh_topic_candidates = [
        item for item in candidates
        if item.get("topic") not in used_topics and item.get("topic") not in recent_topics
    ]
    if fresh_topic_candidates:
        return fresh_topic_candidates

    unused_in_card_candidates = [item for item in candidates if item.get("topic") not in used_topics]
    if unused_in_card_candidates:
        return unused_in_card_candidates

    return candidates


def _family_first_candidates(candidates, state, used_topics=None, used_families=None):
    candidates = _coverage_first_candidates(candidates, state, used_topics=used_topics)
    if not candidates:
        return []

    used_families = set(used_families or set())
    recent_families = _recent_unique_families(state, limit=4)

    fresh_family_candidates = [
        item for item in candidates
        if _topic_family(item.get("topic")) not in used_families
        and _topic_family(item.get("topic")) not in recent_families
    ]
    if fresh_family_candidates:
        return fresh_family_candidates

    unused_family_candidates = [
        item for item in candidates
        if _topic_family(item.get("topic")) not in used_families
    ]
    if unused_family_candidates:
        return unused_family_candidates

    return candidates


def _selection_score(
    item,
    state,
    preferred_topic=None,
    preferred_item_type=None,
    preferred_difficulty_level=None,
    preferred_question_style=None,
    reinforcement=None,
):
    score = 0
    topic = item.get("topic")

    if preferred_item_type and item.get("item_type") == preferred_item_type:
        score += 30
    if preferred_topic and item.get("topic") == preferred_topic:
        score += 28
    if preferred_difficulty_level is not None:
        item_level = int(item.get("difficulty_level") or 0)
        distance = abs(item_level - int(preferred_difficulty_level))
        score += max(0, 34 - (distance * 10))
    if preferred_question_style and item.get("question_style") == preferred_question_style:
        score += 20
    if reinforcement:
        if topic == reinforcement.get("topic"):
            score += 26
        elif reinforcement.get("decision_point") and item.get("decision_point") == reinforcement.get("decision_point"):
            score += 14

    if item.get("item_type") == "mcq":
        score += 18
        if item.get("decision_point"):
            score += 10
        if item.get("exam_clue"):
            score += 8
        score += _difficulty_rank(item) * 5
    else:
        score += 8

    source_excerpt = (item.get("source_excerpt") or "").strip()
    if source_excerpt:
        score += 4
    if item.get("last_reviewed_at"):
        score += 3
    if item.get("review_status") == "source_grounded":
        score += 4

    recent_topics = state.get("recent_topic_history") or []
    if topic in recent_topics[-2:]:
        score -= 8
    if topic in recent_topics[-4:]:
        score -= 4

    if _recent_topic_streak(state, topic) >= MAX_CONSECUTIVE_TOPIC_REPEATS:
        score -= 25

    topic_seen_count = _topic_seen_count(state, topic)
    if topic_seen_count == 0:
        score += 10
    elif topic_seen_count == 1:
        score += 4
    elif topic_seen_count >= 4:
        score -= min(10, (topic_seen_count - 3) * 2)

    cards_shown_history = state.get("cards_shown_history") or []
    if item.get("id") in cards_shown_history[-6:]:
        score -= 10

    cards_clicked_history = state.get("cards_clicked_history") or []
    if item.get("id") in cards_clicked_history[-6:]:
        score -= 12

    recent_mistakes = state.get("recent_mistake_topics") or []
    if item.get("topic") and item.get("topic") in recent_mistakes[-3:]:
        score += 12

    return score


def _pick_best_item(
    session_id,
    candidates,
    salt,
    state,
    preferred_topic=None,
    preferred_item_type=None,
    preferred_difficulty_level=None,
    preferred_question_style=None,
    reinforcement=None,
):
    if not candidates:
        return None
    scored = []
    for item in candidates:
        score = _selection_score(
            item,
            state,
            preferred_topic=preferred_topic,
            preferred_item_type=preferred_item_type,
            preferred_difficulty_level=preferred_difficulty_level,
            preferred_question_style=preferred_question_style,
            reinforcement=reinforcement,
        )
        scored.append((score, item))

    if not scored:
        return None

    scored.sort(key=lambda entry: entry[0], reverse=True)
    top_score = scored[0][0]
    shortlist = [item for score, item in scored if score >= top_score - 4]
    return _pick_item(session_id, shortlist, salt)


def _title_for_dynamic(item, has_history):
    topic = item.get("topic", "Topic")
    if has_history:
        if len(topic) <= 18:
            return f"Revisit {topic}"
        return f"Back to {topic}"
    return "Recommended Topic"


def _subtitle_for_dynamic(has_history):
    return "Quick revisit"


def _source_payload(item):
    return [
        {
            "source_id": "E1",
            "title": item["source_name"],
            "url": item["source_url"],
            "source_type": item["source_type"],
            "updated_at": item.get("last_reviewed_at"),
        }
    ]


def _normalize_text(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _option_text_by_key(item, option_key):
    if not option_key:
        return None
    normalized_key = str(option_key).upper()
    for option in item.get("options", []):
        if option.get("key", "").upper() == normalized_key:
            return option.get("text")
    return None


def _mcq_takeaway_text(item):
    takeaway = (item.get("board_takeaway") or "").strip()
    if takeaway:
        return takeaway
    exam_clue = (item.get("exam_clue") or "").strip()
    if exam_clue:
        return exam_clue
    return ""


def _build_mcq_feedback_reply(item, correct, selected_option=None):
    status = "Correct." if correct else "Incorrect."
    correct_key = item.get("correct_answer_key")
    correct_text = _option_text_by_key(item, correct_key)
    lines = [status]
    if correct_key and correct_text:
        lines.append(f"Best answer: {correct_key}: {correct_text}")

    explanation = (item.get("explanation") or "").strip()
    if explanation:
        lines.append(f"Why: {explanation}")

    takeaway = _mcq_takeaway_text(item)
    if takeaway:
        lines.append(f"Takeaway: {takeaway}")

    why_not_key = None
    why_not_reason = None
    tempting_wrong_key = item.get("tempting_wrong_option")
    tempting_wrong_reason = item.get("tempting_wrong_reason")
    if correct:
        why_not_key = tempting_wrong_key
        why_not_reason = tempting_wrong_reason
    else:
        selected_text = _option_text_by_key(item, selected_option)
        if selected_option and selected_text:
            why_not_key = selected_option
            if selected_option == tempting_wrong_key and tempting_wrong_reason:
                why_not_reason = tempting_wrong_reason
            else:
                why_not_reason = "It does not fit the main exam clue as well as the best answer."

    if why_not_key and why_not_reason:
        lines.append(f"Why not {why_not_key}: {why_not_reason}")

    return "\n".join(lines)


def _board_rule_text(item):
    rule = (item.get("board_takeaway") or item.get("board_rule") or "").strip()
    if rule:
        return rule
    if item.get("item_type") == "pearl" and item.get("bullets"):
        return " ".join(item["bullets"][:2])
    return item.get("key_takeaway", "").strip()


def _build_mcq_explain_reply(item, state):
    correct_key = item.get("correct_answer_key")
    correct_text = _option_text_by_key(item, correct_key)
    selected_key = (state.get("last_answered_option") or "").upper() or None
    selected_text = _option_text_by_key(item, selected_key)
    answered_correctly = state.get("last_answer_correct")
    lines = []

    opening = "Why this answer:"
    if answered_correctly is True:
        opening = "Why that answer is right:"
    elif answered_correctly is False:
        opening = "Why your answer was off:"
    lines.append(opening)

    if correct_key and correct_text:
        lines.append(f"Best answer: {correct_key}: {correct_text}")

    explanation = (item.get("explanation") or "").strip()
    if explanation:
        lines.append(f"Why: {explanation}")

    takeaway = _mcq_takeaway_text(item)
    if takeaway:
        lines.append(f"Takeaway: {takeaway}")

    why_not_key = None
    why_not_reason = None
    tempting_wrong_key = item.get("tempting_wrong_option")
    tempting_wrong_reason = item.get("tempting_wrong_reason")
    if answered_correctly is False and selected_key and selected_text:
        why_not_key = selected_key
        if selected_key == tempting_wrong_key and tempting_wrong_reason:
            why_not_reason = tempting_wrong_reason
        else:
            why_not_reason = "It does not match the main clue as well as the best answer."
    elif tempting_wrong_key and tempting_wrong_reason:
        why_not_key = tempting_wrong_key
        why_not_reason = tempting_wrong_reason

    if why_not_key and why_not_reason:
        lines.append(f"Why not {why_not_key}: {why_not_reason}")

    return "\n".join(lines)


def _get_active_item(session_id):
    state = _load_state(session_id)
    item_id = state.get("last_incomplete_item_id") or state.get("last_active_item_id")
    if not item_id:
        return None, state
    item = _normalize_study_item(study_content_collection.find_one({"id": item_id, "enabled": True}, {"_id": 0}))
    return item, state


def _build_card(card_id, card_type, item, title, subtitle, cta):
    return {
        "id": card_id,
        "type": card_type,
        "title": title,
        "subtitle": subtitle,
        "cta": cta,
        "content_item_id": item["id"],
        "topic": item["topic"],
    }


def _pick_targeted_item(
    session_id,
    state,
    candidates,
    salt,
    preferred_item_type=None,
    preferred_topic=None,
    preferred_difficulty_level=None,
    preferred_question_style=None,
    reinforcement=None,
):
    if not candidates:
        return None
    return _pick_best_item(
        session_id,
        candidates,
        salt,
        state,
        preferred_topic=preferred_topic,
        preferred_item_type=preferred_item_type,
        preferred_difficulty_level=preferred_difficulty_level,
        preferred_question_style=preferred_question_style,
        reinforcement=reinforcement,
    )


def get_idle_study_cards(session_id):
    ensure_study_content_seed()
    state = _load_state(session_id)
    user_profile = get_user_profile(session_id)
    policy = _difficulty_policy_for_profile(user_profile)
    recent_topics = state.get("recent_topic_history") or []
    recent_exclude_ids = _recent_study_exclude_ids(state)
    reinforcement = _pending_reinforcement(state)
    preferred_topic = (reinforcement or {}).get("topic") or (recent_topics[-1] if recent_topics else None)

    used_ids = set()
    cards = []

    mcq_pool = _get_items(item_type="mcq", exclude_ids=recent_exclude_ids) or _get_items(item_type="mcq")
    available_levels = sorted({item.get("difficulty_level") for item in mcq_pool if item.get("difficulty_level")})
    target_level = _target_difficulty_level(state, policy, available_levels)
    available_styles = {item.get("question_style") for item in mcq_pool if item.get("question_style")}
    style_distribution = _prune_distribution(_distribution_for_level(target_level), available_styles)
    target_style = _target_question_style(state, target_level, style_distribution.keys())

    practice_candidates = [
        item for item in mcq_pool
        if item["id"] not in used_ids and item.get("difficulty_level", 0) <= target_level
    ]
    practice_item = _pick_targeted_item(
        session_id,
        state,
        practice_candidates,
        "practice",
        preferred_item_type="mcq",
        preferred_topic=preferred_topic,
        preferred_difficulty_level=target_level,
        preferred_question_style=target_style,
        reinforcement=reinforcement,
    )
    if practice_item:
        used_ids.add(practice_item["id"])
        cards.append(_build_card("practice_card", "practice", practice_item, "Quick MCQ", "1-min practice", "Start"))

    challenge_level = min(policy["max_level"], target_level + 1)
    challenge_style = _target_question_style(state, challenge_level, available_styles)
    challenge_candidates = [
        item for item in mcq_pool
        if item["id"] not in used_ids and item.get("difficulty_level", 0) >= max(target_level, challenge_level - 1)
    ]
    challenge_item = _pick_targeted_item(
        session_id,
        state,
        challenge_candidates,
        "challenge",
        preferred_item_type="mcq",
        preferred_topic=preferred_topic,
        preferred_difficulty_level=challenge_level,
        preferred_question_style=challenge_style,
    )
    if challenge_item:
        used_ids.add(challenge_item["id"])
        cards.append(_build_card("dynamic_card", "dynamic", challenge_item, _title_for_dynamic(challenge_item, bool(preferred_topic)), "Push a bit harder", "Continue"))

    pearl_pool = _get_items(item_type="pearl", exclude_ids=used_ids | recent_exclude_ids) or _get_items(item_type="pearl", exclude_ids=used_ids)
    pearl_pool = _family_first_candidates(pearl_pool, state)
    pearl_item = _pick_targeted_item(
        session_id,
        state,
        pearl_pool,
        "pearl",
        preferred_item_type="pearl",
        preferred_topic=preferred_topic,
        preferred_difficulty_level=min(target_level, policy["max_level"]),
        preferred_question_style="pearl",
    )
    if pearl_item:
        used_ids.add(pearl_item["id"])
        cards.append(_build_card("pearl_card", "pearl", pearl_item, "Quick Pearl", "Quick takeaway", "Open"))

    if len(cards) < 3:
        fallback_pool = _get_items(exclude_ids=used_ids | recent_exclude_ids) or _get_items(exclude_ids=used_ids)
        scored_fallback = sorted(
            fallback_pool,
            key=lambda item: _selection_score(item, state, preferred_difficulty_level=target_level),
            reverse=True,
        )
        for item in scored_fallback:
            if item["id"] in used_ids:
                continue
            used_ids.add(item["id"])
            cards.append(
                _build_card(
                    f"fallback_{item['id']}",
                    "practice" if item["item_type"] == "mcq" else "pearl",
                    item,
                    "Quick MCQ" if item["item_type"] == "mcq" else "Quick Pearl",
                    "1-min practice" if item["item_type"] == "mcq" else "Quick takeaway",
                    "Start" if item["item_type"] == "mcq" else "Open",
                )
            )
            if len(cards) == 3:
                break

    shown_history = _trim_history((state.get("cards_shown_history") or []) + [card["content_item_id"] for card in cards], 18)
    _save_state(
        session_id,
        {
            "last_opened_at": _utc_now(),
            "cards_shown_history": shown_history,
        },
    )
    log_event("study_cards_impression", session_id, {"card_ids": [card["content_item_id"] for card in cards]})
    return {"cards": cards[:3]}


def _build_study_item_payload(item):
    payload = {
        "item_type": item["item_type"],
        "content_item_id": item["id"],
        "topic": item["topic"],
    }
    if item["item_type"] == "mcq":
        payload.update(
            {
                "question_stem": item["question_stem"],
                "options": item["options"],
                "estimated_time_seconds": item.get("estimated_time_seconds", 60),
                "difficulty_band": item.get("difficulty_band"),
                "difficulty_level": item.get("difficulty_level"),
                "question_style": item.get("question_style"),
                "decision_point": item.get("decision_point"),
            }
        )
    else:
        payload.update(
            {
                "title": item["title"],
                "key_fact": item.get("key_fact"),
                "clinical_consequence": item.get("clinical_consequence"),
                "clinical_meaning": item.get("clinical_meaning") or item.get("clinical_consequence"),
                "board_focus": item.get("board_focus"),
                "bullets": item["bullets"],
                "estimated_time_seconds": item.get("estimated_time_seconds", 30),
                "difficulty_level": item.get("difficulty_level"),
                "actions": [
                    {"action": "quiz_me", "label": "Quiz me on this"},
                    {"action": "another_pearl", "label": "Another pearl"},
                    {"action": "show_source", "label": "Show source"},
                    {"action": "quick_recap", "label": "Quick recap"},
                ],
            }
        )
    return payload


def open_study_card(session_id, content_item_id, card_type):
    ensure_study_content_seed()
    item = _normalize_study_item(study_content_collection.find_one({"id": content_item_id, "enabled": True}, {"_id": 0}))
    if not item:
        return {"reply": "I don’t have an approved study item for that yet."}

    state = _load_state(session_id)
    _save_state(
        session_id,
        {
            "last_card_clicked": card_type,
            "last_interaction_type": item["item_type"],
            "last_incomplete_item_id": item["id"],
            "last_incomplete_item_type": item["item_type"],
            "last_active_item_id": item["id"],
            "last_active_item_type": item["item_type"],
            "last_studied_topic": item["topic"],
            "cards_clicked_history": _trim_history((state.get("cards_clicked_history") or []) + [item["id"]], 18),
            "recent_topic_history": _trim_history((state.get("recent_topic_history") or []) + [item["topic"]], 12),
            "recent_study_item_history": _record_studied_item(state, item["id"]),
        },
    )
    log_event("study_card_clicked", session_id, {"card_type": card_type, "content_item_id": item["id"], "topic": item["topic"]})
    intro = (
        f"Here’s a quick question on {item['topic']}."
        if item["item_type"] == "mcq"
        else f"Here’s a quick pearl on {item['topic']}."
    )
    if item["item_type"] == "pearl":
        log_event("pearl_opened", session_id, {"content_item_id": item["id"], "topic": item["topic"]})
    else:
        log_event("mcq_started", session_id, {"content_item_id": item["id"], "topic": item["topic"]})
    return {"reply": intro, "study_item": _build_study_item_payload(item)}


def answer_mcq(session_id, content_item_id, selected_option):
    item = _normalize_study_item(study_content_collection.find_one({"id": content_item_id, "item_type": "mcq", "enabled": True}, {"_id": 0}))
    if not item:
        return {"reply": "I couldn’t load that question anymore."}

    state = _load_state(session_id)
    user_profile = get_user_profile(session_id)
    policy = _difficulty_policy_for_profile(user_profile)
    topic = item["topic"]
    correct = (selected_option or "").upper() == item["correct_answer_key"]
    correct_counts = dict(state.get("topics_correct_count") or {})
    incorrect_counts = dict(state.get("topics_incorrect_count") or {})
    recent_mistakes = list(state.get("recent_mistake_topics") or [])
    answer_results = list(state.get("recent_answer_results") or [])
    answer_levels = list(state.get("recent_answer_levels") or [])
    answer_styles = list(state.get("recent_answer_styles") or [])
    answer_topics = list(state.get("recent_answer_topics") or [])

    if correct:
        correct_counts[topic] = correct_counts.get(topic, 0) + 1
    else:
        incorrect_counts[topic] = incorrect_counts.get(topic, 0) + 1
        recent_mistakes.append(topic)

    answer_results = _trim_history(answer_results + [correct], DEMOTION_WINDOW)
    answer_levels = _trim_history(answer_levels + [item.get("difficulty_level")], LEVEL_HISTORY_WINDOW)
    answer_styles = _trim_history(answer_styles + [item.get("question_style")], STYLE_HISTORY_WINDOW)
    answer_topics = _trim_history(answer_topics + [topic], DEMOTION_WINDOW)
    next_level = _next_difficulty_level_after_answer(state, policy, answer_levels, answer_results)

    pending_reinforcement_topic = None
    pending_reinforcement_decision_point = None
    pending_reinforcement_remaining = 0
    if not correct:
        pending_reinforcement_topic = topic
        pending_reinforcement_decision_point = item.get("decision_point")
        pending_reinforcement_remaining = 2
    elif state.get("pending_reinforcement_remaining", 0):
        pending_reinforcement_remaining = max(0, int(state.get("pending_reinforcement_remaining") or 0) - 1)
        if pending_reinforcement_remaining > 0:
            pending_reinforcement_topic = state.get("pending_reinforcement_topic")
            pending_reinforcement_decision_point = state.get("pending_reinforcement_decision_point")

    _save_state(
        session_id,
        {
            "last_interaction_type": "mcq_feedback",
            "last_incomplete_item_id": None,
            "last_incomplete_item_type": None,
            "last_active_item_id": item["id"],
            "last_active_item_type": item["item_type"],
            "topics_seen": _trim_history((state.get("topics_seen") or []) + [topic], 30),
            "topics_correct_count": correct_counts,
            "topics_incorrect_count": incorrect_counts,
            "recent_mistake_topics": _trim_history(recent_mistakes, 8),
            "recent_answer_results": answer_results,
            "recent_answer_levels": answer_levels,
            "recent_answer_styles": answer_styles,
            "recent_answer_topics": answer_topics,
            "last_studied_topic": topic,
            "last_answered_option": (selected_option or "").upper(),
            "last_answer_correct": correct,
            "current_difficulty_level": next_level,
            "pending_reinforcement_topic": pending_reinforcement_topic,
            "pending_reinforcement_decision_point": pending_reinforcement_decision_point,
            "pending_reinforcement_remaining": pending_reinforcement_remaining,
        },
    )
    log_event(
        "mcq_answered",
        session_id,
        {
            "content_item_id": item["id"],
            "topic": topic,
            "selected_option": selected_option,
            "correct": correct,
            "difficulty_level": item.get("difficulty_level"),
            "next_difficulty_level": next_level,
            "question_style": item.get("question_style"),
        },
    )
    log_event("mcq_correct" if correct else "mcq_incorrect", session_id, {"content_item_id": item["id"], "topic": topic})

    reply = _build_mcq_feedback_reply(item, correct, (selected_option or "").upper())
    return {
        "reply": reply,
        "study_context_item_id": item["id"],
        "study_followups": [
            {"action": "another_question", "label": "Another question"},
            {"action": "explain_why", "label": "Explain why"},
            {"action": "show_source", "label": "Show source"},
            {"action": "quick_recap", "label": "Give me the rule"},
        ],
    }


def _pick_related_item(session_id, item, item_type, exclude_self=False):
    state = _load_state(session_id)
    user_profile = get_user_profile(session_id)
    policy = _difficulty_policy_for_profile(user_profile)
    exclude_ids = _recent_study_exclude_ids(state)
    if exclude_self:
        exclude_ids.add(item["id"])
    candidates = _get_items(item_type=item_type, topic=item["topic"], exclude_ids=exclude_ids) or _get_items(item_type=item_type, exclude_ids=exclude_ids)
    if not candidates:
        fallback_exclude_ids = {item["id"]} if exclude_self else None
        candidates = _get_items(item_type=item_type, topic=item["topic"], exclude_ids=fallback_exclude_ids) or _get_items(item_type=item_type, exclude_ids=fallback_exclude_ids)
    target_level = _current_difficulty_level(state, policy)
    return _pick_best_item(
        session_id,
        candidates,
        f"{item['id']}:{item_type}",
        state,
        preferred_topic=item.get("topic"),
        preferred_item_type=item_type,
        preferred_difficulty_level=target_level if item_type == "mcq" else item.get("difficulty_level"),
        preferred_question_style=item.get("question_style") if item_type == "mcq" else "pearl",
        reinforcement=_pending_reinforcement(state) if item_type == "mcq" else None,
    )


def handle_study_action(session_id, content_item_id, action):
    item = _normalize_study_item(study_content_collection.find_one({"id": content_item_id, "enabled": True}, {"_id": 0}))
    if not item:
        return {"reply": "I couldn’t find that study item anymore."}
    state = _load_state(session_id)

    log_event("mcq_followup_clicked" if item["item_type"] == "mcq" else "pearl_followup_clicked", session_id, {"content_item_id": item["id"], "action": action})

    if action == "show_source":
        log_event("source_requested", session_id, {"content_item_id": item["id"], "topic": item["topic"]})
        return {
            "reply": None,
            "sources": _source_payload(item),
        }

    if action == "explain_why":
        if item["item_type"] == "mcq":
            return {"reply": _build_mcq_explain_reply(item, state)}
        return {"reply": "Why this pearl matters: " + _board_rule_text(item)}

    if action == "quick_recap":
        return {"reply": "Board rule: " + _board_rule_text(item)}

    if action == "another_question":
        next_item = _pick_related_item(session_id, item, "mcq", exclude_self=True)
        if not next_item:
            return {"reply": "I don’t have another approved question ready on that yet."}
        _save_state(
            session_id,
            {
                "last_incomplete_item_id": next_item["id"],
                "last_incomplete_item_type": next_item["item_type"],
                "last_active_item_id": next_item["id"],
                "last_active_item_type": next_item["item_type"],
                "last_studied_topic": next_item["topic"],
                "recent_study_item_history": _record_studied_item(state, next_item["id"]),
            },
        )
        return {
            "reply": f"Another quick question on {next_item['topic']}.",
            "study_item": _build_study_item_payload(next_item),
        }

    if action == "quiz_me":
        next_item = _pick_related_item(session_id, item, "mcq")
        if not next_item:
            return {"reply": "I don’t have an approved quiz item on that topic yet."}
        _save_state(
            session_id,
            {
                "last_incomplete_item_id": next_item["id"],
                "last_incomplete_item_type": next_item["item_type"],
                "last_active_item_id": next_item["id"],
                "last_active_item_type": next_item["item_type"],
                "last_studied_topic": next_item["topic"],
                "recent_study_item_history": _record_studied_item(state, next_item["id"]),
            },
        )
        return {
            "reply": f"Quick question on {next_item['topic']}.",
            "study_item": _build_study_item_payload(next_item),
        }

    if action == "another_pearl":
        next_item = _pick_related_item(session_id, item, "pearl", exclude_self=True)
        if not next_item:
            return {"reply": "I don’t have another approved pearl ready yet."}
        _save_state(
            session_id,
            {
                "last_incomplete_item_id": next_item["id"],
                "last_incomplete_item_type": next_item["item_type"],
                "last_active_item_id": next_item["id"],
                "last_active_item_type": next_item["item_type"],
                "last_studied_topic": next_item["topic"],
                "recent_study_item_history": _record_studied_item(state, next_item["id"]),
            },
        )
        return {
            "reply": f"Another quick pearl on {next_item['topic']}.",
            "study_item": _build_study_item_payload(next_item),
        }

    return {"reply": "I can keep going on this topic, but I don’t have that action wired yet."}


def _match_mcq_answer(item, user_message):
    normalized = _normalize_text(user_message)
    compact = re.sub(r"[^a-z0-9]", "", normalized)
    if compact in {"a", "b", "c", "d"}:
        return compact.upper()

    for option in item.get("options", []):
        option_key = option["key"].lower()
        option_text = _normalize_text(option["text"])
        if normalized == option_text or normalized in {option_key, f"option {option_key}", f"answer {option_key}"}:
            return option["key"]
        if option_text and option_text in normalized:
            return option["key"]
    return None


def _infer_followup_action(item_type, user_message):
    normalized = _normalize_text(user_message)
    if not normalized:
        return None

    # Only treat short command-like followups as study actions.
    # Longer freeform questions should fall through to the normal clinical chat flow.
    if len(normalized.split()) > 6:
        return None

    source_markers = ("show source", "source", "reference", "show me the source", "מקור", "תראה מקור")
    explain_markers = ("why", "explain", "explain why", "why not", "למה", "תסביר", "תסבירי")
    recap_markers = ("rule", "recap", "summary", "summarize", "quick recap", "הכלל", "סיכום", "בקצרה")
    quiz_markers = ("quiz me", "test me", "ask me", "בחן אותי", "תבחן אותי")
    another_markers = ("another", "another one", "next one", "עוד", "עוד אחת", "עוד שאלה")
    pearl_markers = ("another pearl", "another review", "עוד פנינה")

    if any(marker in normalized for marker in source_markers):
        return "show_source"
    if any(marker in normalized for marker in explain_markers):
        return "explain_why"
    if any(marker in normalized for marker in recap_markers):
        return "quick_recap"
    if item_type == "pearl" and any(marker in normalized for marker in quiz_markers):
        return "quiz_me"
    if item_type == "pearl" and any(marker in normalized for marker in pearl_markers + another_markers):
        return "another_pearl"
    if item_type == "mcq" and any(marker in normalized for marker in another_markers):
        return "another_question"
    return None


def resolve_study_chat_message(session_id, user_message):
    item, _state = _get_active_item(session_id)
    if not item:
        return None

    answer_key = _match_mcq_answer(item, user_message) if item["item_type"] == "mcq" else None
    if answer_key:
        return answer_mcq(session_id, item["id"], answer_key)

    action = _infer_followup_action(item["item_type"], user_message)
    if action:
        return handle_study_action(session_id, item["id"], action)

    return None
