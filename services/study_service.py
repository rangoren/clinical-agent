from datetime import datetime
from random import Random
import re

from db import study_content_collection, study_user_state_collection
from services.logging_service import log_event
from services.profile_service import build_effective_user_profile, get_user_profile


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

STAGE_B_QUESTION_RUBRIC = {
    "advanced_mcq": [
        "At least two options must be clinically reasonable near-miss choices",
        "At least one threshold, timing cue, or measured value must meaningfully change management",
        "At least one competing variable or risk axis must create real tension",
        "Avoid throwaway distractors, absolutes, and obviously impossible management steps",
        "The question should force best-next-step judgment, not simple recall",
    ],
}

ABSOLUTE_DISTRACTOR_MARKERS = (" never ", " always ", " only ", " immediately ", " solely ")
OBVIOUS_WRONG_DISTRACTOR_MARKERS = (
    " immediate hysterectomy",
    " reassure",
    " observation only",
    " no treatment",
    " do nothing",
    " wait for culture",
    " delay all ",
)
HIGH_JUDGMENT_STYLE_NAMES = {"trap", "overlap", "diagnosis_refinement"}
TEMPLATE_FAMILY_HISTORY_WINDOW = 8
SESSION_TARGET_QUESTIONS = {
    "R1": 5,
    "R2": 5,
    "R3": 5,
    "R4": 6,
    "R5": 6,
    "R6": 7,
}
SESSION_DIFFICULTY_RULES = {
    "increase_after_consecutive_correct": 2,
    "decrease_after_consecutive_incorrect": 1,
}
DECISION_FRAME_MARKERS = (
    "best next step",
    "best next move",
    "most appropriate next step",
    "what is the best next step now",
    "what is the most appropriate next step",
)

DIFFICULTY_ENGINE_RULES = (
    {
        "min_target": 1,
        "max_target": 2,
        "required": (),
        "recommended": ("decision_frame", "no_obvious_wrong_distractors"),
    },
    {
        "min_target": 3,
        "max_target": 4,
        "required": ("decision_frame", "no_obvious_wrong_distractors"),
        "recommended": ("near_miss_distractors", "threshold_variable", "conflicting_axes"),
    },
    {
        "min_target": 5,
        "max_target": 5,
        "required": (
            "decision_frame",
            "near_miss_distractors",
            "plausible_option_count",
            "time_progression_or_response",
            "no_obvious_wrong_distractors",
        ),
        "recommended": ("conflicting_axes", "clinical_noise", "tradeoff_axes"),
    },
    {
        "min_target": 6,
        "max_target": 6,
        "required": (
            "decision_frame",
            "near_miss_distractors",
            "near_correct_trap",
            "plausible_option_count",
            "second_best_strength",
            "decision_pressure",
            "conflicting_axes",
            "time_progression_or_response",
            "partial_response_or_trend",
            "no_obvious_wrong_distractors",
        ),
        "recommended": ("tradeoff_axes", "clinical_noise", "management_nuance"),
    },
    {
        "min_target": 7,
        "max_target": 10,
        "required": (
            "decision_frame",
            "near_miss_distractors",
            "near_correct_trap",
            "plausible_option_count",
            "second_best_strength",
            "tradeoff_axes",
            "decision_pressure",
            "conflicting_axes",
            "threshold_variable",
            "dynamic_progression",
            "clinical_noise",
            "no_obvious_wrong_distractors",
            "deep_conflict",
        ),
        "recommended": ("high_ambiguity", "management_nuance", "high_judgment_style", "template_family"),
    },
)

DIFFICULTY_LEVEL_DISTRIBUTIONS = {
    "R1": {1: 70, 2: 20, 3: 10},
    "R2": {2: 50, 3: 30, 1: 20},
    "R3": {4: 60, 3: 40},
    "R4": {5: 45, 4: 35, 6: 20},
    "R5": {6: 55, 5: 30, 4: 15},
    "R6": {6: 85, 5: 15},
}

DIFFICULTY_LEVEL_BOUNDS = {
    "R1": (1, 3),
    "R2": (1, 3),
    "R3": (3, 4),
    "R4": (4, 6),
    "R5": (5, 6),
    "R6": (5, 6),
}

RESIDENCY_YEAR_MAIN_FLOW_RULES = {
    "R1": {
        "enforce_main_flow_gate": True,
        "main_flow_min_target_10": 1,
        "main_flow_max_target_10": 3,
        "required_checks": ("no_obvious_wrong_distractors",),
        "required_any": (("decision_frame", "near_miss_distractors"),),
        "require_difficulty_engine_ready": False,
        "block_disguised_recall": False,
        "blocked_archetypes": (),
        "blocked_template_families": ("response_over_time", "conflicting_risk_axes", "timing_threshold", "borderline_threshold"),
        "min_stage_b_quality_score": 2,
        "min_second_best_strength_score": 0,
        "min_decision_pressure_score": 0,
    },
    "R2": {
        "enforce_main_flow_gate": True,
        "main_flow_min_target_10": 2,
        "main_flow_max_target_10": 4,
        "required_checks": ("decision_frame", "no_obvious_wrong_distractors"),
        "required_any": (("near_miss_distractors", "plausible_option_count"),),
        "require_difficulty_engine_ready": False,
        "block_disguised_recall": False,
        "blocked_archetypes": (),
        "blocked_template_families": ("response_over_time", "conflicting_risk_axes", "timing_threshold"),
        "min_stage_b_quality_score": 4,
        "min_second_best_strength_score": 0,
        "min_decision_pressure_score": 0,
    },
    "R3": {
        "enforce_main_flow_gate": True,
        "main_flow_min_target_10": 4,
        "main_flow_max_target_10": 4,
        "required_checks": ("decision_frame", "near_miss_distractors", "no_obvious_wrong_distractors"),
        "required_any": (("conflicting_axes", "threshold_variable", "dynamic_progression"),),
        "require_difficulty_engine_ready": False,
        "block_disguised_recall": False,
        "blocked_archetypes": (),
        "blocked_template_families": (),
        "min_stage_b_quality_score": 6,
        "min_second_best_strength_score": 0,
        "min_decision_pressure_score": 0,
    },
    "R4": {
        "enforce_main_flow_gate": True,
        "main_flow_min_target_10": 5,
        "main_flow_max_target_10": 6,
        "required_checks": ("decision_frame", "near_miss_distractors", "plausible_option_count", "no_obvious_wrong_distractors"),
        "required_any": (("conflicting_axes", "threshold_variable", "dynamic_progression"),),
        "require_difficulty_engine_ready": False,
        "block_disguised_recall": False,
        "blocked_archetypes": (),
        "blocked_template_families": (),
        "min_stage_b_quality_score": 8,
        "min_second_best_strength_score": 3,
        "min_decision_pressure_score": 0,
    },
    "R5": {
        "enforce_main_flow_gate": True,
        "main_flow_min_target_10": 6,
        "main_flow_max_target_10": 7,
        "required_checks": (
            "decision_frame",
            "near_miss_distractors",
            "plausible_option_count",
            "second_best_strength",
            "conflicting_axes",
            "time_progression_or_response",
            "no_obvious_wrong_distractors",
        ),
        "required_any": (("threshold_variable", "tradeoff_axes", "decision_pressure"),),
        "require_difficulty_engine_ready": True,
        "block_disguised_recall": True,
        "blocked_archetypes": (),
        "blocked_template_families": (),
        "min_stage_b_quality_score": 11,
        "min_second_best_strength_score": 4,
        "min_decision_pressure_score": 0,
    },
    "R6": {
        "enforce_main_flow_gate": True,
        "main_flow_min_target_10": 7,
        "main_flow_max_target_10": 10,
        "required_checks": (
            "decision_frame",
            "near_miss_distractors",
            "near_correct_trap",
            "plausible_option_count",
            "second_best_strength",
            "tradeoff_axes",
            "decision_pressure",
            "high_decision_pressure",
            "conflicting_axes",
            "time_progression_or_response",
            "partial_response_or_trend",
            "clinical_noise",
            "no_obvious_wrong_distractors",
            "deep_conflict",
        ),
        "required_any": (("threshold_variable", "dynamic_progression"),),
        "require_difficulty_engine_ready": True,
        "block_disguised_recall": True,
        "blocked_archetypes": ("contraindication_context",),
        "blocked_template_families": (),
        "min_stage_b_quality_score": 14,
        "min_second_best_strength_score": 5,
        "min_decision_pressure_score": 6,
    },
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
        "clinical_decision": 35,
        "trap": 35,
        "overlap": 20,
        "diagnosis_refinement": 10,
    },
}

SEED_ITEM_METADATA_OVERRIDES = {
    "mcq_preeclampsia_delivery": {"difficulty_level": 6, "question_style": "clinical_decision"},
    "mcq_pph_first_step": {"difficulty_level": 1, "question_style": "clinical_decision"},
    "mcq_ctg_late_decels": {"difficulty_level": 6, "question_style": "diagnosis_refinement"},
    "mcq_pprom_antibiotics": {"difficulty_level": 6, "question_style": "clinical_decision"},
    "pearl_pph_atony": {"difficulty_level": 2, "question_style": "pearl"},
    "pearl_ctg_core": {"difficulty_level": 3, "question_style": "pearl"},
    "pearl_cervix_hsil": {"difficulty_level": 4, "question_style": "pearl"},
    "mcq_contraception_migraine_aura": {"difficulty_level": 6, "question_style": "overlap"},
    "mcq_pid_inpatient_toa": {"difficulty_level": 4, "question_style": "clinical_decision"},
    "pearl_aub_palm_coein": {"difficulty_level": 3, "question_style": "pearl"},
    "pearl_emergency_contraception": {"difficulty_level": 3, "question_style": "pearl"},
    "pearl_endometriosis_first_line": {"difficulty_level": 4, "question_style": "pearl"},
    "pearl_menopause_bleeding": {"difficulty_level": 4, "question_style": "pearl"},
    "mcq_unexplained_infertility_escalation": {"difficulty_level": 6, "question_style": "clinical_decision"},
    "mcq_adnexal_mass_referral": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_postmenopausal_bleeding_biopsy": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_aub_age45_sampling": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_aub_sampling_r3": {"difficulty_level": 4, "question_style": "clinical_decision"},
    "pearl_adnexal_mass_referral": {"difficulty_level": 5, "question_style": "pearl"},
    "pearl_postmenopausal_bleeding_workup": {"difficulty_level": 5, "question_style": "pearl"},
    "mcq_uti_nonpregnant_first_line": {"difficulty_level": 5, "question_style": "overlap"},
    "mcq_postpartum_endometritis_antibiotics": {"difficulty_level": 6, "question_style": "clinical_decision"},
    "mcq_postpartum_pe_headache": {"difficulty_level": 6, "question_style": "trap"},
    "mcq_vte_postpartum_estrogen": {"difficulty_level": 6, "question_style": "overlap"},
    "mcq_vte_postpartum_estrogen_r5": {"difficulty_level": 6, "question_style": "overlap"},
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
IDLE_CARDS_CACHE_TTL_SECONDS = 300
SOURCE_DATE_PLACEHOLDERS = {"2025-01-01"}


STUDY_SEED_ITEMS = [
    {
        "id": "mcq_preeclampsia_delivery",
        "item_type": "mcq",
        "topic": "Preeclampsia",
        "subtopic": "Timing of delivery",
        "question_stem": "A 33+5-week patient with preeclampsia has persistent blood pressures around 156-162/102-108, platelets 118,000, creatinine 0.9, reassuring fetal testing, and completed betamethasone 24 hours ago. She has no headache, no visual symptoms, and urine output is adequate. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Continue inpatient expectant management with frequent maternal-fetal reassessment and treat severe-range pressures if they recur"},
            {"key": "B", "text": "Proceed to delivery now because severe features before 34 weeks should never be managed expectantly"},
            {"key": "C", "text": "Discharge for home blood pressure monitoring because laboratory values are still reassuring"},
            {"key": "D", "text": "Delay any decision until a full second course of corticosteroids has been completed"},
        ],
        "correct_answer_key": "A",
        "explanation": "Before 34 weeks, carefully selected patients with severe disease but stable maternal-fetal status may undergo inpatient expectant management. The nuance is that intermittently severe pressures alone do not force delivery if they respond and there is no evolving end-organ or fetal deterioration.",
        "exam_clue": "Severe preeclampsia before 34 weeks but currently stable maternal-fetal status",
        "board_takeaway": "Before 34 weeks, severe preeclampsia can still be managed expectantly only in a tightly monitored inpatient setting when both maternal and fetal conditions remain stable.",
        "decision_point": "Choose expectant management versus delivery in early severe preeclampsia",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Immediate delivery is a reasonable reflex, but the gestational age and currently stable maternal-fetal picture make expectant inpatient management the better choice for now.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 9,
        "ambiguity_level": 8,
        "threshold_variable": "<34 weeks with stable maternal-fetal status",
        "threshold_type": "gestational_age_plus_stability",
        "conflicting_axes": ["prematurity risk vs maternal severe disease", "intermittent severe blood pressure vs reassuring labs/testing"],
        "management_nuance": ["expectant inpatient management", "delivery trigger thresholds", "steroid timing should not dominate the decision"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["completed betamethasone 24 hours ago", "creatinine remains normal"],
        "dynamic_progression": ["completed betamethasone 24 hours ago", "blood pressures have been persistently elevated but without progressive end-organ change"],
        "template_family": "timing_threshold",
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
        "question_stem": "Immediately after vaginal birth, heavy bleeding is attributed to uterine atony. What is the best next medication step now?",
        "options": [
            {"key": "A", "text": "Oxytocin"},
            {"key": "B", "text": "Methylergonovine if bleeding continues after initial oxytocin and there is no hypertension"},
            {"key": "C", "text": "Tranexamic acid early as an adjunct while uterotonic treatment is being started"},
            {"key": "D", "text": "Carboprost if atony persists after first-line therapy and there is no asthma"},
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
        "decision_frame": "best_next_step",
        "difficulty_target_10": 2,
        "ambiguity_level": 2,
        "near_miss_options": ["A", "B"],
        "plausible_option_count": 2,
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
        "question_stem": "A laboring nulliparous patient has recurrent decelerations that begin after the peak of contractions and return to baseline after the contraction ends. Baseline variability is still moderate, membranes were ruptured 2 hours ago, and cervical exam is unchanged at 6 cm over the last 4 hours. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Initiate intrauterine resuscitative measures for suspected uteroplacental insufficiency and reassess the tracing promptly"},
            {"key": "B", "text": "Proceed directly to operative delivery because any late decelerations require immediate delivery even with preserved variability"},
            {"key": "C", "text": "Treat the tracing as cord compression and perform amnioinfusion as the first maneuver"},
            {"key": "D", "text": "Continue unchanged management because moderate variability excludes clinically important hypoxemia"},
        ],
        "correct_answer_key": "A",
        "explanation": "The question is no longer just about labeling a tracing. Recurrent late decelerations point to uteroplacental insufficiency, but preserved variability means the next step is intrauterine resuscitation and reassessment rather than automatic immediate delivery.",
        "exam_clue": "Recurrent late decelerations with preserved variability",
        "board_takeaway": "CTG questions at higher level are about what to do with the pattern, not only what the pattern is called.",
        "decision_point": "Choose resuscitation versus immediate delivery for recurrent late decelerations",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Immediate delivery is the near-miss because the tracing is concerning, but preserved variability means there is still room for resuscitative intervention first.",
        "estimated_time_seconds": 80,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 8,
        "ambiguity_level": 8,
        "threshold_variable": "Late decelerations with preserved moderate variability",
        "threshold_type": "tracing_pattern_plus_reserve",
        "conflicting_axes": ["recurrent late decelerations vs preserved variability", "lack of labor progress vs no terminal tracing yet"],
        "management_nuance": ["resuscitate first vs deliver now", "pattern interpretation should change action", "avoid treating late decelerations like variables"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["membranes ruptured 2 hours ago", "cervix unchanged at 6 cm for 4 hours"],
        "dynamic_progression": ["recurrent decelerations are ongoing", "labor progress has stalled over the last 4 hours"],
        "template_family": "conflicting_risk_axes",
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
        "question_stem": "A 30+2-week patient with PPROM has been inpatient for 18 hours. She has no uterine tenderness, no fetal tachycardia, reassuring fetal testing, WBC 14,800, and mild irregular contractions that have now settled after hydration. She asks whether continued observation alone is enough if the tracing stays normal. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Begin latency antibiotics as part of ongoing expectant inpatient management"},
            {"key": "B", "text": "Proceed to delivery now because the white count is elevated and contractions were present earlier"},
            {"key": "C", "text": "Use prolonged maintenance tocolysis to gain the greatest latency benefit before giving antibiotics"},
            {"key": "D", "text": "Continue observation alone because reassuring fetal testing makes antibiotics unnecessary"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is not just a recall question about antibiotics. The patient has borderline noise that could push some learners toward delivery, but the absence of convincing infection or fetal compromise keeps her in the expectant-management pathway, where latency antibiotics remain part of the optimal next step.",
        "exam_clue": "PPROM before 34 weeks with borderline but not convincing infection signals",
        "board_takeaway": "In PPROM, mild noise such as leukocytosis or settled contractions should not automatically override the expectant-management bundle when infection and fetal compromise are not established.",
        "decision_point": "Choose expectant management with latency antibiotics versus premature delivery in PPROM",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Earlier contractions and modest leukocytosis create tension, but without convincing clinical infection or fetal compromise they do not yet mandate delivery.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 8,
        "ambiguity_level": 8,
        "threshold_variable": "30+2 weeks without clear infection or fetal compromise",
        "threshold_type": "gestational_age_plus_clinical_trajectory",
        "conflicting_axes": ["preterm latency benefit vs concern for evolving infection", "reassuring fetal status vs earlier contractions and leukocytosis"],
        "management_nuance": ["expectant bundle vs delivery", "latency antibiotics vs observation alone", "do not overcall infection from one noisy variable"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["WBC 14,800", "irregular contractions that settled after hydration"],
        "dynamic_progression": ["18 hours into inpatient PPROM management", "earlier contractions have improved"],
        "template_family": "conflicting_risk_axes",
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
        "question_stem": "A 29-year-old 8 weeks postpartum is no longer breastfeeding and wants the patch because she struggles with daily pills. She has migraine with aura about twice a year, normotension, and no history of thrombosis. She asks whether the low frequency of aura makes combined hormonal contraception reasonable. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Avoid estrogen-containing contraception and recommend an effective non-estrogen option even though the aura is infrequent"},
            {"key": "B", "text": "Start the patch because she is young, normotensive, and no longer in the highest-risk postpartum window"},
            {"key": "C", "text": "Trial a combined pill with close follow-up because her aura is rare and she strongly prefers estrogen"},
            {"key": "D", "text": "Delay all hormonal contraception until neurology reassesses the migraine pattern"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is not just a one-line contraindication test because several details soften the stem: young age, normotension, remote postpartum status, and patient preference. The nuance is that aura still outweighs those reassuring features, so the best move is to redirect her to a non-estrogen option rather than negotiate a trial of estrogen.",
        "exam_clue": "Migraine with aura despite otherwise favorable contraceptive profile",
        "board_takeaway": "High-level contraception questions should force you to weigh patient preference and reassuring context against the one risk feature that still changes the method choice.",
        "decision_point": "Choose whether patient preference and reassuring context can overcome an estrogen contraindication",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "The rest of the vignette is deliberately reassuring, but aura remains the deciding feature that keeps estrogen off the table.",
        "estimated_time_seconds": 80,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 5,
        "ambiguity_level": 6,
        "threshold_variable": "Migraine with aura despite low overall vascular-risk profile",
        "threshold_type": "contraindication_vs_context",
        "conflicting_axes": ["patient preference for estrogen vs stroke-risk contraindication", "reassuring vascular profile vs persistent aura history"],
        "management_nuance": ["redirect rather than delay", "context softens the stem but does not reverse the contraindication"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["no longer breastfeeding", "aura only twice a year"],
        "dynamic_progression": ["now 8 weeks postpartum", "asks whether lower postpartum risk changes eligibility"],
        "template_family": "conflicting_risk_axes",
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
        "question_stem": "A 27-year-old with PID has a 4.8-cm tubo-ovarian abscess on ultrasound. She has received 48 hours of broad-spectrum IV antibiotics, is hemodynamically stable, still has temperature 38.1 C, and continues to have significant pelvic pain. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Continue IV antibiotics alone for another 24-48 hours because the abscess is still under 5 cm"},
            {"key": "B", "text": "Arrange image-guided drainage while continuing antibiotics because she has not improved after 48 hours"},
            {"key": "C", "text": "Switch to oral outpatient antibiotics because she is hemodynamically stable"},
            {"key": "D", "text": "Proceed directly to salpingo-oophorectomy because any tubo-ovarian abscess requires surgery"},
        ],
        "correct_answer_key": "B",
        "explanation": "The hard part here is that both continuing IV therapy and drainage are plausible. Ongoing fever and pain after 48 hours of appropriate inpatient treatment favor drainage rather than simply extending the same regimen.",
        "exam_clue": "Stable TOA without improvement after 48 hours of IV therapy",
        "board_takeaway": "TOA management is not just admit-or-not; lack of early clinical improvement should push you toward drainage rather than passive continuation.",
        "decision_point": "Choose continued IV therapy versus drainage in a stable tubo-ovarian abscess",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "The abscess size alone is not the decisive variable; the failure to improve after 48 hours is what shifts management toward drainage.",
        "estimated_time_seconds": 90,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 9,
        "ambiguity_level": 9,
        "threshold_variable": "No meaningful clinical improvement after 48 hours of IV therapy",
        "threshold_type": "response_to_treatment",
        "conflicting_axes": ["hemodynamic stability vs persistent infection", "moderate abscess size vs failure of medical therapy"],
        "management_nuance": ["continued IV therapy vs drainage", "size does not override trajectory", "inpatient status alone is not enough"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "A",
        "plausible_option_count": 2,
        "clinical_noise": ["abscess remains under 5 cm", "hemodynamically stable on exam"],
        "dynamic_progression": ["48 hours of IV antibiotics completed", "persistent fever and pain despite treatment"],
        "template_family": "response_over_time",
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
        "question_stem": "A 36-year-old with 2 years of infertility has regular ovulation, normal HSG, normal semen analysis, and reassuring ovarian reserve testing. She has already completed 3 cycles of letrozole with IUI without conception and asks whether one more stimulated IUI cycle is more reasonable than moving on to IVF. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Proceed to IVF because age and failure of several medicated IUI cycles make further low-yield escalation less attractive"},
            {"key": "B", "text": "Repeat another stimulated IUI cycle first because she still ovulates and IVF should be reserved for later"},
            {"key": "C", "text": "Return to expectant management for 6-12 months because the workup remains normal"},
            {"key": "D", "text": "Perform diagnostic laparoscopy before any further fertility treatment despite the otherwise normal evaluation"},
        ],
        "correct_answer_key": "A",
        "explanation": "This question is about sequencing, not just naming the standard first treatment. Once several stimulated IUI cycles have already failed in a 36-year-old, the more appropriate move is IVF rather than repeating the same lower-yield strategy again.",
        "exam_clue": "Age 36 with failed medicated IUI cycles despite otherwise unexplained infertility",
        "board_takeaway": "In unexplained infertility, the right answer changes once time and failed treatment cycles accumulate.",
        "decision_point": "Choose continued IUI versus IVF after failed first-line escalation",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Another IUI cycle is not absurd, but age plus repeated failure shifts the balance toward IVF now.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 8,
        "ambiguity_level": 8,
        "threshold_variable": "Failure after 3 letrozole-IUI cycles at age 36",
        "threshold_type": "treatment_response_plus_age",
        "conflicting_axes": ["patient preference to avoid IVF vs declining time efficiency", "normal workup vs repeated failure of lower-intensity treatment"],
        "management_nuance": ["repeat IUI vs move to IVF", "normal tests do not justify indefinite low-yield repetition"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["reassuring ovarian reserve", "regular ovulation"],
        "dynamic_progression": ["already completed 3 medicated IUI cycles", "asks whether one more IUI is still reasonable"],
        "template_family": "response_over_time",
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
        "question_stem": "A 52-year-old reports 3 months of bloating and early satiety. Ultrasound shows a 7-cm complex adnexal mass with papillary projections and moderate free fluid. She is otherwise stable and asks whether you can just repeat imaging first because the pain is mild. What is the best next step?",
        "options": [
            {"key": "A", "text": "Repeat ultrasound in 6-8 weeks because most adnexal masses in stable patients are managed initially with surveillance"},
            {"key": "B", "text": "Obtain tumor-marker risk stratification and refer to gynecologic oncology without delaying for interval imaging"},
            {"key": "C", "text": "Schedule laparoscopic cystectomy with a general gynecologist because she is only mildly symptomatic"},
            {"key": "D", "text": "Treat empirically for pelvic infection and re-image after antibiotics"},
        ],
        "correct_answer_key": "B",
        "explanation": "Several options sound superficially reasonable because the patient is stable and symptoms are not dramatic. The decisive nuance is the malignant ultrasound morphology plus symptom pattern, which makes oncologic referral preferable to surveillance or routine benign surgery planning.",
        "exam_clue": "Complex postmenopausal mass with papillary projections and ascites-type free fluid",
        "board_takeaway": "Adnexal mass decisions turn on risk pattern, not just symptom intensity; suspicious morphology plus symptoms should move you toward oncologic triage, not watchful waiting.",
        "decision_point": "Choose surveillance versus oncologic escalation for a suspicious adnexal mass",
        "difficulty_band": "standard",
        "tempting_wrong_option": "C",
        "tempting_wrong_reason": "Operating without oncologic planning is the near-miss error; the issue is not whether surgery may occur, but who should own the initial escalation.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 8,
        "ambiguity_level": 8,
        "threshold_variable": "Complex morphology with papillary projections and free fluid",
        "threshold_type": "imaging_risk_pattern",
        "conflicting_axes": ["clinical stability vs oncologic sonographic risk", "patient preference for surveillance vs referral threshold"],
        "management_nuance": ["surveillance vs referral", "general gyne surgery vs gyn-onc triage"],
        "near_miss_options": ["B", "C"],
        "near_correct_trap": "C",
        "plausible_option_count": 2,
        "clinical_noise": ["pain is mild", "patient prefers repeat imaging first"],
        "dynamic_progression": ["3 months of bloating and early satiety", "new complex mass identified now"],
        "template_family": "conflicting_risk_axes",
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
        "question_stem": "A 58-year-old with a single episode of light postmenopausal bleeding is hemodynamically stable and not using hormone therapy. Transvaginal ultrasound performed the same day shows a homogeneous endometrial stripe of 3 mm. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Offer reassurance with return precautions because a thin endometrial stripe makes immediate biopsy unnecessary after a first isolated episode"},
            {"key": "B", "text": "Perform endometrial biopsy now because every case of postmenopausal bleeding requires tissue diagnosis regardless of ultrasound findings"},
            {"key": "C", "text": "Start empiric vaginal estrogen and reassess only if symptoms worsen"},
            {"key": "D", "text": "Repeat transvaginal ultrasound in 6 weeks before making any management decision"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is exactly the kind of question where both ultrasound-first and biopsy-first can sound reasonable. The thin stripe after a first isolated episode allows reassurance with follow-up instructions rather than mandatory biopsy at the first visit.",
        "exam_clue": "Postmenopausal bleeding with endometrial thickness 3 mm",
        "board_takeaway": "Postmenopausal bleeding is not one-size-fits-all; endometrial thickness changes whether biopsy is mandatory immediately or can be deferred after an isolated episode.",
        "decision_point": "Choose biopsy versus reassurance after ultrasound in postmenopausal bleeding",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Biopsy is tempting because cancer exclusion is the overall frame, but the thin stripe is the threshold variable that changes the immediate next step.",
        "estimated_time_seconds": 90,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 5,
        "ambiguity_level": 6,
        "threshold_variable": "Endometrial stripe 3 mm after first isolated bleeding episode",
        "threshold_type": "ultrasound_threshold",
        "conflicting_axes": ["malignancy concern vs reassuring ultrasound threshold", "first episode vs desire for immediate tissue diagnosis"],
        "management_nuance": ["TVUS-first vs biopsy-first", "reassurance with return precautions vs invasive testing"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["single light episode", "not using hormone therapy"],
        "dynamic_progression": ["same-day ultrasound already performed", "this is the first isolated bleeding episode"],
        "template_family": "borderline_threshold",
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
        "id": "mcq_aub_sampling_r3",
        "item_type": "mcq",
        "topic": "General gynecology",
        "subtopic": "AUB biopsy threshold",
        "question_stem": "A 45-year-old with new irregular bleeding asks whether a known small fibroid is enough to explain the bleeding without further workup. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Perform endometrial sampling as part of the initial evaluation"},
            {"key": "B", "text": "Treat the fibroid first and defer sampling unless bleeding continues"},
            {"key": "C", "text": "Repeat ultrasound after the next cycle before deciding on biopsy"},
            {"key": "D", "text": "Prefer hysteroscopy before office sampling because the bleeding has already become irregular"},
        ],
        "correct_answer_key": "A",
        "explanation": "At age 45 or older, abnormal uterine bleeding lowers the threshold for endometrial sampling even if a plausible structural explanation is present.",
        "exam_clue": "Age 45 with new abnormal uterine bleeding",
        "board_takeaway": "In patients aged 45 or older, AUB should prompt sampling rather than assuming a small fibroid explains everything.",
        "decision_point": "Apply the age-based biopsy rule in abnormal uterine bleeding",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Treating the fibroid sounds reasonable, but age 45 changes the rule and pushes biopsy into the initial workup.",
        "estimated_time_seconds": 55,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 4,
        "ambiguity_level": 4,
        "threshold_variable": "Age 45 or older with new AUB",
        "threshold_type": "age_threshold_rule_application",
        "conflicting_axes": ["visible structural explanation vs age-based biopsy rule"],
        "management_nuance": ["apply biopsy threshold before empiric treatment"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["small known fibroid"],
        "dynamic_progression": ["new irregular bleeding this cycle"],
        "template_family": "core_decision",
        "source_id": "study_src_acog_aub",
        "source_name": "ACOG: Abnormal Uterine Bleeding",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/womens-health/faqs/abnormal-uterine-bleeding",
        "source_excerpt": "Endometrial evaluation is indicated when age or risk profile raises concern, even when structural causes coexist.",
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
        "question_stem": "A 46-year-old with obesity and chronic anovulation presents with 4 months of heavier irregular bleeding. Ultrasound shows a 3-cm intramural fibroid that does not distort the cavity. Hemoglobin is 10.8. She asks whether you can just adjust medical therapy because the fibroid already explains the bleeding. What is the best next step?",
        "options": [
            {"key": "A", "text": "Begin fibroid-directed medical therapy first and defer biopsy unless bleeding fails to improve"},
            {"key": "B", "text": "Perform endometrial sampling as part of the initial evaluation even though a fibroid is present"},
            {"key": "C", "text": "Repeat ultrasound after the next cycle because the fibroid is the most likely explanation"},
            {"key": "D", "text": "Proceed directly to hysteroscopy because office-based sampling is not appropriate when fibroids are seen on ultrasound"},
        ],
        "correct_answer_key": "B",
        "explanation": "The trap is anchoring on the fibroid. Age 45 or older already lowers the threshold for sampling, and obesity plus anovulation add competing endometrial risk that makes biopsy part of the initial evaluation.",
        "exam_clue": "AUB age 46 with obesity/anovulation despite a plausible fibroid explanation",
        "board_takeaway": "A structural explanation does not cancel biopsy thresholds; age and endometrial risk factors can outweigh the temptation to blame the fibroid.",
        "decision_point": "Decide when fibroid-directed management is insufficient without initial endometrial sampling",
        "difficulty_band": "standard",
        "tempting_wrong_option": "A",
        "tempting_wrong_reason": "Medical therapy is reasonable for fibroid-related bleeding, but here it becomes a near-miss because age and endometrial risk factors shift the first step toward sampling.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 5,
        "ambiguity_level": 6,
        "threshold_variable": "Age >=45 with additional endometrial risk factors",
        "threshold_type": "age_plus_risk_factor_threshold",
        "conflicting_axes": ["visible structural cause vs endometrial cancer risk", "medical management preference vs biopsy threshold"],
        "management_nuance": ["sampling first vs empiric treatment first", "fibroid presence does not end the workup"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "A",
        "plausible_option_count": 2,
        "clinical_noise": ["hemoglobin 10.8", "3-cm intramural fibroid not distorting the cavity"],
        "dynamic_progression": ["4 months of heavier irregular bleeding", "existing fibroid already known before this visit"],
        "template_family": "conflicting_risk_axes",
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
        "question_stem": "A 27-year-old nonpregnant patient has dysuria, urinary frequency, no fever, no flank pain, and no vaginal discharge. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Treat as uncomplicated cystitis with a first-line oral regimen guided by local resistance patterns"},
            {"key": "B", "text": "Send urine testing and hold antibiotics until culture confirms the organism and sensitivities"},
            {"key": "C", "text": "Treat as pyelonephritis with parenteral therapy because urinary symptoms can represent early upper tract disease"},
            {"key": "D", "text": "Shift the workup toward vaginitis before treating the urinary symptoms because there is no systemic illness"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is a classic uncomplicated cystitis presentation in a nonpregnant patient, so empiric first-line oral therapy is appropriate, guided by local susceptibility patterns and antibiotic stewardship.",
        "exam_clue": "Dysuria and frequency without fever, flank pain, or vaginal symptoms",
        "board_takeaway": "Lower urinary tract symptoms without systemic features point to uncomplicated cystitis, not pyelonephritis.",
        "decision_point": "Distinguish uncomplicated cystitis from pyelonephritis or vaginitis in an OB-GYN overlap scenario",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Confirmatory testing sounds careful, but this symptom cluster is still appropriate for empiric uncomplicated cystitis treatment rather than delaying therapy.",
        "estimated_time_seconds": 60,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 3,
        "ambiguity_level": 3,
        "near_miss_options": ["A", "B"],
        "plausible_option_count": 2,
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
        "question_stem": "A day-2 post-cesarean patient developed postpartum endometritis and was started on broad-spectrum IV antibiotics. After 36 hours she is afebrile, uterine tenderness is improving, but she still has mild fundal discomfort and asks whether she can switch to oral antibiotics and go home now. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Continue IV therapy until she remains clinically improved and afebrile, then stop without adding an oral step-down course routinely"},
            {"key": "B", "text": "Switch now to oral antibiotics because the major physiologic abnormalities have already improved"},
            {"key": "C", "text": "Continue IV therapy and add routine imaging because mild residual fundal tenderness means treatment failure"},
            {"key": "D", "text": "Stop antibiotics now because clinical improvement after 36 hours excludes persistent postpartum infection"},
        ],
        "correct_answer_key": "A",
        "explanation": "The higher-level decision is not whether endometritis needs antibiotics, but when and how to stop treatment once the patient improves. Continued IV therapy until sustained clinical improvement is preferable to premature oral step-down or reflex imaging for mild residual tenderness alone.",
        "exam_clue": "Improving endometritis after 36 hours of IV therapy",
        "board_takeaway": "To make endometritis harder, ask about treatment trajectory and discharge timing, not just the initial antibiotic choice.",
        "decision_point": "Choose IV continuation versus oral step-down in improving postpartum endometritis",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Oral step-down sounds efficient because she is improving, but the better-supported approach is to continue IV therapy until clear sustained clinical response before stopping.",
        "estimated_time_seconds": 80,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 7,
        "ambiguity_level": 7,
        "threshold_variable": "36 hours of partial clinical improvement on IV therapy",
        "threshold_type": "response_to_treatment",
        "conflicting_axes": ["clear improvement vs not fully normalized exam", "desire for discharge vs ensuring adequate treatment"],
        "management_nuance": ["continue IV vs oral step-down", "residual tenderness does not automatically mean failure"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["still has mild fundal discomfort", "asks whether she can go home now"],
        "dynamic_progression": ["36 hours into IV therapy", "fever has resolved and tenderness is improving"],
        "template_family": "response_over_time",
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
        "question_stem": "Five days postpartum, a patient presents with severe headache, blood pressure 170/112, visual spots, normal oxygen saturation, and only trace protein on dipstick. She is neurologically intact and asks whether this could wait until formal labs result because she had no hypertension before discharge. What is the best next step?",
        "options": [
            {"key": "A", "text": "Treat as postpartum preeclampsia with severe features, begin acute blood pressure control, and give magnesium sulfate now"},
            {"key": "B", "text": "Repeat blood pressure after analgesia and wait for urine protein quantification before treating aggressively"},
            {"key": "C", "text": "Admit for observation and labs but hold magnesium unless proteinuria is confirmed"},
            {"key": "D", "text": "Arrange urgent outpatient follow-up the same day because postpartum hypertension often peaks after discharge"},
        ],
        "correct_answer_key": "A",
        "explanation": "The nuance is that several conservative options sound defensible because she is postpartum and dipstick protein is minimal. Severe-range pressure plus neurologic symptoms is enough to treat now; postpartum timing and limited proteinuria do not make it safe to wait.",
        "exam_clue": "Postpartum severe-range blood pressure with neurologic symptoms despite minimal proteinuria",
        "board_takeaway": "In postpartum hypertension, severe symptoms and severe-range blood pressure matter more than waiting for protein confirmation.",
        "decision_point": "Choose immediate treatment versus delayed confirmation in postpartum severe hypertension",
        "difficulty_band": "standard",
        "tempting_wrong_option": "C",
        "tempting_wrong_reason": "Admission is reasonable, but withholding magnesium is the near-miss error because the severe neurologic presentation already crosses the treatment threshold.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 8,
        "ambiguity_level": 8,
        "threshold_variable": "Severe-range blood pressure with headache/visual symptoms postpartum",
        "threshold_type": "symptom_plus_severity_threshold",
        "conflicting_axes": ["minimal proteinuria vs severe symptoms", "clinically stable appearance vs need for urgent treatment"],
        "management_nuance": ["treat now vs await confirmation", "magnesium threshold in postpartum disease"],
        "near_miss_options": ["A", "C"],
        "near_correct_trap": "C",
        "plausible_option_count": 2,
        "clinical_noise": ["trace protein on dipstick", "no hypertension before discharge"],
        "dynamic_progression": ["symptoms developed 5 days postpartum", "hypertension was not recognized before discharge"],
        "template_family": "response_over_time",
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
        "id": "mcq_vte_postpartum_estrogen_r5",
        "item_type": "mcq",
        "topic": "Contraception",
        "subtopic": "Recent VTE timing nuance",
        "question_stem": "A 15-week postpartum patient is no longer breastfeeding and finished anticoagulation 8 weeks ago for a pregnancy-associated DVT. She strongly prefers the vaginal ring and asks whether the added time since treatment changes the safest next step now. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Continue to avoid estrogen-containing contraception and recommend an effective non-estrogen method instead"},
            {"key": "B", "text": "Start the vaginal ring now because she is farther from the thrombotic event and no longer breastfeeding"},
            {"key": "C", "text": "Hold combined hormonal contraception for now and reassess after a formal thrombosis-risk review"},
            {"key": "D", "text": "Offer a short combined-hormonal trial and stop if leg symptoms recur"},
        ],
        "correct_answer_key": "A",
        "explanation": "The timing shift makes this harder than the basic postpartum VTE rule. More time has passed and treatment is complete, but a recent pregnancy-associated DVT still keeps estrogen off the table, so the safest move remains a non-estrogen method.",
        "exam_clue": "More remote postpartum timing but still recent pregnancy-associated DVT",
        "board_takeaway": "R5-style contraception questions should hinge on timing nuance: more reassuring context can make estrogen tempting without actually making it correct.",
        "decision_point": "Reassess estrogen eligibility after more time has passed from a pregnancy-associated DVT",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "The longer interval from treatment completion makes estrogen feel more reasonable, but the recent pregnancy-associated thrombotic history still keeps it off the table.",
        "estimated_time_seconds": 85,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 7,
        "ambiguity_level": 7,
        "threshold_variable": "15 weeks postpartum and 8 weeks from completing anticoagulation",
        "threshold_type": "timing_after_recent_vte",
        "conflicting_axes": ["patient preference for estrogen vs recurrent VTE risk", "greater time distance from event vs still recent thrombosis history"],
        "management_nuance": ["timing softens the stem without flipping the answer", "offer high-efficacy non-estrogen contraception now"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["no longer breastfeeding", "asks whether more time changes the answer"],
        "dynamic_progression": ["now 15 weeks postpartum", "completed anticoagulation 8 weeks ago"],
        "template_family": "response_over_time",
        "source_id": "study_src_cdc_usmec",
        "source_name": "CDC U.S. Medical Eligibility Criteria for Contraceptive Use",
        "source_type": "Guideline",
        "source_url": "https://www.cdc.gov/contraception/hcp/usmec/index.html",
        "source_excerpt": "Recent thrombosis history remains a major reason to avoid estrogen-containing contraceptive methods even when surrounding context becomes more reassuring.",
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
        "question_stem": "A 10-week postpartum patient is no longer breastfeeding and completed treatment for a pregnancy-associated DVT 2 weeks ago. She strongly prefers the vaginal ring and says the clot has resolved clinically. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Avoid estrogen-containing contraception and offer a highly effective non-estrogen method instead"},
            {"key": "B", "text": "Start the vaginal ring because she is now beyond the earliest postpartum period and no longer breastfeeding"},
            {"key": "C", "text": "Wait several more months before offering any contraception because recent VTE means all methods should be deferred"},
            {"key": "D", "text": "Use combined hormonal contraception if Doppler confirms that the thrombosis has resolved"},
        ],
        "correct_answer_key": "A",
        "explanation": "Several reassuring features tempt you toward estrogen: later postpartum timing, no breastfeeding, and symptom resolution. But a very recent pregnancy-associated DVT still makes estrogen the wrong choice, while a non-estrogen method remains appropriate now.",
        "exam_clue": "Recent pregnancy-associated DVT despite otherwise favorable postpartum contraceptive context",
        "board_takeaway": "The hard contraception questions are about when reassuring context still does not neutralize a major thrombotic risk factor.",
        "decision_point": "Choose estrogen avoidance versus reintroduction after recent pregnancy-associated DVT",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Later postpartum timing and no breastfeeding make estrogen feel more acceptable, but the recent pregnancy-associated DVT still outweighs those reassuring details.",
        "estimated_time_seconds": 80,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 5,
        "ambiguity_level": 7,
        "threshold_variable": "Very recent completed treatment for pregnancy-associated DVT",
        "threshold_type": "recent_vte_vs_postpartum_context",
        "conflicting_axes": ["strong preference for estrogen method vs recurrent VTE risk", "later postpartum timing vs still-recent thrombosis history"],
        "management_nuance": ["offer effective non-estrogen contraception now", "do not confuse symptom resolution with risk normalization"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["no longer breastfeeding", "clot symptoms have resolved clinically"],
        "dynamic_progression": ["10 weeks postpartum now", "completed DVT treatment only 2 weeks ago"],
        "template_family": "conflicting_risk_axes",
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
        "question_stem": "A 24-week pregnant patient has had 18 hours of fever, flank pain, tachycardia, and CVA tenderness despite oral hydration and acetaminophen at home. She is still hemodynamically stable and asks whether she can avoid admission if she starts antibiotics now. What is the best next step now?",
        "options": [
            {"key": "A", "text": "Hospitalize for pyelonephritis treatment with parenteral antibiotics and maternal-fetal monitoring"},
            {"key": "B", "text": "Treat as uncomplicated cystitis with outpatient nitrofurantoin"},
            {"key": "C", "text": "Give the first antibiotic dose now and reassess later the same day as an outpatient if symptoms begin to improve"},
            {"key": "D", "text": "Send urine studies now and reconsider admission once microbiology confirms the diagnosis"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is no longer just recall of 'pyelo equals admit.' The outpatient option becomes tempting because she is still hemodynamically stable, but ongoing systemic symptoms in pregnancy after trying supportive care still make inpatient parenteral therapy the safer next step.",
        "exam_clue": "Persistent pregnancy pyelonephritis symptoms after attempted home measures",
        "board_takeaway": "Pregnancy pyelonephritis becomes harder when the patient is stable enough to tempt outpatient management; persistence of systemic symptoms still favors inpatient treatment.",
        "decision_point": "Choose inpatient pyelonephritis care versus same-day outpatient treatment in pregnancy",
        "difficulty_band": "standard",
        "tempting_wrong_option": "C",
        "tempting_wrong_reason": "Same-day outpatient reassessment sounds efficient because she is stable, but persistent systemic symptoms in pregnancy still favor inpatient parenteral therapy.",
        "estimated_time_seconds": 60,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 4,
        "ambiguity_level": 4,
        "threshold_variable": "Persistent systemic symptoms after attempted home care",
        "threshold_type": "response_to_treatment",
        "conflicting_axes": ["hemodynamic stability vs pregnancy pyelonephritis risk", "desire to avoid admission vs persistent systemic symptoms"],
        "management_nuance": ["stable appearance does not make this uncomplicated cystitis", "same-day outpatient reassessment is the near-miss"],
        "near_miss_options": ["A", "C"],
        "near_correct_trap": "C",
        "plausible_option_count": 2,
        "clinical_noise": ["oral hydration and acetaminophen already tried", "asks to avoid admission"],
        "dynamic_progression": ["18 hours of ongoing fever and flank pain", "symptoms persisted despite home measures"],
        "template_family": "response_over_time",
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
        "question_stem": "A laboring patient with severe preeclampsia requests epidural analgesia. Platelets were 92,000 six hours ago and are now 68,000 with an otherwise normal coagulation profile. She is uncomfortable but fetal status remains reassuring and induction is ongoing. What is the best next step?",
        "options": [
            {"key": "A", "text": "Avoid routine neuraxial placement at this platelet count, discuss alternative analgesia, and continue the obstetric plan"},
            {"key": "B", "text": "Proceed with epidural placement because the coagulation profile is otherwise normal and she strongly desires neuraxial analgesia"},
            {"key": "C", "text": "Stop the induction until platelet count recovers to above 100,000"},
            {"key": "D", "text": "Give platelet transfusion solely to facilitate epidural placement in an otherwise stable labor course"},
        ],
        "correct_answer_key": "A",
        "explanation": "This is not a question about abandoning delivery; it is about separating the neuraxial decision from the obstetric plan. The falling platelet trend into the high-60s makes neuraxial placement difficult to justify even though some surrounding details sound reassuring.",
        "exam_clue": "Rapid platelet decline to 68,000 in severe preeclampsia",
        "board_takeaway": "Neuraxial decisions in preeclampsia depend on platelet range and trajectory; a reassuring labor course does not neutralize hematoma risk.",
        "decision_point": "Choose alternative analgesia versus neuraxial placement when platelets are falling in severe preeclampsia",
        "difficulty_band": "standard",
        "tempting_wrong_option": "B",
        "tempting_wrong_reason": "Normal coagulation studies and patient preference are not enough to overcome the platelet threshold and downward trend.",
        "estimated_time_seconds": 90,
        "decision_frame": "best_next_step",
        "difficulty_target_10": 9,
        "ambiguity_level": 8,
        "threshold_variable": "Platelets 68,000 and falling",
        "threshold_type": "lab_trend_threshold",
        "conflicting_axes": ["analgesia request vs neuraxial bleeding risk", "normal coagulation profile vs unsafe platelet threshold"],
        "management_nuance": ["neuraxial decision separate from delivery plan", "platelet trend matters, not just one number"],
        "near_miss_options": ["A", "B"],
        "near_correct_trap": "B",
        "plausible_option_count": 2,
        "clinical_noise": ["otherwise normal coagulation profile", "fetal status remains reassuring"],
        "dynamic_progression": ["platelets fell from 92,000 to 68,000 over 6 hours", "induction remains ongoing despite analgesia dilemma"],
        "template_family": "borderline_threshold",
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
        "decision_frame": "most_likely_diagnosis",
        "difficulty_target_10": 4,
        "ambiguity_level": 4,
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


def _difficulty_level_from_target_10(target_10):
    try:
        normalized = int(target_10)
    except (TypeError, ValueError):
        normalized = 4
    normalized = max(1, min(10, normalized))
    if normalized <= 2:
        return 1
    if normalized == 3:
        return 2
    if normalized == 4:
        return 3
    if normalized == 5:
        return 4
    if normalized == 6:
        return 5
    return 6


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
    user_profile = build_effective_user_profile(user_profile)
    residency_year = (user_profile.get("residency_year") or "").strip().upper()
    if residency_year in DIFFICULTY_LEVEL_DISTRIBUTIONS:
        return residency_year
    training_stage = (user_profile.get("training_stage") or "").strip().lower()
    if training_stage in {"specialist", "fellowship"}:
        return "R6"
    return None


def _difficulty_policy_for_profile(user_profile):
    residency_year = _residency_year_from_profile(user_profile) or "R6"
    distribution = dict(DIFFICULTY_LEVEL_DISTRIBUTIONS.get(residency_year, DIFFICULTY_LEVEL_DISTRIBUTIONS["R6"]))
    min_level, max_level = DIFFICULTY_LEVEL_BOUNDS.get(residency_year, (4, 6))
    baseline_level = max(distribution, key=distribution.get)
    main_flow_rule = dict(RESIDENCY_YEAR_MAIN_FLOW_RULES.get(residency_year, RESIDENCY_YEAR_MAIN_FLOW_RULES["R6"]))
    return {
        "residency_year": residency_year,
        "distribution": distribution,
        "baseline_level": baseline_level,
        "min_level": min_level,
        "max_level": max_level,
        "prefer_advanced_mcq_stack": residency_year in {"R5", "R6"},
        "prefer_stage_b_judgment": residency_year in {"R4", "R5", "R6"},
        "practice_floor": max(min_level, baseline_level - 1) if residency_year in {"R5", "R6"} else min_level,
        **main_flow_rule,
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
    legacy_declared_difficulty_level = int(
        normalized.get("difficulty_level")
        or _level_for_legacy_band(normalized.get("difficulty_band"))
    )
    normalized["difficulty_target_10"] = int(normalized.get("difficulty_target_10") or legacy_declared_difficulty_level)
    canonical_difficulty_level = _difficulty_level_from_target_10(normalized["difficulty_target_10"])
    normalized["legacy_declared_difficulty_level"] = legacy_declared_difficulty_level
    normalized["declared_difficulty_level"] = canonical_difficulty_level
    normalized["difficulty_level"] = canonical_difficulty_level
    normalized["difficulty_level_alignment"] = (
        "aligned" if canonical_difficulty_level == legacy_declared_difficulty_level else "auto_recalculated_from_target"
    )
    normalized["question_style"] = normalized.get("question_style") or _infer_question_style(normalized)
    normalized["difficulty_band"] = _difficulty_band_for_level(normalized["difficulty_level"])
    normalized["ambiguity_level"] = int(normalized.get("ambiguity_level") or 0)
    normalized["conflicting_axes"] = [axis for axis in (normalized.get("conflicting_axes") or []) if axis]
    normalized["management_nuance"] = [axis for axis in (normalized.get("management_nuance") or []) if axis]
    normalized["near_miss_options"] = [option for option in (normalized.get("near_miss_options") or []) if option]
    normalized["clinical_noise"] = [entry for entry in (normalized.get("clinical_noise") or []) if entry]
    normalized["dynamic_progression"] = [entry for entry in (normalized.get("dynamic_progression") or []) if entry]
    normalized["tradeoff_axes"] = [axis for axis in (normalized.get("tradeoff_axes") or normalized.get("conflicting_axes") or []) if axis]
    normalized["decision_pressure"] = [entry for entry in (normalized.get("decision_pressure") or normalized.get("dynamic_progression") or []) if entry]
    normalized["threshold_type"] = normalized.get("threshold_type")
    normalized["near_correct_trap"] = normalized.get("near_correct_trap")
    normalized["plausible_option_count"] = int(normalized.get("plausible_option_count") or 0)
    normalized["decision_frame"] = (
        normalized.get("decision_frame")
        or _infer_decision_frame(normalized.get("question_stem"))
    )
    normalized["template_family"] = (
        normalized.get("template_family")
        or _infer_template_family(normalized)
    )
    normalized["decision_archetype"] = (
        normalized.get("decision_archetype")
        or _infer_decision_archetype(normalized)
    )

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
        quality = _stage_b_quality_metadata(normalized)
        normalized.update(quality)

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


def _infer_decision_frame(question_stem):
    stem = " ".join((question_stem or "").strip().lower().split())
    if not stem:
        return None
    if any(marker in stem for marker in DECISION_FRAME_MARKERS):
        return "best_next_step"
    if stem.startswith("what is the most likely") or "most likely" in stem:
        return "most_likely_diagnosis"
    return "general"


def _difficulty_engine_rule_for_target(difficulty_target):
    try:
        normalized = int(difficulty_target)
    except (TypeError, ValueError):
        normalized = 5
    for rule in DIFFICULTY_ENGINE_RULES:
        if rule["min_target"] <= normalized <= rule["max_target"]:
            return rule
    return DIFFICULTY_ENGINE_RULES[-1]


def _infer_template_family(item):
    if item.get("dynamic_progression"):
        return "response_over_time"
    if item.get("threshold_variable"):
        threshold_text = (item.get("threshold_variable") or "").lower()
        if any(marker in threshold_text for marker in ("week", "+", "gestation")):
            return "timing_threshold"
        return "borderline_threshold"
    if item.get("conflicting_axes"):
        return "conflicting_risk_axes"
    return "core_decision"


def _infer_decision_archetype(item):
    threshold_type = (item.get("threshold_type") or "").strip().lower()
    template_family = (item.get("template_family") or "").strip().lower()
    if threshold_type == "contraindication_vs_context":
        return "contraindication_context"
    if threshold_type in {"response_to_treatment", "treatment_response_plus_age"} or template_family == "response_over_time":
        return "continue_vs_escalate"
    if "timing" in threshold_type or template_family == "timing_threshold":
        return "timing_pivot"
    if template_family == "conflicting_risk_axes":
        return "conflicting_strategies"
    if template_family == "borderline_threshold":
        return "threshold_reassessment"
    return "core_decision"


def _stage_b_quality_metadata(item):
    options = item.get("options") or []
    correct_key = (item.get("correct_answer_key") or "").upper()
    question_stem = (item.get("question_stem") or "").strip()
    normalized_stem = question_stem.lower()
    option_texts = [" " + (option.get("text") or "").strip().lower() + " " for option in options]
    distractor_texts = [
        " " + (option.get("text") or "").strip().lower() + " "
        for option in options
        if (option.get("key") or "").upper() != correct_key
    ]
    absolute_option_count = sum(
        1 for text in distractor_texts if any(marker in text for marker in ABSOLUTE_DISTRACTOR_MARKERS)
    )
    obvious_wrong_distractor_count = sum(
        1 for text in distractor_texts if any(marker in text for marker in OBVIOUS_WRONG_DISTRACTOR_MARKERS)
    )
    high_quality_distractors = len(item.get("near_miss_options") or [])
    has_threshold = bool(item.get("threshold_variable"))
    has_conflict = bool(item.get("conflicting_axes"))
    ambiguity_level = int(item.get("ambiguity_level") or 0)
    management_nuance = len(item.get("management_nuance") or [])
    difficulty_target = int(item.get("difficulty_target_10") or item.get("difficulty_level") or 0)
    high_judgment_style = item.get("question_style") in HIGH_JUDGMENT_STYLE_NAMES
    has_decision_frame = item.get("decision_frame") == "best_next_step"
    clinical_noise_count = len(item.get("clinical_noise") or [])
    dynamic_progression_count = len(item.get("dynamic_progression") or [])
    tradeoff_axes_count = len(item.get("tradeoff_axes") or [])
    decision_pressure_count = len(item.get("decision_pressure") or [])
    plausible_option_count = int(item.get("plausible_option_count") or 0)
    has_near_correct_trap = bool(item.get("near_correct_trap"))
    has_template_family = bool(item.get("template_family"))
    decision_archetype = item.get("decision_archetype")
    threshold_type = (item.get("threshold_type") or "").strip().lower()
    dynamic_text = " ".join(item.get("dynamic_progression") or []).lower()
    stem_word_count = len(question_stem.split())
    correct_option_text = (_option_text_by_key(item, correct_key) or "").strip()
    correct_option_word_count = len(correct_option_text.split())
    time_progression_or_response = bool(
        dynamic_progression_count >= 1
        or threshold_type in {
            "response_to_treatment",
            "treatment_response_plus_age",
            "lab_trend_threshold",
            "gestational_age_plus_clinical_trajectory",
            "gestational_age_plus_stability",
        }
    )
    partial_response_or_trend = bool(
        threshold_type in {"response_to_treatment", "treatment_response_plus_age", "lab_trend_threshold"}
        or any(
            marker in dynamic_text
            for marker in ("improv", "persist", "still", "fell", "declin", "trend", "completed", "despite")
        )
    )
    near_miss_keys = {
        str(option_key).upper()
        for option_key in (item.get("near_miss_options") or [])
        if option_key
    }
    tempting_wrong_option = (item.get("tempting_wrong_option") or "").strip().upper()
    second_best_key = (item.get("near_correct_trap") or tempting_wrong_option or "").strip().upper()
    second_best_strength_score = 0
    if second_best_key:
        second_best_strength_score += 1
    if second_best_key and second_best_key in near_miss_keys:
        second_best_strength_score += 1
    if second_best_key and second_best_key == tempting_wrong_option:
        second_best_strength_score += 1
    if plausible_option_count >= 2:
        second_best_strength_score += 1
    if ambiguity_level >= 8:
        second_best_strength_score += 1
    if has_conflict:
        second_best_strength_score += 1
    if tradeoff_axes_count >= 1:
        second_best_strength_score += 1
    if decision_pressure_count >= 1:
        second_best_strength_score += 1
    if dynamic_progression_count >= 1 or has_threshold:
        second_best_strength_score += 1
    decision_pressure_score = 0
    if decision_pressure_count >= 1:
        decision_pressure_score += 1
    if tradeoff_axes_count >= 1:
        decision_pressure_score += 1
    if time_progression_or_response:
        decision_pressure_score += 1
    if partial_response_or_trend:
        decision_pressure_score += 1
    if ambiguity_level >= 8:
        decision_pressure_score += 1
    if plausible_option_count >= 2:
        decision_pressure_score += 1
    high_decision_pressure = decision_pressure_score >= 5
    deep_conflict = (
        plausible_option_count >= 2
        and has_conflict
        and tradeoff_axes_count >= 1
        and decision_pressure_count >= 1
        and (dynamic_progression_count >= 1 or has_threshold)
    )
    clear_decision_point = bool(item.get("decision_point")) and has_decision_frame
    dominant_clue_present = bool((item.get("exam_clue") or "").strip()) and stem_word_count >= 12
    plausible_distractors = absolute_option_count == 0 and obvious_wrong_distractor_count == 0 and (high_quality_distractors >= 1 or plausible_option_count >= 2)
    vague_stem_risk = difficulty_target >= 4 and (stem_word_count < 12 or "best next step" not in normalized_stem)
    keyword_only_answer_risk = difficulty_target >= 4 and correct_option_word_count <= 2 and not time_progression_or_response and not has_threshold
    quality_checks = {
        "clear_decision_point": clear_decision_point,
        "dominant_clue": dominant_clue_present,
        "plausible_distractors": plausible_distractors,
        "not_vague_stem": not vague_stem_risk,
        "not_keyword_only_answer": not keyword_only_answer_risk,
    }
    quality_failures = [name for name, passed in quality_checks.items() if not passed]
    quality_score_10 = 0
    if clear_decision_point:
        quality_score_10 += 2
    if dominant_clue_present:
        quality_score_10 += 2
    if plausible_distractors:
        quality_score_10 += 2
    if not vague_stem_risk:
        quality_score_10 += 2
    if not keyword_only_answer_risk:
        quality_score_10 += 2

    score = 0
    if has_decision_frame:
        score += 2
    if ambiguity_level >= 7:
        score += 2
    if high_quality_distractors >= 2:
        score += 2
    if has_threshold:
        score += 2
    if has_conflict:
        score += 2
    if management_nuance >= 2:
        score += 1
    if high_judgment_style:
        score += 1
    if difficulty_target >= 7:
        score += 1
    if absolute_option_count == 0 and obvious_wrong_distractor_count == 0:
        score += 1
    if clinical_noise_count >= 1:
        score += 1
    if dynamic_progression_count >= 1:
        score += 1
    if tradeoff_axes_count >= 1:
        score += 1
    if decision_pressure_count >= 1:
        score += 1
    if plausible_option_count >= 2:
        score += 1
    if has_near_correct_trap:
        score += 1
    if second_best_strength_score >= 5:
        score += 2
    elif second_best_strength_score >= 3:
        score += 1
    if high_decision_pressure:
        score += 2
    elif decision_pressure_score >= 3:
        score += 1
    if deep_conflict:
        score += 2

    checks = {
        "decision_frame": has_decision_frame,
        "near_miss_distractors": high_quality_distractors >= 2,
        "near_correct_trap": has_near_correct_trap,
        "plausible_option_count": plausible_option_count >= 2,
        "second_best_strength": second_best_strength_score >= 5,
        "tradeoff_axes": tradeoff_axes_count >= 1,
        "decision_pressure": decision_pressure_count >= 1,
        "high_decision_pressure": high_decision_pressure,
        "conflicting_axes": has_conflict,
        "threshold_variable": has_threshold,
        "threshold_or_progression": has_threshold or dynamic_progression_count >= 1,
        "time_progression_or_response": time_progression_or_response,
        "partial_response_or_trend": partial_response_or_trend,
        "dynamic_progression": dynamic_progression_count >= 1,
        "clinical_noise": clinical_noise_count >= 1,
        "no_obvious_wrong_distractors": absolute_option_count == 0 and obvious_wrong_distractor_count == 0,
        "high_ambiguity": ambiguity_level >= 7,
        "management_nuance": management_nuance >= 2,
        "high_judgment_style": high_judgment_style,
        "template_family": has_template_family,
        "deep_conflict": deep_conflict,
        "question_quality_ready": not quality_failures,
    }
    rule = _difficulty_engine_rule_for_target(difficulty_target)
    required_failures = [name for name in rule["required"] if not checks.get(name)]
    recommended_gaps = [name for name in rule["recommended"] if not checks.get(name)]
    difficulty_engine_ready = not required_failures and not quality_failures
    effective_target = difficulty_target
    quality_status = "ready"
    rewrite_actions = []
    if difficulty_target >= 7 and not difficulty_engine_ready:
        effective_target = min(difficulty_target, 6)
        quality_status = "needs_rewrite" if len(required_failures) >= 2 else "downgraded"
        rewrite_actions = list(required_failures + quality_failures)

    disguised_recall_archetype = None
    if difficulty_target >= 7:
        if decision_archetype == "contraindication_context":
            disguised_recall_archetype = "contraindication_only"
        elif (
            "first-line" in (" " + (item.get("question_stem") or "").lower() + " ")
            and dynamic_progression_count == 0
        ):
            disguised_recall_archetype = "first_line_only"
        elif (
            item.get("question_style") == "diagnosis_refinement"
            and not item.get("decision_frame") == "best_next_step"
        ):
            disguised_recall_archetype = "classic_pattern_identification"

    return {
        "stage_b_quality_score": score,
        "stage_b_ready": score >= 8 and difficulty_engine_ready,
        "absolute_option_count": absolute_option_count,
        "obvious_wrong_distractor_count": obvious_wrong_distractor_count,
        "second_best_strength_score": second_best_strength_score,
        "decision_pressure_score": decision_pressure_score,
        "difficulty_engine_ready": difficulty_engine_ready,
        "difficulty_engine_checks": checks,
        "difficulty_engine_required_failures": required_failures,
        "difficulty_engine_recommended_gaps": recommended_gaps,
        "difficulty_engine_rule": f"{rule['min_target']}-{rule['max_target']}",
        "effective_difficulty_target_10": effective_target,
        "difficulty_engine_status": quality_status,
        "rewrite_actions": rewrite_actions,
        "disguised_recall_archetype": disguised_recall_archetype,
        "question_quality_score_10": quality_score_10,
        "question_quality_checks": quality_checks,
        "question_quality_failures": quality_failures,
    }


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
        "template_family_correct_count": {},
        "template_family_incorrect_count": {},
        "recent_mistake_topics": [],
        "recent_topic_history": [],
        "recent_answer_topics": [],
        "recent_answer_results": [],
        "recent_answer_levels": [],
        "recent_answer_styles": [],
        "recent_answer_template_families": [],
        "cards_shown_history": [],
        "cards_clicked_history": [],
        "recent_study_item_history": [],
        "current_difficulty_level": None,
        "pending_reinforcement_topic": None,
        "pending_reinforcement_decision_point": None,
        "pending_reinforcement_remaining": 0,
        "session_cards_requested": 0,
        "session_answered_count": 0,
        "session_correct_count": 0,
        "session_incorrect_count": 0,
        "session_focus_topic": None,
        "session_focus_template_family": None,
        "session_mode": None,
        "session_last_recommendation": None,
        "session_last_outcome": None,
        "study_session_id": None,
        "study_session_started_at": None,
        "study_session_target_questions": 0,
        "study_session_working_level": None,
        "study_session_consecutive_correct": 0,
        "study_session_consecutive_incorrect": 0,
        "study_session_completed_at": None,
        "study_session_last_summary": None,
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


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _count_for(mapping, key):
    if not key:
        return 0
    return _safe_int((mapping or {}).get(key), 0)


def _accuracy_snapshot(correct_count, incorrect_count):
    total = correct_count + incorrect_count
    if total <= 0:
        return 0.0
    return correct_count / total


def _priority_topics(state, limit=3):
    topic_keys = set((state.get("topics_correct_count") or {}).keys()) | set((state.get("topics_incorrect_count") or {}).keys())
    ranked = []
    for topic in topic_keys:
        correct_count = _count_for(state.get("topics_correct_count"), topic)
        incorrect_count = _count_for(state.get("topics_incorrect_count"), topic)
        if correct_count + incorrect_count <= 0:
            continue
        mistake_weight = 1 if topic in (state.get("recent_mistake_topics") or [])[-5:] else 0
        accuracy = _accuracy_snapshot(correct_count, incorrect_count)
        ranked.append(((incorrect_count * 3) + mistake_weight - accuracy, incorrect_count, -correct_count, topic))
    ranked.sort(reverse=True)
    return [topic for *_ignored, topic in ranked[:limit]]


def _priority_template_families(state, limit=2):
    family_keys = set((state.get("template_family_correct_count") or {}).keys()) | set((state.get("template_family_incorrect_count") or {}).keys())
    ranked = []
    for family in family_keys:
        correct_count = _count_for(state.get("template_family_correct_count"), family)
        incorrect_count = _count_for(state.get("template_family_incorrect_count"), family)
        if correct_count + incorrect_count <= 0:
            continue
        accuracy = _accuracy_snapshot(correct_count, incorrect_count)
        ranked.append(((incorrect_count * 3) - accuracy, incorrect_count, -correct_count, family))
    ranked.sort(reverse=True)
    return [family for *_ignored, family in ranked[:limit]]


def _session_progress_summary(state):
    answered = _safe_int(state.get("session_answered_count"))
    correct = _safe_int(state.get("session_correct_count"))
    incorrect = _safe_int(state.get("session_incorrect_count"))
    target = _safe_int(state.get("study_session_target_questions"))
    remaining = max(0, target - answered) if target else 0
    progress_percent = int(round((answered / target) * 100)) if target else 0
    return {
        "answered": answered,
        "correct": correct,
        "incorrect": incorrect,
        "target": target,
        "remaining": remaining,
        "progress_percent": progress_percent,
    }


def _build_session_plan(state, policy, mcq_pool):
    reinforcement = _pending_reinforcement(state)
    weak_topics = _priority_topics(state)
    weak_template_families = _priority_template_families(state)
    target_level = _safe_int(state.get("study_session_working_level"), 0) or _current_difficulty_level(state, policy)
    available_topics = {item.get("topic") for item in mcq_pool if item.get("topic")}
    available_template_families = {item.get("template_family") for item in mcq_pool if item.get("template_family")}

    focus_topic = None
    focus_template_family = None
    mode = "build"
    goal = "Keep breadth while staying close to your current level."

    if reinforcement and reinforcement.get("topic") in available_topics:
        focus_topic = reinforcement.get("topic")
        mode = "reinforce"
        goal = "Revisit the topic you just missed before moving on."
    elif weak_topics:
        focus_topic = next((topic for topic in weak_topics if topic in available_topics), None)
        if focus_topic:
            mode = "reinforce"
            goal = "Bias the next cards toward weaker topics until performance stabilizes."

    if weak_template_families:
        focus_template_family = next(
            (family for family in weak_template_families if family in available_template_families),
            focus_template_family,
        )

    if mode != "reinforce" and policy.get("residency_year") in {"R5", "R6"}:
        mode = "advance"
        goal = "Prefer higher-pressure decision questions while preserving topic spread."

    return {
        "mode": mode,
        "goal": goal,
        "focus_topic": focus_topic,
        "focus_template_family": focus_template_family,
        "target_level": target_level,
    }


def _session_target_questions_for_policy(policy):
    return _safe_int(SESSION_TARGET_QUESTIONS.get(policy.get("residency_year")), 5)


def _desired_decision_pressure_for_session(state, policy):
    residency_year = policy.get("residency_year") or "R6"
    baseline = {
        "R1": 1,
        "R2": 2,
        "R3": 3,
        "R4": 4,
        "R5": 5,
        "R6": 6,
    }.get(residency_year, 4)
    progress = _session_progress_summary(state)
    answered = progress["answered"]
    target = progress["target"] or _session_target_questions_for_policy(policy)
    completion_ratio = (answered / target) if target else 0
    if completion_ratio < 0.34:
        return max(1, baseline - 1)
    if completion_ratio < 0.67:
        return baseline
    return min(6, baseline + 1)


def _session_is_active(state):
    target = _safe_int(state.get("study_session_target_questions"))
    answered = _safe_int(state.get("session_answered_count"))
    return bool(state.get("study_session_id")) and not state.get("study_session_completed_at") and target > 0 and answered < target


def _start_new_study_session(state, policy):
    target_questions = _session_target_questions_for_policy(policy)
    return {
        "study_session_id": f"study-{_utc_now().strftime('%Y%m%d%H%M%S%f')}",
        "study_session_started_at": _utc_now(),
        "study_session_target_questions": target_questions,
        "study_session_working_level": _current_difficulty_level(state, policy),
        "study_session_consecutive_correct": 0,
        "study_session_consecutive_incorrect": 0,
        "study_session_completed_at": None,
        "study_session_last_summary": None,
        "session_answered_count": 0,
        "session_correct_count": 0,
        "session_incorrect_count": 0,
        "session_mode": "build",
        "session_last_outcome": None,
    }


def _session_summary_payload(state, policy):
    progress = _session_progress_summary(state)
    weak_topics = _priority_topics(state, limit=2)
    takeaway_topic = state.get("last_studied_topic") or "this session"
    takeaway_text = f"Clinical pearl: keep your eyes on the key management hinge in {takeaway_topic}."
    next_action = "continue_progression"
    next_action_label = "Continue with another question"
    if weak_topics:
        next_action = "repeat_weak_topic"
    return {
        "session_id": state.get("study_session_id"),
        "accuracy_percent": int(round(_accuracy_snapshot(progress["correct"], progress["incorrect"]) * 100)) if progress["answered"] else 0,
        "weak_topics": weak_topics,
        "takeaway": takeaway_text,
        "next_action": next_action,
        "next_action_label": next_action_label,
        "questions_completed": progress["answered"],
        "questions_target": progress["target"],
        "residency_year": policy.get("residency_year"),
    }


def _next_session_level_after_answer(state, policy, correct):
    working_level = _safe_int(state.get("study_session_working_level"), _current_difficulty_level(state, policy))
    consecutive_correct = _safe_int(state.get("study_session_consecutive_correct"))
    consecutive_incorrect = _safe_int(state.get("study_session_consecutive_incorrect"))
    if correct:
        consecutive_correct += 1
        consecutive_incorrect = 0
        if consecutive_correct >= SESSION_DIFFICULTY_RULES["increase_after_consecutive_correct"]:
            working_level = min(policy["max_level"], working_level + 1)
            consecutive_correct = 0
    else:
        consecutive_incorrect += 1
        consecutive_correct = 0
        if consecutive_incorrect >= SESSION_DIFFICULTY_RULES["decrease_after_consecutive_incorrect"]:
            working_level = max(policy["min_level"], working_level - 1)
            consecutive_incorrect = 0
    return {
        "working_level": working_level,
        "consecutive_correct": consecutive_correct,
        "consecutive_incorrect": consecutive_incorrect,
    }


def _session_meta_payload(state_or_plan, policy=None):
    if policy is None:
        return state_or_plan
    progress = _session_progress_summary(state_or_plan)
    current_question_number = min(progress["answered"] + (0 if state_or_plan.get("session_completed") or state_or_plan.get("study_session_completed_at") else 1), progress["target"]) if progress["target"] else 0
    return {
        "mode": state_or_plan.get("session_mode"),
        "goal": state_or_plan.get("session_last_recommendation"),
        "focus_topic": state_or_plan.get("session_focus_topic"),
        "focus_template_family": state_or_plan.get("session_focus_template_family"),
        "residency_year": policy.get("residency_year"),
        "current_level": _safe_int(state_or_plan.get("current_difficulty_level"), policy.get("baseline_level")),
        "working_level": _safe_int(state_or_plan.get("study_session_working_level"), _safe_int(state_or_plan.get("current_difficulty_level"), policy.get("baseline_level"))),
        "session_id": state_or_plan.get("study_session_id"),
        "session_started_at": state_or_plan.get("study_session_started_at"),
        "session_completed": bool(state_or_plan.get("study_session_completed_at")),
        "current_question_number": current_question_number,
        "session_progress": progress,
    }


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


def _get_cached_idle_cards(state):
    cards = state.get("idle_cards_cache")
    generated_at = state.get("idle_cards_cache_generated_at")
    if not cards or not generated_at:
        return None

    try:
        age_seconds = (_utc_now() - generated_at).total_seconds()
    except Exception:
        return None

    if age_seconds < 0 or age_seconds > IDLE_CARDS_CACHE_TTL_SECONDS:
        return None
    return {"cards": cards}


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
    preferred_template_family=None,
    preferred_decision_pressure=None,
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
    if preferred_template_family and item.get("template_family") == preferred_template_family:
        score += 18
    if preferred_difficulty_level is not None:
        item_level = int(item.get("difficulty_level") or 0)
        distance = abs(item_level - int(preferred_difficulty_level))
        score += max(0, 34 - (distance * 10))
    if preferred_question_style and item.get("question_style") == preferred_question_style:
        score += 20
    if preferred_decision_pressure is not None:
        pressure_gap = abs(int(item.get("decision_pressure_score") or 0) - int(preferred_decision_pressure))
        score += max(0, 18 - (pressure_gap * 6))
    if item.get("difficulty_engine_ready"):
        score += 24
    elif item.get("stage_b_ready"):
        score += 18
    score += int(item.get("stage_b_quality_score") or 0) * 3
    score += int(item.get("question_quality_score_10") or 0) * 3
    score += int(item.get("second_best_strength_score") or 0) * 4
    if item.get("difficulty_engine_checks", {}).get("deep_conflict"):
        score += 8
    score += int(item.get("difficulty_target_10") or 0) * 2
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
    recent_template_families = state.get("recent_answer_template_families") or []
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

    template_family = item.get("template_family")
    if template_family and template_family in recent_template_families[-TEMPLATE_FAMILY_HISTORY_WINDOW:]:
        score -= 10

    return score


def _eligible_for_main_flow(item, policy):
    if not item:
        return False
    if not policy.get("enforce_main_flow_gate"):
        return True
    target_10 = int(item.get("effective_difficulty_target_10") or item.get("difficulty_target_10") or 0)
    if target_10 < int(policy.get("main_flow_min_target_10") or 0):
        return False
    if target_10 > int(policy.get("main_flow_max_target_10") or 10):
        return False
    if policy.get("block_disguised_recall") and item.get("disguised_recall_archetype"):
        return False
    if item.get("decision_archetype") in set(policy.get("blocked_archetypes") or ()):
        return False
    if item.get("template_family") in set(policy.get("blocked_template_families") or ()):
        return False

    checks = item.get("difficulty_engine_checks") or {}
    for check_name in policy.get("required_checks") or ():
        if not checks.get(check_name):
            return False
    for group in policy.get("required_any") or ():
        if not any(checks.get(check_name) for check_name in group):
            return False
    if int(item.get("stage_b_quality_score") or 0) < int(policy.get("min_stage_b_quality_score") or 0):
        return False
    if int(item.get("second_best_strength_score") or 0) < int(policy.get("min_second_best_strength_score") or 0):
        return False
    if int(item.get("decision_pressure_score") or 0) < int(policy.get("min_decision_pressure_score") or 0):
        return False
    if policy.get("require_difficulty_engine_ready") and not item.get("difficulty_engine_ready"):
        return False
    return True


def _pick_best_item(
    session_id,
    candidates,
    salt,
    state,
    preferred_topic=None,
    preferred_template_family=None,
    preferred_decision_pressure=None,
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
            preferred_template_family=preferred_template_family,
            preferred_decision_pressure=preferred_decision_pressure,
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
    updated_at = item.get("last_reviewed_at")
    if updated_at in SOURCE_DATE_PLACEHOLDERS:
        updated_at = None
    return [
        {
            "source_id": "E1",
            "title": item["source_name"],
            "url": item["source_url"],
            "source_type": item["source_type"],
            "updated_at": updated_at,
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
    preferred_template_family=None,
    preferred_decision_pressure=None,
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
        preferred_template_family=preferred_template_family,
        preferred_decision_pressure=preferred_decision_pressure,
        preferred_item_type=preferred_item_type,
        preferred_difficulty_level=preferred_difficulty_level,
        preferred_question_style=preferred_question_style,
        reinforcement=reinforcement,
    )


def _practice_candidates_for_policy(mcq_pool, used_ids, target_level, policy):
    practice_floor = min(policy["max_level"], int(policy.get("practice_floor") or policy["min_level"]))
    candidates = [
        item for item in mcq_pool
        if item["id"] not in used_ids
        and practice_floor <= item.get("difficulty_level", 0) <= target_level
    ]
    if candidates:
        if policy.get("enforce_main_flow_gate"):
            strict = [item for item in candidates if _eligible_for_main_flow(item, policy)]
            if strict:
                return strict
        if policy.get("prefer_stage_b_judgment"):
            advanced = [item for item in candidates if item.get("difficulty_engine_ready")]
            if advanced:
                return advanced
            advanced = [item for item in candidates if item.get("stage_b_ready")]
            if advanced:
                return advanced
        return candidates
    fallback = [
        item for item in mcq_pool
        if item["id"] not in used_ids and item.get("difficulty_level", 0) >= practice_floor
    ]
    if policy.get("enforce_main_flow_gate"):
        strict = [item for item in fallback if _eligible_for_main_flow(item, policy)]
        if strict:
            return strict
        return []
    if policy.get("prefer_stage_b_judgment"):
        advanced = [item for item in fallback if item.get("difficulty_engine_ready")]
        if advanced:
            return advanced
        advanced = [item for item in fallback if item.get("stage_b_ready")]
        if advanced:
            return advanced
    return fallback


def _advanced_mcq_candidates_for_policy(mcq_pool, used_ids, target_level, policy):
    challenge_floor = max(target_level, policy["max_level"] - 1)
    candidates = [
        item for item in mcq_pool
        if item["id"] not in used_ids and item.get("difficulty_level", 0) >= challenge_floor
    ]
    if candidates:
        if policy.get("enforce_main_flow_gate"):
            strict = [item for item in candidates if _eligible_for_main_flow(item, policy)]
            if strict:
                return strict
        if policy.get("prefer_stage_b_judgment"):
            advanced = [item for item in candidates if item.get("difficulty_engine_ready")]
            if advanced:
                return advanced
            advanced = [item for item in candidates if item.get("stage_b_ready")]
            if advanced:
                return advanced
        return candidates
    fallback = [
        item for item in mcq_pool
        if item["id"] not in used_ids and item.get("difficulty_level", 0) >= target_level
    ]
    if policy.get("enforce_main_flow_gate"):
        strict = [item for item in fallback if _eligible_for_main_flow(item, policy)]
        if strict:
            return strict
        return []
    if policy.get("prefer_stage_b_judgment"):
        advanced = [item for item in fallback if item.get("difficulty_engine_ready")]
        if advanced:
            return advanced
        advanced = [item for item in fallback if item.get("stage_b_ready")]
        if advanced:
            return advanced
    return fallback


def get_idle_study_cards(session_id):
    ensure_study_content_seed()
    state = _load_state(session_id)
    cached_cards = _get_cached_idle_cards(state)
    if cached_cards:
        log_event("study_cards_cache_hit", session_id, {"card_ids": [card["content_item_id"] for card in cached_cards["cards"]]})
        return cached_cards

    user_profile = get_user_profile(session_id)
    policy = _difficulty_policy_for_profile(user_profile)
    recent_topics = state.get("recent_topic_history") or []
    recent_exclude_ids = _recent_study_exclude_ids(state)
    reinforcement = _pending_reinforcement(state)

    used_ids = set()
    cards = []

    mcq_pool = _get_items(item_type="mcq", exclude_ids=recent_exclude_ids) or _get_items(item_type="mcq")
    session_plan = _build_session_plan(state, policy, mcq_pool)
    preferred_topic = session_plan.get("focus_topic") or (reinforcement or {}).get("topic") or (recent_topics[-1] if recent_topics else None)
    preferred_template_family = session_plan.get("focus_template_family")
    preferred_decision_pressure = _desired_decision_pressure_for_session(state, policy)
    available_levels = sorted({item.get("difficulty_level") for item in mcq_pool if item.get("difficulty_level")})
    target_level = session_plan.get("target_level") or _target_difficulty_level(state, policy, available_levels)
    available_styles = {item.get("question_style") for item in mcq_pool if item.get("question_style")}
    style_distribution = _prune_distribution(_distribution_for_level(target_level), available_styles)
    target_style = _target_question_style(state, target_level, style_distribution.keys())

    practice_candidates = _practice_candidates_for_policy(mcq_pool, used_ids, target_level, policy)
    practice_item = _pick_targeted_item(
        session_id,
        state,
        practice_candidates,
        "practice",
        preferred_item_type="mcq",
        preferred_topic=preferred_topic,
        preferred_template_family=preferred_template_family,
        preferred_decision_pressure=max(1, preferred_decision_pressure - 1),
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
        preferred_template_family=preferred_template_family,
        preferred_decision_pressure=preferred_decision_pressure,
        preferred_difficulty_level=challenge_level,
        preferred_question_style=challenge_style,
    )
    if challenge_item:
        used_ids.add(challenge_item["id"])
        cards.append(_build_card("dynamic_card", "dynamic", challenge_item, _title_for_dynamic(challenge_item, bool(preferred_topic)), "Push a bit harder", "Continue"))

    if policy.get("prefer_advanced_mcq_stack"):
        advanced_candidates = _advanced_mcq_candidates_for_policy(mcq_pool, used_ids, target_level, policy)
        advanced_item = _pick_targeted_item(
            session_id,
            state,
            advanced_candidates,
            "advanced",
            preferred_item_type="mcq",
            preferred_topic=preferred_topic,
            preferred_template_family=preferred_template_family,
            preferred_decision_pressure=min(6, preferred_decision_pressure + 1),
            preferred_difficulty_level=policy["max_level"],
            preferred_question_style=challenge_style or target_style,
            reinforcement=reinforcement,
        )
        if advanced_item:
            used_ids.add(advanced_item["id"])
            cards.append(_build_card("advanced_card", "dynamic", advanced_item, "Senior Challenge", "Board-style trap", "Continue"))

    if len(cards) < 3:
        pearl_pool = _get_items(item_type="pearl", exclude_ids=used_ids | recent_exclude_ids) or _get_items(item_type="pearl", exclude_ids=used_ids)
        pearl_pool = _family_first_candidates(pearl_pool, state)
        pearl_item = _pick_targeted_item(
            session_id,
            state,
            pearl_pool,
            "pearl",
            preferred_item_type="pearl",
            preferred_topic=preferred_topic,
            preferred_template_family=preferred_template_family,
            preferred_decision_pressure=preferred_decision_pressure,
            preferred_difficulty_level=min(target_level, policy["max_level"]),
            preferred_question_style="pearl",
        )
        if pearl_item:
            used_ids.add(pearl_item["id"])
            cards.append(_build_card("pearl_card", "pearl", pearl_item, "Quick Pearl", "Quick takeaway", "Open"))

    if len(cards) < 3:
        fallback_pool = _get_items(exclude_ids=used_ids | recent_exclude_ids) or _get_items(exclude_ids=used_ids)
        if policy.get("enforce_main_flow_gate"):
            fallback_pool = [item for item in fallback_pool if _eligible_for_main_flow(item, policy)]
        scored_fallback = sorted(
            fallback_pool,
            key=lambda item: _selection_score(
                item,
                state,
                preferred_topic=preferred_topic,
                preferred_template_family=preferred_template_family,
                preferred_decision_pressure=preferred_decision_pressure,
                preferred_difficulty_level=target_level,
            ),
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
            "idle_cards_cache": cards[:3],
            "idle_cards_cache_generated_at": _utc_now(),
            "session_cards_requested": _safe_int(state.get("session_cards_requested")) + len(cards[:3]),
            "session_focus_topic": session_plan.get("focus_topic"),
            "session_focus_template_family": session_plan.get("focus_template_family"),
            "session_mode": session_plan.get("mode"),
            "session_last_recommendation": session_plan.get("goal"),
        },
    )
    log_event("study_cards_impression", session_id, {"card_ids": [card["content_item_id"] for card in cards]})
    session_state = dict(state)
    session_state.update(
        {
            "session_cards_requested": _safe_int(state.get("session_cards_requested")) + len(cards[:3]),
            "session_focus_topic": session_plan.get("focus_topic"),
            "session_focus_template_family": session_plan.get("focus_template_family"),
            "session_mode": session_plan.get("mode"),
            "session_last_recommendation": session_plan.get("goal"),
            "current_difficulty_level": target_level,
        }
    )
    return {"cards": cards[:3], "session_meta": _session_meta_payload(session_state, policy)}


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
                "difficulty_target_10": item.get("difficulty_target_10"),
                "question_quality_score_10": item.get("question_quality_score_10"),
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
    user_profile = get_user_profile(session_id)
    policy = _difficulty_policy_for_profile(user_profile)
    session_updates = {}
    if item["item_type"] == "mcq" and not _session_is_active(state):
        previous_target = _safe_int(state.get("study_session_target_questions"))
        previous_answered = _safe_int(state.get("session_answered_count"))
        if state.get("study_session_id") and previous_target > 0 and 0 < previous_answered < previous_target:
            log_event(
                "study_session_abandoned",
                session_id,
                {
                    "study_session_id": state.get("study_session_id"),
                    "drop_off_question_index": previous_answered,
                    "questions_target": previous_target,
                },
            )
        session_updates.update(_start_new_study_session(state, policy))
        log_event(
            "study_session_started",
            session_id,
            {
                "study_session_id": session_updates.get("study_session_id"),
                "questions_target": session_updates.get("study_session_target_questions"),
                "residency_year": policy.get("residency_year"),
                "starting_level": session_updates.get("study_session_working_level"),
            },
        )
    _save_state(
        session_id,
        {
            **session_updates,
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
        log_event(
            "mcq_started",
            session_id,
            {
                "content_item_id": item["id"],
                "topic": item["topic"],
                "study_session_id": session_updates.get("study_session_id") or state.get("study_session_id"),
                "question_index": _safe_int(state.get("session_answered_count")) + 1,
                "session_target": _safe_int(session_updates.get("study_session_target_questions"), _safe_int(state.get("study_session_target_questions"))),
            },
        )
    session_state = dict(state)
    session_state.update(
        {
            **session_updates,
            "last_studied_topic": item["topic"],
            "last_active_item_id": item["id"],
            "last_active_item_type": item["item_type"],
        }
    )
    return {
        "reply": intro,
        "study_item": _build_study_item_payload(item),
        "session_meta": _session_meta_payload(session_state, policy),
    }


def _recommendation_after_answer(correct, item, next_level, policy):
    topic = item.get("topic")
    if not correct:
        return {
            "mode": "reinforce",
            "message": f"Stay on {topic} for one more pass and compare the near-miss choice.",
        }
    if next_level > _safe_int(item.get("difficulty_level"), next_level):
        return {
            "mode": "advance",
            "message": "You cleared this one cleanly. The next session can push a little harder.",
        }
    if policy.get("residency_year") in {"R5", "R6"}:
        return {
            "mode": "advance",
            "message": "Keep the pressure high and rotate to another decision-heavy topic.",
        }
    return {
        "mode": "build",
        "message": "Keep building with a nearby topic at the same level before stepping up.",
    }


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
    answer_template_families = list(state.get("recent_answer_template_families") or [])
    template_family_correct_count = dict(state.get("template_family_correct_count") or {})
    template_family_incorrect_count = dict(state.get("template_family_incorrect_count") or {})
    template_family = item.get("template_family")

    if correct:
        correct_counts[topic] = correct_counts.get(topic, 0) + 1
        if template_family:
            template_family_correct_count[template_family] = template_family_correct_count.get(template_family, 0) + 1
    else:
        incorrect_counts[topic] = incorrect_counts.get(topic, 0) + 1
        recent_mistakes.append(topic)
        if template_family:
            template_family_incorrect_count[template_family] = template_family_incorrect_count.get(template_family, 0) + 1

    answer_results = _trim_history(answer_results + [correct], DEMOTION_WINDOW)
    answer_levels = _trim_history(answer_levels + [item.get("difficulty_level")], LEVEL_HISTORY_WINDOW)
    answer_styles = _trim_history(answer_styles + [item.get("question_style")], STYLE_HISTORY_WINDOW)
    answer_topics = _trim_history(answer_topics + [topic], DEMOTION_WINDOW)
    answer_template_families = _trim_history(
        answer_template_families + [item.get("template_family")],
        TEMPLATE_FAMILY_HISTORY_WINDOW,
    )
    next_level = _next_difficulty_level_after_answer(state, policy, answer_levels, answer_results)
    session_level_state = _next_session_level_after_answer(state, policy, correct)
    recommendation = _recommendation_after_answer(correct, item, session_level_state["working_level"], policy)

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

    answered_count = _safe_int(state.get("session_answered_count")) + 1
    updated_fields = {
        "last_interaction_type": "mcq_feedback",
        "last_incomplete_item_id": None,
        "last_incomplete_item_type": None,
        "last_active_item_id": item["id"],
        "last_active_item_type": item["item_type"],
        "topics_seen": _trim_history((state.get("topics_seen") or []) + [topic], 30),
        "topics_correct_count": correct_counts,
        "topics_incorrect_count": incorrect_counts,
        "template_family_correct_count": template_family_correct_count,
        "template_family_incorrect_count": template_family_incorrect_count,
        "recent_mistake_topics": _trim_history(recent_mistakes, 8),
        "recent_answer_results": answer_results,
        "recent_answer_levels": answer_levels,
        "recent_answer_styles": answer_styles,
        "recent_answer_topics": answer_topics,
        "recent_answer_template_families": answer_template_families,
        "last_studied_topic": topic,
        "last_answered_option": (selected_option or "").upper(),
        "last_answer_correct": correct,
        "current_difficulty_level": next_level,
        "pending_reinforcement_topic": pending_reinforcement_topic,
        "pending_reinforcement_decision_point": pending_reinforcement_decision_point,
        "pending_reinforcement_remaining": pending_reinforcement_remaining,
        "session_answered_count": answered_count,
        "session_correct_count": _safe_int(state.get("session_correct_count")) + (1 if correct else 0),
        "session_incorrect_count": _safe_int(state.get("session_incorrect_count")) + (0 if correct else 1),
        "session_focus_topic": pending_reinforcement_topic if pending_reinforcement_topic else state.get("session_focus_topic"),
        "session_focus_template_family": template_family if not correct else state.get("session_focus_template_family"),
        "session_mode": recommendation.get("mode"),
        "session_last_recommendation": recommendation.get("message"),
        "session_last_outcome": "correct" if correct else "incorrect",
        "study_session_working_level": session_level_state["working_level"],
        "study_session_consecutive_correct": session_level_state["consecutive_correct"],
        "study_session_consecutive_incorrect": session_level_state["consecutive_incorrect"],
    }

    updated_state = dict(state)
    updated_state.update(updated_fields)
    session_target = _safe_int(updated_state.get("study_session_target_questions"))
    session_complete = bool(session_target and answered_count >= session_target)
    next_session_item = None
    session_summary = None
    if session_complete:
        updated_fields["study_session_completed_at"] = _utc_now()
        updated_state["study_session_completed_at"] = updated_fields["study_session_completed_at"]
        session_summary = _session_summary_payload(updated_state, policy)
        updated_fields["study_session_last_summary"] = session_summary
        updated_state["study_session_last_summary"] = session_summary
    else:
        next_session_item = _pick_next_session_item(session_id, updated_state, policy, anchor_topic=topic)
        if next_session_item:
            updated_fields["last_incomplete_item_id"] = next_session_item["id"]
            updated_fields["last_incomplete_item_type"] = next_session_item["item_type"]
            updated_fields["last_active_item_id"] = next_session_item["id"]
            updated_fields["last_active_item_type"] = next_session_item["item_type"]
            updated_fields["last_studied_topic"] = next_session_item["topic"]
            updated_fields["recent_study_item_history"] = _record_studied_item(updated_state, next_session_item["id"])
            updated_state.update(
                {
                    "last_incomplete_item_id": next_session_item["id"],
                    "last_incomplete_item_type": next_session_item["item_type"],
                    "last_active_item_id": next_session_item["id"],
                    "last_active_item_type": next_session_item["item_type"],
                    "last_studied_topic": next_session_item["topic"],
                    "recent_study_item_history": updated_fields["recent_study_item_history"],
                }
            )

    _save_state(session_id, updated_fields)
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
            "next_session_working_level": session_level_state["working_level"],
            "question_style": item.get("question_style"),
        },
    )
    log_event("mcq_correct" if correct else "mcq_incorrect", session_id, {"content_item_id": item["id"], "topic": topic})
    log_event(
        "study_session_progress",
        session_id,
        {
            "study_session_id": updated_state.get("study_session_id"),
            "question_index": answered_count,
            "questions_target": session_target,
            "correct": correct,
            "working_level": session_level_state["working_level"],
            "accuracy_percent": int(round(_accuracy_snapshot(updated_state.get("session_correct_count"), updated_state.get("session_incorrect_count")) * 100)),
        },
    )
    if session_complete and session_summary:
        log_event("study_session_completed", session_id, session_summary)
    elif next_session_item:
        log_event(
            "study_session_question_presented",
            session_id,
            {
                "study_session_id": updated_state.get("study_session_id"),
                "content_item_id": next_session_item["id"],
                "question_index": answered_count + 1,
                "questions_target": session_target,
                "difficulty_level": next_session_item.get("difficulty_level"),
                "question_quality_score_10": next_session_item.get("question_quality_score_10"),
            },
        )

    reply = _build_mcq_feedback_reply(item, correct, (selected_option or "").upper())
    response = {
        "reply": f"{reply}\n\nNext: {recommendation.get('message')}",
        "study_context_item_id": item["id"],
        "study_feedback": {
            "result": "correct" if correct else "incorrect",
            "recommended_mode": recommendation.get("mode"),
            "recommended_next_step": recommendation.get("message"),
            "next_difficulty_level": next_level,
            "next_session_working_level": session_level_state["working_level"],
            "topic": topic,
            "template_family": template_family,
        },
        "session_meta": _session_meta_payload(updated_state, policy),
    }
    if session_complete and session_summary:
        response["session_summary"] = session_summary
        response["study_followups"] = [
            {"action": session_summary.get("next_action", "another_question"), "label": session_summary.get("next_action_label", "Continue")},
            {"action": "another_question", "label": "Continue progression"},
            {"action": "quick_recap", "label": "Give me the rule"},
            {"action": "show_source", "label": "Show source"},
        ]
    elif next_session_item:
        response["study_item"] = _build_study_item_payload(next_session_item)
    else:
        response["study_followups"] = [
            {"action": "another_question", "label": "Another question"},
            {"action": "explain_why", "label": "Explain why"},
            {"action": "show_source", "label": "Show source"},
            {"action": "quick_recap", "label": "Give me the rule"},
        ]
    return response


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
    session_plan = _build_session_plan(state, policy, [candidate for candidate in candidates if candidate.get("item_type") == "mcq"] or candidates)
    return _pick_best_item(
        session_id,
        candidates,
        f"{item['id']}:{item_type}",
        state,
        preferred_topic=item.get("topic"),
        preferred_template_family=session_plan.get("focus_template_family"),
        preferred_decision_pressure=_desired_decision_pressure_for_session(state, policy),
        preferred_item_type=item_type,
        preferred_difficulty_level=target_level if item_type == "mcq" else item.get("difficulty_level"),
        preferred_question_style=item.get("question_style") if item_type == "mcq" else "pearl",
        reinforcement=_pending_reinforcement(state) if item_type == "mcq" else None,
    )


def _pick_next_session_item(session_id, state, policy, anchor_topic=None):
    recent_exclude_ids = _recent_study_exclude_ids(state)
    active_item_id = state.get("last_active_item_id")
    if active_item_id:
        recent_exclude_ids.add(active_item_id)
    mcq_pool = _get_items(item_type="mcq", exclude_ids=recent_exclude_ids) or _get_items(item_type="mcq", exclude_ids={active_item_id} if active_item_id else None)
    if not mcq_pool:
        return None

    session_plan = _build_session_plan(state, policy, mcq_pool)
    preferred_topic = session_plan.get("focus_topic") or anchor_topic or state.get("last_studied_topic")
    preferred_template_family = session_plan.get("focus_template_family")
    preferred_decision_pressure = _desired_decision_pressure_for_session(state, policy)
    target_level = session_plan.get("target_level") or _safe_int(state.get("study_session_working_level"), _current_difficulty_level(state, policy))
    available_styles = {item.get("question_style") for item in mcq_pool if item.get("question_style")}
    style_distribution = _prune_distribution(_distribution_for_level(target_level), available_styles)
    target_style = _target_question_style(state, target_level, style_distribution.keys())

    candidate_pool = [
        item for item in mcq_pool
        if abs(_safe_int(item.get("difficulty_level"), target_level) - target_level) <= 1
    ] or mcq_pool
    if policy.get("enforce_main_flow_gate"):
        candidate_pool = [item for item in candidate_pool if _eligible_for_main_flow(item, policy)] or candidate_pool

    return _pick_targeted_item(
        session_id,
        state,
        candidate_pool,
        "session-next",
        preferred_item_type="mcq",
        preferred_topic=preferred_topic,
        preferred_template_family=preferred_template_family,
        preferred_decision_pressure=preferred_decision_pressure,
        preferred_difficulty_level=target_level,
        preferred_question_style=target_style,
        reinforcement=_pending_reinforcement(state),
    )


def handle_study_action(session_id, content_item_id, action):
    item = _normalize_study_item(study_content_collection.find_one({"id": content_item_id, "enabled": True}, {"_id": 0}))
    if not item:
        return {"reply": "I couldn’t find that study item anymore."}
    state = _load_state(session_id)
    user_profile = get_user_profile(session_id)
    policy = _difficulty_policy_for_profile(user_profile)

    log_event("mcq_followup_clicked" if item["item_type"] == "mcq" else "pearl_followup_clicked", session_id, {"content_item_id": item["id"], "action": action})

    if action == "show_source":
        log_event("source_requested", session_id, {"content_item_id": item["id"], "topic": item["topic"]})
        return {
            "reply": None,
            "sources": _source_payload(item),
        }

    if action == "repeat_weak_topic":
        weak_topics = ((state.get("study_session_last_summary") or {}).get("weak_topics") or [])
        weak_topic = weak_topics[0] if weak_topics else state.get("session_focus_topic") or item.get("topic")
        target_item = _pick_next_session_item(session_id, state, policy, anchor_topic=weak_topic)
        if not target_item:
            return {"reply": "I don’t have a good next question on that weaker topic yet."}
        _save_state(
            session_id,
            {
                "last_incomplete_item_id": target_item["id"],
                "last_incomplete_item_type": target_item["item_type"],
                "last_active_item_id": target_item["id"],
                "last_active_item_type": target_item["item_type"],
                "last_studied_topic": target_item["topic"],
                "recent_study_item_history": _record_studied_item(state, target_item["id"]),
                "session_focus_topic": weak_topic,
            },
        )
        return {
            "reply": f"Let’s reinforce {weak_topic}.",
            "study_item": _build_study_item_payload(target_item),
            "session_meta": _session_meta_payload(state, policy),
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
            "session_meta": _session_meta_payload(state, policy),
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
            "session_meta": _session_meta_payload(state, policy),
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
            "session_meta": _session_meta_payload(state, policy),
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
