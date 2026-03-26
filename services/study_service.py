from datetime import datetime
from random import Random
import re

from db import study_content_collection, study_user_state_collection
from services.logging_service import log_event


STUDY_SEED_ITEMS = [
    {
        "id": "mcq_preeclampsia_delivery",
        "item_type": "mcq",
        "topic": "Preeclampsia",
        "subtopic": "Timing of delivery",
        "question_stem": "At 35 weeks, preeclampsia with severe features is diagnosed. What is the board-oriented next step?",
        "options": [
            {"key": "A", "text": "Expectant management until 37 weeks"},
            {"key": "B", "text": "Delivery after maternal stabilization"},
            {"key": "C", "text": "Discharge with home BP checks"},
            {"key": "D", "text": "Repeat labs in one week"},
        ],
        "correct_option": "B",
        "short_explanation": "Correct. Severe features usually shift the plan toward delivery once the mother is stabilized.",
        "key_takeaway": "The exam clue is severe features: management becomes more urgent.",
        "board_rule": "Severe features at 34 weeks or more: stabilize the mother, then deliver.",
        "difficulty": "medium",
        "estimated_time_seconds": 60,
        "source_id": "study_src_nice_hypertension",
        "source_name": "NICE Guideline: Hypertension in Pregnancy",
        "source_type": "Guideline",
        "source_url": "https://www.nice.org.uk/guidance/ng133",
        "source_excerpt": "Severe maternal disease changes timing and usually favors delivery after stabilization rather than routine expectant care.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "mcq_pph_first_step",
        "item_type": "mcq",
        "topic": "PPH",
        "subtopic": "Initial management",
        "question_stem": "Postpartum hemorrhage starts immediately after vaginal birth from uterine atony. What is the best first-line uterotonic?",
        "options": [
            {"key": "A", "text": "Oxytocin"},
            {"key": "B", "text": "Methotrexate"},
            {"key": "C", "text": "Magnesium sulfate"},
            {"key": "D", "text": "Heparin"},
        ],
        "correct_option": "A",
        "short_explanation": "Correct. Oxytocin is the standard first-line uterotonic for atony-related PPH.",
        "key_takeaway": "On boards, uterine atony plus immediate bleeding points to oxytocin first.",
        "board_rule": "Immediate PPH from uterine atony: uterine massage plus oxytocin first.",
        "difficulty": "easy",
        "estimated_time_seconds": 45,
        "source_id": "study_src_acog_pph",
        "source_name": "ACOG Practice Bulletin: Postpartum Hemorrhage",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2017/10/postpartum-hemorrhage",
        "source_excerpt": "Initial treatment of uterine atony includes uterine massage and oxytocin as first-line uterotonic therapy.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "mcq_ctg_late_decels",
        "item_type": "mcq",
        "topic": "CTG",
        "subtopic": "Interpretation",
        "question_stem": "Recurrent late decelerations on CTG most strongly suggest which problem?",
        "options": [
            {"key": "A", "text": "Cord compression"},
            {"key": "B", "text": "Uteroplacental insufficiency"},
            {"key": "C", "text": "Fetal sleep cycle"},
            {"key": "D", "text": "Maternal fever only"},
        ],
        "correct_option": "B",
        "short_explanation": "Correct. Late decelerations classically point to uteroplacental insufficiency.",
        "key_takeaway": "Variable decelerations suggest cord compression; late decelerations suggest placental compromise.",
        "board_rule": "Late decelerations point to uteroplacental insufficiency; variable decelerations point to cord compression.",
        "difficulty": "medium",
        "estimated_time_seconds": 50,
        "source_id": "study_src_nice_ctg",
        "source_name": "NICE Guideline: Fetal Monitoring in Labour",
        "source_type": "Guideline",
        "source_url": "https://www.nice.org.uk/guidance/ng229",
        "source_excerpt": "Late decelerations are associated with uteroplacental insufficiency and possible fetal hypoxia.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "mcq_pprom_antibiotics",
        "item_type": "mcq",
        "topic": "PPROM",
        "subtopic": "Latency management",
        "question_stem": "PPROM at 30 weeks with no signs of infection: what supportive step improves latency and outcomes?",
        "options": [
            {"key": "A", "text": "Latency antibiotics"},
            {"key": "B", "text": "Immediate hysteroscopy"},
            {"key": "C", "text": "Routine tocolysis for weeks"},
            {"key": "D", "text": "No treatment if fetal tracing is normal"},
        ],
        "correct_option": "A",
        "short_explanation": "Correct. In appropriate PPROM cases, latency antibiotics help prolong pregnancy and reduce infection risk.",
        "key_takeaway": "Boards often test antibiotics as part of expectant PPROM care before term.",
        "board_rule": "PPROM before term without infection: give latency antibiotics as part of expectant management.",
        "difficulty": "medium",
        "estimated_time_seconds": 55,
        "source_id": "study_src_acog_prom",
        "source_name": "ACOG Practice Bulletin: Prelabor Rupture of Membranes",
        "source_type": "Guideline",
        "source_url": "https://www.acog.org/clinical/clinical-guidance/practice-bulletin/articles/2020/03/prelabor-rupture-of-membranes",
        "source_excerpt": "Latency antibiotics are recommended in eligible PPROM cases to prolong pregnancy and lower infectious morbidity.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
    {
        "id": "pearl_pph_atony",
        "item_type": "pearl",
        "topic": "PPH",
        "title": "Quick Pearl: PPH",
        "bullets": [
            "Uterine atony is the most common cause of primary PPH.",
            "The first practical move is uterine massage plus oxytocin.",
            "If bleeding continues, escalate fast and think causes systematically.",
        ],
        "board_rule": "Primary PPH from uterine atony: massage and oxytocin come first.",
        "board_relevance": "Common board pattern",
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
        "bullets": [
            "Early decelerations usually fit head compression.",
            "Variable decelerations point more to cord compression.",
            "Late decelerations should make you think placental insufficiency.",
        ],
        "board_rule": "Early = head compression, variable = cord compression, late = placental insufficiency.",
        "board_relevance": "High-yield interpretation rule",
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
        "bullets": [
            "HSIL is not a watch-and-wait board answer.",
            "The exam focus is timely colposcopic evaluation or expedited management when appropriate.",
            "High-grade cytology raises the stakes even when the patient feels well.",
        ],
        "board_rule": "HSIL is not watchful waiting; it needs prompt risk-based evaluation or treatment.",
        "board_relevance": "Classic exam escalation rule",
        "estimated_time_seconds": 35,
        "source_id": "study_src_asccp_hsil",
        "source_name": "ASCCP Clinical Practice",
        "source_type": "Guideline",
        "source_url": "https://www.asccp.org/clinical-practice",
        "source_excerpt": "High-grade screening abnormalities require prompt risk-based evaluation and management.",
        "approved_for_stage_b": True,
        "last_reviewed_at": "2025-01-01",
        "enabled": True,
    },
]


def _utc_now():
    return datetime.utcnow()


def ensure_study_content_seed():
    now = _utc_now()
    for item in STUDY_SEED_ITEMS:
        if study_content_collection.find_one({"id": item["id"]}, {"id": 1}):
            continue
        payload = dict(item)
        payload["created_at"] = now
        payload["updated_at"] = now
        study_content_collection.insert_one(payload)


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
        "cards_shown_history": [],
        "cards_clicked_history": [],
        "recent_study_item_history": [],
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
    items = list(study_content_collection.find(query, {"_id": 0}))
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


def _title_for_dynamic(item, has_history):
    topic = item.get("topic", "Topic")
    if has_history:
        if len(topic) <= 18:
            return f"Revisit {topic}"
        return f"Back to {topic}"
    return "Recommended Topic"


def _subtitle_for_dynamic(has_history):
    return "Quick topic revisit" if has_history else "Fast board-oriented pick"


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


def _build_mcq_feedback_reply(item, correct):
    status = "Correct." if correct else "Not quite."
    explanation = re.sub(r"^(Correct|Not quite)\.\s*", "", item["short_explanation"]).strip()
    return f"{status} {explanation} {item['key_takeaway']}"


def _board_rule_text(item):
    rule = (item.get("board_rule") or "").strip()
    if rule:
        return rule
    if item.get("item_type") == "pearl" and item.get("bullets"):
        return " ".join(item["bullets"][:2])
    return item.get("key_takeaway", "").strip()


def _build_mcq_explain_reply(item, state):
    correct_key = item.get("correct_option")
    correct_text = _option_text_by_key(item, correct_key)
    selected_key = (state.get("last_answered_option") or "").upper() or None
    selected_text = _option_text_by_key(item, selected_key)
    answered_correctly = state.get("last_answer_correct")

    opening = "Why this answer:"
    if answered_correctly is True:
        opening = "Why that answer is right:"
    elif answered_correctly is False:
        opening = "Why your answer was off:"

    parts = [opening]
    if correct_key and correct_text:
        parts.append(f"The best answer is {correct_key}: {correct_text}.")

    explanation = re.sub(r"^(Correct|Not quite)\.\s*", "", item["short_explanation"]).strip()
    if explanation:
        parts.append(explanation)

    rule = _board_rule_text(item)
    if rule:
        parts.append(f"Board rule: {rule}")

    if answered_correctly is False and selected_key and selected_text:
        parts.append(f"You picked {selected_key}: {selected_text}, but the board clue points away from that choice.")

    return " ".join(parts)


def _get_active_item(session_id):
    state = _load_state(session_id)
    item_id = state.get("last_incomplete_item_id") or state.get("last_active_item_id")
    if not item_id:
        return None, state
    item = study_content_collection.find_one({"id": item_id, "enabled": True}, {"_id": 0})
    return item, state


def get_idle_study_cards(session_id):
    ensure_study_content_seed()
    state = _load_state(session_id)
    recent_mistakes = state.get("recent_mistake_topics") or []
    recent_topics = state.get("recent_topic_history") or []
    recent_exclude_ids = _recent_study_exclude_ids(state)

    weak_topic = recent_mistakes[-1] if recent_mistakes else None
    recent_topic = recent_topics[-1] if recent_topics else None

    used_ids = set()

    practice_pool = _get_items(item_type="mcq", topic=weak_topic, exclude_ids=recent_exclude_ids) or _get_items(item_type="mcq", exclude_ids=recent_exclude_ids)
    if not practice_pool:
        practice_pool = _get_items(item_type="mcq", topic=weak_topic) or _get_items(item_type="mcq")
    practice_item = _pick_item(session_id, practice_pool, "practice")
    if practice_item:
        used_ids.add(practice_item["id"])

    pearl_pool = _get_items(item_type="pearl", exclude_ids=used_ids | recent_exclude_ids)
    if not pearl_pool:
        pearl_pool = _get_items(item_type="pearl", exclude_ids=used_ids)
    pearl_item = _pick_item(session_id, pearl_pool, "pearl")
    if pearl_item:
        used_ids.add(pearl_item["id"])

    dynamic_topic = weak_topic or recent_topic
    dynamic_pool = (
        _get_items(topic=dynamic_topic, exclude_ids=used_ids | recent_exclude_ids)
        if dynamic_topic
        else _get_items(exclude_ids=used_ids | recent_exclude_ids)
    )
    if not dynamic_pool:
        dynamic_pool = (
            _get_items(topic=dynamic_topic, exclude_ids=used_ids)
            if dynamic_topic
            else _get_items(exclude_ids=used_ids)
        )
    dynamic_item = _pick_item(session_id, dynamic_pool, "dynamic")

    cards = []
    if practice_item:
        cards.append(
            {
                "id": "practice_card",
                "type": "practice",
                "title": "Quick MCQ",
                "subtitle": "1-minute exam-style practice",
                "cta": "Start",
                "content_item_id": practice_item["id"],
                "topic": practice_item["topic"],
            }
        )
    if pearl_item:
        cards.append(
            {
                "id": "pearl_card",
                "type": "pearl",
                "title": "Quick Pearl",
                "subtitle": "Fast board takeaway",
                "cta": "Open",
                "content_item_id": pearl_item["id"],
                "topic": pearl_item["topic"],
            }
        )
    if dynamic_item:
        cards.append(
            {
                "id": "dynamic_card",
                "type": "dynamic",
                "title": _title_for_dynamic(dynamic_item, bool(dynamic_topic)),
                "subtitle": _subtitle_for_dynamic(bool(dynamic_topic)),
                "cta": "Continue",
                "content_item_id": dynamic_item["id"],
                "topic": dynamic_item["topic"],
            }
        )

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
            }
        )
    else:
        payload.update(
            {
                "title": item["title"],
                "bullets": item["bullets"],
                "estimated_time_seconds": item.get("estimated_time_seconds", 30),
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
    item = study_content_collection.find_one({"id": content_item_id, "enabled": True}, {"_id": 0})
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
    item = study_content_collection.find_one({"id": content_item_id, "item_type": "mcq", "enabled": True}, {"_id": 0})
    if not item:
        return {"reply": "I couldn’t load that question anymore."}

    state = _load_state(session_id)
    topic = item["topic"]
    correct = (selected_option or "").upper() == item["correct_option"]
    correct_counts = dict(state.get("topics_correct_count") or {})
    incorrect_counts = dict(state.get("topics_incorrect_count") or {})
    recent_mistakes = list(state.get("recent_mistake_topics") or [])

    if correct:
        correct_counts[topic] = correct_counts.get(topic, 0) + 1
    else:
        incorrect_counts[topic] = incorrect_counts.get(topic, 0) + 1
        recent_mistakes.append(topic)

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
            "last_studied_topic": topic,
            "last_answered_option": (selected_option or "").upper(),
            "last_answer_correct": correct,
        },
    )
    log_event(
        "mcq_answered",
        session_id,
        {"content_item_id": item["id"], "topic": topic, "selected_option": selected_option, "correct": correct},
    )
    log_event("mcq_correct" if correct else "mcq_incorrect", session_id, {"content_item_id": item["id"], "topic": topic})

    reply = _build_mcq_feedback_reply(item, correct)
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
    exclude_ids = _recent_study_exclude_ids(state)
    if exclude_self:
        exclude_ids.add(item["id"])
    candidates = _get_items(item_type=item_type, topic=item["topic"], exclude_ids=exclude_ids) or _get_items(item_type=item_type, exclude_ids=exclude_ids)
    if not candidates:
        fallback_exclude_ids = {item["id"]} if exclude_self else None
        candidates = _get_items(item_type=item_type, topic=item["topic"], exclude_ids=fallback_exclude_ids) or _get_items(item_type=item_type, exclude_ids=fallback_exclude_ids)
    return _pick_item(session_id, candidates, f"{item['id']}:{item_type}")


def handle_study_action(session_id, content_item_id, action):
    item = study_content_collection.find_one({"id": content_item_id, "enabled": True}, {"_id": 0})
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
