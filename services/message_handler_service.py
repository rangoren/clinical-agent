from services.chat_service import delete_messages_for_session, load_chat, save_message
from services.external_sources_service import get_external_sources, get_forced_authoritative_source
from services.intent_service import classify_message_intent
from services.logging_service import log_event
from services.memory_service import (
    get_relevant_knowledge_entries,
    get_relevant_protocol_entries,
    knowledge_exists,
    load_knowledge,
    load_principles,
    load_protocols,
    principle_exists,
    protocol_exists,
    save_knowledge,
    save_principle,
    save_protocol,
)
from services.profile_service import (
    activate_chat_mode,
    build_onboarding_intro,
    build_onboarding_question,
    delete_user_profile,
    extract_profile_updates_from_message,
    finalize_onboarding_profile,
    get_user_profile,
    is_core_profile_complete,
    is_general_greeting_message,
    is_profile_only_message,
    start_onboarding,
    touch_user_profile,
    update_user_profile,
    build_soft_onboarding_followup,
)
from services.prompt_service import (
    build_basic_clinical_system_prompt,
    build_clinical_system_prompt,
    build_general_system_prompt,
    build_textbook_system_prompt,
)
from services.response_service import generate_reply
from services.study_service import resolve_study_chat_message
from services.text_formatting import format_basic_clinical_response, format_response
from services.textbook_runtime_service import (
    build_gabbe_textbook_context,
    build_textbook_overload_fallback_reply,
    detect_textbook_request,
)
from services.trusted_source_registry import get_domain_tier, get_source_domain, infer_question_route
from services.undo_service import clear_last_saved, record_last_saved
import re
import time


PROFILE_STATUS_PATTERNS = (
    "what residency year do you have saved",
    "what residency year is saved",
    "what do you have saved for me",
    "what you have saved on me",
    "what do you have saved on me",
    "what have you saved on me",
    "what have you saved for me",
    "what do you know about my training",
    "what do you know about me",
    "what is saved in my profile",
    "what's saved in my profile",
    "what training level do you have",
    "what training stage do you have",
)

RESIDENCY_YEAR_STATUS_PATTERNS = (
    "what residency year do you have saved",
    "what residency year is saved",
    "which residency year do you have",
    "which residency year is saved",
)

TRAINING_STAGE_STATUS_PATTERNS = (
    "what training level do you have",
    "what training stage do you have",
    "what do you know about my training",
)


def _reply_has_visible_text(reply):
    if not reply:
        return False
    normalized = str(reply)
    normalized = re.sub(r"<br\s*/?>", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</p\s*>", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"<[^>]+>", "", normalized)
    normalized = normalized.replace("&nbsp;", " ").strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return bool(normalized)


def _build_message_response(
    reply,
    undo=False,
    undo_type=None,
    show_feedback=False,
    assistant_message_id=None,
    needs_onboarding=False,
    sources=None,
    suggested_save=None,
):
    normalized_reply = reply
    if not _reply_has_visible_text(normalized_reply):
        if sources:
            normalized_reply = _fallback_empty_clinical_reply(sources)
        else:
            normalized_reply = "I couldn’t generate a reliable answer right now. Please try again."

    return {
        "reply": normalized_reply,
        "undo": undo,
        "undo_type": undo_type,
        "show_feedback": show_feedback,
        "assistant_message_id": assistant_message_id,
        "needs_onboarding": needs_onboarding,
        "sources": sources or [],
        "suggested_save": suggested_save,
    }


def _collect_linked_sources(protocol_entries, knowledge_entries):
    deduped = []
    seen_urls = set()
    for entry in protocol_entries + knowledge_entries:
        source = entry.get("source")
        if not source or not source.get("url") or source["url"] in seen_urls:
            continue
        seen_urls.add(source["url"])
        deduped.append(
            {
                "source_id": source["source_id"],
                "title": source["title"],
                "url": source["url"],
                "source_type": source.get("source_type", "reference"),
            }
        )
    return deduped


def _collect_internal_sources(principles, protocol_entries, knowledge_entries):
    collected = []

    for index, principle in enumerate(principles[:3], start=1):
        collected.append(
            {
                "source_id": f"PR{index}",
                "title": principle[:140],
                "url": None,
                "source_type": "Internal source",
                "source_detail": "Saved principle",
                "is_internal": True,
            }
        )

    protocol_index = 1
    for entry in protocol_entries:
        if entry.get("source"):
            continue
        collected.append(
            {
                "source_id": f"LP{protocol_index}",
                "title": entry["text"][:140],
                "url": None,
                "source_type": "Internal source",
                "source_detail": "Local protocol memory",
                "is_internal": True,
            }
        )
        protocol_index += 1

    knowledge_index = 1
    for entry in knowledge_entries:
        if entry.get("source"):
            continue
        collected.append(
            {
                "source_id": f"IK{knowledge_index}",
                "title": entry["text"][:140],
                "url": None,
                "source_type": "Internal source",
                "source_detail": "Internal knowledge memory",
                "is_internal": True,
            }
        )
        knowledge_index += 1

    return collected


def _extract_cited_source_ids(reply):
    return set(re.findall(r"\[((?:E|P|K|T|PR|LP|IK)\d+)\]", reply or ""))


def _filter_sources_by_citation(reply, sources):
    cited_source_ids = _extract_cited_source_ids(reply)
    if not cited_source_ids:
        return []
    return [source for source in sources if source["source_id"] in cited_source_ids]


def _fallback_display_sources(sources):
    if not sources:
        return []

    preferred_sources = []
    internal_only_sources = []
    seen_source_ids = set()

    for source in sources:
        source_id = str(source.get("source_id", ""))
        if not source_id or source_id in seen_source_ids:
            continue
        if source.get("is_internal"):
            internal_only_sources.append(source)
            seen_source_ids.add(source_id)
            continue
        if source.get("url") or source_id.startswith(("E", "P", "K", "T")):
            preferred_sources.append(source)
            seen_source_ids.add(source_id)

    if preferred_sources:
        external_first = sorted(
            preferred_sources,
            key=lambda source: (0 if str(source.get("source_id", "")).startswith("E") else 1),
        )
        return external_first[:3]

    if internal_only_sources:
        return internal_only_sources[:3]

    return []


def _is_authoritative_source(source):
    if not source or source.get("is_internal"):
        return False
    source_id = str(source.get("source_id", ""))
    source_type = str(source.get("source_type", "")).lower()
    domain = get_source_domain(source.get("url") or "")
    tier = source.get("tier") or (get_domain_tier(domain) if domain else None)

    if source_id.startswith(("E", "P", "K")) and tier in {"tier1", "tier2", "tier3", "tier4"}:
        return True
    if "guideline" in source_type and tier in {"tier1", "tier2", "tier3", "tier4"}:
        return True
    return False


def _with_source_confidence_note(sources):
    normalized_sources = list(sources or [])
    if normalized_sources:
        return normalized_sources

    normalized_sources.append(
        {
            "source_id": "MG1",
            "title": "No explicit source available for this answer.",
            "url": None,
            "source_type": "Model-generated answer",
            "is_notice": True,
        }
    )
    return normalized_sources


def _maybe_override_fertility_display_source(user_message, sources):
    normalized = (user_message or "").strip().lower()
    if not sources:
        return sources

    fertility_markers = ("trying to conceive", "ttc", "infertility", "fertility")
    evaluation_markers = ("next step in evaluation", "evaluation", "workup", "hsg", "semen analysis", "ovarian reserve")
    if not any(marker in normalized for marker in fertility_markers):
        return sources
    if not any(marker in normalized for marker in evaluation_markers):
        return sources

    preferred_title = "ASRM: Fertility Evaluation of Infertile Women"
    has_other_fertility_source = any(
        source.get("title", "").startswith("ASRM:") or source.get("title", "").startswith("ESHRE")
        for source in sources
    )
    already_preferred = any(source.get("title") == preferred_title for source in sources)
    if not has_other_fertility_source or already_preferred:
        return sources

    preferred_source = get_forced_authoritative_source(user_message)
    if not preferred_source:
        return sources

    filtered_sources = [
        source
        for source in sources
        if source.get("title") != preferred_title and not source.get("title", "").startswith("ASRM:")
    ]
    return preferred_source + filtered_sources


def _maybe_override_targeted_display_source(user_message, sources):
    if not sources:
        return sources

    forced_sources = get_forced_authoritative_source(user_message)
    if not forced_sources:
        return sources

    forced_title = forced_sources[0].get("title")
    if any(source.get("title") == forced_title for source in sources):
        return sources

    override_titles = {
        "AIUM Practice Parameter: Ultrasound in Pregnancy",
        "AIUM Practice Topic: Gynecologic Ultrasound",
        "ASRM: Definition of Infertility",
    }
    current_titles = {source.get("title") for source in sources}
    if not any(title in override_titles for title in current_titles):
        return sources

    filtered_sources = [source for source in sources if source.get("title") not in override_titles]
    return forced_sources + filtered_sources


def _looks_like_basic_clinical_question(user_message):
    cleaned = user_message.strip().lower()
    acute_markers = (
        "patient",
        "weeks",
        "pregnant",
        "bleeding",
        "pain",
        "fever",
        "bp",
        "blood pressure",
        "unstable",
        "what should i do",
        "next step",
        "management",
    )
    basic_markers = (
        "do women",
        "should women",
        "how often",
        "when should",
        "what is",
        "what's the difference",
        "whats the difference",
        "difference between",
        "diffrece between",
        "diff between",
        "vs",
        "can you explain",
        "do i need",
        "screening",
        "pap",
        "pap smear",
        "paps smear",
        "hpv",
        "hpv typing",
    )
    if any(marker in cleaned for marker in acute_markers):
        return False
    return any(marker in cleaned for marker in basic_markers) or (cleaned.endswith("?") and len(cleaned.split()) <= 14)


def _build_unsupported_textbook_reply(textbook_request):
    book_title = textbook_request.get("book_title") or "that textbook"
    return (
        f"I only have on-demand textbook access wired into runtime for Gabbe right now. "
        f"{book_title} is not connected yet."
    )


def _build_missing_textbook_context_reply(textbook_context):
    return textbook_context.get("message") or (
        "I couldn't confidently map this textbook request to one of the indexed topics yet. "
        "Try phrasing it as 'What does Gabbe say about <topic>?'."
    )


def _build_textbook_overload_fallback(textbook_context):
    return build_textbook_overload_fallback_reply(textbook_context)


def _handle_new_user_onboarding(session_id):
    start_onboarding(session_id)
    log_event("session_started", session_id, {"mode": "onboarding"})
    onboarding_intro = build_onboarding_intro()
    assistant_message_id = save_message(
        "assistant",
        onboarding_intro,
        session_id,
        metadata={"intent": "onboarding_intro"},
    )
    return _build_message_response(
        reply=onboarding_intro,
        assistant_message_id=assistant_message_id,
        needs_onboarding=True,
    )


def _handle_onboarding_message(session_id, user_profile, user_message):
    touch_user_profile(session_id)
    save_message("user", user_message, session_id, metadata={"intent": "onboarding_answer"})
    extracted_updates, extracted_fields = extract_profile_updates_from_message(user_message, user_profile)
    if extracted_updates:
        update_user_profile(session_id, extracted_updates)

    refreshed_profile = get_user_profile(session_id)
    profile_only_message = is_profile_only_message(user_message, extracted_fields)
    greeting_message = is_general_greeting_message(user_message)

    log_event(
        "onboarding_step_processed",
        session_id,
        {
            "extracted_fields": extracted_fields,
            "profile_only_message": profile_only_message,
            "greeting_message": greeting_message,
        },
    )

    if is_core_profile_complete(refreshed_profile):
        completed_profile = finalize_onboarding_profile(session_id, refreshed_profile)
        if profile_only_message or greeting_message:
            reply = "Great, I’ve got what I need.<br><br>I’m ready when you are."
            assistant_message_id = save_message(
                "assistant",
                reply,
                session_id,
                metadata={"intent": "onboarding_complete"},
            )
            return _build_message_response(
                reply=reply,
                assistant_message_id=assistant_message_id,
                needs_onboarding=False,
            )
        return _handle_regular_message(session_id, completed_profile, user_message, save_user_message=False)

    if profile_only_message or greeting_message:
        reply = build_soft_onboarding_followup(refreshed_profile)
        assistant_message_id = save_message(
            "assistant",
            reply,
            session_id,
            metadata={"intent": "onboarding_followup"},
        )
        return _build_message_response(
            reply=reply,
            assistant_message_id=assistant_message_id,
            needs_onboarding=True,
        )

    completed_profile = finalize_onboarding_profile(session_id, refreshed_profile)
    return _handle_regular_message(session_id, completed_profile, user_message, save_user_message=False)


def get_session_state(session_id):
    if not session_id:
        return {"state": "missing_session"}

    user_profile = get_user_profile(session_id)
    if not user_profile:
        start_onboarding(session_id)
        log_event("session_bootstrap", session_id, {"state": "new_onboarding"})
        onboarding_intro = build_onboarding_intro()
        assistant_message_id = save_message(
            "assistant",
            onboarding_intro,
            session_id,
            metadata={"intent": "onboarding_intro"},
        )
        return {
            "state": "new_onboarding",
            "reply": onboarding_intro,
            "assistant_message_id": assistant_message_id,
            "needs_onboarding": True,
        }

    if not user_profile.get("onboarding_done"):
        touch_user_profile(session_id)
        log_event("session_bootstrap", session_id, {"state": "incomplete_onboarding", "step": user_profile.get("onboarding_step")})
        return {
            "state": "incomplete_onboarding",
            "onboarding_step": user_profile.get("onboarding_step") or "country",
            "needs_onboarding": True,
        }

    touch_user_profile(session_id)
    log_event("session_bootstrap", session_id, {"state": "ready"})
    return {"state": "ready", "needs_onboarding": False}


def continue_onboarding(session_id):
    if not session_id:
        return {"reply": "Missing session_id."}

    user_profile = get_user_profile(session_id)
    if not user_profile:
        return _handle_new_user_onboarding(session_id)

    if user_profile.get("onboarding_done"):
        return _build_message_response(reply="Chat mode is already active.")

    touch_user_profile(session_id)
    reply = (
        "You didn’t finish onboarding last time.<br><br>"
        "Let’s continue from where we stopped.<br><br>"
        f"{build_onboarding_question(user_profile.get('onboarding_step'))}"
    )
    assistant_message_id = save_message(
        "assistant",
        reply,
        session_id,
        metadata={"intent": "onboarding_resume"},
    )
    return _build_message_response(
        reply=reply,
        assistant_message_id=assistant_message_id,
        needs_onboarding=True,
    )


def start_clean_chat_mode(session_id):
    if not session_id:
        return {"reply": "Missing session_id."}

    delete_messages_for_session(session_id)
    clear_last_saved(session_id)
    activate_chat_mode(session_id)
    log_event("chat_mode_started", session_id, {"reset_messages": True})
    reply = "Clean chat mode is active. Ask any question."
    assistant_message_id = save_message(
        "assistant",
        reply,
        session_id,
        metadata={"intent": "chat_mode_start"},
    )
    return _build_message_response(
        reply=reply,
        assistant_message_id=assistant_message_id,
        needs_onboarding=False,
    )


def reset_session(session_id):
    if not session_id:
        return {"reply": "Missing session_id."}

    delete_messages_for_session(session_id)
    clear_last_saved(session_id)
    delete_user_profile(session_id)
    log_event("session_reset", session_id)
    return _handle_new_user_onboarding(session_id)


def _handle_memory_save(intent, user_message, session_id, principles, knowledge_items, protocol_items):
    if intent == "protocol":
        if not protocol_exists(user_message, protocol_items):
            save_protocol(user_message)
            record_last_saved(session_id, "protocol", user_message)
            reply = "Saved as a protocol. I’ll use this as department-level guidance."
            undo = True
        else:
            reply = "Already saved as a protocol."
            undo = False
    elif intent == "principle":
        if not principle_exists(user_message, principles):
            save_principle(user_message)
            record_last_saved(session_id, "principle", user_message)
            reply = "Saved as a principle. I’ll apply this going forward."
            undo = True
        else:
            reply = "Already saved. Still applying it."
            undo = False
    else:
        if not knowledge_exists(user_message, knowledge_items):
            save_knowledge(user_message)
            record_last_saved(session_id, "knowledge", user_message)
            reply = "Saved as knowledge for future use."
            undo = True
        else:
            reply = "Already saved in knowledge."
            undo = False

    save_message("user", user_message, session_id)
    save_message("assistant", reply, session_id)
    log_event("memory_saved", session_id, {"intent": intent, "undo_enabled": undo})
    return _build_message_response(
        reply=reply,
        undo=undo,
        undo_type=intent if undo else None,
    )


def _build_memory_confirmation_response(intent):
    if intent == "protocol":
        return "This sounds like a local protocol."
    if intent == "principle":
        return "This sounds like a reply principle."
    return "This sounds like reusable knowledge."


def _build_suggested_save_payload(intent, user_message):
    prefix_map = {
        "protocol": "save this protocol: ",
        "principle": "remember that ",
        "knowledge": "save this: ",
    }
    label_map = {
        "protocol": "Save as protocol",
        "principle": "Save as principle",
        "knowledge": "Save as knowledge",
    }
    prefix = prefix_map.get(intent)
    if not prefix:
        return None

    return {
        "intent": intent,
        "label": label_map.get(intent, "Save"),
        "message": f"{prefix}{user_message}",
    }


def _build_general_greeting_reply(user_profile):
    training_stage = (user_profile or {}).get("training_stage")
    if training_stage == "resident":
        return "Hi. I’m here and ready when you are."
    if training_stage == "specialist":
        return "Hi. Ready when you are."
    return "Hi. I’m here and ready to help."


def _normalize_plain_text(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _looks_like_profile_status_question(user_message):
    normalized = _normalize_plain_text(user_message)
    if not normalized or "?" not in normalized:
        return False
    if any(pattern in normalized for pattern in PROFILE_STATUS_PATTERNS):
        return True
    if "saved" in normalized and any(token in normalized for token in (" me", "profile", "training", "residency")):
        return True
    if "what do you know" in normalized and any(token in normalized for token in ("me", "training", "profile")):
        return True
    return False


def _build_profile_status_reply(user_profile):
    if not user_profile:
        return "I don’t have a saved profile for you yet."

    training_stage = user_profile.get("training_stage")
    residency_year = user_profile.get("residency_year")
    country = user_profile.get("country")
    subspecialty = user_profile.get("subspecialty")
    answer_style = user_profile.get("answer_style")

    lines = []
    if training_stage:
        lines.append(f"Training stage saved: {training_stage}.")
    else:
        lines.append("I don’t have a training stage saved for you yet.")

    if training_stage == "resident":
        if residency_year:
            lines.append(f"Residency year saved: {residency_year}.")
        else:
            lines.append("I don’t have a residency year saved for you yet.")
    elif training_stage in {"specialist", "fellowship"}:
        lines.append("Residency year is not applicable for your current training stage.")
    elif residency_year:
        lines.append(f"Residency year saved: {residency_year}.")

    if country:
        lines.append(f"Country saved: {country}.")
    if subspecialty:
        lines.append(f"Subspecialty saved: {subspecialty}.")
    if answer_style:
        lines.append(f"Preferred answer style: {answer_style}.")

    return "<br>".join(lines)


def _build_residency_year_status_reply(user_profile):
    if not user_profile:
        return "I don’t have a saved profile for you yet."

    training_stage = user_profile.get("training_stage")
    residency_year = user_profile.get("residency_year")

    if training_stage == "resident" and residency_year:
        return f"Residency year saved: {residency_year}."
    if training_stage == "resident":
        return "I don’t have a residency year saved for you yet."
    if training_stage in {"specialist", "fellowship"}:
        return "Residency year is not applicable for your current training stage."
    if residency_year:
        return f"Residency year saved: {residency_year}."
    return "I don’t have a residency year saved for you yet."


def _build_training_stage_status_reply(user_profile):
    if not user_profile:
        return "I don’t have a saved profile for you yet."

    training_stage = user_profile.get("training_stage")
    if training_stage:
        return f"Training stage saved: {training_stage}."
    return "I don’t have a training stage saved for you yet."


def _handle_profile_status_message(session_id, user_profile, user_message):
    normalized = _normalize_plain_text(user_message)
    if any(pattern in normalized for pattern in RESIDENCY_YEAR_STATUS_PATTERNS):
        reply = _build_residency_year_status_reply(user_profile)
    elif any(pattern in normalized for pattern in TRAINING_STAGE_STATUS_PATTERNS):
        reply = _build_training_stage_status_reply(user_profile)
    else:
        reply = _build_profile_status_reply(user_profile)
    save_message("user", user_message, session_id, metadata={"intent": "profile_status"})
    assistant_message_id = save_message(
        "assistant",
        reply,
        session_id,
        metadata={"intent": "profile_status_reply"},
    )
    return _build_message_response(
        reply=reply,
        assistant_message_id=assistant_message_id,
    )


def _handle_profile_update_message(session_id, user_profile, user_message, save_user_message=True):
    extracted_updates, extracted_fields = extract_profile_updates_from_message(user_message, user_profile or {})
    if not extracted_updates or not is_profile_only_message(user_message, extracted_fields):
        return None

    update_user_profile(session_id, extracted_updates)
    refreshed_profile = get_user_profile(session_id)

    lines = []
    if "training_stage" in extracted_updates:
        lines.append(f"Saved training stage: {refreshed_profile.get('training_stage')}.")
    if "residency_year" in extracted_updates:
        lines.append(f"Saved residency year: {refreshed_profile.get('residency_year')}.")
    if "country" in extracted_updates:
        lines.append(f"Saved country: {refreshed_profile.get('country')}.")
    if "subspecialty" in extracted_updates:
        lines.append(f"Saved subspecialty: {refreshed_profile.get('subspecialty')}.")
    if "answer_style" in extracted_updates:
        lines.append(f"Saved answer style: {refreshed_profile.get('answer_style')}.")
    if not lines:
        lines.append("I updated your profile.")

    reply = "<br>".join(lines)
    if save_user_message:
        save_message("user", user_message, session_id, metadata={"intent": "profile_update"})
    assistant_message_id = save_message(
        "assistant",
        reply,
        session_id,
        metadata={"intent": "profile_update_reply", "updated_fields": list(extracted_updates.keys())},
    )
    log_event("profile_updated_from_chat", session_id, {"updated_fields": list(extracted_updates.keys())})
    return _build_message_response(
        reply=reply,
        assistant_message_id=assistant_message_id,
    )


def _fallback_empty_clinical_reply(display_sources):
    if display_sources:
        return (
            "I couldn’t produce a reliable board-style answer from the available context right now. "
            "Please try once more, or ask a narrower next-step question."
        )
    return (
        "I couldn’t generate a reliable clinical answer right now. "
        "Please try again in a moment."
    )


def _handle_regular_message(session_id, user_profile, user_message, save_user_message=True):
    if is_general_greeting_message(user_message):
        reply = _build_general_greeting_reply(user_profile)
        if save_user_message:
            save_message("user", user_message, session_id, metadata={"intent": "general_chat"})
        assistant_message_id = save_message(
            "assistant",
            reply,
            session_id,
            metadata={"intent": "general_chat", "source": "greeting_shortcut"},
        )
        log_event("greeting_shortcut_used", session_id, {"message_length": len(user_message.strip())})
        return _build_message_response(
            reply=reply,
            assistant_message_id=assistant_message_id,
        )

    if _looks_like_profile_status_question(user_message):
        return _handle_profile_status_message(session_id, user_profile, user_message)

    profile_update_response = _handle_profile_update_message(
        session_id,
        user_profile,
        user_message,
        save_user_message=save_user_message,
    )
    if profile_update_response:
        return profile_update_response

    started_at = time.perf_counter()
    timing = {}

    def mark(stage_name, stage_started_at):
        timing[stage_name] = round((time.perf_counter() - stage_started_at) * 1000, 1)

    touch_user_profile(session_id)
    stage_started_at = time.perf_counter()
    chat_history = load_chat(session_id, limit=8)
    mark("load_chat_ms", stage_started_at)

    stage_started_at = time.perf_counter()
    classifier_result = classify_message_intent(user_message, chat_history)
    mark("intent_ms", stage_started_at)
    intent = classifier_result["label"]
    confidence = classifier_result["confidence"]
    textbook_request = detect_textbook_request(user_message)
    principles = []
    knowledge_items = []
    protocol_items = []
    relevant_knowledge_entries = []
    relevant_protocol_entries = []
    relevant_knowledge = []
    relevant_protocols = []
    linked_sources = []
    internal_sources = []
    external_sources = []
    question_route = infer_question_route(user_message)
    candidate_sources = []
    basic_clinical_question = intent == "clinical_consult" and _looks_like_basic_clinical_question(user_message)
    textbook_context = None
    log_event(
        "intent_classified",
        session_id,
        {
            "intent": intent,
            "confidence": confidence,
            "source": classifier_result.get("source", "unknown"),
        },
    )

    if intent in {"protocol", "principle", "knowledge"} and confidence == "high":
        stage_started_at = time.perf_counter()
        principles = load_principles()
        knowledge_items = load_knowledge()
        protocol_items = load_protocols()
        mark("memory_load_ms", stage_started_at)
        return _handle_memory_save(
            intent=intent,
            user_message=user_message,
            session_id=session_id,
            principles=principles,
            knowledge_items=knowledge_items,
            protocol_items=protocol_items,
        )

    if intent in {"principle", "knowledge", "protocol"} and confidence != "high":
        reply = _build_memory_confirmation_response(intent)
        if save_user_message:
            save_message("user", user_message, session_id)
        assistant_message_id = save_message(
            "assistant",
            reply,
            session_id,
            metadata={"intent": f"{intent}_confirmation"},
        )
        return _build_message_response(
            reply=reply,
            assistant_message_id=assistant_message_id,
            suggested_save=_build_suggested_save_payload(intent, user_message),
        )

    if textbook_request and not textbook_request.get("supported"):
        reply = _build_unsupported_textbook_reply(textbook_request)
        if save_user_message:
            save_message("user", user_message, session_id)
        assistant_message_id = save_message(
            "assistant",
            reply,
            session_id,
            metadata={"intent": "textbook_request_unsupported", "confidence": confidence},
        )
        return _build_message_response(
            reply=reply,
            assistant_message_id=assistant_message_id,
        )

    if intent == "clinical_consult":
        if textbook_request and textbook_request.get("book_id") == "gabbe_9":
            stage_started_at = time.perf_counter()
            textbook_context = build_gabbe_textbook_context(user_message)
            mark("textbook_context_ms", stage_started_at)

            if textbook_context.get("status") != "ok":
                reply = _build_missing_textbook_context_reply(textbook_context)
                if save_user_message:
                    save_message("user", user_message, session_id)
                assistant_message_id = save_message(
                    "assistant",
                    reply,
                    session_id,
                    metadata={"intent": "textbook_request_unresolved", "confidence": confidence},
                )
                return _build_message_response(
                    reply=reply,
                    assistant_message_id=assistant_message_id,
                )

            candidate_sources = textbook_context.get("sources") or []
            stage_started_at = time.perf_counter()
            system_prompt = build_textbook_system_prompt(
                book_title=textbook_context["book_title"],
                edition=textbook_context["edition"],
                matched_topic=textbook_context["matched_topic"],
                textbook_excerpts=textbook_context["excerpts"],
                user_profile=user_profile,
            )
            mark("prompt_build_ms", stage_started_at)

            log_event(
                "textbook_runtime_context_built",
                session_id,
                {
                    "book_id": textbook_context.get("book_id"),
                    "matched_topic": textbook_context.get("matched_topic"),
                    "source_count": len(candidate_sources),
                },
            )
        else:
            stage_started_at = time.perf_counter()
            principles = load_principles()
            relevant_knowledge_entries = get_relevant_knowledge_entries(user_message, user_profile=user_profile)
            relevant_protocol_entries = get_relevant_protocol_entries(user_message, user_profile=user_profile)
            relevant_knowledge = [entry["text"] for entry in relevant_knowledge_entries]
            relevant_protocols = [entry["text"] for entry in relevant_protocol_entries]
            linked_sources = _collect_linked_sources(relevant_protocol_entries, relevant_knowledge_entries)
            internal_sources = _collect_internal_sources(principles, relevant_protocol_entries, relevant_knowledge_entries)
            mark("memory_load_ms", stage_started_at)

            stage_started_at = time.perf_counter()
            external_sources = get_external_sources(
                user_message,
                user_profile=user_profile,
                include_live=True,
            )
            mark("external_sources_ms", stage_started_at)
            candidate_sources = linked_sources + external_sources + internal_sources

            stage_started_at = time.perf_counter()
            if basic_clinical_question:
                system_prompt = build_basic_clinical_system_prompt(
                    principles=principles,
                    knowledge_entries=relevant_knowledge_entries,
                    protocol_entries=relevant_protocol_entries,
                    external_sources=external_sources,
                    user_profile=user_profile,
                )
            else:
                system_prompt = build_clinical_system_prompt(
                    principles=principles,
                    knowledge_entries=relevant_knowledge_entries,
                    protocol_entries=relevant_protocol_entries,
                    external_sources=external_sources,
                    user_profile=user_profile,
                )
            mark("prompt_build_ms", stage_started_at)
    else:
        stage_started_at = time.perf_counter()
        system_prompt = build_general_system_prompt(user_profile)
        mark("prompt_build_ms", stage_started_at)

    log_event(
        "memory_retrieved",
        session_id,
        {
            "knowledge_count": len(relevant_knowledge),
            "protocol_count": len(relevant_protocols),
            "source_count": len(linked_sources),
            "external_source_count": len(external_sources),
            "internal_source_count": len(internal_sources),
            "intent": intent,
            "textbook_request": bool(textbook_request),
            "basic_clinical_question": basic_clinical_question,
            "question_route": question_route,
            "external_sources": [
                {
                    "source_id": source.get("source_id"),
                    "domain": get_source_domain(source.get("url") or ""),
                    "tier": get_domain_tier(get_source_domain(source.get("url") or "")) if source.get("url") else None,
                    "title": source.get("title"),
                    "excerpt": source.get("excerpt"),
                    "updated_at": source.get("updated_at"),
                }
                for source in external_sources
            ],
            "used_operational_fallback": any(
                get_domain_tier(get_source_domain(source.get("url") or "")) == "operational"
                for source in external_sources
            ),
        },
    )
    stage_started_at = time.perf_counter()
    raw_reply = generate_reply(
        system_prompt=system_prompt,
        chat_history=chat_history,
        user_message=user_message,
        fallback_reply=_build_textbook_overload_fallback(textbook_context) if textbook_context else None,
    )
    mark("llm_reply_ms", stage_started_at)

    stage_started_at = time.perf_counter()
    display_sources = _filter_sources_by_citation(raw_reply, candidate_sources)
    if intent == "clinical_consult" and not display_sources:
        display_sources = _fallback_display_sources(candidate_sources)
    if intent == "clinical_consult" and not display_sources and not textbook_request:
        display_sources = get_forced_authoritative_source(user_message)
    if intent == "clinical_consult" and not textbook_request:
        display_sources = _maybe_override_fertility_display_source(user_message, display_sources)
        display_sources = _maybe_override_targeted_display_source(user_message, display_sources)

    reply = raw_reply
    if intent == "clinical_consult":
        if textbook_request:
            reply = raw_reply.strip()
        elif basic_clinical_question:
            reply = format_basic_clinical_response(reply, user_message=user_message)
        else:
            reply = format_response(reply)
        if not _reply_has_visible_text(reply):
            reply = _fallback_empty_clinical_reply(display_sources)
            log_event(
                "empty_clinical_reply_fallback",
                session_id,
                {
                    "basic_clinical_question": basic_clinical_question,
                    "source_count": len(display_sources),
                },
                level="warning",
            )
    mark("postprocess_ms", stage_started_at)

    stage_started_at = time.perf_counter()
    if save_user_message:
        save_message("user", user_message, session_id)
    assistant_message_id = save_message(
        "assistant",
        reply,
        session_id,
        metadata={
            "used_knowledge": relevant_knowledge,
            "used_protocols": relevant_protocols,
            "used_sources": display_sources,
            "intent": intent,
            "confidence": confidence,
        },
    )
    mark("save_messages_ms", stage_started_at)
    log_event(
        "response_generated",
        session_id,
        {
            "intent": intent,
            "confidence": confidence,
            "reply_length": len(reply),
            "feedback_enabled": intent == "clinical_consult" and len(reply) > 80,
            "source_count": len(display_sources),
            "basic_clinical_question": basic_clinical_question,
            "timing_ms": timing,
            "total_ms": round((time.perf_counter() - started_at) * 1000, 1),
        },
    )

    final_sources = display_sources if intent == "clinical_consult" else []
    if intent == "clinical_consult":
        final_sources = _with_source_confidence_note(final_sources)

    return _build_message_response(
        reply=reply,
        show_feedback=intent == "clinical_consult" and len(reply) > 80,
        assistant_message_id=assistant_message_id,
        sources=final_sources,
    )


def process_message(user_message, session_id):
    if not user_message:
        return {"reply": "Empty message."}

    if not session_id:
        return {"reply": "Missing session_id."}

    user_profile = get_user_profile(session_id)
    if not user_profile:
        return _handle_new_user_onboarding(session_id)

    if not user_profile.get("onboarding_done"):
        return _handle_onboarding_message(session_id, user_profile, user_message)

    study_response = resolve_study_chat_message(session_id, user_message)
    if study_response:
        save_message("user", user_message, session_id, metadata={"intent": "study_followup"})
        assistant_message_id = save_message(
            "assistant",
            study_response.get("reply", ""),
            session_id,
            metadata={"intent": "study_followup_reply"},
        )
        response = _build_message_response(
            reply=study_response.get("reply", ""),
            assistant_message_id=assistant_message_id,
            sources=study_response.get("sources", []),
        )
        if study_response.get("study_item"):
            response["study_item"] = study_response["study_item"]
        if study_response.get("study_followups"):
            response["study_followups"] = study_response["study_followups"]
        if study_response.get("study_context_item_id"):
            response["study_context_item_id"] = study_response["study_context_item_id"]
        return response

    return _handle_regular_message(session_id, user_profile, user_message)
