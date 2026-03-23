from services.chat_service import delete_messages_for_session, load_chat, save_message
from services.external_sources_service import get_external_sources
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
from services.prompt_service import build_basic_clinical_system_prompt, build_clinical_system_prompt, build_general_system_prompt
from services.response_service import generate_reply
from services.text_formatting import format_basic_clinical_response, format_response
from services.undo_service import clear_last_saved, record_last_saved


def _build_message_response(reply, undo=False, undo_type=None, show_feedback=False, assistant_message_id=None, needs_onboarding=False, sources=None):
    return {
        "reply": reply,
        "undo": undo,
        "undo_type": undo_type,
        "show_feedback": show_feedback,
        "assistant_message_id": assistant_message_id,
        "needs_onboarding": needs_onboarding,
        "sources": sources or [],
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
                "source_type": "Based on saved principle",
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
                "source_type": "Based on local protocol memory",
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
                "source_type": "Based on user-provided internal knowledge",
                "is_internal": True,
            }
        )
        knowledge_index += 1

    return collected


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
        "can you explain",
        "do i need",
        "screening",
        "pap smear",
        "hpv",
    )
    if any(marker in cleaned for marker in acute_markers):
        return False
    return any(marker in cleaned for marker in basic_markers) or (cleaned.endswith("?") and len(cleaned.split()) <= 14)


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


def _handle_regular_message(session_id, user_profile, user_message, save_user_message=True):
    touch_user_profile(session_id)
    chat_history = load_chat(session_id)
    principles = load_principles()
    knowledge_items = load_knowledge()
    protocol_items = load_protocols()
    relevant_knowledge_entries = get_relevant_knowledge_entries(user_message)
    relevant_protocol_entries = get_relevant_protocol_entries(user_message)
    relevant_knowledge = [entry["text"] for entry in relevant_knowledge_entries]
    relevant_protocols = [entry["text"] for entry in relevant_protocol_entries]
    linked_sources = _collect_linked_sources(relevant_protocol_entries, relevant_knowledge_entries)
    internal_sources = _collect_internal_sources(principles, relevant_protocol_entries, relevant_knowledge_entries)
    external_sources = get_external_sources(user_message)
    display_sources = linked_sources + external_sources + internal_sources

    classifier_result = classify_message_intent(user_message, chat_history)
    intent = classifier_result["label"]
    confidence = classifier_result["confidence"]
    basic_clinical_question = intent == "clinical_consult" and _looks_like_basic_clinical_question(user_message)
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
        return _handle_memory_save(
            intent=intent,
            user_message=user_message,
            session_id=session_id,
            principles=principles,
            knowledge_items=knowledge_items,
            protocol_items=protocol_items,
        )

    if intent in {"principle", "knowledge", "protocol"} and confidence != "high":
        intent = "general_chat"

    if intent == "clinical_consult":
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
    else:
        system_prompt = build_general_system_prompt(user_profile)

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
            "basic_clinical_question": basic_clinical_question,
        },
    )
    reply = generate_reply(
        system_prompt=system_prompt,
        chat_history=chat_history,
        user_message=user_message,
    )

    if intent == "clinical_consult":
        if basic_clinical_question:
            reply = format_basic_clinical_response(reply, user_message=user_message)
        else:
            reply = format_response(reply)

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
        },
    )

    return _build_message_response(
        reply=reply,
        show_feedback=intent == "clinical_consult" and len(reply) > 80,
        assistant_message_id=assistant_message_id,
        sources=display_sources if intent == "clinical_consult" else [],
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

    return _handle_regular_message(session_id, user_profile, user_message)
