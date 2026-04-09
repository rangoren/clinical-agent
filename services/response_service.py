import time

from llm_client import client
from settings import ANTHROPIC_MODEL


MAX_REPLY_RETRIES = 4
RETRY_BACKOFF_SECONDS = 1.0


def _sanitize_reply(text):
    cleaned = text.strip()
    prefixes_to_strip = (
        "User:",
        "Assistant:",
        "System:",
    )

    lines = []
    for raw_line in cleaned.splitlines():
        stripped = raw_line.strip()
        if any(stripped.startswith(prefix) for prefix in prefixes_to_strip):
            continue
        lines.append(raw_line)

    cleaned = "\n".join(lines).strip()
    return cleaned or text.strip()


def _is_transient_llm_error(exc):
    message = str(exc or "").lower()
    transient_markers = (
        "overloaded",
        "overloaded_error",
        "529",
        "timeout",
        "temporar",
        "rate limit",
        "connection reset",
        "service unavailable",
    )
    return any(marker in message for marker in transient_markers)


def _as_text_block_content(value):
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    return [{"type": "text", "text": text}]


def _build_message_entry(item):
    role = item.get("role")
    if role not in {"user", "assistant"}:
        return None
    content = _as_text_block_content(item.get("content"))
    if not content:
        return None
    return {
        "role": role,
        "content": content,
    }


def generate_reply(system_prompt, chat_history, user_message, fallback_reply=None):
    messages = []
    normalized_user_message = (user_message or "").strip().lower()
    is_fresh_case_prompt = (
        "please answer in a board-prep style" in normalized_user_message
        or "most appropriate next step" in normalized_user_message
        or "what is the most likely diagnosis" in normalized_user_message
    )

    history_window = 2 if is_fresh_case_prompt else 6

    for item in chat_history[-history_window:]:
        entry = _build_message_entry(item)
        if entry:
            messages.append(entry)

    user_content = _as_text_block_content(user_message)
    if not user_content:
        raise ValueError("user_message must not be empty")
    messages.append({"role": "user", "content": user_content})

    last_exc = None
    for attempt in range(MAX_REPLY_RETRIES + 1):
        try:
            response = client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=500,
                system=system_prompt,
                messages=messages,
            )
            return _sanitize_reply(response.content[0].text)
        except Exception as exc:
            last_exc = exc
            is_transient = _is_transient_llm_error(exc)
            if attempt >= MAX_REPLY_RETRIES or not is_transient:
                if is_transient and fallback_reply:
                    return _sanitize_reply(fallback_reply)
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * (2 ** attempt))

    if fallback_reply and _is_transient_llm_error(last_exc):
        return _sanitize_reply(fallback_reply)
    raise last_exc
