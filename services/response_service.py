import time

from llm_client import client
from settings import ANTHROPIC_MODEL


MAX_REPLY_RETRIES = 2
RETRY_BACKOFF_SECONDS = 0.45


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
    text = "" if value is None else str(value)
    return [{"type": "text", "text": text}]


def generate_reply(system_prompt, chat_history, user_message):
    messages = [
        {
            "role": item["role"],
            "content": _as_text_block_content(item.get("content")),
        }
        for item in chat_history[-6:]
        if item.get("role") in {"user", "assistant"}
    ]
    messages.append({"role": "user", "content": _as_text_block_content(user_message)})

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
            if attempt >= MAX_REPLY_RETRIES or not _is_transient_llm_error(exc):
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))

    raise last_exc
