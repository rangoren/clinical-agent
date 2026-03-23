from llm_client import client
from settings import ANTHROPIC_MODEL


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


def generate_reply(system_prompt, chat_history, user_message):
    messages = [{"role": item["role"], "content": item["content"]} for item in chat_history[-6:]]
    messages.append({"role": "user", "content": user_message})

    response = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=500,
        system=system_prompt,
        messages=messages,
    )
    return _sanitize_reply(response.content[0].text)
