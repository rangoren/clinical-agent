from llm_client import client
from settings import ANTHROPIC_MODEL


def generate_reply(system_prompt, chat_history, user_message):
    messages = [{"role": item["role"], "content": item["content"]} for item in chat_history[-6:]]
    messages.append({"role": "user", "content": user_message})

    response = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=500,
        system=system_prompt,
        messages=messages,
    )
    return response.content[0].text
