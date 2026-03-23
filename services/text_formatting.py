def format_response(text):
    sections = [
        "Most likely:",
        "Danger to rule out:",
        "What changes management now:",
        "Next step:",
    ]

    text = text.strip()

    for section in sections:
        text = text.replace(section, f"\n{section}")

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    text = "\n".join(lines)

    for section in sections:
        if section not in text:
            text += f"\n\n{section}\nNot clearly stated."

    text = text.replace("\n\n", "<br><br>")
    text = text.replace("\n", "<br>")

    for section in sections:
        text = text.replace(section, f"<b>{section}</b>")

    return text.strip()
