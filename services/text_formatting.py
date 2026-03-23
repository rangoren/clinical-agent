def format_response(text):
    sections = [
        "Most likely:",
        "Danger to rule out:",
        "What changes management now:",
        "Next step:",
    ]

    text = text.strip()
    text = text.replace("**", "")

    lines = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        normalized = stripped.lower().strip("*").strip()
        if normalized.startswith("most likely context:") or stripped == "---":
            continue
        lines.append(stripped)

    text = "\n".join(lines)

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


def format_basic_clinical_response(text):
    text = text.strip().replace("**", "")

    cleaned_lines = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        normalized = stripped.lower().strip("*").strip()
        if normalized.startswith("most likely context:") or stripped == "---":
            continue
        cleaned_lines.append(stripped)

    if not cleaned_lines:
        return text

    rebuilt_lines = []
    for line in cleaned_lines:
        if line.startswith("- "):
            rebuilt_lines.append(line)
            continue

        if " - " in line and any(token in line for token in [":", "[E", "[P", "[K", "[PR", "[IK"]):
            first_part, *rest = line.split(" - ")
            rebuilt_lines.append(first_part.strip())
            for item in rest:
                rebuilt_lines.append(f"- {item.strip()}")
            continue

        rebuilt_lines.append(line)

    html_parts = []
    for line in rebuilt_lines:
        if line.startswith("- "):
            html_parts.append(f"__LIST_ITEM__{line[2:].strip()}")
        else:
            html_parts.append(line)

    result = []
    inside_list = False
    for part in html_parts:
        if part.startswith("__LIST_ITEM__"):
            if not inside_list:
                result.append("<ul>")
                inside_list = True
            result.append(f"<li>{part.replace('__LIST_ITEM__', '', 1)}</li>")
        else:
            if inside_list:
                result.append("</ul>")
                inside_list = False
            result.append(f"<p>{part}</p>")

    if inside_list:
        result.append("</ul>")

    return "".join(result).strip()
