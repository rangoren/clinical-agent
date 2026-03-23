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


def _clean_basic_lines(text):
    cleaned_lines = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        normalized = stripped.lower().strip("*").strip()
        if normalized.startswith("most likely context:") or stripped == "---":
            continue
        cleaned_lines.append(stripped)
    return cleaned_lines


def _soften_basic_clinical_phrasing(text):
    replacements = {
        "Pap + HPV co-test": "Pap + HPV testing",
        "Pap + HPV co-testing": "Pap + HPV testing",
        "(preferred), or": "or",
        "(preferred)": "",
        "adequate prior negative history": "prior screening has been normal",
        "adequate prior normal history": "prior screening has been normal",
        "if there is an adequate history of normal results": "if prior screening has been normal",
        "if adequate prior negative history": "if prior screening has been normal",
        "No screening needed": "No screening",
    }

    softened = text
    for source, target in replacements.items():
        softened = softened.replace(source, target)

    while "  " in softened:
        softened = softened.replace("  ", " ")

    return softened


def _looks_like_cervical_screening_question(user_message):
    cleaned = user_message.strip().lower()
    markers = (
        "pap smear",
        "pap",
        "hpv screening",
        "cervical screening",
        "cervical cancer screening",
    )
    return any(marker in cleaned for marker in markers)


def _is_short_labeled_line(line):
    if ":" not in line:
        return False

    label, remainder = line.split(":", 1)
    label_words = label.strip().split()
    if not remainder.strip():
        return False

    return 1 <= len(label_words) <= 6


def _normalize_basic_lines(lines):
    normalized = []

    for line in lines:
        if line.startswith("- "):
            normalized.append(("bullet", line[2:].strip()))
            continue

        if " - " in line and any(token in line for token in [":", "[E", "[P", "[K", "[PR", "[IK"]):
            first_part, *rest = line.split(" - ")
            normalized.append(("paragraph", first_part.strip()))
            for item in rest:
                item = item.strip()
                if item:
                    normalized.append(("bullet", item))
            continue

        lowered = line.lower()
        if lowered.startswith(("exception:", "exceptions:", "high-risk", "the key exception", "main exception")):
            normalized.append(("exception", line))
            continue

        if _is_short_labeled_line(line) and not lowered.startswith(("note:", "answer:", "summary:")):
            normalized.append(("bullet", line))
            continue

        normalized.append(("paragraph", line))

    return normalized


def _format_exception_line(line):
    lower_line = line.lower()

    if lower_line.startswith("the key exception is "):
        body = line[len("The key exception is ") :].strip()
        return f"<p><strong>Exception:</strong> {body}</p>"

    if lower_line.startswith("main exception:"):
        body = line.split(":", 1)[1].strip()
        return f"<p><strong>Exception:</strong> {body}</p>"

    if lower_line.startswith("exceptions:"):
        body = line.split(":", 1)[1].strip()
        return f"<p><strong>Exceptions:</strong> {body}</p>"

    if lower_line.startswith("exception:"):
        body = line.split(":", 1)[1].strip()
        return f"<p><strong>Exception:</strong> {body}</p>"

    if lower_line.startswith("high-risk patients"):
        return f"<p><strong>Exception:</strong> {line}</p>"

    if lower_line.startswith("high-risk "):
        return f"<p><strong>Exception:</strong> {line}</p>"

    return f"<p><strong>Exception:</strong> {line}</p>"


def _ensure_cervical_screening_coverage(normalized_parts):
    existing_bullets = [content.lower() for part_type, content in normalized_parts if part_type == "bullet"]
    existing_exceptions = [content.lower() for part_type, content in normalized_parts if part_type == "exception"]

    additions = []

    if not any("under 21" in bullet for bullet in existing_bullets):
        additions.append(("bullet", "Under 21: No screening"))

    if not any("over 65" in bullet for bullet in existing_bullets):
        additions.append(("bullet", "Over 65: Can usually stop if prior screening has been normal"))

    if not any("high-risk" in exception for exception in existing_exceptions):
        additions.append(("exception", "High-risk patients need more frequent screening, and these routine intervals do not apply [E3]."))

    if not additions:
        return normalized_parts

    insert_at = len(normalized_parts)
    for index, (part_type, _) in enumerate(normalized_parts):
        if part_type == "exception":
            insert_at = index
            break

    return normalized_parts[:insert_at] + additions + normalized_parts[insert_at:]


def format_basic_clinical_response(text, user_message=None):
    text = _soften_basic_clinical_phrasing(text.strip().replace("**", ""))
    cleaned_lines = _clean_basic_lines(text)

    if not cleaned_lines:
        return text

    normalized_parts = _normalize_basic_lines(cleaned_lines)
    if user_message and _looks_like_cervical_screening_question(user_message):
        normalized_parts = _ensure_cervical_screening_coverage(normalized_parts)

    result = []
    inside_list = False
    for part_type, content in normalized_parts:
        if part_type == "bullet":
            if not inside_list:
                result.append("<ul>")
                inside_list = True
            result.append(f"<li>{content}</li>")
        else:
            if inside_list:
                result.append("</ul>")
                inside_list = False
            if part_type == "exception":
                result.append(_format_exception_line(content))
            else:
                result.append(f"<p>{content}</p>")

    if inside_list:
        result.append("</ul>")

    return "".join(result).strip()
