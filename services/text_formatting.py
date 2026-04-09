import re


def _strip_inline_source_citations(text):
    cleaned = re.sub(r"\s*\[(?:E|P|K|T|PR|IK|LP)\d+\]", "", text)
    cleaned = re.sub(r" {2,}", " ", cleaned)
    return cleaned.strip()


def _clean_broken_source_phrasing(text):
    cleaned = text
    cleaned = re.sub(r"\bPer,\s*", "", cleaned)
    cleaned = re.sub(r"\bPer\s+\.\s*", "", cleaned)
    cleaned = re.sub(r"\bSource:\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"No external sources were provided.*?(?=(Most likely:|Danger to rule out:|What changes management now:|Next step:|$))",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned = re.sub(
        r"This is outside my obstetrics and gynecology scope.*?(?=(Most likely:|Danger to rule out:|What changes management now:|Next step:|$))",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned = re.sub(
        r"This is outside my .*? scope.*?(?=(Most likely:|Danger to rule out:|What changes management now:|Next step:|$))",
        "",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned = re.sub(r" {2,}", " ", cleaned)
    return cleaned.strip()


def format_response(text):
    sections = [
        "Most likely:",
        "Danger to rule out:",
        "What changes management now:",
        "Next step:",
    ]

    text = _strip_inline_source_citations(text.strip())
    text = _clean_broken_source_phrasing(text)
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


def format_basic_clinical_response(text, user_message=None):
    text = _strip_inline_source_citations(text.strip().replace("**", ""))
    text = _clean_broken_source_phrasing(text)
    text = _soften_basic_clinical_phrasing(text)
    cleaned_lines = _clean_basic_lines(text)

    if not cleaned_lines:
        return text

    normalized_parts = _normalize_basic_lines(cleaned_lines)

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
