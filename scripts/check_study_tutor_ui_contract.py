from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / "templates" / "index.html"
SERVICE_PATH = ROOT / "services" / "study_service.py"


def _extract_block_between(source: str, start_marker: str, end_marker: str) -> str:
    start = source.find(start_marker)
    if start < 0:
        raise AssertionError(f"Missing start marker: {start_marker}")
    end = source.find(end_marker, start)
    if end < 0:
        raise AssertionError(f"Missing end marker: {end_marker}")
    return source[start:end]


def main():
    template_source = TEMPLATE_PATH.read_text(encoding="utf-8")
    service_source = SERVICE_PATH.read_text(encoding="utf-8")

    submit_block = _extract_block_between(
        template_source,
        "async function submitStudyAnswer(",
        "async function handleStudyAction(",
    )
    if "appendStudyInteractionCard(data.study_item)" in submit_block:
        raise AssertionError("Tutor UI regression: submitStudyAnswer still auto-advances to the next question.")
    if "renderStudySessionBanner(" in submit_block:
        raise AssertionError("Tutor UI regression: submitStudyAnswer still renders session metadata.")
    if "appendStudyFollowupCard(contentItemId, data.study_followups)" not in submit_block:
        raise AssertionError("Tutor UI regression: submitStudyAnswer no longer shows post-answer action buttons.")

    open_block = _extract_block_between(
        template_source,
        "async function openStudyCard(",
        "async function submitStudyAnswer(",
    )
    if "renderStudySessionBanner(" in open_block:
        raise AssertionError("Tutor UI regression: openStudyCard still renders session metadata.")

    action_block = _extract_block_between(
        template_source,
        "async function handleStudyAction(",
        "function escapeHtml(",
    )
    if "renderStudySessionBanner(" in action_block:
        raise AssertionError("Tutor UI regression: handleStudyAction still renders session metadata.")

    if "Question ${" in template_source or "Focus:" in template_source or "Session complete ·" in template_source:
        raise AssertionError("Tutor UI regression: internal session labels leaked into the template.")

    answer_match = re.search(r"def answer_mcq\(.*?^\s*return response", service_source, re.DOTALL | re.MULTILINE)
    if not answer_match:
        raise AssertionError("Missing answer_mcq implementation block.")
    answer_block = answer_match.group(0)
    if 'response["study_item"]' in answer_block:
        raise AssertionError("Tutor UI regression: answer_mcq still injects the next question automatically.")

    required_labels = [
        '{"action": "quick_recap", "label": "give me a rule"}',
        '{"action": "another_question", "label": "another question"}',
        '{"action": "explain_why", "label": "explain why"}',
        '{"action": "show_source", "label": "מקור"}',
    ]
    for label in required_labels:
        if label not in answer_block:
            raise AssertionError(f"Missing tutor follow-up action: {label}")

    print("Study tutor UI contract passed.")


if __name__ == "__main__":
    main()
