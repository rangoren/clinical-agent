from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from services.textbook_prompt_resolver import detect_textbook_request_components


SMOKE_CASES = [
    {
        "message": "When to induce labor in early severe pet by Gabbe",
        "expected_book": "gabbe_9",
        "expected_terms": ("preeclampsia",),
    },
    {
        "message": "pph gabbe",
        "expected_book": "gabbe_9",
        "expected_terms": ("postpartum hemorrhage",),
    },
    {
        "message": "gdm gabbe",
        "expected_book": "gabbe_9",
        "expected_terms": ("gestational diabetes",),
    },
    {
        "message": "Gabbe on severe pec",
        "expected_book": "gabbe_9",
        "expected_terms": ("preeclampsia",),
    },
    {
        "message": "amenorrhea speroff",
        "expected_book": "speroff_10",
        "expected_terms": ("amenorrhea",),
    },
    {
        "message": "pcos speroff",
        "expected_book": "speroff_10",
        "expected_terms": ("pcos",),
    },
    {
        "message": "מה גאבי אומר על רעלת?",
        "expected_book": "gabbe_9",
        "expected_terms": ("רעלת",),
    },
    {
        "message": "what does gabbe say about postpartum hemorrhage",
        "expected_book": "gabbe_9",
        "expected_terms": ("postpartum hemorrhage",),
    },
]


def main():
    failures = []
    for case in SMOKE_CASES:
        result = detect_textbook_request_components(case["message"])
        matched_book = result.get("book_id") if result else None
        normalized = result.get("normalized_message") if result else ""
        ok = matched_book == case["expected_book"] and all(
            term.lower() in normalized for term in case["expected_terms"]
        )
        status = "PASS" if ok else "FAIL"
        print(f"{status}: {case['message']}")
        print(f"  expected_book={case['expected_book']} matched_book={matched_book}")
        print(f"  normalized={normalized}")
        if not ok:
            failures.append(case["message"])

    if failures:
        print("\nFailed cases:")
        for message in failures:
            print(f"- {message}")
        return 1

    print("\nAll textbook routing smoke cases passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
