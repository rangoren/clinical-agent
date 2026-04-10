from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from services.profile_prompt_resolver import detect_profile_status_intent, looks_like_profile_update_message


SMOKE_CASES = [
    {
        "message": "what year residency am i?",
        "expected_status": "residency_year",
        "expected_update": False,
        "fields": ["training_stage", "residency_year"],
    },
    {
        "message": "what year Residency am i",
        "expected_status": "residency_year",
        "expected_update": False,
        "fields": ["training_stage", "residency_year"],
    },
    {
        "message": "what do you have saved for me",
        "expected_status": "profile_status",
        "expected_update": False,
        "fields": ["training_stage"],
    },
    {
        "message": "what training stage do you have",
        "expected_status": "training_stage",
        "expected_update": False,
        "fields": ["training_stage"],
    },
    {
        "message": "i am 6 year residet",
        "expected_status": None,
        "expected_update": True,
        "fields": ["training_stage", "residency_year"],
    },
    {
        "message": "r6",
        "expected_status": None,
        "expected_update": True,
        "fields": ["residency_year"],
    },
]


def main():
    failures = []
    for case in SMOKE_CASES:
        detected_status = detect_profile_status_intent(case["message"])
        detected_update = looks_like_profile_update_message(case["message"], case["fields"])
        ok = detected_status == case["expected_status"] and detected_update == case["expected_update"]
        status = "PASS" if ok else "FAIL"
        print(f"{status}: {case['message']}")
        print(f"  status={detected_status} expected_status={case['expected_status']}")
        print(f"  update={detected_update} expected_update={case['expected_update']}")
        if not ok:
            failures.append(case["message"])

    if failures:
        print("\nFailed cases:")
        for message in failures:
            print(f"- {message}")
        return 1

    print("\nAll profile routing smoke cases passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
