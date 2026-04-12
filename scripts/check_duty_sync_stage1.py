from services.duty_sync_parsing import (
    STRUCTURAL_CHANGE_MESSAGE,
    analyze_candidate_tab,
    select_relevant_tab_from_values_map,
)


def _run():
    april_values = [
        ["טקסט פתיחה"],
        ["תאריך", "יום", "חדר לידה", "קבלה", "מיון", "ב", "תורן חצי", "תורן ד", "מחלקות"],
        ["10/04/2026", "ו", "", "גורן", "", "", "", "", ""],
        ["12/04/2026", "א", "", "", "", "", "גורן", "", ""],
    ]
    march_values = [
        ["תאריך", "יום", "חדר לידה", "קבלה", "מיון", "ב", "תורן חצי", "תורן ד", "מחלקות"],
        ["29/03/2026", "א", "גורן", "", "", "", "", "", ""],
    ]

    april = analyze_candidate_tab("תורנויות אפריל", april_values, "גורן", "session-1")
    assert april["source_month"] == "2026-04"
    assert len(april["duties"]) == 2
    assert april["duties"][0].title == "תורנות/קבלה"
    assert april["duties"][1].title == "תורנות/תורן חצי"

    selected = select_relevant_tab_from_values_map(
        {
            "תורנויות אפריל": april_values,
            "תורנויות מרץ": march_values,
        },
        "גורן",
        "session-1",
    )
    assert selected["tab_name"] == "תורנויות אפריל"

    ambiguous_values = [
        ["תאריך", "יום", "חדר לידה", "קבלה", "מיון", "ב", "תורן חצי", "תורן ד", "מחלקות"],
        ["14/04/2026", "ג", "", "גורן / כהן", "", "", "", "", ""],
    ]
    try:
        analyze_candidate_tab("תורנויות בעיה", ambiguous_values, "גורן", "session-1")
    except Exception as exc:
        assert str(exc) == STRUCTURAL_CHANGE_MESSAGE
    else:
        raise AssertionError("Ambiguous duty cell should fail closed.")

    print("Duty Sync Stage 1 parser checks passed.")


if __name__ == "__main__":
    _run()
