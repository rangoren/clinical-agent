from services.duty_sync_diff import build_diff_changes


def _sample_duty(duty_key, date, role, title, start_datetime, end_datetime):
    return {
        "duty_key": duty_key,
        "date": date,
        "role": role,
        "title": title,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }


def _run():
    approved = [
        _sample_duty(
            "session-1:2026-04-10:קבלה",
            "2026-04-10",
            "קבלה",
            "תורנות/קבלה",
            "2026-04-10T05:00:00Z",
            "2026-04-11T06:00:00Z",
        )
    ]
    detected_same = [
        _sample_duty(
            "session-1:2026-04-10:קבלה",
            "2026-04-10",
            "קבלה",
            "תורנות/קבלה",
            "2026-04-10T05:00:00Z",
            "2026-04-11T06:00:00Z",
        )
    ]
    assert build_diff_changes(approved, detected_same) == []

    detected_time_changed = [
        _sample_duty(
            "session-1:2026-04-10:קבלה",
            "2026-04-10",
            "קבלה",
            "תורנות/קבלה",
            "2026-04-10T06:00:00Z",
            "2026-04-11T07:00:00Z",
        )
    ]
    changes = build_diff_changes(approved, detected_time_changed)
    assert len(changes) == 1
    assert changes[0]["change_type"] == "changed"
    assert changes[0]["old_duty"]["start_datetime"] == "2026-04-10T05:00:00Z"
    assert changes[0]["new_duty"]["start_datetime"] == "2026-04-10T06:00:00Z"

    detected_role_changed = [
        _sample_duty(
            "session-1:2026-04-10:מיון",
            "2026-04-10",
            "מיון",
            "תורנות/מיון",
            "2026-04-10T05:00:00Z",
            "2026-04-11T06:00:00Z",
        )
    ]
    changes = build_diff_changes(approved, detected_role_changed)
    assert len(changes) == 1
    assert changes[0]["change_type"] == "changed"

    detected_removed = []
    changes = build_diff_changes(approved, detected_removed)
    assert len(changes) == 1
    assert changes[0]["change_type"] == "removed"

    detected_added = approved + [
        _sample_duty(
            "session-1:2026-04-12:מחלקות",
            "2026-04-12",
            "מחלקות",
            "תורנות/מחלקות",
            "2026-04-12T12:00:00Z",
            "2026-04-12T20:00:00Z",
        )
    ]
    changes = build_diff_changes(approved, detected_added)
    assert len(changes) == 1
    assert changes[0]["change_type"] == "added"

    print("Duty Sync diff engine checks passed.")


if __name__ == "__main__":
    _run()
