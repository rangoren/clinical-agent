def duty_map_by_key(duties):
    return {item.get("duty_key"): item for item in duties or [] if item.get("duty_key")}


def duty_map_by_date(duties):
    mapped = {}
    for item in duties or []:
        duty_date = item.get("date")
        if duty_date:
            mapped[duty_date] = item
    return mapped


def duty_signature(duty):
    duty = duty or {}
    return (
        duty.get("duty_key"),
        duty.get("date"),
        duty.get("role"),
        duty.get("title"),
        duty.get("start_datetime"),
        duty.get("end_datetime"),
    )


def duties_equivalent(left_duty, right_duty):
    return duty_signature(left_duty) == duty_signature(right_duty)


def build_diff_changes(approved_duties, detected_duties):
    approved_by_key = duty_map_by_key(approved_duties)
    detected_by_key = duty_map_by_key(detected_duties)
    approved_by_date = duty_map_by_date(approved_duties)
    detected_by_date = duty_map_by_date(detected_duties)

    consumed_old_keys = set()
    consumed_new_keys = set()
    changes = []

    for duty_date, old_item in approved_by_date.items():
        new_item = detected_by_date.get(duty_date)
        if not new_item:
            continue
        if old_item.get("duty_key") == new_item.get("duty_key") and duties_equivalent(old_item, new_item):
            consumed_old_keys.add(old_item["duty_key"])
            consumed_new_keys.add(new_item["duty_key"])
            continue
        changes.append(
            {
                "change_type": "changed",
                "change_key": f"changed:{duty_date}",
                "date": duty_date,
                "included": True,
                "old_duty": old_item,
                "new_duty": new_item,
            }
        )
        consumed_old_keys.add(old_item["duty_key"])
        consumed_new_keys.add(new_item["duty_key"])

    unmatched_added = []
    for duty_key, new_item in detected_by_key.items():
        if duty_key in consumed_new_keys:
            continue
        if duty_key in approved_by_key:
            consumed_old_keys.add(duty_key)
            continue
        unmatched_added.append((duty_key, new_item))

    unmatched_removed = []
    for duty_key, old_item in approved_by_key.items():
        if duty_key in consumed_old_keys:
            continue
        if duty_key in detected_by_key:
            continue
        unmatched_removed.append((duty_key, old_item))

    consumed_added_keys = set()
    consumed_removed_keys = set()
    for removed_key, old_item in unmatched_removed:
        for added_key, new_item in unmatched_added:
            if added_key in consumed_added_keys:
                continue
            if old_item.get("role") == new_item.get("role") and old_item.get("title") == new_item.get("title"):
                changes.append(
                    {
                        "change_type": "changed",
                        "change_key": f"moved:{removed_key}:{added_key}",
                        "date": new_item.get("date") or old_item.get("date"),
                        "included": True,
                        "old_duty": old_item,
                        "new_duty": new_item,
                        "change_hint": "moved",
                    }
                )
                consumed_removed_keys.add(removed_key)
                consumed_added_keys.add(added_key)
                break

    for duty_key, new_item in unmatched_added:
        if duty_key in consumed_added_keys:
            continue
        changes.append(
            {
                "change_type": "added",
                "change_key": f"added:{duty_key}",
                "date": new_item.get("date"),
                "included": True,
                "new_duty": new_item,
            }
        )

    for duty_key, old_item in unmatched_removed:
        if duty_key in consumed_removed_keys:
            continue
        changes.append(
            {
                "change_type": "removed",
                "change_key": f"removed:{duty_key}",
                "date": old_item.get("date"),
                "included": True,
                "old_duty": old_item,
            }
        )

    changes.sort(key=lambda item: (item.get("date") or "", item.get("change_type") or ""))
    return changes
