from pathlib import Path
import sys
import types

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _DummyCollection:
    def find_one(self, *args, **kwargs):
        return None

    def find(self, *args, **kwargs):
        return []

    def insert_one(self, *args, **kwargs):
        return None

    def update_one(self, *args, **kwargs):
        return None

    def delete_one(self, *args, **kwargs):
        return None

    def create_index(self, *args, **kwargs):
        return None


if "db" not in sys.modules:
    dummy = _DummyCollection()
    sys.modules["db"] = types.SimpleNamespace(
        study_content_collection=dummy,
        study_user_state_collection=dummy,
        user_profiles_collection=dummy,
        interaction_logs_collection=dummy,
    )

from services.study_service import (
    STUDY_SEED_ITEMS,
    _difficulty_policy_for_profile,
    _eligible_for_main_flow,
    _normalize_study_item,
)


OUTPUT_PATH = ROOT / "docs" / "study_question_matrix.md"
RESIDENCY_YEARS = ("R1", "R2", "R3", "R4", "R5", "R6")


def _profile_for_year(year):
    return {
        "training_stage": "resident",
        "residency_year": year,
        "subspecialty": "General OB-GYN",
    }


def _normalized_mcqs():
    items = []
    for item in STUDY_SEED_ITEMS:
        if item.get("item_type") != "mcq":
            continue
        if item.get("enabled") is False:
            continue
        normalized = _normalize_study_item(item)
        if not normalized:
            continue
        items.append(normalized)
    return items


def _topic_sort_key(item):
    return (
        item.get("topic") or "",
        -(int(item.get("effective_difficulty_target_10") or item.get("difficulty_target_10") or 0)),
        item.get("id") or "",
    )


def _render_question_block(item):
    lines = []
    lines.append(f"### {item.get('topic')} — {item.get('subtopic')}")
    lines.append(f"- `id`: `{item.get('id')}`")
    lines.append(
        "- `difficulty_level`: "
        f"`{item.get('difficulty_level')}` | `declared_level`: "
        f"`{item.get('declared_difficulty_level')}` | `target_10`: "
        f"`{item.get('effective_difficulty_target_10') or item.get('difficulty_target_10')}`"
    )
    lines.append(
        "- `style`: "
        f"`{item.get('question_style')}` | `archetype`: "
        f"`{item.get('decision_archetype')}` | `template_family`: "
        f"`{item.get('template_family')}`"
    )
    lines.append(
        "- `quality`: "
        f"`stage_b_score={item.get('stage_b_quality_score')}` | "
        f"`question_quality={item.get('question_quality_score_10')}` | "
        f"`second_best={item.get('second_best_strength_score')}` | "
        f"`decision_pressure={item.get('decision_pressure_score')}` | "
        f"`engine_status={item.get('difficulty_engine_status')}`"
    )
    if item.get("disguised_recall_archetype"):
        lines.append(f"- `disguised_recall`: `{item.get('disguised_recall_archetype')}`")
    lines.append("")
    lines.append(item.get("question_stem") or "")
    lines.append("")
    option_lines = []
    for option in item.get("options") or []:
        option_lines.append(f"- **{option.get('key')}**. {option.get('text')}")
    lines.extend(option_lines)
    lines.append("")
    return lines


def build_markdown():
    items = _normalized_mcqs()
    lines = [
        "# Study Question Matrix by Residency Year",
        "",
        "This file lists the MCQ pool that is eligible for the main study flow after the current difficulty gates.",
        "It shows inventory eligibility per year, not the exact runtime order shown in cards.",
        "",
    ]

    for year in RESIDENCY_YEARS:
        policy = _difficulty_policy_for_profile(_profile_for_year(year))
        eligible_items = [item for item in items if _eligible_for_main_flow(item, policy)]
        eligible_items = sorted(eligible_items, key=_topic_sort_key)

        lines.append(f"## {year}")
        lines.append("")
        lines.append(
            f"- `eligible_questions`: **{len(eligible_items)}**"
        )
        lines.append(
            "- `gate`: "
            f"`min_target_10={policy.get('main_flow_min_target_10')}` | "
            f"`max_target_10={policy.get('main_flow_max_target_10')}` | "
            f"`require_engine_ready={policy.get('require_difficulty_engine_ready')}` | "
            f"`block_disguised_recall={policy.get('block_disguised_recall')}` | "
            f"`min_decision_pressure={policy.get('min_decision_pressure_score')}`"
        )
        lines.append("")

        if not eligible_items:
            lines.append("_No questions currently pass this year's main-flow gate._")
            lines.append("")
            continue

        for item in eligible_items:
            lines.extend(_render_question_block(item))

    return "\n".join(lines).rstrip() + "\n"


def main():
    OUTPUT_PATH.write_text(build_markdown(), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
