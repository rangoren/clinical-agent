import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from services.textbook_cache_service import get_textbook_cache, merge_textbook_cache_topics
from services.textbook_catalog_service import build_gabbe_topic_mapping_batch, build_gabbe_topic_mapping_summary


def main():
    parser = argparse.ArgumentParser(description="Incrementally build Gabbe topic mapping.")
    parser.add_argument("--tier", choices=["A", "B"], default="A", help="Topic tier to map.")
    parser.add_argument("--batch-size", type=int, default=5, help="Topics per batch.")
    args = parser.parse_args()

    batch_size = max(1, min(args.batch_size, 10))
    tier = args.tier
    offset = 0

    while True:
        payload = build_gabbe_topic_mapping_batch(offset=offset, limit=batch_size, tier=tier)
        topics = payload.get("topics") or []
        if not topics:
            print(f"No more topics left for tier {tier}.")
            break

        cached = merge_textbook_cache_topics(
            "gabbe_topic_mapping",
            topics,
            metadata={"book_id": payload.get("book_id", "gabbe_9")},
        )
        summary = build_gabbe_topic_mapping_summary(cached.get("payload") or {})
        print(
            f"Mapped tier {tier} topics offset={offset} count={len(topics)} "
            f"(cached total={summary['topic_count']}, mapped={summary['mapped_count']}, unmapped={summary['unmapped_count']})"
        )

        offset += batch_size
        if offset >= payload.get("total_available_topics", 0):
            break

    cached = get_textbook_cache("gabbe_topic_mapping") or {}
    summary = build_gabbe_topic_mapping_summary(cached.get("payload") or {})
    print("Final summary:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
