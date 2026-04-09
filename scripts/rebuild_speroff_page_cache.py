import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from services.textbook_cache_service import get_textbook_page_cache_progress
from services.textbook_catalog_service import cache_speroff_page_text_batch


def main():
    parser = argparse.ArgumentParser(description="Incrementally build Speroff page-text cache.")
    parser.add_argument("--batch-size", type=int, default=25, help="Pages per batch.")
    parser.add_argument("--target-page", type=int, default=400, help="Stop once this page is cached.")
    args = parser.parse_args()

    batch_size = max(1, args.batch_size)
    target_page = max(1, args.target_page)

    progress = get_textbook_page_cache_progress("speroff_10_page_text")
    start_page = (progress.get("cached_through_page") or 0) + 1
    total_pages = progress.get("total_pages") or 0

    if total_pages and start_page > total_pages:
        print("Speroff page cache is already complete.")
        return 0

    if start_page > target_page:
        print(f"Speroff page cache already built through page {progress.get('cached_through_page')}.")
        return 0

    while start_page <= target_page:
        payload = cache_speroff_page_text_batch(start_page=start_page, limit=batch_size)
        print(
            f"Cached pages {payload['start_page']}-{payload['end_page']} "
            f"(batch_count={payload['batch_count']}, total_pages={payload['total_pages']})"
        )

        total_pages = payload["total_pages"]
        if payload["end_page"] >= total_pages:
            print("Reached end of Speroff PDF.")
            break

        start_page = payload["end_page"] + 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
