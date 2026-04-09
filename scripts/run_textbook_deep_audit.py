import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from services.textbook_audit_service import run_deep_textbook_audit


def main():
    parser = argparse.ArgumentParser(description="Run a deep streamed textbook audit and cache the result.")
    parser.add_argument("--book-id", required=True, help="Book id, for example gabbe_9 or berek_17.")
    parser.add_argument("--sample-pages", type=int, default=5, help="Number of early pages to sample for text extraction.")
    args = parser.parse_args()

    payload = run_deep_textbook_audit(args.book_id, sample_pages=max(1, min(args.sample_pages, 10)))
    audit = payload.get("deep_audit") or {}
    print(
        {
            "book_id": payload.get("book_id"),
            "page_count": audit.get("page_count"),
            "sampled_page_count": audit.get("sampled_page_count"),
            "probe_pages": audit.get("probe_pages"),
            "extractable_sample_pages": audit.get("extractable_sample_pages"),
            "sample_extractable_ratio": audit.get("sample_extractable_ratio"),
            "has_outlines": audit.get("has_outlines"),
            "outline_count": audit.get("outline_count"),
            "cache_updated_at": payload.get("cache_updated_at"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
