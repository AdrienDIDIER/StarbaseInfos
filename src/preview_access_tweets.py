from __future__ import annotations

import argparse
import os

from Scraper_RC import (
    ACCESS_URL,
    build_post_card,
    build_post_text,
    check_page_structure,
    extract_access_updates,
    get_html,
    records_with_keys,
)
from utils.notify import tweet_len


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview Starbase beach/road access posts without posting anything."
    )
    parser.add_argument("--url", default=ACCESS_URL, help="Beach and Road Access page URL.")
    parser.add_argument("--file", default=None, help="Read a saved HTML page instead of fetching the URL.")
    parser.add_argument(
        "--section",
        choices=["all", "beach", "road"],
        default="all",
        help="Limit the preview to one section.",
    )
    parser.add_argument(
        "--event",
        choices=["new", "updated", "cancelled", "lifted"],
        default="new",
        help="Which kind of post to preview ('updated' compares each record with itself).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of posts to preview.")
    parser.add_argument("--cards", default=None, help="Directory where the image cards are written.")
    args = parser.parse_args()

    if args.file:
        with open(args.file, encoding="utf-8") as f:
            html = f.read()
    else:
        html = get_html(args.url)
    check_page_structure(html)

    records = records_with_keys(extract_access_updates(html, source_url=args.url))
    if args.section != "all":
        records = [r for r in records if r["category"] == args.section]

    if not records:
        print("No matching beach or road access updates found.")
        return

    if args.limit is not None:
        records = records[: args.limit]

    print("Preview only: no Twitter API or MongoDB calls are made.\n")

    for index, record in enumerate(records, start=1):
        previous = record if args.event == "updated" else None
        post = build_post_text(args.event, record, previous=previous)

        print("=" * 80)
        print(f"{index}. {record['category'].upper()} | {tweet_len(post)} chars | key={record['uniq_key']}")
        print("-" * 80)
        print(post)
        print()

        if args.cards:
            card = build_post_card(args.event, record, previous=previous)
            if card:
                os.makedirs(args.cards, exist_ok=True)
                path = os.path.join(args.cards, f"access_{index}_{args.event}.png")
                with open(path, "wb") as f:
                    f.write(card)
                print(f"Card written to {path}\n")


if __name__ == "__main__":
    main()
