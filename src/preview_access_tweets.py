from __future__ import annotations

import argparse

from Scraper_RC import (
    ACCESS_URL,
    _build_tweet_text,
    _make_uniq_key,
    _optional_text,
    extract_access_updates,
    get_html,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview Starbase beach/road access tweets without posting anything."
    )
    parser.add_argument("--url", default=ACCESS_URL, help="Beach and Road Access page URL.")
    parser.add_argument(
        "--section",
        choices=["all", "beach", "road"],
        default="all",
        help="Limit the preview to one section.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of tweets to preview.")
    args = parser.parse_args()

    df = extract_access_updates(get_html(args.url), source_url=args.url)
    if args.section != "all" and not df.empty:
        df = df[df["category"] == args.section].reset_index(drop=True)

    if df.empty:
        print("No matching beach or road access updates found.")
        return

    if args.limit is not None:
        df = df.head(args.limit)

    print("Preview only: no Twitter API or MongoDB calls are made.\n")

    for index, row in df.iterrows():
        category = _optional_text(row.get("category")) or "road"
        backup_date = _optional_text(row.get("backup_date"))
        uniq_key = _make_uniq_key(
            row["date"],
            row["description"],
            category=category,
            backup_date=backup_date,
        )
        tweet = _build_tweet_text(
            status=row["status"],
            description=row["description"],
            date_str=row["date"],
            uniq_key=uniq_key,
            category=category,
            backup_date=backup_date,
        )

        print("=" * 80)
        print(f"{index + 1}. {category.upper()} | {len(tweet)} chars | key={uniq_key}")
        print("-" * 80)
        print(tweet)
        print()


if __name__ == "__main__":
    main()
