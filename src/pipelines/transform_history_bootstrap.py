"""Build full historical NYT bestseller table from raw history JSON files."""

import json
import csv
from pathlib import Path

RAW_DIR = Path("data/raw/historical")
HISTORY_DIR = Path("data/processed/history")
HISTORY_FILE = HISTORY_DIR / "nyt_history_weekly.csv"

COLUMNS = [
    "published_date",
    "bestsellers_date",
    "list_name",
    "title",
    "author",
    "primary_isbn13",
    "publisher",
    "rank",
    "rank_last_week",
    "weeks_on_list",
    "amazon_product_url",
    "book_image",
    "description",
]


def build_history() -> str:
    if not RAW_DIR.exists():
        raise FileNotFoundError("data/raw/historical not found")

    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError("No historical NYT JSON files found")

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    seen = set()
    rows = []

    for file in files:
        with open(file, "r") as f:
            data = json.load(f)

        results = data["results"]
        list_name = results.get("display_name")
        published_date = results["published_date"]
        bestsellers_date = results["bestsellers_date"]

        for b in results["books"]:
            key = (published_date, b.get("title"), b.get("author"))
            if key in seen:
                continue

            seen.add(key)

            rows.append([
                published_date,
                bestsellers_date,
                list_name,
                b.get("title"),
                b.get("author"),
                b.get("primary_isbn13"),
                b.get("publisher"),
                b.get("rank"),
                b.get("rank_last_week"),
                b.get("weeks_on_list"),
                b.get("amazon_product_url"),
                b.get("book_image"),
                b.get("description"),
            ])

    with open(HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(rows)

    print(f"Built history with {len(rows)} rows")
    print(f"Saved to {HISTORY_FILE}")

    return str(HISTORY_FILE)


if __name__ == "__main__":
    build_history()
