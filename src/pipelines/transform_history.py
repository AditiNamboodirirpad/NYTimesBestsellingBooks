"""Build and maintain a historical NYT bestseller table (append weekly rows)."""

import os
import json
import csv
from pathlib import Path

RAW_DIR = Path("data/raw/weekly")
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


def transform_history() -> str:
    """Append the newest NYT weekly list to the historical CSV."""

    files = sorted([f for f in RAW_DIR.iterdir() if f.suffix == ".json"])
    if not files:
        raise FileNotFoundError("No raw NYT JSON files found in data/raw/weekly/")

    latest = files[-1]

    with open(latest, "r") as f:
        data = json.load(f)

    results = data["results"]
    list_name = results.get("display_name")
    published_date = results["published_date"]
    bestsellers_date = results["bestsellers_date"]

    rows = []
    for b in results["books"]:
        rows.append(
            [
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
            ]
        )

    # ensure directory exists
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    # if file does NOT exist → create + write header
    if not HISTORY_FILE.exists():
        with open(HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)
            writer.writerows(rows)

    else:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_dates = {row["published_date"] for row in reader}

        if published_date in existing_dates:
            print(f"Week {published_date} already exists — skipping append.")
            return str(HISTORY_FILE)


        with open(HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    print(f"Updated history file: {HISTORY_FILE}")
    return str(HISTORY_FILE)


if __name__ == "__main__":
    transform_history()
