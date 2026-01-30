"""Build and maintain a historical NYT bestseller table (append weekly rows)."""

import json
import csv
from pathlib import Path
import os
import tempfile

from src.services.gcs_service import download_file, upload_file, list_files

HISTORY_BLOB = "processed/history/nyt_history_weekly.csv"
RAW_PREFIX = "raw/weekly/"


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
    """Append the newest NYT weekly list to the historical CSV stored in GCS."""

    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise ValueError("GCS_BUCKET is not set.")

    # List raw weekly JSONs from GCS
    raw_blobs = sorted(list_files(prefix=RAW_PREFIX))
    if not raw_blobs:
        raise FileNotFoundError("No raw NYT JSON files found in GCS.")

    latest_blob = raw_blobs[-1]

    # Download latest raw JSON to temp file
    tmp_raw_path = Path(tempfile.gettempdir()) / Path(latest_blob).name
    raw_path = download_file(latest_blob, str(tmp_raw_path))

    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = data["results"]
    list_name = results.get("display_name")
    published_date = results["published_date"]
    bestsellers_date = results["bestsellers_date"]

    new_rows = []
    for b in results["books"]:
        new_rows.append(
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


    # Download existing history if present
    try:
        tmp_history_path = Path(tempfile.gettempdir()) / Path(HISTORY_BLOB).name
        existing_path = download_file(HISTORY_BLOB, str(tmp_history_path))
    except FileNotFoundError:
        existing_path = None


    existing_dates: set[str] = set()
    existing_rows: list[list[str]] = []

    if existing_path is not None:
        with open(existing_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_dates.add(row["published_date"])
                existing_rows.append([row.get(col, "") for col in COLUMNS])

    if published_date in existing_dates:
        print(f"Week {published_date} already exists — skipping append.")
        return HISTORY_BLOB

    # Combine existing rows + new rows
    combined_rows = existing_rows + new_rows

    # Write to a temp file, then upload
    with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False) as tmp:
        writer = csv.writer(tmp)
        writer.writerow(COLUMNS)
        writer.writerows(combined_rows)
        tmp_path = tmp.name

    upload_file(tmp_path, HISTORY_BLOB)

    print(f"Updated history file in GCS: {HISTORY_BLOB}")
    return HISTORY_BLOB


if __name__ == "__main__":
    transform_history()
