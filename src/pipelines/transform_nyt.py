"""Transform raw NYT API JSON into a flattened CSV."""

import json
import csv
import os
import tempfile
from pathlib import Path

from src.services.gcs_service import list_files, download_file, upload_file

RAW_PREFIX = "raw/weekly/"
PROCESSED_PREFIX = "processed/weekly/"


def transform_latest() -> str:
    """Convert the most recent raw NYT JSON in GCS into a structured CSV and upload it back to GCS."""

    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise ValueError("GCS_BUCKET is not set.")

    # List raw weekly JSONs from GCS
    raw_blobs = sorted(list_files(prefix=RAW_PREFIX))
    if not raw_blobs:
        raise FileNotFoundError("No raw NYT JSON files found in GCS.")

    latest_blob = raw_blobs[-1]

    # Download latest raw JSON to temp file
    tmp_path = Path(tempfile.gettempdir()) / Path(latest_blob).name
    raw_path = download_file(latest_blob, str(tmp_path))

    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = data["results"]
    week = results["published_date"]
    books = results["books"]

    output_blob = f"{PROCESSED_PREFIX}nyt_transformed_{week}.csv"

    # Write CSV to temp file
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8", delete=False
    ) as tmp:
        writer = csv.writer(tmp)

        writer.writerow(
            [
                "week",
                "rank",
                "title",
                "author",
                "isbn13",
                "publisher",
                "description",
                "amazon_url",
            ]
        )

        for b in books:
            writer.writerow(
                [
                    week,
                    b["rank"],
                    b["title"],
                    b["author"],
                    b["primary_isbn13"],
                    b["publisher"],
                    b["description"],
                    b["amazon_product_url"],
                ]
            )
        tmp_path = tmp.name

    # Upload to GCS
    upload_file(tmp_path, output_blob)

    print(f"Uploaded: {output_blob}")
    return output_blob



if __name__ == "__main__":
    transform_latest()
