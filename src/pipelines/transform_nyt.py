"""Transform raw NYT API JSON into a flattened CSV."""

import json
import csv
import os

RAW_DIR = "data/raw/weekly"
PROCESSED_DIR = "data/processed/weekly"


def transform_latest() -> str:
    """Convert the most recent raw NYT JSON file into a structured CSV.

    The function finds the newest JSON in ``data/raw/``, extracts weekly list metadata
    and book entries, and writes a normalized CSV to ``data/processed/``.

    Returns
    -------
    str
        Path to the generated CSV file.
    """
    files = sorted([f for f in os.listdir(RAW_DIR) if f.endswith(".json")])
    latest = os.path.join(RAW_DIR, files[-1])

    with open(latest, "r") as f:
        data = json.load(f)

    results = data["results"]
    week = results["published_date"]
    books = results["books"]

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_file = os.path.join(PROCESSED_DIR, f"nyt_transformed_{week}.csv")

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

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

    print(f"Saved: {output_file}")
    return output_file


if __name__ == "__main__":
    transform_latest()
