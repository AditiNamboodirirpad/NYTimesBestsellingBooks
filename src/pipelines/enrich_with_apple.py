"""Enrich NYT titles with Apple Books pricing/ratings."""

import os
import pandas as pd
import tempfile
from pathlib import Path

from src.api_clients.apple_books_client import fetch_apple_book
from src.services.gcs_service import download_file, upload_file


def enrich_with_apple(input_csv: str, output_csv: str) -> str:
    """
    Enrich a NYT weekly CSV with Apple Books metadata.

    Parameters
    ----------
    input_csv_blob : str
        GCS blob path to the input NYT CSV (e.g. processed/weekly/nyt_*.csv)
    output_csv_blob : str
        GCS blob path where the enriched CSV will be written

    Returns
    -------
    str
        GCS blob path of the enriched CSV
    """

    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise ValueError("GCS_BUCKET is not set")

    # Download input CSV from GCS
    tmp_path = Path(tempfile.gettempdir()) / Path(input_csv).name
    local_input_path = download_file(input_csv, str(tmp_path))


    # Read CSV locally
    df = pd.read_csv(local_input_path)

    apple_prices = []
    apple_ratings = []
    apple_ratings_count = []
    apple_links = []

    for _, row in df.iterrows():
        isbn = str(row.get("isbn13") or "")
        title = row.get("title")
        author = row.get("author")

        details = fetch_apple_book(isbn=isbn, title=title, author=author)

        apple_prices.append(details.get("price"))
        apple_ratings.append(details.get("rating"))
        apple_ratings_count.append(details.get("ratings_count"))
        apple_links.append(details.get("store_link"))

    df["apple_price"] = apple_prices
    df["apple_rating"] = apple_ratings
    df["apple_ratings_count"] = apple_ratings_count
    df["apple_store_link"] = apple_links

    # --- NEW: write to temp + upload to GCS ---
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8", delete=False
    ) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    upload_file(tmp_path, output_csv)

    return output_csv
