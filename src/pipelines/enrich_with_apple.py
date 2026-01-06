"""Enrich NYT titles with Apple Books pricing/ratings."""

import pandas as pd

from src.api_clients.apple_books_client import fetch_apple_book


def enrich_with_apple(input_csv: str, output_csv: str) -> pd.DataFrame:
    """Append Apple Books price, ratings, and store link columns to a CSV.

    Parameters
    ----------
    input_csv : str
        Path to a NYT-transformed CSV containing book rows.
    output_csv : str
        Destination path for the enriched CSV.

    Returns
    -------
    pandas.DataFrame
        DataFrame with the added Apple Books columns.
    """
    df = pd.read_csv(input_csv)

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

    df.to_csv(output_csv, index=False)
    return df
