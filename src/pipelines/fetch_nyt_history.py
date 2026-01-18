"""Fetch historical NYT bestseller lists."""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta

from src.api_clients.nyt_client import get_best_list
from src.utils.config import NYT_API_KEY

import time
import requests

RAW_DIR = "data/raw/historical"


def fetch_historical_weeks(
    list_name: str = "hardcover-fiction",
    weeks_back: int = 26
) -> list[str]:
    """
    Fetch NYT bestseller lists for the last N weeks.

    Returns
    -------
    list[str]
        Paths to saved JSON files.
    """

    os.makedirs(RAW_DIR, exist_ok=True)

    saved_files = []

    today = datetime.today()

    for i in range(weeks_back):
        date = today - timedelta(weeks=i)
        date_str = date.strftime("%Y-%m-%d")

        print(f"Fetching: {date_str}")

        retries = 8
        wait = 5

        while retries > 0:
            try:
                response = get_best_list(list_name=list_name, date=date_str)
                break

            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    print(f"Rate limit — waiting {wait} seconds...")
                    time.sleep(wait)
                    retries -= 1
                    wait *= 2
                else:
                    raise

        else:
            print(f"❌ Could not fetch {date_str} even after many retries — continuing")
            continue

        filename = f"{RAW_DIR}/nyt_{list_name}_{date_str}.json"

        with open(filename, "w") as f:
            json.dump(response, f, indent=2)

        saved_files.append(filename)

        time.sleep(2)

    print(f"\nSaved {len(saved_files)} files to {RAW_DIR}")
    return saved_files



if __name__ == "__main__":
    fetch_historical_weeks(
        list_name="hardcover-fiction",
        weeks_back=26,
    )
