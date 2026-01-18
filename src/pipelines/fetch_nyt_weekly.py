"""Pipeline step to fetch and persist the latest NYT bestseller list."""

import json
import os
from datetime import datetime

from src.api_clients.nyt_client import get_best_list

RAW_DIR = "data/raw/weekly"


def fetch_and_save(list_name: str = "hardcover-fiction") -> str:
    """Fetch the current list and write it to a timestamped JSON file.

    Parameters
    ----------
    list_name : str, optional
        NYT list name to request (e.g., ``"hardcover-fiction"``), by default "hardcover-fiction".

    Returns
    -------
    str
        Path to the saved JSON file.
    """
    data = get_best_list(list_name)
    os.makedirs(RAW_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{RAW_DIR}/nyt_{list_name}_{timestamp}.json"

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved: {filename}")
    return filename


if __name__ == "__main__":
    fetch_and_save()
