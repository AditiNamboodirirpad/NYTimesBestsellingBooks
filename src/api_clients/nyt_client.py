"""NYT Books API client helpers."""

import requests

from src.utils.config import NYT_API_KEY

BASE_URL = "https://api.nytimes.com/svc/books/v3"


def get_best_list(list_name: str = "hardcover-fiction", date: str = "current") -> dict[str, object]:
    """Fetch the NYT bestseller list JSON for a category and date token."""
    url = f"{BASE_URL}/lists/{date}/{list_name}.json"
    params = {"api-key": NYT_API_KEY}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()
