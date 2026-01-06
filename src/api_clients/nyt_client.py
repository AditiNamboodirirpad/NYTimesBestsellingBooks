"""NYT Books API client helpers."""

import requests

from src.utils.config import NYT_API_KEY

BASE_URL = "https://api.nytimes.com/svc/books/v3"


def get_best_list(list_name: str = "hardcover-fiction") -> dict:
    """Fetch the current NYT bestseller list for a category.

    Parameters
    ----------
    list_name : str, optional
        NYT list name to fetch (e.g., ``"hardcover-fiction"``), by default "hardcover-fiction".

    Returns
    -------
    dict
        Parsed JSON payload returned by the NYT Books API.

    Raises
    ------
    requests.HTTPError
        If the NYT API responds with an error status.
    """
    url = f"{BASE_URL}/lists/current/{list_name}.json"
    params = {"api-key": NYT_API_KEY}

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
