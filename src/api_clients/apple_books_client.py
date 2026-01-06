"""Apple Books search client."""

import requests

BASE_URL = "https://itunes.apple.com/search"


def fetch_apple_book(isbn: str, title: str | None = None, author: str | None = None) -> dict:
    """Fetch Apple Books metadata using ISBN with a title/author fallback.

    Parameters
    ----------
    isbn : str
        ISBN-13 identifier to search for.
    title : str, optional
        Book title used when ISBN search returns no results, by default None.
    author : str, optional
        Author name used with the title fallback, by default None.

    Returns
    -------
    dict
        A mapping with basic Apple Books details (title, author, price, currency,
        rating, ratings_count, store_link). Returns an empty dict when no match is found.

    Raises
    ------
    requests.HTTPError
        If the Apple Books API responds with an error status.
    """
    params = {
        "term": isbn,
        "media": "ebook",
        "entity": "ebook",
        "limit": 5,
    }

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])

    if not results and title:
        query = f"{title} {author or ''}"
        params = {
            "term": query,
            "media": "ebook",
            "entity": "ebook",
            "limit": 5,
        }

        resp = requests.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

    if not results:
        return {}

    book = results[0]
    return {
        "title": book.get("trackName"),
        "author": book.get("artistName"),
        "price": book.get("price") or book.get("trackPrice"),
        "currency": book.get("currency"),
        "rating": book.get("averageUserRating"),
        "ratings_count": book.get("userRatingCount"),
        "store_link": book.get("trackViewUrl"),
    }
