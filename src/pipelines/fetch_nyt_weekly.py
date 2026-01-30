"""Pipeline step to fetch and persist the latest NYT bestseller list."""

import json
import os
from datetime import datetime
import tempfile

from src.api_clients.nyt_client import get_best_list
from src.services.gcs_service import upload_file

RAW_PREFIX = "raw/weekly/"


def fetch_and_save(list_name: str = "hardcover-fiction") -> str:
    """Fetch the current list and upload it to GCS as a JSON file."""

    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise ValueError("GCS_BUCKET is not set.")
    

    data = get_best_list(list_name)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    blob_name = f"{RAW_PREFIX}nyt_{list_name}_{timestamp}.json"

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp:
        json.dump(data, tmp, indent=2)
        tmp_path = tmp.name

    # Upload to GCS
    upload_file(tmp_path, blob_name)

    print(f"Uploaded: {blob_name}")
    return blob_name


if __name__ == "__main__":
    fetch_and_save()
