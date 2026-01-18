"""Google Cloud Storage helpers for uploads, downloads, and listing blobs."""

import os
from google.cloud import storage
from dotenv import load_dotenv

# Environment should already be loaded by the process (e.g., FastAPI startup).
BUCKET_NAME = os.getenv("GCS_BUCKET")

if not BUCKET_NAME:
    raise ValueError("GCS_BUCKET is not set — make sure it's in docker compose!")

client = storage.Client()

def upload_file(local_path: str, blob_path: str) -> str:
    """Upload a local file to the configured bucket and return the gs:// URI."""
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    return f"gs://{BUCKET_NAME}/{blob_path}"


def download_file(blob_path: str, local_path: str) -> str:
    """Download a blob to a local path and return the destination path."""
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(local_path)
    return local_path


def list_files(prefix: str = "") -> list[str]:
    """Return blob names in the bucket filtered by optional prefix."""
    bucket = client.bucket(BUCKET_NAME)
    return [b.name for b in bucket.list_blobs(prefix=prefix)]
