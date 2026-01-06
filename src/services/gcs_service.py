import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv("GCS_BUCKET")

client = storage.Client()

def upload_file(local_path: str, blob_path: str):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    return f"gs://{BUCKET_NAME}/{blob_path}"


def download_file(blob_path: str, local_path: str):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(local_path)
    return local_path


def list_files(prefix=""):
    bucket = client.bucket(BUCKET_NAME)
    return [b.name for b in bucket.list_blobs(prefix=prefix)]
