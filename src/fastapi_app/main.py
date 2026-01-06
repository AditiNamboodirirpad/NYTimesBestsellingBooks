from fastapi import FastAPI
from pathlib import Path
from src.pipelines.fetch_nyt_weekly import fetch_and_save
from src.pipelines.transform_nyt import transform_latest
from src.services.gcs_service import upload_file, list_files

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/pipeline/run")
def run_pipeline():
    # 1. fetch NYT JSON locally
    raw_path = fetch_and_save()

    # 2. transform to CSV
    csv_path = transform_latest()

    # 3. upload both to GCS
    raw_uri = upload_file(raw_path, f"raw/{Path(raw_path).name}")
    csv_uri = upload_file(csv_path, f"processed/{Path(csv_path).name}")

    return {
        "message": "Pipeline complete",
        "raw_uploaded": raw_uri,
        "csv_uploaded": csv_uri
    }


@app.get("/files")
def files():
    return list_files()
