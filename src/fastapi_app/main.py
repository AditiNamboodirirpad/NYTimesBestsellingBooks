"""FastAPI entrypoint to run the NYT pipeline and expose GCS listings."""

from fastapi import FastAPI
from pathlib import Path
from src.pipelines.fetch_nyt_weekly import fetch_and_save
from src.pipelines.transform_nyt import transform_latest
from src.services.gcs_service import list_files
from src.pipelines.enrich_with_apple import enrich_with_apple
from src.pipelines.transform_history import transform_history
from src.pipelines.build_recommendations import build_recommendations

app = FastAPI()


@app.get("/health")
def health() -> dict[str, str]:
    """Simple liveness probe."""
    return {"status": "ok"}


@app.post("/pipeline/run")
def run_pipeline() -> dict[str, str]:
    """Run fetch → transform → enrich, then upload artifacts to GCS."""
    # 1. Fetch NYT JSON and upload to GCS
    raw_blob = fetch_and_save()

    # 2. Transform to CSV and upload to GCS
    csv_blob = transform_latest()

    # 3. Enrich with Apple and save new CSV to GCS
    apple_csv_blob = str(
        Path(csv_blob).with_name(
            Path(csv_blob).stem + "_apple" + Path(csv_blob).suffix
        )
    )

    enrich_with_apple(csv_blob, apple_csv_blob)

    # 4. Append to historical table in GCS
    history_blob = transform_history()

    # 5. Rebuild recommendations in GCS
    recommendations_blob = build_recommendations()

    return {
        "message": "Pipeline complete",
        "raw_uploaded": raw_blob,
        "csv_uploaded": csv_blob,
        "apple_uploaded": apple_csv_blob,
        "history_uploaded": history_blob,
        "recommendations_uploaded": recommendations_blob,
    }


@app.get("/files")
def files() -> list[str]:
    """List all blobs in the configured GCS bucket."""
    return list_files()

@app.get("/files/processed")
def processed_files() -> list[str]:
    """List processed (weekly) CSV blobs."""
    return list_files(prefix="processed/weekly/")

@app.get("/files/raw")
def raw_files() -> list[str]:
    """List raw NYT JSON blobs."""
    return list_files(prefix="raw/weekly/")
