"""FastAPI entrypoint to run the NYT pipeline and expose GCS listings."""

from fastapi import FastAPI
from pathlib import Path
from src.pipelines.fetch_nyt_weekly import fetch_and_save
from src.pipelines.transform_nyt import transform_latest
from src.services.gcs_service import upload_file, list_files
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
    # 1. Fetch NYT JSON locally
    raw_path = fetch_and_save()

    # 2. Transform to CSV
    csv_path = transform_latest()

    # 3. Enrich with Apple and save new CSV
    apple_csv_path = str(
        Path(csv_path).with_name(
            Path(csv_path).stem + "_apple" + Path(csv_path).suffix
        )
    )

    enrich_with_apple(csv_path, apple_csv_path)

    # 4. Append to historical table
    history_path = transform_history()

    # 5. Rebuild recommendations
    recommendations_path = build_recommendations()

    # 4. Upload artifacts to GCS
    raw_uri = upload_file(raw_path, f"raw/weekly/{Path(raw_path).name}")
    csv_uri = upload_file(csv_path, f"processed/weekly/{Path(csv_path).name}")
    apple_uri = upload_file(apple_csv_path, f"processed/weekly/{Path(apple_csv_path).name}")
    history_uri = upload_file(
        history_path,
        "processed/history/nyt_history_weekly.csv"
    )

    recs_uri = upload_file(
        recommendations_path,
        "processed/recommendations/recommendations.csv"
    )

    return {
        "message": "Pipeline complete",
        "raw_uploaded": raw_uri,
        "csv_uploaded": csv_uri,
        "apple_uploaded": apple_uri,
        "history_uploaded": history_uri,
        "recommendations_uploaded": recs_uri,
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
