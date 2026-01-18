# NYT Bestselling Books – Data Pipeline, API, and Dashboard

End-to-end pipeline that fetches the NYT bestseller list, normalizes it to CSV, enriches rows with Apple Books metadata, optionally uploads artifacts to Google Cloud Storage (GCS), and serves both an API (FastAPI) and a Streamlit UI for exploring the results.

## What's Inside
- Data pipeline (`src/pipelines/`) to fetch → transform → enrich weekly lists.
- FastAPI service (`src/fastapi_app/`) to trigger the pipeline and list uploaded files.
- Streamlit dashboard (`streamlit_app/`) that reads processed CSVs from `data/processed/`.
- Dockerfiles + docker-compose for containerized FastAPI and Streamlit services.

## Repo Layout (key parts)
```
src/
  api_clients/         # NYT + Apple Books API helpers
  pipelines/           # fetch_nyt_weekly, transform_nyt, enrich_with_apple, run_full_pipeline
  fastapi_app/         # FastAPI entrypoint and routes
  services/gcs_service.py  # Upload/list helpers for GCS
streamlit_app/app.py   # Streamlit UI
data/                  # raw/ and processed/ artifacts (raw is gitignored)
Dockerfile.fastapi     # API image
Dockerfile.streamlit   # UI image
docker-compose.yml     # Compose services (FastAPI + Streamlit)
```

## Prerequisites
- Python 3.11+
- pip
- (Optional) Docker and Docker Compose
- (Optional) Google Cloud project + service account with Storage access if you want uploads to GCS

## Environment Variables
Create `.env` in the repo root:
```
NYT_API_KEY=your-nyt-api-key
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=your-gcs-bucket
# Point to your service account key JSON (local path or in-container path)
GOOGLE_APPLICATION_CREDENTIALS=/app/keys/your-sa-key.json
```
Place the service account JSON under `keys/` (gitignored) and align the path you set above. The FastAPI upload endpoints expect `GCS_BUCKET` and valid credentials.

## Local Setup
```bash
pip install -r requirements.txt
```

## Running the Pipeline Locally
- End-to-end:
  ```bash
  python -m src.pipelines.run_full_pipeline
  ```
  Outputs:
  - Raw NYT JSON in `data/raw/nyt_<list>_<timestamp>.json`
  - Transformed CSV in `data/processed/nyt_transformed_<date>.csv`
  - Enriched CSV (Apple Books) in `data/processed/nyt_transformed_<date>_apple.csv`

- Individual steps (optional):
  ```bash
  python -m src.pipelines.fetch_nyt_weekly           # fetch raw JSON
  python -m src.pipelines.transform_nyt              # convert latest raw to CSV
  # Enrich an existing transformed CSV:
  python - <<'PY'
from src.pipelines.enrich_with_apple import enrich_with_apple
enrich_with_apple(
    "data/processed/nyt_transformed_<date>.csv",
    "data/processed/nyt_transformed_<date>_apple.csv",
)
PY
  ```

## FastAPI Service
Start locally:
```bash
uvicorn src.fastapi_app.main:app --reload --port 8000
```
Routes:
- `GET /health` – service check
- `POST /pipeline/run` – fetch, transform, enrich, upload raw/CSV to GCS
- `GET /files` – list blobs in `GCS_BUCKET`

Ensure `.env` is loaded and credentials are available (see above).

## Streamlit Dashboard
```bash
streamlit run streamlit_app/app.py
```
The UI reads the most recent `nyt_transformed_*_apple.csv` in `data/processed/`. Run the pipeline first or drop a file matching that pattern into the directory.

## Docker & Compose
Build and run both services:
```bash
docker-compose up --build
```
Notes:
- FastAPI already loads `.env` via `env_file`. If Streamlit needs the same env vars, add `env_file: .env` to that service.
- To share data/keys between containers and your host, mount volumes in `docker-compose.yml`, e.g.:
  ```yaml
  volumes:
    - ./data:/app/data
    - ./keys:/app/keys:ro
  ```
  Add those under both services so Streamlit can see the CSVs the pipeline writes.

## Tests
```bash
pytest
```
Some tests call live APIs (NYT, Apple) and require network access plus a valid `NYT_API_KEY`.

## Troubleshooting
- Empty dashboard: ensure a processed file exists in `data/processed/` with the expected naming pattern.
- GCS upload errors: confirm `GCS_BUCKET` is set and `GOOGLE_APPLICATION_CREDENTIALS` points to a readable service account key with Storage access.
