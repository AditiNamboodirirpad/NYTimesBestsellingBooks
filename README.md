# NYT Bestselling Books – Data Pipeline, API, and Dashboard

End-to-end portfolio project that pulls the New York Times bestseller list, normalizes it to CSV, enriches it with Apple Books metadata, and exposes the results through a FastAPI service and a Streamlit dashboard. Artifacts can be persisted locally or pushed to Google Cloud Storage (GCS), with an optional Cloud Scheduler job to refresh weekly.

## Live Demo
- Streamlit Dashboard URL: https://nytimes-streamlit-761198761992.us-central1.run.app

## Highlights
- Automated pipeline: fetch → transform → enrich → history → recommendations
- API service to run the pipeline and list stored files
- Streamlit UI with filters, recommendations, and LLM insights
- Dockerized services and Cloud Run/Scheduler deployment flow

## Architecture (high level)
1. FastAPI triggers the pipeline on demand.
2. The pipeline writes raw + processed CSVs locally and (optionally) uploads to GCS.
3. Streamlit loads the latest processed outputs from GCS and renders the UI.
4. (Optional) Cloud Scheduler calls the API on a schedule.

```
NYT API → pipeline → GCS → Streamlit UI
               ↘︎ FastAPI (trigger + file listing)
```

## Demo
- Add screenshots/GIFs here (Streamlit dashboard + API response)

## Repo Layout (key parts)
```
src/
  api_clients/         # NYT + Apple Books API helpers
  pipelines/           # fetch_nyt_weekly, transform_nyt, enrich_with_apple, run_full_pipeline
  fastapi_app/         # FastAPI entrypoint and routes
  services/gcs_service.py  # Upload/list helpers for GCS
streamlit_app/app.py   # Streamlit UI
Analytics/             # Notebooks + EDA
Dockerfile.fastapi     # API image
Dockerfile.streamlit   # UI image
docker-compose.yml     # Compose services (FastAPI + Streamlit)
```

## Prerequisites
- Python 3.11+
- pip
- (Optional) Docker and Docker Compose
- (Optional) Google Cloud project + service account with Storage access

## Environment Variables
Create a `.env` in the repo root:
```
NYT_API_KEY=your-nyt-api-key
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=your-gcs-bucket
# Local/compose only: path inside container
GOOGLE_APPLICATION_CREDENTIALS=/app/keys/your-sa-key.json
# Streamlit behavior
FASTAPI_URL=http://fastapi:8000/pipeline/run
RUN_PIPELINE_ON_LOAD=true
```

Notes:
- `.env` is for local development and Docker Compose only.
- Cloud Run environment variables are set via `gcloud run deploy --set-env-vars`.
- Cloud Run uses service account-based authentication (no `GOOGLE_APPLICATION_CREDENTIALS` file).
- Set `RUN_PIPELINE_ON_LOAD=false` in production to avoid retriggers on every page load.

## Quickstart (local)
```bash
pip install -r requirements.txt
python -m src.pipelines.run_full_pipeline
streamlit run streamlit_app/app.py
```

## Pipeline Outputs
- Raw NYT JSON: `data/raw/weekly/nyt_<list>_<timestamp>.json`
- Processed weekly CSV: `data/processed/weekly/nyt_transformed_<date>.csv`
- Enriched weekly CSV: `data/processed/weekly/nyt_transformed_<date>_apple.csv`
- History table: `data/processed/history/nyt_history_weekly.csv`
- Recommendations: `data/processed/recommendations/recommendations.csv`

## FastAPI Service
Start locally:
```bash
uvicorn src.fastapi_app.main:app --reload --port 8000
```

Routes:
- `GET /health` – health check
- `POST /pipeline/run` – fetch, transform, enrich, upload artifacts
- `GET /files` – list all blobs in `GCS_BUCKET`
- `GET /files/processed` – list processed weekly CSVs
- `GET /files/raw` – list raw weekly JSONs

## Streamlit Dashboard
```bash
streamlit run streamlit_app/app.py
```

Behavior:
- Weekly Snapshot page triggers the pipeline unless `RUN_PIPELINE_ON_LOAD=false`.
- The UI loads the most recent `nyt_transformed_*_apple.csv` from GCS.

## Docker & Compose
Build and run both services:
```bash
docker-compose up --build
```

Notes:
- FastAPI and Streamlit both read `.env` via `env_file` in `docker-compose.yml`.
- To share data/keys between containers and your host, mount volumes:
  ```yaml
  volumes:
    - ./data:/app/data
    - ./keys:/app/keys:ro
  ```

## Docker Hub (publish images)
```bash
# Build
docker build -f Dockerfile.fastapi -t <dockerhub-username>/nyt-fastapi:latest .
docker build -f Dockerfile.streamlit -t <dockerhub-username>/nyt-streamlit:latest .

# Push
docker push <dockerhub-username>/nyt-fastapi:latest
docker push <dockerhub-username>/nyt-streamlit:latest
```

## Cloud Run (deploy)
Assumes `gcloud` is authenticated and a project is set.

```bash
# FastAPI service
gcloud run deploy nyt-fastapi \
  --image <dockerhub-username>/nyt-fastapi:latest \
  --region <region> \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET=<bucket>,NYT_API_KEY=<nyt-key> \
  --service-account <service-account-email>

# Streamlit service
gcloud run deploy nyt-streamlit \
  --image <dockerhub-username>/nyt-streamlit:latest \
  --region <region> \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET=<bucket>,FASTAPI_URL=<fastapi-url>/pipeline/run,RUN_PIPELINE_ON_LOAD=false \
  --service-account <service-account-email>
```

Notes:
- Cloud Run provides the `PORT` env var; Dockerfiles already respect it.
- Use a Cloud Run service account with GCS permissions (Storage Object Admin or scoped custom role).

## Cloud Scheduler (trigger pipeline)
Create a scheduled weekly POST to the FastAPI endpoint (Sunday at 9:00 AM PT):
```bash
gcloud scheduler jobs create http nyt-pipeline-weekly \
  --schedule="0 9 * * 0" \
  --http-method=POST \
  --uri=<fastapi-url>/pipeline/run \
  --oidc-service-account-email=<service-account-email> \
  --oidc-token-audience=<fastapi-url>
```
Runs every Sunday at 09:00 PT (adjust as needed).

## Tests
```bash
pytest
```

Some tests hit live APIs (NYT, Apple) and require a valid `NYT_API_KEY` and network access.

## Troubleshooting
- Empty dashboard: ensure `processed/weekly/nyt_transformed_*_apple.csv` exists in GCS.
- GCS upload errors: confirm `GCS_BUCKET` and service account permissions.
- Cloud Run 403s: check IAM and whether the service is public or requires OIDC.

## Roadmap (optional)
- Add caching and incremental updates to reduce API calls
- Add a public demo URL + CI/CD
- Add monitoring (Cloud Logging, error alerts)

## License
Add a license if you plan to make this public.
