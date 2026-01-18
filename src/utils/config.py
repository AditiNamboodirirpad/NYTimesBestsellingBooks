"""Environment loading and canonical data paths for the project."""

import os
from pathlib import Path
from dotenv import load_dotenv

# ---- ENV VARS ----
load_dotenv()

NYT_API_KEY = os.getenv("NYT_API_KEY")

if NYT_API_KEY is None:
    raise ValueError("NYT_API_KEY is missing. Add it to your .env file.")


# ---- DATA PATHS ----

# Base project directory (root folder of repo)
BASE_DIR = Path(__file__).resolve().parent.parent.parent

DATA_DIR = BASE_DIR / "data"

RAW_WEEKLY_DIR = DATA_DIR / "raw" / "weekly"
RAW_HIST_DIR = DATA_DIR / "raw" / "historical"

PROC_WEEKLY_DIR = DATA_DIR / "processed" / "weekly"
PROC_HIST_DIR = DATA_DIR / "processed" / "historical"
MASTER_DIR = DATA_DIR / "processed" / "master"

# Automatically create directories if missing
for d in [
    RAW_WEEKLY_DIR,
    RAW_HIST_DIR,
    PROC_WEEKLY_DIR,
    PROC_HIST_DIR,
    MASTER_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)
