"""Streamlit app to visualize NYT bestselling books from the latest CSV."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
FILENAME_PREFIX = "nyt_transformed_"
FILENAME_SUFFIX = "_apple"
FILENAME_PATTERN = f"{FILENAME_PREFIX}*{FILENAME_SUFFIX}.csv"


def extract_date(path: Path) -> datetime:
    """Parse date from filename; fall back to file modified time.

    Parameters
    ----------
    path : pathlib.Path
        File path to parse.

    Returns
    -------
    datetime.datetime
        Parsed datetime extracted from the filename or the file's modified time.
    """
    stem = path.stem
    date_str = stem.replace(FILENAME_PREFIX, "").replace(FILENAME_SUFFIX, "")
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return datetime.fromtimestamp(path.stat().st_mtime)


def get_latest_file() -> tuple[Path | None, str | None]:
    """Resolve the most recent processed CSV based on filename date.

    Returns
    -------
    tuple[pathlib.Path | None, str | None]
        Latest file path and filename, or ``(None, None)`` if no files are found.
    """
    files = list(DATA_DIR.glob(FILENAME_PATTERN))
    if not files:
        return None, None
    latest = sorted(files, key=extract_date, reverse=True)[0]
    return latest, latest.name


@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    """Load the processed CSV into a DataFrame.

    Parameters
    ----------
    path : pathlib.Path
        CSV path to read.

    Returns
    -------
    pandas.DataFrame
        DataFrame of the CSV contents with rank coerced to numeric when present.
    """
    df = pd.read_csv(path)
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    return df


def main() -> None:
    """Render the Streamlit app for NYT fiction bestsellers."""
    st.set_page_config(page_title="NYT Fiction Bestsellers – Weekly Snapshot", page_icon="📚", layout="wide")
    st.title("NYT Fiction Bestsellers – Weekly Snapshot")

    latest_path, filename = get_latest_file()
    if not latest_path or not filename:
        st.error("No processed NYT CSV files found in data/processed/. Add files named nyt_transformed_<date>_apple.csv.")
        st.stop()

    st.caption(f"Loaded file: {filename}")

    df = load_data(latest_path)
    if df.empty:
        st.warning("The CSV is empty.")
        st.stop()

    rank_min = int(df["rank"].min()) if "rank" in df.columns else 1
    rank_max = int(df["rank"].max()) if "rank" in df.columns else 100

    with st.sidebar:
        st.header("Filters")
        max_rank = st.slider("Maximum rank", min_value=rank_min, max_value=rank_max, value=rank_max)
        authors = sorted(df["author"].dropna().unique().tolist()) if "author" in df.columns else []
        author_options = ["All"] + authors
        selected_author = st.selectbox("Author", options=author_options, index=0)

    filtered = df.copy()
    if "rank" in filtered.columns:
        filtered = filtered[filtered["rank"] <= max_rank]
    if selected_author != "All" and "author" in filtered.columns:
        filtered = filtered[filtered["author"] == selected_author]

    st.subheader("Books")
    display_cols = [col for col in filtered.columns if col not in {"apple_store_link"}]
    st.dataframe(
        filtered.reset_index(drop=True)[display_cols],
        column_config={
            "description": st.column_config.TextColumn(label="Description", width="large"),
            "amazon_url": st.column_config.LinkColumn(label="Amazon", display_text="Buy"),
        },
        use_container_width=True,
    )

    if filtered.empty:
        st.info("No books match the current filters.")
        return

    first = filtered.iloc[0]
    st.subheader("Top on list")
    st.markdown(f"**Title:** {first.get('title', 'N/A')}")
    st.markdown(f"**Author:** {first.get('author', 'N/A')}")
    st.markdown(f"**Rank:** {first.get('rank', 'N/A')}")
    st.markdown(f"**Publisher:** {first.get('publisher', 'N/A')}")
    st.markdown(f"**Description:** {first.get('description', 'N/A')}")

    amazon_url = first.get("amazon_url")
    if isinstance(amazon_url, str) and amazon_url.strip():
        st.markdown(f"[Amazon link]({amazon_url})", unsafe_allow_html=True)
    else:
        st.markdown("Amazon link: N/A")


if __name__ == "__main__":
    main()
