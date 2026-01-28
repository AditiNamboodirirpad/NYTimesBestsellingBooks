"""Streamlit app to refresh and visualize the latest Apple-enriched NYT CSV."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os

import pandas as pd
import streamlit as st

from src.services.gcs_service import list_files, download_file
import tempfile

import requests

FILENAME_PREFIX = "nyt_transformed_"
FILENAME_SUFFIX = "_apple"
FILENAME_PATTERN = f"{FILENAME_PREFIX}*{FILENAME_SUFFIX}.csv"

FASTAPI_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000/pipeline/run")
RUN_PIPELINE_ON_LOAD = os.getenv("RUN_PIPELINE_ON_LOAD", "true").lower() in {"1", "true", "yes"}

@st.cache_data
def get_recommendations_from_gcs() -> pd.DataFrame:
    files = list_files(prefix="processed/recommendations/")
    if not files:
        raise FileNotFoundError("No recommendations found in GCS")

    rec_file = files[0]  # only one expected
    tmp_path = Path(tempfile.gettempdir()) / Path(rec_file).name
    download_file(rec_file, str(tmp_path))
    return pd.read_csv(tmp_path)

@st.cache_data
def get_history_from_gcs() -> pd.DataFrame:
    files = list_files(prefix="processed/history/")
    if not files:
        raise FileNotFoundError("No history file found in GCS")

    history_file = files[0]
    tmp_path = Path(tempfile.gettempdir()) / Path(history_file).name
    download_file(history_file, str(tmp_path))
    return pd.read_csv(tmp_path, parse_dates=["published_date", "bestsellers_date"])


def extract_date(path: Path) -> datetime:
    """Parse the week date from a filename; fall back to file modified time.

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
    """Download the newest Apple-enriched CSV from GCS into a temp path."""
    files = list_files(prefix="processed/weekly/")

    apple_files = [
        f for f in files
        if f.startswith("processed/weekly/nyt_transformed_") and f.endswith("_apple.csv")
    ]

    if not apple_files:
        return None, None

    def extract_date_from_name(name: str):
        stem = Path(name).stem
        date_str = (
            stem.replace("nyt_transformed_", "")
                .replace("_apple", "")
        )
        try:
            return datetime.fromisoformat(date_str)
        except ValueError:
            return datetime.min

    latest = sorted(apple_files, key=extract_date_from_name, reverse=True)[0]

    tmp_dir = Path(tempfile.gettempdir())
    local_path = tmp_dir / Path(latest).name

    download_file(latest, str(local_path))

    return local_path, Path(latest).name



@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    """Load a processed CSV into a DataFrame, coercing rank to numeric when present.

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
    """Trigger the pipeline then render the Streamlit view for NYT fiction bestsellers."""
    st.set_page_config(page_title="NYT Fiction Bestsellers – Weekly Snapshot", page_icon="📚", layout="wide")
    st.title("NYT Fiction Bestsellers – Weekly Snapshot")

    page = st.sidebar.radio(
    "Navigation",
    ["Weekly Snapshot", "Recommendations","AI Insights"]
)
    if page == "Weekly Snapshot":

        st.subheader("Refreshing data…")

        if RUN_PIPELINE_ON_LOAD:
            with st.spinner("Running pipeline… please wait"):
                try:
                    resp = requests.post(FASTAPI_URL, timeout=120)
                    resp.raise_for_status()
                    st.success("Pipeline refreshed successfully! 🎉")

                except Exception as e:
                    st.error(f"Pipeline failed: {e}")
        else:
            st.info("Pipeline trigger is disabled for this environment.")


        latest_path, filename = get_latest_file()
        if not latest_path or not filename:
            st.error("No processed NYT CSV files found in GCS (processed/weekly/). Add files named nyt_transformed_<date>_apple.csv.")
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
    elif page == "Recommendations":
        st.title("Book Recommendations")

        rec_df = get_recommendations_from_gcs()

        if rec_df.empty:
            st.warning("No recommendations available yet.")
            st.stop()

        tab_fast, tab_consistent, tab_momentum = st.tabs([
            "Fast Movers",
            "Consistent Performers",
            "Sustained Momentum"
        ])

        with tab_fast:
            df_fast = rec_df[rec_df["category"] == "Fast Movers"]
            for _, row in df_fast.iterrows():
                st.markdown(f"### {row['title']}")
                st.markdown(f"**Author:** {row.get('author', 'N/A')}")
                st.markdown(f"**Publisher:** {row.get('publisher', 'N/A')}")
                st.markdown(row["reason"])
                st.divider()

        with tab_consistent:
            df_consistent = rec_df[rec_df["category"] == "Consistent Performers"]
            for _, row in df_consistent.iterrows():
                st.markdown(f"### {row['title']}")
                st.markdown(f"**Author:** {row.get('author', 'N/A')}")
                st.markdown(f"**Publisher:** {row.get('publisher', 'N/A')}")
                st.markdown(row["reason"])
                st.divider()

        with tab_momentum:
            df_momentum = rec_df[rec_df["category"] == "Sustained Momentum"]
            for _, row in df_momentum.iterrows():
                st.markdown(f"### {row['title']}")
                st.markdown(f"**Author:** {row.get('author', 'N/A')}")
                st.markdown(f"**Publisher:** {row.get('publisher', 'N/A')}")
                st.markdown(row["reason"])
                st.divider()
    elif page == "AI Insights":
        st.title("AI Insights (LLM-powered)")

        st.markdown(
            """
            LLM responses are generated from structured historical signals, not raw text.
            """
        )

        try:
            df = get_history_from_gcs()
        except FileNotFoundError as e:
            st.error(str(e))
            st.stop()

        user_question = st.text_input(
            "Ask a question about the latest NYT Fiction bestsellers:",
            placeholder="Type your question here…"
        )

        if user_question:
            with st.spinner("Analyzing data and generating answer…"):
                try:
                    from src.llm.router import classify_query
                    from src.llm.weekly_summary import (
                        prepare_weekly_context,
                        prepare_trend_context,
                        prepare_author_context,
                        prepare_publisher_context,
                    )
                    from src.llm.claude_client import ClaudeClient

                    intent = classify_query(user_question)

                    if intent == "trend":
                        context = prepare_trend_context(df)
                    elif intent == "author":
                        context = prepare_author_context(df)
                    elif intent == "publisher":
                        context = prepare_publisher_context(df)
                    else:
                        context = prepare_weekly_context(df)


                    client = ClaudeClient()
                    answer = client.answer_question(
                        question=user_question,
                        context=context,
                    )

                    st.subheader("Answer")
                    st.write(answer)

                except Exception as e:
                    st.error(f"Failed to answer question: {e}")

        st.markdown("---")
        st.markdown(
            """
            Some of the questions I tried:

            **Weekly summary**
            - "Summarize this week."
            - "Give me a weekly editorial summary."
            - "What stands out this week?"

            **Trend (rank changes)**
            - "What is the trend this week?"
            - "Which books are moving up or down?"
            - "Show the biggest risers and fallers."

            **Publishers**
            - "Which publishers dominate this week?"
            - "Top publishers by number of titles."
            - "Average rank by publisher this week."
            - "Publisher trends?"

            **Authors**
            - "Which authors have multiple titles this week?"
            - "Who are the most consistent authors?"
            - "Which authors have the best average rank?"
            - "Author trends?"

            **General**
            - "What stands out on the list right now?"
            - "What are the biggest surprises this week?"
            - "Which titles are noteworthy this week?"
            - "Give me quick highlights from the latest list."
            - "Any notable shifts in the top 5?"
            - "What should I pay attention to this week?"
            """
        )



if __name__ == "__main__":
    main()
