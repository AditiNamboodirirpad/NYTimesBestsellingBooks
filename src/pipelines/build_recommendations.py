"""
Build book recommendations from historical NYT bestseller data.

This module reads the historical weekly bestseller table,
computes multiple recommendation categories, and writes
a single recommendations CSV for downstream consumption
(e.g., Streamlit).
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd


# =========================
# Paths
# =========================
HISTORY_FILE = Path("data/processed/history/nyt_history_weekly.csv")
OUTPUT_DIR = Path("data/processed/recommendations")
OUTPUT_FILE = OUTPUT_DIR / "recommendations.csv"


# =========================
# Core entry point
# =========================
def build_recommendations() -> str:
    """
    Build all recommendation categories and save final table.

    Returns
    -------
    str
        Path to saved recommendations CSV.
    """

    # --- Load history ---
    df = load_history()

    # --- Build categories (placeholders for now) ---
    rec_category_1 = build_category_1(df)
    rec_category_2 = build_category_2(df)
    rec_category_3 = build_category_3(df)

    # --- Combine all categories ---
    recommendations = combine_categories(
        rec_category_1,
        rec_category_2,
        rec_category_3,
    )

    # --- Save ---
    save_recommendations(recommendations)

    return str(OUTPUT_FILE)


# =========================
# Helpers
# =========================
def load_history() -> pd.DataFrame:
    """Load historical NYT bestseller data."""
    if not HISTORY_FILE.exists():
        raise FileNotFoundError(f"History file not found: {HISTORY_FILE}")

    df = pd.read_csv(
        HISTORY_FILE,
        parse_dates=["published_date", "bestsellers_date"]
    )
    return df


def save_recommendations(df: pd.DataFrame) -> None:
    """Persist recommendations to disk."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)


# =========================
# Category builders (STUBS)
# =========================
def build_category_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Category 1: Fast Movers
    Books whose rank improved compared to last week.
    """

    # --- Ensure rank_change exists ---
    if "rank_change" not in df.columns:
        df = df.sort_values(["title", "published_date"])
        df["rank_change"] = df["rank"] - df["rank_last_week"]

    # --- Latest row per title ---
    latest_per_title = (
        df.sort_values(["title", "published_date"])
          .groupby("title", as_index=False)
          .tail(1)
          .reset_index(drop=True)
    )

    # --- Total appearances per title ---
    title_counts = (
        df.groupby("title")
          .size()
          .rename("total_appearances")
    )

    latest_per_title = latest_per_title.merge(
        title_counts,
        on="title",
        how="left"
    )

    # --- Filter: trending up ---
    trending_up = latest_per_title[
        (latest_per_title["total_appearances"] >= 2) &
        (latest_per_title["rank_last_week"] != 0) &
        (latest_per_title["rank_change"].notna()) &
        (latest_per_title["rank_change"] < 0)
    ].copy()

    if trending_up.empty:
        return pd.DataFrame()

    # --- Sort by strongest improvement ---
    trending_up = trending_up.sort_values(
        by=["rank_change", "rank", "weeks_on_list"],
        ascending=[True, True, False]
    )

    # --- Build output ---
    trending_up["category"] = "Fast Movers"

    trending_up["reason"] = (
        "Rank improved by "
        + (-trending_up["rank_change"]).astype(int).astype(str)
        + " vs last week (from #"
        + trending_up["rank_last_week"].astype(int).astype(str)
        + " to #"
        + trending_up["rank"].astype(int).astype(str)
        + ")."
    )

    return trending_up[
        [
            "title",
            "author",
            "publisher",
            "published_date",
            "rank",
            "rank_last_week",
            "rank_change",
            "weeks_on_list",
            "total_appearances",
            "category",
            "reason",
        ]
    ].reset_index(drop=True)



def build_category_2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Category 2: Consistent Performers
    Books that stay on the list for many weeks with strong average ranks.
    """

    MIN_WEEKS = 5
    TOP_RANK_THRESHOLD = 10
    TOP_N = 10

    # --- Aggregate per title ---
    consistent_df = (
        df.groupby("title")
          .agg(
              total_appearances=("published_date", "count"),
              avg_rank=("rank", "mean"),
              median_rank=("rank", "median"),
              max_weeks_on_list=("weeks_on_list", "max"),
          )
          .reset_index()
    )

    # --- Filter for consistency ---
    consistent_df = consistent_df[
        (consistent_df["total_appearances"] >= MIN_WEEKS) &
        (consistent_df["avg_rank"] <= TOP_RANK_THRESHOLD)
    ]

    if consistent_df.empty:
        return pd.DataFrame()

    # --- Sort by quality + longevity ---
    consistent_df = consistent_df.sort_values(
        by=["median_rank", "total_appearances", "avg_rank"],
        ascending=[True, False, True]
    ).head(TOP_N)

    # --- Bring in metadata (author, publisher) ---
    meta = (
        df.sort_values("published_date")
          .groupby("title", as_index=False)
          .last()[["title", "author", "publisher"]]
    )

    consistent_df = consistent_df.merge(meta, on="title", how="left")

    # --- Add category + explanation ---
    consistent_df["category"] = "Consistent Performers"

    consistent_df["reason"] = (
        "Appeared on the bestseller list for "
        + consistent_df["total_appearances"].astype(str)
        + " weeks with a median rank of #"
        + consistent_df["median_rank"].astype(int).astype(str)
    )

    return consistent_df[
        [
            "title",
            "author",
            "publisher",
            "total_appearances",
            "avg_rank",
            "median_rank",
            "max_weeks_on_list",
            "category",
            "reason",
        ]
    ].reset_index(drop=True)



def build_category_3(df: pd.DataFrame) -> pd.DataFrame:
    """
    Category 3: Sustained Momentum
    Books showing consistent upward movement across multiple weeks.
    """

    MIN_APPEARANCES = 2
    TOP_N = 10

    # --- Sort for time-based diffs ---
    df_sorted = df.sort_values(
        ["title", "published_date"]
    ).copy()

    # --- Rank change over time ---
    df_sorted["rank_change"] = (
        df_sorted.groupby("title")["rank"].diff()
    )

    # --- Aggregate momentum per title ---
    momentum_df = (
        df_sorted
        .dropna(subset=["rank_change"])
        .groupby("title")
        .agg(
            appearances=("rank", "count"),
            avg_rank=("rank", "mean"),
            total_rank_change=("rank_change", "sum"),
            best_rank=("rank", "min"),
        )
        .reset_index()
    )

    # --- Filter for real momentum ---
    rising_books = momentum_df[
        (momentum_df["appearances"] >= MIN_APPEARANCES) &
        (momentum_df["total_rank_change"] < 0)
    ]

    if rising_books.empty:
        return pd.DataFrame()

    rising_books = rising_books.sort_values(
        by="total_rank_change"
    ).head(TOP_N)

    # --- Bring in metadata ---
    meta = (
        df.sort_values("published_date")
          .groupby("title", as_index=False)
          .last()[["title", "author", "publisher"]]
    )

    rising_books = rising_books.merge(meta, on="title", how="left")

    # --- Add category + explanation ---
    rising_books["category"] = "Sustained Momentum"

    rising_books["reason"] = (
        "Improved by "
        + (-rising_books["total_rank_change"]).astype(int).astype(str)
        + " positions overall across "
        + rising_books["appearances"].astype(str)
        + " weeks (best rank #"
        + rising_books["best_rank"].astype(int).astype(str)
        + ")."
    )

    return rising_books[
        [
            "title",
            "author",
            "publisher",
            "appearances",
            "avg_rank",
            "total_rank_change",
            "best_rank",
            "category",
            "reason",
        ]
    ].reset_index(drop=True)



def combine_categories(*dfs: pd.DataFrame) -> pd.DataFrame:
    """
    Combine all recommendation categories into one table.
    """
    non_empty = [df for df in dfs if not df.empty]

    if not non_empty:
        return pd.DataFrame()

    return pd.concat(non_empty, ignore_index=True)


# =========================
# CLI execution
# =========================
if __name__ == "__main__":
    output = build_recommendations()
    print(f"Recommendations written to: {output}")
