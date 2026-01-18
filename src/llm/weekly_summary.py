"""
Generate a weekly editorial summary of NYT Fiction bestsellers.

This module prepares structured weekly context data
to be consumed by an LLM (Claude) for natural language summarization.
"""

from __future__ import annotations

import pandas as pd
from datetime import datetime


def _normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if "published_date" not in df.columns:
        raise KeyError("Expected 'published_date' column in input data.")

    df = df.copy()
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

    if "rank_change" not in df.columns and "rank_last_week" in df.columns:
        df["rank_change"] = df["rank"] - df["rank_last_week"]
    elif "rank_change" not in df.columns:
        df = df.sort_values(["title", "published_date"])
        df["rank_change"] = df.groupby("title")["rank"].diff()

    return df


def prepare_weekly_context(df: pd.DataFrame) -> dict:
    """
    Prepare structured weekly summary context from NYT history data.
    """
    context = {}

    # -----------------------------
    # Normalize dates and compute rank_change from history
    # -----------------------------
    df = _normalize_history_df(df)
    
    # -----------------------------
    # 1. Identify latest week
    # -----------------------------
    latest_week = df["published_date"].max()
    if isinstance(latest_week, str):
        latest_week = pd.to_datetime(latest_week, errors="coerce")
    week_df = df[df["published_date"] == latest_week]

    context["week"] = (
        latest_week.strftime("%Y-%m-%d")
        if pd.notna(latest_week)
        else "unknown"
    )

    # -----------------------------
    # 2. Top 5 books this week
    # -----------------------------
    top_books = (
        week_df.sort_values("rank")
        .head(5)
        .loc[:, ["title", "author", "rank", "weeks_on_list"]]
        .to_dict(orient="records")
    )

    context["top_books"] = top_books

    # -----------------------------
    # 3. Rank movements (week-over-week)
    # -----------------------------
    movement_df = week_df[
        (week_df["rank_last_week"] != 0) &
        (week_df["rank_change"].notna())
    ].copy()

    # Biggest climbers
    climbers = (
        movement_df[movement_df["rank_change"] < 0]
        .sort_values("rank_change")
        .head(3)
    )

    # Biggest fallers
    fallers = (
        movement_df[movement_df["rank_change"] > 0]
        .sort_values("rank_change", ascending=False)
        .head(3)
    )

    movements = []

    for _, row in climbers.iterrows():
        movements.append({
            "title": row["title"],
            "direction": "up",
            "change": int(abs(row["rank_change"])),
            "from_rank": int(row["rank_last_week"]),
            "to_rank": int(row["rank"]),
        })

    for _, row in fallers.iterrows():
        movements.append({
            "title": row["title"],
            "direction": "down",
            "change": int(row["rank_change"]),
            "from_rank": int(row["rank_last_week"]),
            "to_rank": int(row["rank"]),
        })

    context["rank_movements"] = movements

    # -----------------------------
    # 4. Notable authors (multiple titles this week)
    # -----------------------------
    author_counts = (
        week_df.groupby("author")
        .agg(
            titles=("title", list),
            count=("title", "count")
        )
        .reset_index()
    )

    notable_authors = author_counts[author_counts["count"] >= 2]

    authors_output = []

    for _, row in notable_authors.iterrows():
        author_titles = week_df[week_df["author"] == row["author"]][
            ["title", "rank"]
        ].sort_values("rank")

        authors_output.append({
            "author": row["author"],
            "num_titles": int(row["count"]),
            "titles": [
                f"{t} (#{int(r)})"
                for t, r in zip(author_titles["title"], author_titles["rank"])
            ]
        })

    context["notable_authors"] = authors_output

    # -----------------------------
    # 5. Publisher trends
    # -----------------------------
    publisher_stats = (
        week_df.groupby("publisher")
        .agg(
            num_titles=("title", "count"),
            avg_rank=("rank", "mean")
        )
        .reset_index()
        .sort_values("num_titles", ascending=False)
        .head(3)
    )

    publishers_output = []

    for _, row in publisher_stats.iterrows():
        publishers_output.append({
            "publisher": row["publisher"],
            "num_titles": int(row["num_titles"]),
            "avg_rank": round(row["avg_rank"], 1)
        })

    context["publisher_trends"] = publishers_output


    return context

def prepare_trend_context(df: pd.DataFrame) -> dict:
    df = _normalize_history_df(df)
    latest_week = df["published_date"].max()
    week_df = df[df["published_date"] == latest_week]

    movers = week_df[
        (week_df["rank_last_week"] != 0) &
        (week_df["rank_change"].notna())
    ]

    top_risers = (
        movers[movers["rank_change"] < 0]
        .sort_values("rank_change")
        .head(5)[["title", "rank_last_week", "rank", "rank_change"]]
        .to_dict(orient="records")
    )

    top_fallers = (
        movers[movers["rank_change"] > 0]
        .sort_values("rank_change", ascending=False)
        .head(5)[["title", "rank_last_week", "rank", "rank_change"]]
        .to_dict(orient="records")
    )

    return {
        "week": latest_week.strftime("%Y-%m-%d"),
        "top_risers": top_risers,
        "top_fallers": top_fallers,
    }

def prepare_author_context(df: pd.DataFrame) -> dict:
    df = _normalize_history_df(df)
    author_stats = (
        df.groupby("author")
        .agg(
            total_titles=("title", "nunique"),
            avg_rank=("rank", "mean"),
            best_rank=("rank", "min"),
            weeks_on_list=("weeks_on_list", "max"),
        )
        .reset_index()
        .sort_values("total_titles", ascending=False)
        .head(10)
    )

    return {
        "top_authors": author_stats.to_dict(orient="records")
    }

def prepare_publisher_context(df: pd.DataFrame) -> dict:
    df = _normalize_history_df(df)
    publisher_stats = (
        df.groupby("publisher")
        .agg(
            num_titles=("title", "count"),
            avg_rank=("rank", "mean"),
        )
        .reset_index()
        .sort_values("num_titles", ascending=False)
        .head(10)
    )

    return {
        "top_publishers": publisher_stats.to_dict(orient="records")
    }
