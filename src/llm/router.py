"""
LLM query router.

Maps user questions to structured context + prompt styles.
"""

from __future__ import annotations


def classify_query(query: str) -> str:
    q = query.lower()

    has_trend = any(k in q for k in ["trend", "moving", "rising", "falling", "momentum"])
    has_author = any(k in q for k in ["author", "writer", "who wrote", "authors", "novelist"])
    has_publisher = any(k in q for k in ["publisher", "publishing", "imprint", "house", "label"])

    if has_author:
        return "author"

    if has_publisher:
        return "publisher"

    if has_trend:
        return "trend"

    if any(k in q for k in ["summary", "week", "editorial"]):
        return "weekly_summary"

    return "general"
