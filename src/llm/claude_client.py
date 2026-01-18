"""
Claude LLM client wrapper.

Responsible only for:
- Formatting prompts
- Calling Claude
- Returning text output

No data processing or business logic here.
"""

from __future__ import annotations

import os
from typing import Optional

from anthropic import Anthropic

from src.llm.prompts import WEEKLY_EDITORIAL_SUMMARY_PROMPT


class ClaudeClient:
    """Thin wrapper around Anthropic Claude API."""

    def __init__(
        self,
        model: str = "claude-3-haiku-20240307",
        max_tokens: int = 600,
        temperature: float = 0.3,
    ) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "ANTHROPIC_API_KEY not found in environment variables."
            )

        self.client = Anthropic(api_key=api_key)
        self.model = os.getenv("ANTHROPIC_MODEL", model)
        self.max_tokens = max_tokens
        self.temperature = temperature

    def generate_weekly_summary(self, context: dict) -> str:
        """
        Generate an editorial weekly summary using Claude.

        Parameters
        ----------
        context : dict
            Structured weekly context produced by prepare_weekly_context().

        Returns
        -------
        str
            Editorial summary text.
        """

        prompt = WEEKLY_EDITORIAL_SUMMARY_PROMPT.format(
            week=context.get("week"),
            top_books=context.get("top_books"),
            rank_movements=context.get("rank_movements"),
            notable_authors=context.get("notable_authors"),
            publisher_trends=context.get("publisher_trends"),
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
        )

        return response.content[0].text.strip()

    def answer_question(self, question: str, context: dict) -> str:
        from src.llm.prompts import LLM_QA_PROMPT

        prompt = LLM_QA_PROMPT.format(
            question=question,
            context=context,
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )

        return response.content[0].text.strip()
