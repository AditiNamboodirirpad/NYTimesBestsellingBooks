"""Logging utilities for consistent formatting across scripts and services."""

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging with a timestamped format."""
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
