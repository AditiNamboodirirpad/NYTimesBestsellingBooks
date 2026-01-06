"""Orchestrate the NYT fetch → transform → enrich pipeline."""

from pathlib import Path

from src.pipelines.enrich_with_apple import enrich_with_apple
from src.pipelines.fetch_nyt_weekly import fetch_and_save
from src.pipelines.transform_nyt import transform_latest


def run_pipeline() -> None:
    """Run the end-to-end pipeline and log output paths."""
    raw_file = fetch_and_save()
    transformed_file = transform_latest()

    transformed_path = Path(transformed_file)
    enriched_file = str(
        transformed_path.with_name(
            transformed_path.stem + "_apple" + transformed_path.suffix
        )
    )

    final_df = enrich_with_apple(transformed_file, enriched_file)

    print("\nPipeline complete!")
    print(f"Raw JSON:        {raw_file}")
    print(f"Transformed CSV: {transformed_file}")
    print(f"Enriched CSV:    {enriched_file}")
    print(f"Rows:            {len(final_df)}")


if __name__ == "__main__":
    run_pipeline()
