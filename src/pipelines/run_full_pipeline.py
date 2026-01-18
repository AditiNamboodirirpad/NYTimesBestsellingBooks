"""Orchestrate the NYT fetch → transform → enrich pipeline."""

from pathlib import Path

from src.pipelines.enrich_with_apple import enrich_with_apple
from src.pipelines.fetch_nyt_weekly import fetch_and_save
from src.pipelines.transform_nyt import transform_latest
from src.pipelines.transform_history import transform_history
from src.pipelines.build_recommendations import build_recommendations



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

    # 4. Append weekly data into historical table
    history_file = transform_history()

    # 5. Rebuild recommendations from updated history
    recommendations_file = build_recommendations()

    print("\nPipeline complete!")
    print(f"Raw JSON:        {raw_file}")
    print(f"Transformed CSV: {transformed_file}")
    print(f"Enriched CSV:    {enriched_file}")
    print(f"History CSV:       {history_file}")
    print(f"Recommendations:   {recommendations_file}")
    print(f"Weekly rows:       {len(final_df)}")


if __name__ == "__main__":
    run_pipeline()
