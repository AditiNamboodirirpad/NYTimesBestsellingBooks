from src.pipelines.enrich_with_apple import enrich_with_apple

INPUT = "data/processed/nyt_transformed_2026-01-04.csv"
OUTPUT = "data/processed/nyt_enriched_apple.csv"

df = enrich_with_apple(INPUT, OUTPUT)

print(df[
    ["title", "apple_price", "apple_rating", "apple_ratings_count"]
].head())
