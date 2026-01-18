# Analytics

This folder contains exploratory and modeling notebooks for the NYT Fiction bestseller data.
The work here focuses on understanding weekly list behavior, author and publisher patterns,
and experimenting with recommendation logic based on historical performance.

## Notebooks

1. `01_exploratory_analysis.ipynb`
   - Data overview, missing values, distribution checks, and early trend signals.

2. `02_rank_dynamics.ipynb`
   - Rank movement analysis, stability vs. volatility, and week-over-week shifts.

3. `03_author_publisher_analysis.ipynb`
   - Author frequency, publisher concentration, and comparative performance metrics.

4. `04_recommendation_engine.ipynb`
   - Feature engineering, scoring heuristics, and early recommendation experiments.

## Data dependencies

These notebooks typically read from the processed datasets in `data/processed/`,
including weekly history tables and recommendation outputs.
Run the pipeline first if the expected CSVs are missing or stale.

## Usage

Open notebooks in order (01 -> 04) for the most coherent narrative,
or jump directly to a topic-specific notebook if you want a focused view.
