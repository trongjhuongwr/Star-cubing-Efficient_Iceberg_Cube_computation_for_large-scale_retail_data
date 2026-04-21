# Star-cubing Iceberg Cube Miner

Project Python + SQL for computing Iceberg Cube on large-scale retail POS data, with benchmark artifacts for the benchmark phase.

## Scope

- Iceberg cube computation with Star-tree aggregation.
- Comparative benchmark across 4 algorithms: Star-cubing baseline, Star-cubing enhanced, BUC, and Bottom-up.
- Log and chart artifacts for runtime, CPU/RAM, and output storage.

## Repository Layout

- `src/algorithm/star_tree.py`: Star-tree data structure and simultaneous aggregation.
- `scripts/benchmark.py`: Benchmark runner and chart renderer.
- `docs/benchmark/logs/`: Raw benchmark logs (`.csv`, `.json`) and summary.
- `docs/benchmark/charts/`: Evidence charts (`runtime_line.png`, `memory_bar.png`, `storage_line.png`).
- `docs/benchmark/`: Benchmark documentation/report markdown files.

## Setup

1. Create and activate a Python environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run Benchmark

Recommended benchmark profile (trend analysis with multiple dataset sizes):

```bash
python scripts/benchmark.py --data-path data/pos_data.csv --algorithm-set full --sizes 2000,5000,10000 --repeats 1 --raw-limit 15000 --min-sup 18000000 --chunk-size 50000 --seed 20260418
```

Full-size stress profile (latest report baseline):

```bash
python scripts/benchmark.py --data-path data/pos_data.csv --algorithm-set full --sizes full --repeats 1 --raw-limit 5000000 --min-sup 18000000 --chunk-size 100000 --seed 20260418
```

The benchmark reads raw POS CSV rows, performs ETL cleaning and encoding, then runs all 4 algorithms on the same shuffled dataset to compare runtime, CPU/RAM, and output storage.

## Latest Full-Size Snapshot

Source: `docs/benchmark/logs/summary_by_algorithm.csv` after `--sizes full --raw-limit 5000000`

| Algorithm            | Elapsed Mean (s) | CPU Mean (s) | Peak tracemalloc (MB) | Output (KB) | Cube Rows |
| :------------------- | ---------------: | -----------: | --------------------: | ----------: | --------: |
| Star-cubing baseline |           81.263 |       79.625 |                50.851 |    1911.252 |     57202 |
| Star-cubing enhanced |          245.856 |      241.078 |                46.627 |    1912.990 |     57252 |
| BUC                  |         1381.075 |     1347.500 |               100.344 |    1925.487 |     57637 |
| Bottom-up            |         3015.181 |     2980.281 |                87.717 |    1925.487 |     57637 |

## Benchmark Outputs

After each successful run:

- `docs/benchmark/logs/performance_log.csv`: Detailed run-level metrics.
- `docs/benchmark/logs/performance_log.json`: JSON version for downstream tooling.
- `docs/benchmark/logs/summary_by_algorithm.csv`: Mean metrics by algorithm.
- `docs/benchmark/charts/runtime_line.png`: Runtime vs dataset size.
- `docs/benchmark/charts/memory_bar.png`: Mean memory peak comparison.
- `docs/benchmark/charts/storage_line.png`: Output storage trend.

## Documentation for Reporting

- `docs/benchmark/benchmark_algorithm_spec.md`: Task 15 technical specification and setup guide.
- `docs/benchmark/benchmark_report.md`: Benchmark evidence report for Word/slide integration.

## Task Assignment and Progress

The following table details the responsibilities and performance progresses for each team member:

| Member                 | Responsibilities                                                                                                                                                         | Progress  |
| :--------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- |
| **Trần Viết Gia Huy**  | - Define Data Contract<br>- Design SQL Schema, optimize Clustered Columnstore Index.<br>- Power BI Visualization                                                         | Excellent |
| **Nguyễn Minh Nhựt**   | - Implement StarNode compressed data structure<br>- Implement `min_sup` conditions and Star-compression logic                                                            | Excellent |
| **Nguyễn Trọng Hưởng** | - Collaborate on StarTree core algorithm development.<br>- Develop recursive functions for Simultaneous Aggregation.                                                     | Excellent |
| **Nguyễn Quốc Khánh**  | - Implement baseline algorithms (BUC and Bottom-up) for benchmarking.<br>- Write scripts for performance measurement (CPU, RAM) across various `min_sup` levels.         | Excellent |
| **Ngô Chánh Phong**    | - Develop scripts to generate simulated Retail POS data.<br>- Create large-scale raw datasets (2 - 5 million rows) ensuring data density and business logic correlation. | Excellent |
| **Dương Quang Đông**   | - Build In-memory ETL processing flows<br>- Automate Integer Encoding and column sorting based on Cardinality.                                                           | Excellent |
| **Nguyễn Đình Lương**  | - Develop DAX measures to prevent duplication.<br>- Collaborate on UI/UX design for the Iceberg Cube analysis dashboard.                                                 | Excellent |
| **Tô Xuân Đông**       | - Analyze benchmark logs, visualize performance and compression ratios using Matplotlib/Seaborn.<br>- Write technical documentation (README.md, docs).                   | Excellent |
