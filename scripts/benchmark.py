"""Benchmark runner: Star-cubing vs BUC vs Bottom-up on POS CSV data."""

from __future__ import annotations

import argparse
import csv
import io
import random
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence

import pandas as pd
import psutil

# Ensure imports work when the script is run from repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.algorithm import (  # noqa: E402
    FactRow,
    compute_bottom_up_cube,
    compute_buc_cube,
    compute_star_cubing_cube,
)
from src.algorithm.star_tree import StarTree  # noqa: E402
from src.ETL import clean_noise_data, etl_pipeline  # noqa: E402

try:
    import matplotlib.pyplot as plt
except Exception as exc:  # pragma: no cover - explicit runtime check
    raise RuntimeError(
        "matplotlib is required to render benchmark charts. "
        "Install dependencies with: pip install -r requirements.txt"
    ) from exc

DEFAULT_DATA_PATH = REPO_ROOT / "data" / "pos_data.csv"


def load_fact_rows_from_csv(
    file_path: Path,
    raw_limit: int | None = None,
) -> tuple[List[FactRow], List[str]]:
    """Load, clean, encode, and convert POS data into benchmark rows."""

    cleaned_df = clean_noise_data(str(file_path), max_rows=raw_limit)
    processed_array, _, dimensions, sale_values, quantity_values = etl_pipeline(
        cleaned_df
    )

    rows: List[FactRow] = []
    for encoded_row, sales_value, quantity_value in zip(
        processed_array, sale_values, quantity_values
    ):
        rows.append(
            FactRow(
                dimensions=tuple(int(value) for value in encoded_row),
                sales=float(sales_value),
                count_txn=int(quantity_value),
            )
        )

    return rows, list(dimensions)


def format_dataset_path(file_path: Path) -> str:
    """Return a stable relative path when possible, otherwise the raw path."""

    try:
        return str(file_path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(file_path)


def serialize_cube_size_bytes(
    cube_rows: Iterable[Dict[str, object]],
    dimension_names: Sequence[str],
) -> int:
    """Estimate serialized cube storage size as CSV bytes."""

    fieldnames = [*dimension_names, "total_sales", "count_txn"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in cube_rows:
        writer.writerow(row)
    return len(output.getvalue().encode("utf-8"))


def benchmark_algorithm(
    name: str,
    compute_fn: Callable[[Iterable[FactRow], Sequence[str], float], List[Dict[str, object]]],
    rows: List[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> Dict[str, float]:
    """Run one benchmark pass and return timing/memory metrics."""

    process = psutil.Process()
    rss_before_mb = process.memory_info().rss / (1024 * 1024)
    cpu_before = time.process_time()
    start = time.perf_counter()

    tracemalloc.start()
    cube_rows = compute_fn(rows, dimension_names, min_sup)
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    elapsed_sec = time.perf_counter() - start
    cpu_sec = time.process_time() - cpu_before
    rss_after_mb = process.memory_info().rss / (1024 * 1024)

    storage_bytes = serialize_cube_size_bytes(cube_rows, dimension_names)
    cpu_utilization_pct = 0.0 if elapsed_sec == 0 else (cpu_sec / elapsed_sec) * 100.0

    return {
        "algorithm": name,
        "elapsed_sec": round(elapsed_sec, 6),
        "cpu_sec": round(cpu_sec, 6),
        "cpu_utilization_pct": round(cpu_utilization_pct, 3),
        "rss_before_mb": round(rss_before_mb, 3),
        "rss_after_mb": round(rss_after_mb, 3),
        "rss_delta_mb": round(rss_after_mb - rss_before_mb, 3),
        "tracemalloc_peak_mb": round(peak_bytes / (1024 * 1024), 3),
        "cube_rows": len(cube_rows),
        "output_storage_kb": round(storage_bytes / 1024.0, 3),
    }


def compute_star_tree_cube(
    rows: Iterable[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> List[Dict[str, object]]:
    """Run StarTree aggregation directly for benchmark comparability."""

    tree = StarTree(dimension_names=dimension_names, min_sup=min_sup)
    for row in rows:
        tree.insert_transaction(
            transaction=list(row.dimensions),
            sales=float(row.sales),
            count=int(row.count_txn),
        )
    return tree.simultaneous_aggregation()


def compute_star_cubing_baseline_cube(
    rows: Iterable[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> List[Dict[str, object]]:
    """Run baseline Star-cubing before top-down and bottom-up enhancements."""

    return compute_star_cubing_cube(
        rows=rows,
        dimension_names=dimension_names,
        min_sup=min_sup,
    )


def resolve_algorithms(
    algorithm_set: str,
) -> Dict[str, Callable[[Iterable[FactRow], Sequence[str], float], List[Dict[str, object]]]]:
    """Resolve which algorithms to run based on CLI mode."""

    if algorithm_set == "star-only":
        return {
            "Star-cubing baseline": compute_star_cubing_baseline_cube,
            "Star-cubing enhanced": compute_star_tree_cube,
        }

    return {
        "Star-cubing baseline": compute_star_cubing_baseline_cube,
        "Star-cubing enhanced": compute_star_tree_cube,
        "BUC": compute_buc_cube,
        "Bottom-up": compute_bottom_up_cube,
    }


def build_charts(df: pd.DataFrame, chart_dir: Path) -> None:
    """Render runtime, memory, and storage comparison charts."""

    chart_dir.mkdir(parents=True, exist_ok=True)

    mean_runtime = (
        df.groupby(["algorithm", "dataset_rows"], as_index=False)["elapsed_sec"].mean()
    )
    plt.figure(figsize=(10, 6))
    for algo, frame in mean_runtime.groupby("algorithm"):
        frame = frame.sort_values("dataset_rows")
        plt.plot(frame["dataset_rows"], frame["elapsed_sec"], marker="o", label=algo)
    plt.title("Runtime Comparison by Dataset Size")
    plt.xlabel("Number of input rows")
    plt.ylabel("Elapsed time (seconds)")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(chart_dir / "runtime_line.png", dpi=160)
    plt.close()

    mean_memory = (
        df.groupby("algorithm", as_index=False)["tracemalloc_peak_mb"].mean()
        .sort_values("tracemalloc_peak_mb")
    )
    plt.figure(figsize=(9, 6))
    plt.bar(mean_memory["algorithm"], mean_memory["tracemalloc_peak_mb"], width=0.6)
    plt.title("Average Python Heap Peak (tracemalloc)")
    plt.xlabel("Algorithm")
    plt.ylabel("Peak memory (MB)")
    plt.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(chart_dir / "memory_bar.png", dpi=160)
    plt.close()

    mean_storage = (
        df.groupby(["algorithm", "dataset_rows"], as_index=False)["output_storage_kb"].mean()
    )
    plt.figure(figsize=(10, 6))
    for algo, frame in mean_storage.groupby("algorithm"):
        frame = frame.sort_values("dataset_rows")
        plt.plot(frame["dataset_rows"], frame["output_storage_kb"], marker="s", label=algo)
    plt.title("Cube Output Storage by Dataset Size")
    plt.xlabel("Number of input rows")
    plt.ylabel("Serialized output size (KB)")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(chart_dir / "storage_line.png", dpi=160)
    plt.close()


def parse_sizes(sizes_raw: str) -> List[int]:
    """Parse comma-separated sizes from CLI argument."""

    if sizes_raw.strip().lower() == "full":
        return []

    sizes = [int(chunk.strip()) for chunk in sizes_raw.split(",") if chunk.strip()]
    if not sizes:
        raise ValueError("At least one dataset size must be provided")
    return sizes


def main() -> None:
    """CLI entry-point for the benchmark workflow."""

    parser = argparse.ArgumentParser(description="Run benchmark suite on POS CSV data")
    parser.add_argument(
        "--sizes",
        default="2000,5000,10000",
        help="Comma-separated list of row counts, or 'full' for all cleaned rows",
    )
    parser.add_argument("--repeats", type=int, default=2, help="Runs per dataset size")
    parser.add_argument(
        "--min-sup",
        type=float,
        default=18_000_000.0,
        help="Iceberg threshold over Total_Sales",
    )
    parser.add_argument("--seed", type=int, default=20260418, help="Random seed")
    parser.add_argument(
        "--data-path",
        default=str(DEFAULT_DATA_PATH),
        help="Path to the raw POS CSV file",
    )
    parser.add_argument(
        "--raw-limit",
        type=int,
        default=15000,
        help="Maximum raw rows to read from the CSV before ETL",
    )
    parser.add_argument(
        "--algorithm-set",
        choices=["star-only", "full"],
        default="full",
        help="Algorithm set: star-only (baseline/enhanced) or full (+BUC/Bottom-up)",
    )
    args = parser.parse_args()

    sizes = parse_sizes(args.sizes)
    data_path = Path(args.data_path)
    if not data_path.is_file():
        raise FileNotFoundError(f"Data file not found: {data_path}")

    rows, dimension_names = load_fact_rows_from_csv(data_path, raw_limit=args.raw_limit)
    random.Random(args.seed).shuffle(rows)

    if not sizes:
        sizes = [len(rows)]

    max_size = max(sizes)
    if max_size > len(rows):
        raise ValueError(
            f"Requested size {max_size} exceeds available cleaned rows {len(rows)}"
        )

    base_dir = REPO_ROOT / "docs" / "benchmark"
    log_dir = base_dir / "logs"
    chart_dir = base_dir / "charts"
    log_dir.mkdir(parents=True, exist_ok=True)

    algorithms = resolve_algorithms(args.algorithm_set)

    records: List[Dict[str, object]] = []
    run_counter = 0

    for size in sizes:
        dataset_rows = rows[:size]
        for repeat_idx in range(args.repeats):
            for algorithm_name, compute_fn in algorithms.items():
                run_counter += 1
                metrics = benchmark_algorithm(
                    name=algorithm_name,
                    compute_fn=compute_fn,
                    rows=dataset_rows,
                    dimension_names=dimension_names,
                    min_sup=args.min_sup,
                )
                metrics.update(
                    {
                        "run_id": run_counter,
                        "repeat": repeat_idx + 1,
                        "dataset_rows": size,
                        "seed": args.seed,
                        "data_path": format_dataset_path(data_path),
                        "raw_limit": args.raw_limit,
                        "clean_rows": len(rows),
                        "min_sup": args.min_sup,
                    }
                )
                records.append(metrics)
                print(
                    f"[{run_counter:02d}] {algorithm_name:<10} rows={size:<6} "
                    f"elapsed={metrics['elapsed_sec']:.4f}s "
                    f"peak={metrics['tracemalloc_peak_mb']:.3f}MB"
                )

    df = pd.DataFrame(records)
    csv_path = log_dir / "performance_log.csv"
    json_path = log_dir / "performance_log.json"
    summary_path = log_dir / "summary_by_algorithm.csv"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    summary_df = (
        df.groupby("algorithm", as_index=False)
        .agg(
            elapsed_sec_mean=("elapsed_sec", "mean"),
            cpu_sec_mean=("cpu_sec", "mean"),
            tracemalloc_peak_mb_mean=("tracemalloc_peak_mb", "mean"),
            output_storage_kb_mean=("output_storage_kb", "mean"),
            cube_rows_mean=("cube_rows", "mean"),
        )
        .sort_values("elapsed_sec_mean")
    )
    summary_df.to_csv(summary_path, index=False)

    build_charts(df=df, chart_dir=chart_dir)

    print("\nBenchmark completed.")
    print(f"Log CSV: {csv_path}")
    print(f"Log JSON: {json_path}")
    print(f"Summary : {summary_path}")
    print(f"Charts  : {chart_dir}")


if __name__ == "__main__":
    main()
