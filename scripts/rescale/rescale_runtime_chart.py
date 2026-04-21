import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def rescale_runtime_chart(
    log_csv: Path,
    chart_dir: Path,
    scale_mode: str = "min_max",
    scale_factor: float = 1.0,
) -> None:
    df = pd.read_csv(log_csv)

    runtime_by_algo = (
        df.groupby("algorithm", as_index=False)["elapsed_sec"]
        .mean()
        .sort_values("elapsed_sec")
    )

    original_values = runtime_by_algo["elapsed_sec"].values
    algorithms = runtime_by_algo["algorithm"].values

    if scale_mode == "min_max":
        min_val = original_values.min()
        max_val = original_values.max()
        margin = (max_val - min_val) * 0.05 if max_val > min_val else max_val * 0.05
        y_min = max(0, min_val - margin)
        y_max = max_val + margin
        scaled_values = original_values
        y_lim = (y_min, y_max)
        title_suffix = ""

    elif scale_mode == "factor":
        scaled_values = original_values * scale_factor
        y_lim = None
        title_suffix = f"(Scaled {scale_factor}x)"

    else:
        raise ValueError(f"Unknown scale_mode: {scale_mode}")

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(algorithms, scaled_values, width=0.6, color="steelblue")

    for bar, orig_val in zip(bars, original_values):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{orig_val:.2f}",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    plot_title = "Runtime Comparison" if not title_suffix else f"Runtime Comparison - {title_suffix}"
    ax.set_title(plot_title)
    ax.set_xlabel("Algorithm")
    ax.set_ylabel("Elapsed time (seconds)")
    ax.grid(axis="y", alpha=0.25)

    if y_lim:
        ax.set_ylim(y_lim)

    plt.tight_layout()
    chart_dir.mkdir(parents=True, exist_ok=True)
    output_file = chart_dir / "runtime_rescaled.png"
    plt.savefig(output_file, dpi=160)
    plt.close()

    print(f"Chart saved: {output_file}")
    print(f"Original runtime values: {dict(zip(algorithms, original_values))}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Rescale and re-plot runtime comparison chart"
    )
    parser.add_argument(
        "--log-csv",
        default=str(REPO_ROOT / "docs" / "benchmark" / "logs" / "performance_log.csv"),
        help="Path to performance_log.csv",
    )
    parser.add_argument(
        "--chart-dir",
        default=str(REPO_ROOT / "docs" / "benchmark" / "charts"),
        help="Output directory for runtime rescaled chart",
    )
    parser.add_argument(
        "--scale-mode",
        choices=["min_max", "factor"],
        default="min_max",
        help="Scaling strategy: min_max (zoom to data) or factor (multiply values)",
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=1.0,
        help="For factor mode: multiply all values by this factor",
    )

    args = parser.parse_args()

    log_csv = Path(args.log_csv)
    chart_dir = Path(args.chart_dir)

    if not log_csv.is_file():
        raise FileNotFoundError(f"Log CSV not found: {log_csv}")

    rescale_runtime_chart(
        log_csv=log_csv,
        chart_dir=chart_dir,
        scale_mode=args.scale_mode,
        scale_factor=args.scale_factor,
    )


if __name__ == "__main__":
    main()