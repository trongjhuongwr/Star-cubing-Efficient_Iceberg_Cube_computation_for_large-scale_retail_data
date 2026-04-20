import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Ensure imports work when the script is run from repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def rescale_storage_chart(
    log_csv: Path,
    chart_dir: Path,
    scale_mode: str = "min_max",
    scale_factor: float = 1.0,
) -> None:

    # Read log
    df = pd.read_csv(log_csv)

    # Group by algorithm and get mean storage
    storage_by_algo = (
        df.groupby("algorithm", as_index=False)["output_storage_kb"]
        .mean()
        .sort_values("output_storage_kb")
    )

    original_values = storage_by_algo["output_storage_kb"].values
    algorithms = storage_by_algo["algorithm"].values

    # Apply scaling based on mode
    if scale_mode == "min_max":
        # Zoom: set y-axis to min-5% to max+5% instead of 0 to max
        min_val = original_values.min()
        max_val = original_values.max()
        margin = (max_val - min_val) * 0.05
        y_min = max(0, min_val - margin)
        y_max = max_val + margin
        scaled_values = original_values
        y_lim = (y_min, y_max)
        title_suffix = ""

    elif scale_mode == "factor":
        # Scale: multiply values by factor (only affects visual difference, not numbers)
        scaled_values = original_values * scale_factor
        y_lim = None
        title_suffix = f"(Scaled {scale_factor}x)"

    else:
        raise ValueError(f"Unknown scale_mode: {scale_mode}")

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(algorithms, scaled_values, width=0.6, color="steelblue")

    # Add value labels on top of bars (original values for reference)
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

    plot_title = "Cube Output Storage" if not title_suffix else f"Cube Output Storage - {title_suffix}"
    ax.set_title(plot_title)
    ax.set_xlabel("Algorithm")
    ax.set_ylabel("Serialized output size (KB)")
    ax.grid(axis="y", alpha=0.25)
    if y_lim:
        ax.set_ylim(y_lim)

    plt.tight_layout()
    chart_dir.mkdir(parents=True, exist_ok=True)
    output_file = chart_dir / "storage_rescaled.png"
    plt.savefig(output_file, dpi=160)
    plt.close()

    print(f"Chart saved: {output_file}")
    print(f"Original values: {dict(zip(algorithms, original_values))}")


def main() -> None:
    """CLI entry-point for re-scaling and re-plotting."""

    import argparse

    parser = argparse.ArgumentParser(
        description="Rescale and re-plot storage comparison chart"
    )
    parser.add_argument(
        "--log-csv",
        default=str(REPO_ROOT / "docs" / "benchmark" / "logs" / "performance_log.csv"),
        help="Path to performance_log.csv",
    )
    parser.add_argument(
        "--chart-dir",
        default=str(REPO_ROOT / "docs" / "benchmark" / "charts"),
        help="Output directory for rescaled chart",
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

    rescale_storage_chart(
        log_csv=log_csv,
        chart_dir=chart_dir,
        scale_mode=args.scale_mode,
        scale_factor=args.scale_factor,
    )


if __name__ == "__main__":
    main()
