"""Pure Star-cubing iceberg cube computation for integer-encoded rows."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Tuple, Union

from .buc import FactRow

DimensionValue = Union[int, str]


def compute_star_cubing_cube(
    rows: Iterable[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> List[Dict[str, Union[int, str, float]]]:
    """Compute iceberg cube with a pure Star-cubing style workflow.

    Steps:
    1) Compute global support (total sales) for each value in each dimension.
    2) Apply star-reduction: values with support < ``min_sup`` become ``'ALL'``.
    3) For each transformed row, enumerate all roll-up cuboids and aggregate.
    4) Keep only cuboids with ``total_sales >= min_sup``.
    """

    materialized_rows = list(rows)
    if not materialized_rows:
        return []

    dim_count = len(dimension_names)
    for row in materialized_rows:
        if len(row.dimensions) != dim_count:
            raise ValueError(
                "row dimension length must match the configured dimension count"
            )

    global_sales_by_dim: List[Dict[int, float]] = [defaultdict(float) for _ in range(dim_count)]
    for row in materialized_rows:
        for dim_index, value in enumerate(row.dimensions):
            global_sales_by_dim[dim_index][value] += float(row.sales)

    aggregated: Dict[Tuple[DimensionValue, ...], List[float]] = defaultdict(
        lambda: [0.0, 0.0]
    )

    for row in materialized_rows:
        compressed_values: List[DimensionValue] = []
        concrete_positions: List[int] = []

        for dim_index, value in enumerate(row.dimensions):
            if global_sales_by_dim[dim_index][value] < min_sup:
                compressed_values.append("ALL")
            else:
                compressed_values.append(value)
                concrete_positions.append(dim_index)

        for mask in range(1 << len(concrete_positions)):
            key_values = list(compressed_values)
            for bit_index, position in enumerate(concrete_positions):
                if mask & (1 << bit_index):
                    continue
                key_values[position] = "ALL"

            key = tuple(key_values)
            aggregated[key][0] += float(row.sales)
            aggregated[key][1] += float(row.count_txn)

    result: List[Dict[str, Union[int, str, float]]] = []
    for key, (total_sales, total_count) in aggregated.items():
        if total_sales < min_sup:
            continue
        record: Dict[str, Union[int, str, float]] = {
            dim_name: key[idx] for idx, dim_name in enumerate(dimension_names)
        }
        record["total_sales"] = float(total_sales)
        record["count_txn"] = int(total_count)
        result.append(record)

    result.sort(key=lambda row: tuple(str(row[dim]) for dim in dimension_names))
    return result


__all__ = ["compute_star_cubing_cube"]
