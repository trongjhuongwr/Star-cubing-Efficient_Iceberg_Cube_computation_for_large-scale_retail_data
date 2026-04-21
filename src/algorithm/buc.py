"""BUC iceberg cube computation for integer-encoded retail transactions."""

from __future__ import annotations

from collections import defaultdict
import gc
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple, Union

DimensionValue = Union[int, str]


@dataclass(frozen=True)
class FactRow:
    """One transaction row used by cube algorithms."""

    dimensions: Tuple[int, ...]
    sales: float
    count_txn: int


def compute_buc_cube(
    rows: Iterable[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> List[Dict[str, Union[int, str, float]]]:
    """Compute iceberg cube using the BUC strategy.

    The implementation recursively partitions the data by dimensions. At each
    level it explores value partitions plus one roll-up branch ("ALL").
    Branches with total sales below ``min_sup`` are pruned.
    """

    materialized_rows = rows if isinstance(rows, list) else list(rows)
    if not materialized_rows:
        return []

    dim_count = len(dimension_names)
    aggregated: Dict[Tuple[DimensionValue, ...], Tuple[float, int]] = {}

    def recurse(partition: List[FactRow], dim_index: int, prefix: Tuple[DimensionValue, ...]) -> None:
        total_sales = 0.0
        total_count = 0
        for row in partition:
            total_sales += float(row.sales)
            total_count += int(row.count_txn)

        if total_sales < min_sup:
            return

        if dim_index == dim_count:
            aggregated[prefix] = (total_sales, total_count)
            return

        # Roll-up branch for this dimension.
        recurse(partition, dim_index + 1, prefix + ("ALL",))

        groups: Dict[int, List[FactRow]] = defaultdict(list)
        for row in partition:
            groups[row.dimensions[dim_index]].append(row)

        values = sorted(groups)
        for value in values:
            subgroup = groups.pop(value)
            recurse(subgroup, dim_index + 1, prefix + (value,))
            del subgroup

        del values
        del groups

    recurse(materialized_rows, 0, tuple())

    result: List[Dict[str, Union[int, str, float]]] = []
    for key, (total_sales, total_count) in aggregated.items():
        record: Dict[str, Union[int, str, float]] = {
            dim_name: key[idx] for idx, dim_name in enumerate(dimension_names)
        }
        record["total_sales"] = float(total_sales)
        record["count_txn"] = int(total_count)
        result.append(record)

    result.sort(key=lambda row: tuple(str(row[dim]) for dim in dimension_names))
    del aggregated
    gc.collect()
    return result


__all__ = ["FactRow", "compute_buc_cube"]