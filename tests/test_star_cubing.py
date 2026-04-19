"""Pytest suite for pure star-cubing implementation."""

from __future__ import annotations

import pytest

from src.algorithm import FactRow, compute_star_cubing_cube


def test_star_cubing_empty_input_returns_empty() -> None:
    """Empty input should return no cuboid rows."""

    rows: list[FactRow] = []
    result = compute_star_cubing_cube(rows, ("A", "B"), min_sup=1.0)
    assert result == []


def test_star_cubing_rollup_for_single_row() -> None:
    """One row with 2 dimensions should produce all 4 roll-up cuboids."""

    rows = [FactRow(dimensions=(1, 2), sales=10.0, count_txn=1)]
    result = compute_star_cubing_cube(rows, ("A", "B"), min_sup=0.0)

    keys = {(row["A"], row["B"]) for row in result}
    assert keys == {(1, 2), (1, "ALL"), ("ALL", 2), ("ALL", "ALL")}
    assert all(row["total_sales"] == pytest.approx(10.0) for row in result)
    assert all(row["count_txn"] == 1 for row in result)


def test_star_cubing_applies_star_reduction_by_global_support() -> None:
    """Low-support values should be mapped to ALL before cuboid expansion."""

    rows = [
        FactRow(dimensions=(1, 10), sales=9.0, count_txn=1),
        FactRow(dimensions=(1, 11), sales=9.0, count_txn=1),
    ]
    result = compute_star_cubing_cube(rows, ("A", "B"), min_sup=15.0)

    assert result == [
        {"A": 1, "B": "ALL", "total_sales": 18.0, "count_txn": 2},
        {"A": "ALL", "B": "ALL", "total_sales": 18.0, "count_txn": 2},
    ]


def test_star_cubing_dimension_length_mismatch_raises() -> None:
    """Rows with mismatched dimension length should raise ValueError."""

    rows = [FactRow(dimensions=(1,), sales=5.0, count_txn=1)]
    with pytest.raises(ValueError):
        compute_star_cubing_cube(rows, ("A", "B"), min_sup=1.0)
