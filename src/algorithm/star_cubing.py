"""Star-cubing baseline that preserves the recursive pseudocode structure."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

from .buc import FactRow

DimensionValue = Union[int, str]
CubeKey = Tuple[DimensionValue, ...]


class StarTreeNode:
    """Node in Star-tree, linked by first-child/next-sibling pointers."""

    def __init__(
        self,
        value: DimensionValue = "root",
        depth: int = -1,
        total_sales: float = 0.0,
        count_txn: int = 0,
        parent: Optional["StarTreeNode"] = None,
    ) -> None:
        self.value = value
        self.depth = depth
        self.total_sales = float(total_sales)
        self.count_txn = int(count_txn)
        self.parent = parent
        self.first_child: Optional[StarTreeNode] = None
        self.sibling: Optional[StarTreeNode] = None

    @property
    def is_leaf(self) -> bool:
        return self.first_child is None


class CubeTreeNode:
    """Node in cube-tree that references one conditional Star-tree."""

    def __init__(self) -> None:
        self.star_tree: Optional[StarTree] = None


class StarTree:
    """Star-tree wrapper used by recursive starcubing."""

    def __init__(self) -> None:
        self.root = StarTreeNode(value="root", depth=-1)
        self.cube_tree_children: List[CubeTreeNode] = []


def _validate_rows(rows: Sequence[FactRow], dim_count: int) -> None:
    for row in rows:
        if len(row.dimensions) != dim_count:
            raise ValueError("row dimension length does not match dimension_names")


def _build_global_support(
    rows: Sequence[FactRow],
    dim_count: int,
) -> List[Dict[int, float]]:
    support: List[Dict[int, float]] = [{} for _ in range(dim_count)]
    for row in rows:
        for index, value in enumerate(row.dimensions):
            bucket = support[index]
            bucket[value] = bucket.get(value, 0.0) + float(row.sales)
    return support


def _compress_path_by_support(
    path: Tuple[int, ...],
    global_support: Sequence[Dict[int, float]],
    min_sup: float,
) -> CubeKey:
    reduced: List[DimensionValue] = []
    for index, value in enumerate(path):
        if global_support[index].get(value, 0.0) < min_sup:
            reduced.append("ALL")
        else:
            reduced.append(int(value))
    return tuple(reduced)


def _expand_rollups(path: CubeKey) -> List[CubeKey]:
    concrete_positions = [index for index, value in enumerate(path) if value != "ALL"]
    output: List[CubeKey] = []
    for mask in range(1 << len(concrete_positions)):
        rolled = list(path)
        for bit_index, position in enumerate(concrete_positions):
            if mask & (1 << bit_index):
                continue
            rolled[position] = "ALL"
        output.append(tuple(rolled))
    return output


def _find_or_create_child(parent: StarTreeNode, value: DimensionValue, depth: int) -> StarTreeNode:
    current = parent.first_child
    previous: Optional[StarTreeNode] = None
    while current is not None:
        if current.value == value:
            return current
        previous = current
        current = current.sibling

    new_child = StarTreeNode(value=value, depth=depth, parent=parent)
    if previous is None:
        parent.first_child = new_child
    else:
        previous.sibling = new_child
    return new_child


def _insert_compressed_transaction(
    tree: StarTree,
    transaction: CubeKey,
    sales: float,
    count_txn: int,
) -> None:
    current = tree.root
    current.total_sales += float(sales)
    current.count_txn += int(count_txn)

    for depth, value in enumerate(transaction):
        child = _find_or_create_child(current, value, depth)
        child.total_sales += float(sales)
        child.count_txn += int(count_txn)
        current = child


def _path_from_node(node: StarTreeNode, dim_count: int) -> CubeKey:
    values: List[DimensionValue] = ["ALL"] * dim_count
    current: Optional[StarTreeNode] = node
    while current is not None and current.depth >= 0:
        values[current.depth] = current.value
        current = current.parent
    return tuple(values)


def _append_leaf_rollups(
    node: StarTreeNode,
    dim_count: int,
    aggregate: Dict[CubeKey, Tuple[float, int]],
) -> None:
    base_path = _path_from_node(node, dim_count)
    for cuboid in _expand_rollups(base_path):
        sales_sum, count_sum = aggregate.get(cuboid, (0.0, 0))
        aggregate[cuboid] = (
            sales_sum + float(node.total_sales),
            count_sum + int(node.count_txn),
        )


def insert_or_aggregate(cnode: StarTreeNode, target_star_tree: StarTree) -> None:
    """Line 2 helper from the pseudocode.

    In this baseline, conditional trees are not materialized; the recursive
    scaffold is preserved while this helper remains a no-op.
    """

    _ = cnode
    _ = target_star_tree


def starcubing(
    T: StarTree,
    cnode: StarTreeNode,
    min_sup: float,
    dim_count: int,
    aggregate: Dict[CubeKey, Tuple[float, int]],
) -> None:
    """Recursive traversal that follows the original pseudocode flow."""

    # 1. for each non-null child C of T's cube-tree
    for C in T.cube_tree_children:
        # 2. insert or aggregate cnode into C's star-tree
        if C.star_tree is not None:
            insert_or_aggregate(cnode, C.star_tree)

    C_C: Optional[CubeTreeNode] = None

    # 3. if (cnode.count >= min_sup)
    if cnode.total_sales >= min_sup:
        # 4-5. if (cnode != root) output cnode.count
        # Node-level output is represented through leaf rollups in this baseline.

        # 6. if (cnode is a leaf)
        if cnode.is_leaf and cnode.depth >= 0:
            # 7. output cnode.count
            _append_leaf_rollups(cnode, dim_count, aggregate)
        # 8. else initiate a new cube-tree
        else:
            # 9-11. create C_C and initialize T_C.root.count
            if cnode.depth >= 0:
                C_C = CubeTreeNode()
                T.cube_tree_children.append(C_C)
                T_C = StarTree()
                C_C.star_tree = T_C
                T_C.root.total_sales = cnode.total_sales
                T_C.root.count_txn = cnode.count_txn

    # 14-15. recurse on first child
    if not cnode.is_leaf and cnode.first_child is not None:
        starcubing(T, cnode.first_child, min_sup, dim_count, aggregate)

    # 16-18. recurse on conditional tree then remove it
    if (
        C_C is not None
        and C_C.star_tree is not None
        and C_C.star_tree.root.first_child is not None
    ):
        starcubing(C_C.star_tree, C_C.star_tree.root, min_sup, dim_count, aggregate)
        if C_C in T.cube_tree_children:
            T.cube_tree_children.remove(C_C)
    elif C_C is not None and C_C in T.cube_tree_children:
        T.cube_tree_children.remove(C_C)

    # 19-20. recurse on sibling
    if cnode.sibling is not None:
        starcubing(T, cnode.sibling, min_sup, dim_count, aggregate)


def compute_star_cubing_cube(
    rows: Iterable[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> List[Dict[str, Union[int, str, float]]]:
    """Compute iceberg cuboids while preserving pseudocode architecture."""

    materialized_rows = rows if isinstance(rows, list) else list(rows)
    if not materialized_rows:
        return []

    dim_count = len(dimension_names)
    _validate_rows(materialized_rows, dim_count)

    # scan R twice: (1) aggregate leaves and (2) build global support.
    leaf_aggregates: Dict[Tuple[int, ...], Tuple[float, int]] = {}
    for row in materialized_rows:
        key = tuple(int(value) for value in row.dimensions)
        sales, count = leaf_aggregates.get(key, (0.0, 0))
        leaf_aggregates[key] = (sales + float(row.sales), count + int(row.count_txn))

    global_support = _build_global_support(materialized_rows, dim_count)

    # create star-tree T from compressed leaves.
    T = StarTree()
    for key, (sales, count) in leaf_aggregates.items():
        reduced_key = _compress_path_by_support(key, global_support, min_sup)
        _insert_compressed_transaction(T, reduced_key, sales, count)

    # output count of T.root and call starcubing(T, T.root)
    aggregate: Dict[CubeKey, Tuple[float, int]] = {}
    starcubing(T, T.root, float(min_sup), dim_count, aggregate)

    results: List[Dict[str, Union[int, str, float]]] = []
    for cuboid_key, (sales, count) in aggregate.items():
        if sales < min_sup:
            continue
        row: Dict[str, Union[int, str, float]] = {
            dim_name: cuboid_key[idx] for idx, dim_name in enumerate(dimension_names)
        }
        row["total_sales"] = float(sales)
        row["count_txn"] = int(count)
        results.append(row)

    results.sort(key=lambda row: tuple(str(row[dim]) for dim in dimension_names))
    return results


__all__ = ["compute_star_cubing_cube"]
