"""Star-cubing baseline that preserves recursive pseudocode flow
while avoiding hashmap-based aggregation.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Tuple, Union

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
        self.first_child: Optional["StarTreeNode"] = None
        self.sibling: Optional["StarTreeNode"] = None

    @property
    def is_leaf(self) -> bool:
        return self.first_child is None


class CubeTreeNode:
    """Node in cube-tree that references one conditional Star-tree."""

    def __init__(self) -> None:
        self.star_tree: Optional["StarTree"] = None


class StarTree:
    """Star-tree wrapper used by recursive starcubing."""

    def __init__(self) -> None:
        self.root = StarTreeNode(value="root", depth=-1)
        self.cube_tree_children: List[CubeTreeNode] = []


class SupportEntry:
    """Linked-list entry for one dimension value support."""

    def __init__(
        self,
        value: int,
        total_sales: float,
        next_entry: Optional["SupportEntry"] = None,
    ) -> None:
        self.value = int(value)
        self.total_sales = float(total_sales)
        self.next = next_entry


class AggregateEntry:
    """Linked-list entry for one cuboid aggregate."""

    def __init__(
        self,
        key: CubeKey,
        total_sales: float,
        count_txn: int,
        next_entry: Optional["AggregateEntry"] = None,
    ) -> None:
        self.key = key
        self.total_sales = float(total_sales)
        self.count_txn = int(count_txn)
        self.next = next_entry


class AggregateList:
    """Linked-list container for cuboid aggregates."""

    def __init__(self) -> None:
        self.head: Optional[AggregateEntry] = None

    def add_or_update(self, key: CubeKey, sales: float, count_txn: int) -> None:
        current = self.head
        while current is not None:
            if current.key == key:
                current.total_sales += float(sales)
                current.count_txn += int(count_txn)
                return
            current = current.next
        self.head = AggregateEntry(
            key=key,
            total_sales=float(sales),
            count_txn=int(count_txn),
            next_entry=self.head,
        )

    def to_rows(self, dimension_names: Sequence[str], min_sup: float) -> List[dict]:
        rows: List[dict] = []
        current = self.head
        while current is not None:
            if current.total_sales >= min_sup:
                row = {}
                index = 0
                while index < len(dimension_names):
                    row[dimension_names[index]] = current.key[index]
                    index += 1
                row["total_sales"] = float(current.total_sales)
                row["count_txn"] = int(current.count_txn)
                rows.append(row)
            current = current.next
        rows.sort(key=lambda r: tuple(str(r[name]) for name in dimension_names))
        return rows


def _validate_rows(rows: Sequence[FactRow], dim_count: int) -> None:
    for row in rows:
        if len(row.dimensions) != dim_count:
            raise ValueError("row dimension length does not match dimension_names")


def _support_add(
    support_heads: List[Optional[SupportEntry]],
    dim_index: int,
    value: int,
    sales: float,
) -> None:
    head = support_heads[dim_index]
    current = head
    while current is not None:
        if current.value == value:
            current.total_sales += float(sales)
            return
        current = current.next
    support_heads[dim_index] = SupportEntry(
        value=value,
        total_sales=float(sales),
        next_entry=head,
    )


def _support_get(
    support_heads: Sequence[Optional[SupportEntry]],
    dim_index: int,
    value: int,
) -> float:
    current = support_heads[dim_index]
    while current is not None:
        if current.value == value:
            return float(current.total_sales)
        current = current.next
    return 0.0


def _build_global_support(
    rows: Sequence[FactRow],
    dim_count: int,
) -> List[Optional[SupportEntry]]:
    support_heads: List[Optional[SupportEntry]] = [None] * dim_count
    for row in rows:
        for dim_index, value in enumerate(row.dimensions):
            _support_add(
                support_heads=support_heads,
                dim_index=dim_index,
                value=int(value),
                sales=float(row.sales),
            )
    return support_heads


def _compress_path_by_support(
    path: Tuple[int, ...],
    global_support: Sequence[Optional[SupportEntry]],
    min_sup: float,
) -> CubeKey:
    reduced: List[DimensionValue] = []
    for dim_index, value in enumerate(path):
        if _support_get(global_support, dim_index, int(value)) < min_sup:
            reduced.append("ALL")
        else:
            reduced.append(int(value))
    return tuple(reduced)


def _expand_rollups(path: CubeKey) -> List[CubeKey]:
    concrete_positions = []
    for idx, value in enumerate(path):
        if value != "ALL":
            concrete_positions.append(idx)

    output: List[CubeKey] = []
    mask = 0
    end = 1 << len(concrete_positions)
    while mask < end:
        rolled = list(path)
        bit_index = 0
        while bit_index < len(concrete_positions):
            position = concrete_positions[bit_index]
            if (mask & (1 << bit_index)) == 0:
                rolled[position] = "ALL"
            bit_index += 1
        output.append(tuple(rolled))
        mask += 1
    return output


def _find_or_create_child(
    parent: StarTreeNode,
    value: DimensionValue,
    depth: int,
) -> StarTreeNode:
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
    aggregate_list: AggregateList,
) -> None:
    base_path = _path_from_node(node, dim_count)
    for cuboid in _expand_rollups(base_path):
        aggregate_list.add_or_update(
            key=cuboid,
            sales=float(node.total_sales),
            count_txn=int(node.count_txn),
        )


def insert_or_aggregate(cnode: StarTreeNode, target_star_tree: StarTree) -> None:
    """Line 2 helper from the pseudocode.

    Conditional trees are not materialized in this baseline scaffold.
    """

    _ = cnode
    _ = target_star_tree


def starcubing(
    T: StarTree,
    cnode: StarTreeNode,
    min_sup: float,
    dim_count: int,
    aggregate_list: AggregateList,
) -> None:
    """Recursive traversal that follows the original pseudocode flow."""

    for C in T.cube_tree_children:
        if C.star_tree is not None:
            insert_or_aggregate(cnode, C.star_tree)

    C_C: Optional[CubeTreeNode] = None

    if cnode.total_sales >= min_sup:
        if cnode.is_leaf and cnode.depth >= 0:
            _append_leaf_rollups(cnode, dim_count, aggregate_list)
        else:
            if cnode.depth >= 0:
                C_C = CubeTreeNode()
                T.cube_tree_children.append(C_C)
                T_C = StarTree()
                C_C.star_tree = T_C
                T_C.root.total_sales = cnode.total_sales
                T_C.root.count_txn = cnode.count_txn

    if not cnode.is_leaf and cnode.first_child is not None:
        starcubing(T, cnode.first_child, min_sup, dim_count, aggregate_list)

    if (
        C_C is not None
        and C_C.star_tree is not None
        and C_C.star_tree.root.first_child is not None
    ):
        starcubing(C_C.star_tree, C_C.star_tree.root, min_sup, dim_count, aggregate_list)
        if C_C in T.cube_tree_children:
            T.cube_tree_children.remove(C_C)
    elif C_C is not None and C_C in T.cube_tree_children:
        T.cube_tree_children.remove(C_C)

    if cnode.sibling is not None:
        starcubing(T, cnode.sibling, min_sup, dim_count, aggregate_list)


def compute_star_cubing_cube(
    rows: Iterable[FactRow],
    dimension_names: Sequence[str],
    min_sup: float,
) -> List[dict]:
    """Compute iceberg cuboids with tree/list based baseline only."""

    materialized_rows = rows if isinstance(rows, list) else list(rows)
    if not materialized_rows:
        return []

    dim_count = len(dimension_names)
    _validate_rows(materialized_rows, dim_count)

    global_support = _build_global_support(materialized_rows, dim_count)

    T = StarTree()
    for row in materialized_rows:
        key = tuple(int(value) for value in row.dimensions)
        reduced_key = _compress_path_by_support(key, global_support, min_sup)
        _insert_compressed_transaction(
            tree=T,
            transaction=reduced_key,
            sales=float(row.sales),
            count_txn=int(row.count_txn),
        )

    aggregate_list = AggregateList()
    starcubing(T, T.root, float(min_sup), dim_count, aggregate_list)

    return aggregate_list.to_rows(dimension_names=dimension_names, min_sup=float(min_sup))


__all__ = ["compute_star_cubing_cube"]