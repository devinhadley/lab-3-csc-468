from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


class PhysicalPlanNode:
    def open(self, relations: dict, indexes: dict) -> None:
        raise NotImplementedError

    def next(self) -> dict | None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def _eval_predicate(record: dict, predicate: list) -> bool:
    conditions = []
    current = []
    for token in predicate:
        if token == "AND":
            if current:
                conditions.append(current)
                current = []
        else:
            current.append(token)
    if current:
        conditions.append(current)

    for cond in conditions:
        lhs, op, rhs = cond[0], cond[1], cond[2]
        lval = record[lhs] if isinstance(lhs, str) and "." in lhs else lhs
        rval = record[rhs] if isinstance(rhs, str) and "." in rhs else rhs
        if lval != rval:
            return False
    return True


@dataclass
class PhysicalSeqScan(PhysicalPlanNode):
    relation: str
    _schema: list = field(default_factory=list, repr=False, compare=False)
    _pages: list = field(default_factory=list, repr=False, compare=False)
    _page_idx: int = field(default=0, repr=False, compare=False)
    _rec_idx: int = field(default=0, repr=False, compare=False)
    _pages_read: int = field(default=0, repr=False, compare=False)

    def open(self, relations: dict, indexes: dict) -> None:
        rel = relations[self.relation]
        self._schema = rel["schema"]
        self._pages = rel["pages"]
        self._page_idx = 0
        self._rec_idx = 0
        self._pages_read = 0

    def next(self) -> dict | None:
        while self._page_idx < len(self._pages):
            page = self._pages[self._page_idx]
            if self._rec_idx < len(page):
                if self._rec_idx == 0:
                    self._pages_read += 1
                raw = page[self._rec_idx]
                self._rec_idx += 1
                return {
                    f"{self.relation}.{col}": raw[i]
                    for i, col in enumerate(self._schema)
                }
            self._page_idx += 1
            self._rec_idx = 0
        return None

    def close(self) -> None:
        self._schema = []
        self._pages = []
        self._page_idx = 0
        self._rec_idx = 0


@dataclass
class PhysicalHashScan(PhysicalPlanNode):
    relation: str
    attribute: str
    value: Any
    _schema: list = field(default_factory=list, repr=False, compare=False)
    _matching_records: list = field(default_factory=list, repr=False, compare=False)
    _pages_read: int = field(default=0, repr=False, compare=False)

    def open(self, relations: dict, indexes: dict) -> None:
        rel = relations[self.relation]
        self._schema = rel["schema"]
        pages = rel["pages"]

        attr = (
            self.attribute.split(".")[-1] if "." in self.attribute else self.attribute
        )
        idx = indexes.get((self.relation, attr))
        if idx is None:
            idx = indexes.get((self.relation, self.attribute))

        matching_pages = idx.lookup(self.value) if idx else list(range(len(pages)))
        self._pages_read = len(matching_pages)

        self._matching_records = []
        attr_pos = self._schema.index(attr)
        for p_idx in matching_pages:
            for raw in pages[p_idx]:
                if raw[attr_pos] == self.value:
                    self._matching_records.append(
                        {
                            f"{self.relation}.{col}": raw[i]
                            for i, col in enumerate(self._schema)
                        }
                    )

    def next(self) -> dict | None:
        if self._matching_records:
            return self._matching_records.pop(0)
        return None

    def close(self) -> None:
        self._schema = []
        self._matching_records = []


@dataclass
class PhysicalFilter(PhysicalPlanNode):
    child: PhysicalPlanNode
    predicate: list

    def open(self, relations: dict, indexes: dict) -> None:
        self.child.open(relations, indexes)

    def next(self) -> dict | None:
        while True:
            rec = self.child.next()
            if rec is None:
                return None
            if _eval_predicate(rec, self.predicate):
                return rec

    def close(self) -> None:
        self.child.close()


@dataclass
class PhysicalProject(PhysicalPlanNode):
    child: PhysicalPlanNode
    attrs: list

    def open(self, relations: dict, indexes: dict) -> None:
        self.child.open(relations, indexes)

    def next(self) -> dict | None:
        rec = self.child.next()
        if rec is None:
            return None
        return {k: v for k, v in rec.items() if k in self.attrs}

    def close(self) -> None:
        self.child.close()


@dataclass
class PhysicalHashJoin(PhysicalPlanNode):
    left: PhysicalPlanNode
    right: PhysicalPlanNode
    condition: list
    _hash_table: dict = field(default_factory=dict, repr=False, compare=False)
    _right_rec: dict | None = field(default=None, repr=False, compare=False)
    _probe_list: list = field(default_factory=list, repr=False, compare=False)

    def _join_key(self, record: dict) -> Any:
        for token in self.condition:
            if isinstance(token, str) and "." in token and token in record:
                return record[token]
        return None

    def open(self, relations: dict, indexes: dict) -> None:
        self.left.open(relations, indexes)
        self._hash_table = {}
        while True:
            rec = self.left.next()
            if rec is None:
                break
            key = None
            for token in self.condition:
                if isinstance(token, str) and "." in token and token in rec:
                    key = rec[token]
                    break
            if key is not None:
                self._hash_table.setdefault(key, []).append(rec)

        self.right.open(relations, indexes)
        self._right_rec = None
        self._probe_list = []

    def next(self) -> dict | None:
        while True:
            if self._probe_list:
                left_rec = self._probe_list.pop(0)
                merged = {**left_rec, **self._right_rec}
                return merged

            self._right_rec = self.right.next()
            if self._right_rec is None:
                return None

            key = None
            for token in self.condition:
                if isinstance(token, str) and "." in token and token in self._right_rec:
                    key = self._right_rec[token]
                    break

            self._probe_list = list(self._hash_table.get(key, []))

    def close(self) -> None:
        self.left.close()
        self.right.close()
        self._hash_table = {}
        self._right_rec = None
        self._probe_list = []


@dataclass
class PhysicalNestedLoopJoin(PhysicalPlanNode):
    outer: PhysicalPlanNode
    inner: PhysicalPlanNode
    condition: list
    _relations: dict = field(default_factory=dict, repr=False, compare=False)
    _indexes: dict = field(default_factory=dict, repr=False, compare=False)
    _cur_outer: dict | None = field(default=None, repr=False, compare=False)
    _inner_open: bool = field(default=False, repr=False, compare=False)

    def open(self, relations: dict, indexes: dict) -> None:
        self._relations = relations
        self._indexes = indexes
        self.outer.open(relations, indexes)
        self._cur_outer = None
        self._inner_open = False

    def next(self) -> dict | None:
        while True:
            if self._cur_outer is None:
                self._cur_outer = self.outer.next()
                if self._cur_outer is None:
                    return None
                if self._inner_open:
                    self.inner.close()
                self.inner.open(self._relations, self._indexes)
                self._inner_open = True

            inner_rec = self.inner.next()
            if inner_rec is None:
                self._cur_outer = None
                continue

            merged = {**self._cur_outer, **inner_rec}
            if _eval_predicate(merged, self.condition):
                return merged

    def close(self) -> None:
        self.outer.close()
        if self._inner_open:
            self.inner.close()
        self._cur_outer = None
        self._inner_open = False
        self._relations = {}
        self._indexes = {}


def count_pages_read(node: PhysicalPlanNode) -> int:
    total = 0
    if isinstance(node, (PhysicalSeqScan, PhysicalHashScan)):
        total += node._pages_read
    if hasattr(node, "child"):
        total += count_pages_read(node.child)
    if hasattr(node, "left"):
        total += count_pages_read(node.left)
    if hasattr(node, "right"):
        total += count_pages_read(node.right)
    if hasattr(node, "outer"):
        total += count_pages_read(node.outer)
    if hasattr(node, "inner"):
        total += count_pages_read(node.inner)
    return total
