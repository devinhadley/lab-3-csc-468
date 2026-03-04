from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Union

Predicates = List[Union[str, int]]


class PlanNode:
    @staticmethod
    def from_json(data: Dict[str, Any]) -> "PlanNode":
        op = data.get("op")
        if op == "Scan":
            return Scan.from_json(data)
        if op == "Select":
            return Select.from_json(data)
        if op == "Project":
            return Project.from_json(data)
        if op == "Join":
            return Join.from_json(data)
        raise ValueError(f"Unsupported op: {op}")


def parse_plan_file(path: Union[str, Path]) -> PlanNode:
    plan_path = Path(path)
    with plan_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return PlanNode.from_json(data)


def print_plan_bfs(root: PlanNode) -> None:
    def node_label(node: PlanNode) -> str:
        if isinstance(node, Scan):
            return f"Scan(relation={node.relation})"
        if isinstance(node, Select):
            return f"Select(predicate={node.predicate})"
        if isinstance(node, Project):
            return f"Project(attrs={node.attrs})"
        if isinstance(node, Join):
            return f"Join(condition={node.condition})"
        return node.__class__.__name__

    def children(node: PlanNode) -> List[PlanNode]:
        if isinstance(node, Select):
            return [node.child]
        if isinstance(node, Project):
            return [node.child]
        if isinstance(node, Join):
            return [node.left, node.right]
        return []

    def print_node(node: PlanNode, prefix: str, is_last: bool) -> None:
        connector = "`-- " if is_last else "|-- "
        print(prefix + connector + node_label(node))
        next_prefix = prefix + ("    " if is_last else "|   ")
        child_nodes = children(node)
        for index, child in enumerate(child_nodes):
            print_node(child, next_prefix, index == len(child_nodes) - 1)

    print(node_label(root))
    child_nodes = children(root)
    for index, child in enumerate(child_nodes):
        print_node(child, "", index == len(child_nodes) - 1)


@dataclass(frozen=True)
class Scan(PlanNode):
    relation: str

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Scan":
        relation = data.get("relation")
        if not isinstance(relation, str):
            raise ValueError("Scan requires relation name")
        return Scan(relation=relation)


@dataclass(frozen=True)
class Select(PlanNode):
    predicate: Predicates
    child: PlanNode

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Select":
        predicate = data.get("predicate")
        child = data.get("child")
        if not isinstance(predicate, list):
            raise ValueError("Select requires predicate list")
        if child is None:
            raise ValueError("Select requires child")
        return Select(predicate=predicate, child=PlanNode.from_json(child))


@dataclass(frozen=True)
class Project(PlanNode):
    attrs: List[str]
    child: PlanNode

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Project":
        attrs = data.get("attrs")
        child = data.get("child")
        if not isinstance(attrs, list) or not all(isinstance(a, str) for a in attrs):
            raise ValueError("Project requires attrs list of strings")
        if child is None:
            raise ValueError("Project requires child")
        return Project(attrs=attrs, child=PlanNode.from_json(child))


@dataclass(frozen=True)
class Join(PlanNode):
    condition: Predicates
    left: PlanNode
    right: PlanNode

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Join":
        condition = data.get("condition")
        left = data.get("left")
        right = data.get("right")
        if not isinstance(condition, list):
            raise ValueError("Join requires condition list")
        if left is None or right is None:
            raise ValueError("Join requires left and right children")
        return Join(
            condition=condition,
            left=PlanNode.from_json(left),
            right=PlanNode.from_json(right),
        )
