from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set, Union, cast

Predicate = List[Union[str, int, "Predicate"]]


class PlanNode:
    @staticmethod
    def from_json(data: Dict[str, Any]) -> "PlanNode":
        if not isinstance(data, dict):
            raise TypeError("Plan JSON must be an object")
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


def pushdown_selections(root: PlanNode) -> PlanNode:
    def subtree_relations(node: PlanNode) -> Set[str]:
        if isinstance(node, Scan):
            return {node.relation}
        if isinstance(node, (Select, Project)):
            return subtree_relations(node.child)
        if isinstance(node, Join):
            return subtree_relations(node.left) | subtree_relations(node.right)
        return set()

    def predicate_attrs(predicate: Any) -> Set[str]:
        attrs: Set[str] = set()
        if isinstance(predicate, list):
            for item in predicate:
                attrs |= predicate_attrs(item)
        elif isinstance(predicate, str) and "." in predicate:
            attrs.add(predicate)
        return attrs

    def predicate_relations(predicate: Any) -> Set[str]:
        return {attr.split(".", 1)[0] for attr in predicate_attrs(predicate)}

    def split_conjuncts(predicate: Predicate) -> List[Predicate]:
        if (
            isinstance(predicate, list)
            and len(predicate) == 3
            and isinstance(predicate[0], str)
            and predicate[0].lower() == "and"
            and isinstance(predicate[1], list)
            and isinstance(predicate[2], list)
        ):
            left = split_conjuncts(cast(Predicate, predicate[1]))
            right = split_conjuncts(cast(Predicate, predicate[2]))
            return left + right

        if not isinstance(predicate, list) or not predicate:
            return [predicate]

        has_and = any(
            isinstance(item, str) and item.upper() == "AND" for item in predicate
        )
        if not has_and:
            return [predicate]

        parts: List[Predicate] = []
        current: List[Union[str, int, "Predicate"]] = []
        for item in predicate:
            if isinstance(item, str) and item.upper() == "AND":
                if current:
                    parts.append(cast(Predicate, current))
                    current = []
            else:
                current.append(item)
        if current:
            parts.append(cast(Predicate, current))
        return parts or [predicate]

    def combine_conjuncts(predicates: List[Predicate]) -> Predicate:
        if not predicates:
            raise ValueError("Cannot combine empty predicate list")
        combined: List[Union[str, int, "Predicate"]] = []
        for index, predicate in enumerate(predicates):
            if index > 0:
                combined.append("AND")
            if isinstance(predicate, list):
                combined.extend(predicate)
            else:
                combined.append(predicate)
        return cast(Predicate, combined)

    def rewrite(node: PlanNode) -> PlanNode:
        if isinstance(node, Select):
            child = rewrite(node.child)
            if isinstance(child, Select):
                merged = combine_conjuncts(
                    split_conjuncts(node.predicate) + split_conjuncts(child.predicate)
                )
                return rewrite(Select(predicate=merged, child=child.child))
            if isinstance(child, Project):
                predicate_attr_set = predicate_attrs(node.predicate)
                if predicate_attr_set.issubset(set(child.attrs)):
                    pushed = Select(predicate=node.predicate, child=child.child)
                    return Project(attrs=child.attrs, child=rewrite(pushed))
                return Select(predicate=node.predicate, child=child)

            if isinstance(child, Join):
                left_relations = subtree_relations(child.left)
                right_relations = subtree_relations(child.right)
                left_preds: List[Predicate] = []

                right_preds: List[Predicate] = []

                other_preds: List[Predicate] = []

                for predicate in split_conjuncts(node.predicate):
                    rels = predicate_relations(predicate)
                    if rels and rels.issubset(left_relations):
                        left_preds.append(predicate)
                    elif rels and rels.issubset(right_relations):
                        right_preds.append(predicate)
                    else:
                        other_preds.append(predicate)
                new_left = child.left
                new_right = child.right
                if left_preds:
                    new_left = rewrite(
                        Select(predicate=combine_conjuncts(left_preds), child=new_left)
                    )
                if right_preds:
                    new_right = rewrite(
                        Select(
                            predicate=combine_conjuncts(right_preds), child=new_right
                        )
                    )
                joined = Join(condition=child.condition, left=new_left, right=new_right)
                if other_preds:
                    return Select(
                        predicate=combine_conjuncts(other_preds), child=joined
                    )
                return joined
            return Select(predicate=node.predicate, child=child)
        if isinstance(node, Project):
            return Project(attrs=node.attrs, child=rewrite(node.child))
        if isinstance(node, Join):
            return Join(
                condition=node.condition,
                left=rewrite(node.left),
                right=rewrite(node.right),
            )
        return node

    return rewrite(root)


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
    predicate: Predicate
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
    condition: Predicate
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


def predicate_belongs_to(predicate: Predicate, to: PlanNode):
    pass
