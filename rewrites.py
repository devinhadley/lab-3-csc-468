from typing import Any, List, Set, Union, cast

from query_plan_ast import Join, Predicates, PlanNode, Project, Scan, Select


def _predicate_attrs(predicate: Any) -> Set[str]:
    attrs: Set[str] = set()
    if isinstance(predicate, list):
        for item in predicate:
            attrs |= _predicate_attrs(item)
    elif isinstance(predicate, str) and "." in predicate:
        attrs.add(predicate)
    return attrs


def _subtree_relations(node: PlanNode) -> Set[str]:
    if isinstance(node, Scan):
        return {node.relation}
    if isinstance(node, (Select, Project)):
        return _subtree_relations(node.child)
    if isinstance(node, Join):
        return _subtree_relations(node.left) | _subtree_relations(node.right)
    return set()


def _attrs_for_relations(attrs: Set[str], relations: Set[str]) -> Set[str]:
    return {
        attr for attr in attrs if "." in attr and attr.split(".", 1)[0] in relations
    }


# Recursively rewrites the child and performs pushdown for current node.
# If a pushdown occurs for current node, we rewrite the modified child again.
# Includes helpers for splitting and combining conjuctions.
def pushdown_selections(root: PlanNode) -> PlanNode:
    def predicate_relations(predicate: Any) -> Set[str]:
        return {attr.split(".", 1)[0] for attr in _predicate_attrs(predicate)}

    def split_conjuncts(predicate: Predicates) -> List[Predicates]:
        if not isinstance(predicate, list) or not predicate:
            return [predicate]

        has_and = any(
            isinstance(item, str) and item.upper() == "AND" for item in predicate
        )
        if not has_and:
            return [predicate]

        parts: List[Predicates] = []
        current: List[Union[str, int]] = []
        for item in predicate:
            if isinstance(item, str) and item.upper() == "AND":
                if current:
                    parts.append(cast(Predicates, current))
                    current = []
            else:
                current.append(item)
        if current:
            parts.append(cast(Predicates, current))
        return parts or [predicate]

    def combine_conjuncts(predicates: List[Predicates]) -> Predicates:
        if not predicates:
            raise ValueError("Cannot combine empty predicate list")
        combined: List[Union[str, int, Predicates]] = []
        for index, predicate in enumerate(predicates):
            if index > 0:
                combined.append("AND")
            if isinstance(predicate, list):
                combined.extend(predicate)
            else:
                combined.append(predicate)
        return cast(Predicates, combined)

    def rewrite(node: PlanNode) -> PlanNode:
        if isinstance(node, Select):
            child = rewrite(node.child)
            if isinstance(child, Select):
                merged = combine_conjuncts(
                    split_conjuncts(node.predicate) + split_conjuncts(child.predicate)
                )
                return rewrite(Select(predicate=merged, child=child.child))
            if isinstance(child, Project):
                predicate_attr_set = _predicate_attrs(node.predicate)
                if predicate_attr_set.issubset(set(child.attrs)):
                    pushed = Select(predicate=node.predicate, child=child.child)
                    return Project(attrs=child.attrs, child=rewrite(pushed))
                return Select(predicate=node.predicate, child=child)

            if isinstance(child, Join):
                left_relations = _subtree_relations(child.left)
                right_relations = _subtree_relations(child.right)
                left_preds: List[Predicates] = []
                right_preds: List[Predicates] = []
                other_preds: List[Predicates] = []

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


# Traverse down each path in the tree collecting attributes we need as we go.
# Once we encounter leaf (scan) then add the projection over it.
def pushdown_projections(root: PlanNode) -> PlanNode:
    def rewrite(node: PlanNode, collected_attrs: Set[str]) -> PlanNode:
        if isinstance(node, Project):
            child_attrs = set(collected_attrs) | set(node.attrs)
            return Project(attrs=node.attrs, child=rewrite(node.child, child_attrs))

        if isinstance(node, Select):
            child_attrs = set(collected_attrs) | _predicate_attrs(node.predicate)
            return Select(
                predicate=node.predicate, child=rewrite(node.child, child_attrs)
            )

        if isinstance(node, Join):
            condition_attrs = _predicate_attrs(node.condition)
            left_relations = _subtree_relations(node.left)
            right_relations = _subtree_relations(node.right)

            left_attrs = _attrs_for_relations(collected_attrs, left_relations)
            left_attrs |= _attrs_for_relations(condition_attrs, left_relations)

            right_attrs = _attrs_for_relations(collected_attrs, right_relations)
            right_attrs |= _attrs_for_relations(condition_attrs, right_relations)

            return Join(
                condition=node.condition,
                left=rewrite(node.left, left_attrs),
                right=rewrite(node.right, right_attrs),
            )

        if isinstance(node, Scan):
            attrs = sorted(_attrs_for_relations(collected_attrs, {node.relation}))
            if not attrs:
                return node
            return Project(attrs=attrs, child=node)

        return node

    return rewrite(root, set())
