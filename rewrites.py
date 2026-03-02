from typing import List, Set, Union, cast

from query_plan_ast import Join, Predicates, PlanNode, Project, Scan, Select


# Predicate parsing helpers...
def _predicate_attrs(predicate: Predicates) -> Set[str]:
    return {item for item in predicate if isinstance(item, str) and "." in item}


def get_subtree_relations(node: PlanNode) -> Set[str]:
    """
    I return the relaations found in the sub tree by traversing until I find scan and propogate up the relation.
    """
    match node:
        case Scan(relation=r):
            return {r}
        case Select(child=c) | Project(child=c):
            return get_subtree_relations(c)
        case Join(left=l, right=r):
            return get_subtree_relations(l) | get_subtree_relations(r)
        case _:
            return set()


def _attrs_for_relations(attrs: Set[str], relations: Set[str]) -> Set[str]:
    return {
        attr for attr in attrs if "." in attr and attr.split(".", 1)[0] in relations
    }


def pushdown_selections(root: PlanNode) -> PlanNode:
    """
    Recursively rewrites the child and performs pushdown for current node.
    If a pushdown occurs for current node, we rewrite the modified child again.
    Includes helpers for splitting and combining conjuctions.
    """

    def predicate_relations(predicate: Predicates) -> Set[str]:
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
        match node:
            case Select(predicate=pred, child=raw_child):
                child = rewrite(raw_child)
                match child:
                    case Select():
                        merged = combine_conjuncts(
                            split_conjuncts(pred) + split_conjuncts(child.predicate)
                        )
                        return rewrite(Select(predicate=merged, child=child.child))
                    case Project(attrs=attrs):
                        if _predicate_attrs(pred).issubset(set(attrs)):
                            pushed = Select(predicate=pred, child=child.child)
                            return Project(attrs=attrs, child=rewrite(pushed))
                        return Select(predicate=pred, child=child)
                    case Join(condition=cond, left=left, right=right):
                        left_relations = get_subtree_relations(left)
                        right_relations = get_subtree_relations(right)
                        left_preds: List[Predicates] = []
                        right_preds: List[Predicates] = []
                        other_preds: List[Predicates] = []

                        for p in split_conjuncts(pred):
                            rels = predicate_relations(p)
                            if rels and rels.issubset(left_relations):
                                left_preds.append(p)
                            elif rels and rels.issubset(right_relations):
                                right_preds.append(p)
                            else:
                                other_preds.append(p)

                        new_left = left
                        new_right = right
                        if left_preds:
                            new_left = rewrite(
                                Select(
                                    predicate=combine_conjuncts(left_preds),
                                    child=new_left,
                                )
                            )
                        if right_preds:
                            new_right = rewrite(
                                Select(
                                    predicate=combine_conjuncts(right_preds),
                                    child=new_right,
                                )
                            )
                        joined = Join(condition=cond, left=new_left, right=new_right)
                        if other_preds:
                            return Select(
                                predicate=combine_conjuncts(other_preds), child=joined
                            )
                        return joined
                    case _:
                        return Select(predicate=pred, child=child)
            case Project(attrs=attrs, child=c):
                return Project(attrs=attrs, child=rewrite(c))
            case Join(condition=cond, left=l, right=r):
                return Join(condition=cond, left=rewrite(l), right=rewrite(r))
            case _:
                return node

    return rewrite(root)


def pushdown_projections(root: PlanNode) -> PlanNode:
    """
    Traverse down each path in the tree collecting attributes we need as we go.
    Once we encounter leaf (scan) then add the projection over it.
    """

    def rewrite(node: PlanNode, collected_attrs: Set[str]) -> PlanNode:
        match node:
            case Project(attrs=attrs, child=c):
                child_attrs = collected_attrs | set(attrs)
                return Project(attrs=attrs, child=rewrite(c, child_attrs))
            case Select(predicate=pred, child=c):
                child_attrs = collected_attrs | _predicate_attrs(pred)
                return Select(predicate=pred, child=rewrite(c, child_attrs))
            case Join(condition=cond, left=l, right=r):
                condition_attrs = _predicate_attrs(cond)
                left_relations = get_subtree_relations(l)
                right_relations = get_subtree_relations(r)

                left_attrs = _attrs_for_relations(collected_attrs, left_relations)
                left_attrs |= _attrs_for_relations(condition_attrs, left_relations)

                right_attrs = _attrs_for_relations(collected_attrs, right_relations)
                right_attrs |= _attrs_for_relations(condition_attrs, right_relations)

                return Join(
                    condition=cond,
                    left=rewrite(l, left_attrs),
                    right=rewrite(r, right_attrs),
                )
            case Scan(relation=rel):
                attrs = sorted(_attrs_for_relations(collected_attrs, {rel}))
                if not attrs:
                    return node
                return Project(attrs=attrs, child=node)
            case _:
                return node

    return rewrite(root, set())


def join_commutativity(root: PlanNode) -> PlanNode | None:
    """
    Copy the tree as is until we find join, swap left and right and return new tree.
    Returns None if no join found.
    """
    match root:
        case Join(condition=cond, left=l, right=r):
            return Join(condition=cond, left=r, right=l)
        case Select(predicate=pred, child=c):
            new_child = join_commutativity(c)
            if new_child is None:
                return None
            return Select(predicate=pred, child=new_child)
        case Project(attrs=attrs, child=c):
            new_child = join_commutativity(c)
            if new_child is None:
                return None
            return Project(attrs=attrs, child=new_child)
        case _:
            return None
