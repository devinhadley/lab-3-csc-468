from query_plan_ast import PlanNode, Predicates, Project, Scan, Join, Select
from rewrites import get_subtree_relations


def _get_relations_to_attrs_from_predicate(
    predicate: Predicates,
) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for item in predicate:
        if isinstance(item, str) and "." in item:
            relation, _ = item.split(".", 1)
            result.setdefault(relation, []).append(item)
    return result


def get_logical_cost(node: PlanNode, stats: dict) -> float:
    match node:
        case Select(predicate=pred, child=chld):
            t_in = get_logical_cost(chld, stats)

            relation_to_attrs = _get_relations_to_attrs_from_predicate(pred)

            for relation, attrs in relation_to_attrs.items():
                for attr in attrs:
                    t_in *= 1 / stats[relation]["V"][attr]

            return t_in

        case Project(child=chld):
            return get_logical_cost(chld, stats)
        case Join(condition=preds, left=lft, right=rht):
            l_in = get_logical_cost(lft, stats)
            r_in = get_logical_cost(rht, stats)

            # Only one join per query...
            # So there can only be one relation on each side...
            left_rel = get_subtree_relations(lft).pop()
            right_rel = get_subtree_relations(rht).pop()

            relation_to_attrs = _get_relations_to_attrs_from_predicate(preds)

            # NOTE: Assuming join conditions are limited to a single equality predicate...
            # That is one attribute from left rel = one attribute from right rel
            v_left = stats[left_rel]["V"][relation_to_attrs[left_rel][0]]
            v_right = stats[right_rel]["V"][relation_to_attrs[right_rel][0]]

            return l_in * r_in / max(v_left, v_right)

        case Scan(relation=rltn):
            return stats[rltn]["T"]

        case _:
            raise RuntimeError("get logical cost couldnt match node.")
