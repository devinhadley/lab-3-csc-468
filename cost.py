from functools import lru_cache
import json
from pathlib import Path

from query_plan_ast import PlanNode, Predicates, Project, Scan, Join, Select

from physical_plan_ast import (
    PhysicalHashScan,
    PhysicalPlanNode,
    PhysicalSeqScan,
    PhysicalHashJoin,
    PhysicalNestedLoopJoin,
    PhysicalFilter,
    PhysicalProject,
)
from rewrites import get_subtree_relations
import math


def _load_stats() -> dict:
    _STATS_FILE = Path(__file__).parent / "files" / "statistics.json"

    raw = json.loads(_STATS_FILE.read_text())
    return {
        relation: {
            **{k: v for k, v in data.items() if k != "V"},
            "V": {f"{relation}.{attr}": val for attr, val in data["V"].items()},
        }
        for relation, data in raw.items()
    }


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


def get_physical_cost(
    node: PlanNode, stats: dict, predicate=None
) -> tuple[PhysicalPlanNode, float]:
    match node:
        # TODO: Index scan.
        # If predicate above contains id (index field) then we can compute cost of index scan.
        # Pushdown guarntees if select it will be right above scan.
        # and it will be on same relation.
        # Therefore we just check if "sid" in predicate.
        case Scan(relation=rltn):
            # Check if a predicate above was pushed down and targets an indexed attribute
            if predicate is not None:
                rel_attrs = _get_relations_to_attrs_from_predicate(predicate)
                for attr in rel_attrs.get(rltn, []):
                    # Extract the literal value paired with this attribute
                    for i, token in enumerate(predicate):
                        if token == attr:
                            # predicate is [..., attr, "=", value, ...]
                            if i + 2 < len(predicate) and predicate[i + 1] == "=":
                                literal = predicate[i + 2]
                                v = stats[rltn]["V"].get(attr, 1)
                                matching_pages = math.ceil(stats[rltn]["B"] / v)
                                return PhysicalHashScan(rltn, attr, literal), 1 + matching_pages
            return PhysicalSeqScan(rltn), stats[rltn]["B"]

        case Select(
            child=chld, predicate=pred
        ):  # Select has 0 IO cost. It is practically merged with scan.
            best_child, child_cost = get_physical_cost(chld, stats, pred)

            match best_child:
                # Collapse both: either the Index handled the filter,
                # or the SeqScan was given the predicate to handle internally.
                case PhysicalHashScan() | PhysicalSeqScan():
                    return best_child, child_cost

                case _:
                    # Fallback: If the child is something else (like a Join result),
                    # we still need the Filter node because Joins don't "absorb" filters.
                    return PhysicalFilter(best_child, pred), child_cost

        case Project(attrs=proj_attrs, child=chld):
            best_child, child_cost = get_physical_cost(chld, stats)
            return PhysicalProject(best_child, proj_attrs), child_cost

        case Join(condition=cond, left=lft, right=rht):
            best_left, left_cost = get_physical_cost(lft, stats)
            best_right, right_cost = get_physical_cost(rht, stats)

            hash_cost = left_cost + right_cost

            card_left = get_logical_cost(lft, stats)
            card_right = get_logical_cost(rht, stats)
            nlj_cost = left_cost + card_left * math.ceil(card_right / 2)

            best_cost, best_plan = min(
                (hash_cost, PhysicalHashJoin(best_left, best_right, cond)),
                (nlj_cost, PhysicalNestedLoopJoin(best_left, best_right, cond)),
            )
            return best_plan, best_cost

        case _:
            raise RuntimeError(f"Unknown logical node: {type(node)}")
