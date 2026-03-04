import argparse
import json
from pathlib import Path

from hash_index import HashIndex
from query_plan_ast import parse_plan_file, print_plan_bfs, Scan, Select, Project, Join
from rewrites import join_commutativity, pushdown_projections, pushdown_selections
from cost import get_physical_cost, get_logical_cost, _load_stats
from physical_plan_ast import count_pages_read


def get_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse a query into an AST")

    parser.add_argument(
        "--query",
        required=True,
        help="Path to query JSON file",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print parsed plan using BFS",
    )
    parser.add_argument(
        "--relations",
        default="files/relations.json",
        help="Path to relations JSON file",
    )

    return parser


def _node_label(node) -> str:
    if isinstance(node, Scan):
        return f"Scan(relation={node.relation})"
    if isinstance(node, Select):
        return f"Select(predicate={node.predicate})"
    if isinstance(node, Project):
        return f"Project(attrs={node.attrs})"
    if isinstance(node, Join):
        return f"Join(condition={node.condition})"
    return node.__class__.__name__


def _get_children(node) -> list:
    if isinstance(node, (Select, Project)):
        return [node.child]
    if isinstance(node, Join):
        return [node.left, node.right]
    return []


def _print_cardinality_tree(node, stats) -> None:
    def _print_node(n, pfx, last):
        connector = "`-- " if last else "|-- "
        card = get_logical_cost(n, stats)
        kids = _get_children(n)
        print(f"{pfx}{connector}{_node_label(n)} [est. {card:.0f} tuples]")
        next_pfx = pfx + ("    " if last else "|   ")
        for i, child in enumerate(kids):
            _print_node(child, next_pfx, i == len(kids) - 1)

    card = get_logical_cost(node, stats)
    print(f"{_node_label(node)} [est. {card:.0f} tuples]")
    children = _get_children(node)
    for i, child in enumerate(children):
        _print_node(child, "", i == len(children) - 1)


def main() -> None:
    args = get_arg_parser().parse_args()

    relations = json.loads(Path(args.relations).read_text())["relations"]
    indexes = [
        HashIndex("Student", "sid"),
        HashIndex("Student", "major"),
        HashIndex("Enroll", "sid"),
    ]
    for idx in indexes:
        idx.build(relations[idx.relation_name])

    original_logical_plan = parse_plan_file(args.query)

    print("-- Original Logical Plan --")
    print_plan_bfs(original_logical_plan)
    print()

    logical_plan = pushdown_selections(original_logical_plan)
    logical_plan = pushdown_projections(logical_plan)

    print("-- Rewritten Logical Plan --")
    print_plan_bfs(logical_plan)
    print()

    stats = _load_stats()

    print("-- Estimated Cardinality --")
    _print_cardinality_tree(logical_plan, stats)
    print()

    alt_logical_plan = join_commutativity(logical_plan)
    physical_plan, cost = get_physical_cost(logical_plan, stats)

    print("-- Candidate Physical Plans --")
    print(f"[1] {physical_plan}  (est. cost = {cost})")

    chosen_plan_num = 1
    chosen_cost = cost

    if alt_logical_plan:
        alt_physical_plan, alt_cost = get_physical_cost(alt_logical_plan, stats)
        print(f"[2] {alt_physical_plan}  (est. cost = {alt_cost})")

        if alt_cost < cost:
            physical_plan, cost = alt_physical_plan, alt_cost
            chosen_plan_num = 2
            chosen_cost = alt_cost
    print()

    print("-- Chosen Plan --")
    print(f"Plan [{chosen_plan_num}] selected (cost = {chosen_cost})")
    print(physical_plan)
    print()

    print("-- Executing... --")
    print()

    index_map = {(idx.relation_name, idx.attribute): idx for idx in indexes}

    physical_plan.open(relations, index_map)
    results = []
    while (rec := physical_plan.next()) is not None:
        results.append(rec)
    physical_plan.close()

    pages = count_pages_read(physical_plan)
    print("-- Actual I/O (pages read) --")
    print(f"{pages} pages")
    print()

    print(f"-- Result ({len(results)} tuples) --")
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
