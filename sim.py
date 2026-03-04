import argparse
import json
from pathlib import Path

from hash_index import HashIndex
from query_plan_ast import parse_plan_file, print_plan_bfs
from rewrites import join_commutativity, pushdown_projections, pushdown_selections
from cost import get_physical_cost, _load_stats


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

    logical_plan = parse_plan_file(args.query)

    logical_plan = pushdown_selections(logical_plan)

    logical_plan = pushdown_projections(logical_plan)

    alt_logical_plan = join_commutativity(logical_plan)

    if args.debug:
        print_plan_bfs(logical_plan)

    stats = _load_stats()
    physical_plan, cost = get_physical_cost(logical_plan, stats)

    if args.debug:
        print(f"\nPhysical plan (cost={cost}):")
        print(physical_plan)

    index_map = {(idx.relation_name, idx.attribute): idx for idx in indexes}

    physical_plan.open(relations, index_map)
    results = []
    while (rec := physical_plan.next()) is not None:
        results.append(rec)
    physical_plan.close()

    print(f"Result ({len(results)} tuples):")
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
