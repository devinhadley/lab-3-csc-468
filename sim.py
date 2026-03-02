import argparse

from query_plan_ast import parse_plan_file, print_plan_bfs

# TODO:
#    - Selection Pushdown
#    - Combine Selections
#    - Projection Pushdown
#    - Join Commutativity


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

    return parser


def main() -> None:
    args = get_arg_parser().parse_args()
    logical_plan = parse_plan_file(args.query)

    if args.debug:
        print_plan_bfs(logical_plan)


if __name__ == "__main__":
    main()
