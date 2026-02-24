import argparse

from query_plan_ast import parse_plan_file, print_plan_bfs


def main() -> None:
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

    args = parser.parse_args()
    plan = parse_plan_file(args.query)

    if args.debug:
        print_plan_bfs(plan)


if __name__ == "__main__":
    main()
