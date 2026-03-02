import unittest

from query_plan_ast import Join, Project, Scan, Select
from rewrites import pushdown_selections


class TestSelectionPushdown(unittest.TestCase):
    def test_merge_selects(self) -> None:
        plan = Select(
            predicate=["Student.major", "=", "CS"],
            child=Select(
                predicate=["Student.sid", "=", 1],
                child=Scan(relation="Student"),
            ),
        )

        optimized = pushdown_selections(plan)

        self.assertEqual(
            optimized,
            Select(
                predicate=[
                    "Student.major",
                    "=",
                    "CS",
                    "AND",
                    "Student.sid",
                    "=",
                    1,
                ],
                child=Scan(relation="Student"),
            ),
        )

    def test_push_select_below_project(self) -> None:
        plan = Select(
            predicate=["Student.sid", "=", 1],
            child=Project(
                attrs=["Student.sid", "Student.major"],
                child=Scan(relation="Student"),
            ),
        )

        optimized = pushdown_selections(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.sid", "Student.major"],
                child=Select(
                    predicate=["Student.sid", "=", 1],
                    child=Scan(relation="Student"),
                ),
            ),
        )

    def test_push_selects_into_join_sides(self) -> None:
        plan = Select(
            predicate=[
                "Student.major",
                "=",
                "CS",
                "AND",
                "Enroll.course",
                "=",
                "DB",
            ],
            child=Join(
                condition=["Student.sid", "=", "Enroll.sid"],
                left=Scan(relation="Student"),
                right=Scan(relation="Enroll"),
            ),
        )

        optimized = pushdown_selections(plan)

        self.assertEqual(
            optimized,
            Join(
                condition=["Student.sid", "=", "Enroll.sid"],
                left=Select(
                    predicate=["Student.major", "=", "CS"],
                    child=Scan(relation="Student"),
                ),
                right=Select(
                    predicate=["Enroll.course", "=", "DB"],
                    child=Scan(relation="Enroll"),
                ),
            ),
        )

    def test_keep_cross_relation_predicate_above_join(self) -> None:
        plan = Select(
            predicate=[
                "Student.sid",
                "=",
                "Enroll.sid",
                "AND",
                "Student.major",
                "=",
                "CS",
            ],
            child=Join(
                condition=["Student.sid", "=", "Enroll.sid"],
                left=Scan(relation="Student"),
                right=Scan(relation="Enroll"),
            ),
        )

        optimized = pushdown_selections(plan)

        self.assertEqual(
            optimized,
            Select(
                predicate=["Student.sid", "=", "Enroll.sid"],
                child=Join(
                    condition=["Student.sid", "=", "Enroll.sid"],
                    left=Select(
                        predicate=["Student.major", "=", "CS"],
                        child=Scan(relation="Student"),
                    ),
                    right=Scan(relation="Enroll"),
                ),
            ),
        )

    def test_pushdown_and_combine_across_tree(self) -> None:
        plan = Select(
            predicate=["Student.major", "=", "CS"],
            child=Select(
                predicate=["Enroll.course", "=", "DB"],
                child=Join(
                    condition=["Student.sid", "=", "Enroll.sid"],
                    left=Select(
                        predicate=["Student.sid", "=", 1],
                        child=Scan(relation="Student"),
                    ),
                    right=Scan(relation="Enroll"),
                ),
            ),
        )

        optimized = pushdown_selections(plan)

        self.assertEqual(
            optimized,
            Join(
                condition=["Student.sid", "=", "Enroll.sid"],
                left=Select(
                    predicate=[
                        "Student.major",
                        "=",
                        "CS",
                        "AND",
                        "Student.sid",
                        "=",
                        1,
                    ],
                    child=Scan(relation="Student"),
                ),
                right=Select(
                    predicate=["Enroll.course", "=", "DB"],
                    child=Scan(relation="Enroll"),
                ),
            ),
        )


if __name__ == "__main__":
    unittest.main()
