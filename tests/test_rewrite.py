import unittest

from query_plan_ast import Join, Project, Scan, Select
from rewrites import join_commutativity, pushdown_projections, pushdown_selections


def rewrite(plan):
    return pushdown_projections(pushdown_selections(plan))


class TestRewrite(unittest.TestCase):
    def test_combine_selects_then_push_projections(self) -> None:
        plan = Project(
            attrs=["Student.name"],
            child=Select(
                predicate=["Student.major", "=", "CS"],
                child=Select(
                    predicate=["Student.sid", "=", 1],
                    child=Scan(relation="Student"),
                ),
            ),
        )

        optimized = rewrite(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.name"],
                child=Select(
                    predicate=[
                        "Student.major",
                        "=",
                        "CS",
                        "AND",
                        "Student.sid",
                        "=",
                        1,
                    ],
                    child=Project(
                        attrs=["Student.major", "Student.name", "Student.sid"],
                        child=Scan(relation="Student"),
                    ),
                ),
            ),
        )

    def test_push_select_into_join_sides_then_push_projections(self) -> None:
        plan = Project(
            attrs=["Student.name", "Enroll.course"],
            child=Select(
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
            ),
        )

        optimized = rewrite(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.name", "Enroll.course"],
                child=Join(
                    condition=["Student.sid", "=", "Enroll.sid"],
                    left=Select(
                        predicate=["Student.major", "=", "CS"],
                        child=Project(
                            attrs=["Student.major", "Student.name", "Student.sid"],
                            child=Scan(relation="Student"),
                        ),
                    ),
                    right=Select(
                        predicate=["Enroll.course", "=", "DB"],
                        child=Project(
                            attrs=["Enroll.course", "Enroll.sid"],
                            child=Scan(relation="Enroll"),
                        ),
                    ),
                ),
            ),
        )

    def test_split_conjunct_across_join_combines_with_inner_select(self) -> None:
        plan = Project(
            attrs=["Student.name", "Enroll.course"],
            child=Select(
                predicate=[
                    "Student.major",
                    "=",
                    "CS",
                    "AND",
                    "Enroll.course",
                    "=",
                    "DB",
                ],
                child=Select(
                    predicate=["Student.sid", "=", 1],
                    child=Join(
                        condition=["Student.sid", "=", "Enroll.sid"],
                        left=Scan(relation="Student"),
                        right=Scan(relation="Enroll"),
                    ),
                ),
            ),
        )

        optimized = rewrite(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.name", "Enroll.course"],
                child=Join(
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
                        child=Project(
                            attrs=["Student.major", "Student.name", "Student.sid"],
                            child=Scan(relation="Student"),
                        ),
                    ),
                    right=Select(
                        predicate=["Enroll.course", "=", "DB"],
                        child=Project(
                            attrs=["Enroll.course", "Enroll.sid"],
                            child=Scan(relation="Enroll"),
                        ),
                    ),
                ),
            ),
        )

    def test_join_commutativity(self) -> None:
        plan = Project(
            attrs=["Student.name", "Enroll.course"],
            child=Join(
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
                    child=Project(
                        attrs=["Student.major", "Student.name", "Student.sid"],
                        child=Scan(relation="Student"),
                    ),
                ),
                right=Select(
                    predicate=["Enroll.course", "=", "DB"],
                    child=Project(
                        attrs=["Enroll.course", "Enroll.sid"],
                        child=Scan(relation="Enroll"),
                    ),
                ),
            ),
        )

        communative_join = join_commutativity(plan)

        self.assertEqual(
            communative_join,
            Project(
                attrs=["Student.name", "Enroll.course"],
                child=Join(
                    condition=["Student.sid", "=", "Enroll.sid"],
                    left=Select(
                        predicate=["Enroll.course", "=", "DB"],
                        child=Project(
                            attrs=["Enroll.course", "Enroll.sid"],
                            child=Scan(relation="Enroll"),
                        ),
                    ),
                    right=Select(
                        predicate=[
                            "Student.major",
                            "=",
                            "CS",
                            "AND",
                            "Student.sid",
                            "=",
                            1,
                        ],
                        child=Project(
                            attrs=["Student.major", "Student.name", "Student.sid"],
                            child=Scan(relation="Student"),
                        ),
                    ),
                ),
            ),
        )


if __name__ == "__main__":
    unittest.main()
