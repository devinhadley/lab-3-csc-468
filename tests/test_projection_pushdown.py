import unittest

from query_plan_ast import Join, Project, Scan, Select
from rewrites import pushdown_projections as pushdown


class TestProjectionPushdown(unittest.TestCase):
    def test_pushdown_into_single_scan(self) -> None:
        plan = Project(attrs=["Student.sid"], child=Scan(relation="Student"))

        optimized = pushdown(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.sid"],
                child=Project(attrs=["Student.sid"], child=Scan(relation="Student")),
            ),
        )

    def test_collect_attrs_along_path(self) -> None:
        plan = Project(
            attrs=["Student.sid"],
            child=Select(
                predicate=["Student.major", "=", "CS"],
                child=Scan(relation="Student"),
            ),
        )

        optimized = pushdown(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.sid"],
                child=Select(
                    predicate=["Student.major", "=", "CS"],
                    child=Project(
                        attrs=["Student.major", "Student.sid"],
                        child=Scan(relation="Student"),
                    ),
                ),
            ),
        )

    def test_split_attrs_for_each_join_side(self) -> None:
        plan = Project(
            attrs=["Student.name", "Enroll.course"],
            child=Join(
                condition=["Student.sid", "=", "Enroll.sid"],
                left=Scan(relation="Student"),
                right=Scan(relation="Enroll"),
            ),
        )

        optimized = pushdown(plan)

        self.assertEqual(
            optimized,
            Project(
                attrs=["Student.name", "Enroll.course"],
                child=Join(
                    condition=["Student.sid", "=", "Enroll.sid"],
                    left=Project(
                        attrs=["Student.name", "Student.sid"],
                        child=Scan(relation="Student"),
                    ),
                    right=Project(
                        attrs=["Enroll.course", "Enroll.sid"],
                        child=Scan(relation="Enroll"),
                    ),
                ),
            ),
        )


if __name__ == "__main__":
    unittest.main()
