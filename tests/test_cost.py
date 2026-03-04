import unittest

from query_plan_ast import Join, Project, Scan, Select
from cost import get_logical_cost


STATS = {
    "Student": {
        "T": 1000,
        "V": {
            "Student.sid": 1000,
            "Student.major": 50,
            "Student.name": 900,
        },
    },
    "Enroll": {
        "T": 3000,
        "V": {
            "Enroll.sid": 1000,
            "Enroll.course": 200,
        },
    },
}


class TestLogicalCost(unittest.TestCase):
    def test_scan_returns_tuple_count(self) -> None:
        plan = Scan(relation="Student")

        self.assertEqual(get_logical_cost(plan, STATS), 1000)

    def test_project_passes_through_child_cost(self) -> None:
        plan = Project(attrs=["Student.name"], child=Scan(relation="Student"))

        self.assertEqual(get_logical_cost(plan, STATS), 1000)

    def test_select_reduces_by_selectivity(self) -> None:
        # T(Student) * (1 / V(Student, Student.major)) = 1000 / 50 = 20
        plan = Select(
            predicate=["Student.major", "=", "CS"],
            child=Scan(relation="Student"),
        )

        self.assertAlmostEqual(get_logical_cost(plan, STATS), 1000 / 50)

    def test_select_multiple_predicates_multiply_selectivities(self) -> None:
        # 1000 * (1/50) * (1/1000) = 1000 / 50 / 1000 = 0.02
        plan = Select(
            predicate=["Student.major", "=", "CS", "AND", "Student.sid", "=", 1],
            child=Scan(relation="Student"),
        )

        self.assertAlmostEqual(get_logical_cost(plan, STATS), 1000 / 50 / 1000)

    def test_join_cardinality_estimate(self) -> None:
        # T(Student) * T(Enroll) / max(V(Student, sid), V(Enroll, sid))
        # = 1000 * 3000 / max(1000, 1000) = 3000
        plan = Join(
            condition=["Student.sid", "=", "Enroll.sid"],
            left=Scan(relation="Student"),
            right=Scan(relation="Enroll"),
        )

        self.assertAlmostEqual(get_logical_cost(plan, STATS), 1000 * 3000 / 1000)

    def test_join_cardinality_uses_max_v(self) -> None:
        stats = {
            "Student": {"T": 500, "V": {"Student.sid": 100}},
            "Enroll": {"T": 2000, "V": {"Enroll.sid": 500}},
        }
        # 500 * 2000 / max(100, 500) = 1000000 / 500 = 2000
        plan = Join(
            condition=["Student.sid", "=", "Enroll.sid"],
            left=Scan(relation="Student"),
            right=Scan(relation="Enroll"),
        )

        self.assertAlmostEqual(get_logical_cost(plan, stats), 500 * 2000 / 500)

    def test_select_then_join(self) -> None:
        # Select filters Student before the join.
        # T(Student) after select = 1000 / 50 = 20
        # Join: 20 * 3000 / max(1000, 1000) = 60
        plan = Join(
            condition=["Student.sid", "=", "Enroll.sid"],
            left=Select(
                predicate=["Student.major", "=", "CS"],
                child=Scan(relation="Student"),
            ),
            right=Scan(relation="Enroll"),
        )

        self.assertAlmostEqual(
            get_logical_cost(plan, STATS),
            (1000 / 50) * 3000 / 1000,
        )


if __name__ == "__main__":
    unittest.main()
