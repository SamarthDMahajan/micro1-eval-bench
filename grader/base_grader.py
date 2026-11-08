import traceback
from typing import Dict, Any, Tuple

DEFAULT_WEIGHTS = {
    "happy_path": 0.15,
    "edge_case_1_boundary": 0.20,
    "edge_case_2_recovery": 0.20,
    "concurrency_race_safety": 0.25,
    "invariant_stress_test": 0.20,
}

class BaseGrader:
    task_id: str = "base"
    weights: Dict[str, float] = DEFAULT_WEIGHTS

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        raise NotImplementedError

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        raise NotImplementedError

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        raise NotImplementedError

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        raise NotImplementedError

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        raise NotImplementedError

    def grade(self, target_module) -> Dict[str, Any]:
        sub_scores = {}
        feedback = {}
        total_score = 0.0

        tests = [
            ("happy_path", self.test_happy_path),
            ("edge_case_1_boundary", self.test_edge_case_1_boundary),
            ("edge_case_2_recovery", self.test_edge_case_2_recovery),
            ("concurrency_race_safety", self.test_concurrency_race_safety),
            ("invariant_stress_test", self.test_invariant_stress_test),
        ]

        for bucket_name, test_fn in tests:
            weight = self.weights.get(bucket_name, 0.0)
            try:
                passed, msg = test_fn(target_module)
                if passed:
                    sub_scores[bucket_name] = weight
                    feedback[bucket_name] = f"PASS: {msg}"
                    total_score += weight
                else:
                    sub_scores[bucket_name] = 0.0
                    feedback[bucket_name] = f"FAIL: {msg}"
            except Exception as e:
                sub_scores[bucket_name] = 0.0
                feedback[bucket_name] = f"ERROR: Exception during {bucket_name}: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

        total_score = round(total_score, 4)
        return {
            "task_id": self.task_id,
            "total_score": total_score,
            "sub_scores": sub_scores,
            "feedback": feedback,
            "passed": total_score >= 0.999,
        }
