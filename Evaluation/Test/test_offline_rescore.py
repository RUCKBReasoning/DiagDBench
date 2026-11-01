import copy
import json
from pathlib import Path
import unittest

from Evaluation.offline_rescore import TOKEN, POLICY, operation_gate, replace_score, scorer, rescore, safe


class RescoringRules(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compute = staticmethod(scorer(Path(__file__).resolve().parents[1] / "test_eval.py"))

    def details(self, ratio=.5, reasons=None, gate=False):
        return self.compute({"score": .6}, gate, 100, 100 * ratio, None, 8,
                            failure_reasons=reasons if reasons is not None else [TOKEN])

    def test_only_script_reason_is_removed(self):
        old = self.details()
        new, touched = replace_score(old, baseline=100, recovered=50, gate=True,
                                     compute=self.compute, mode="both")
        self.assertTrue(touched)
        self.assertEqual(old["recovery_score"], 0)
        self.assertEqual(new["recovery_score"], .9)
        self.assertEqual(new["score"], .81)
        self.assertEqual(new["diagnosis_score"], old["diagnosis_score"])

    def test_other_reasons_survive(self):
        for reason in ("semantic_mismatch", "result_mismatch", "user_workload_timeout",
                       "initialization_errors_incompatible", "recovered_user_workload_error",
                       "unknown_other_failure", "anomaly_injection_failed"):
            with self.subTest(reason=reason):
                new, _ = replace_score(self.details(reasons=[TOKEN, reason]), baseline=100,
                                       recovered=50, gate=False, compute=self.compute, mode="both")
                self.assertEqual(new["recovery_score"], 0)
                self.assertIn(reason, new["recovery_detail"]["zero_reasons"])
                self.assertNotIn(TOKEN, new["recovery_detail"]["zero_reasons"])

    def test_eight_x_boundary_remains_zero(self):
        for ratio in (8, 9):
            new, _ = replace_score(self.details(ratio), baseline=100, recovered=100*ratio,
                                   gate=True, compute=self.compute, mode="both")
            self.assertEqual(new["recovery_score"], 0)
            self.assertIn("execution_ratio_at_or_above_8x", new["recovery_detail"]["zero_reasons"])

    def test_missing_time_not_fabricated(self):
        old = self.compute({"score": .6}, False, 100, None, None, 8, failure_reasons=[TOKEN])
        new, _ = replace_score(old, baseline=100, recovered=None, gate=False,
                               compute=self.compute, mode="both")
        self.assertEqual(new["recovery_score"], 0)
        self.assertIsNone(new["recovery_detail"]["execution_time_ratio"])
        self.assertIn("recovered_time_unavailable", new["recovery_detail"]["zero_reasons"])

    def test_unrelated_scores_exactly_unchanged(self):
        old = self.details(reasons=[], gate=True)
        new, touched = replace_score(old, baseline=100, recovered=50, gate=True,
                                     compute=self.compute, mode="both")
        self.assertFalse(touched)
        self.assertEqual(old, new)

    def test_semantic_and_fingerprint_gates_distinct(self):
        outcome = {"measurement": {"success": True, "error_count": 0, "errors": []},
                   "recovered_error_free": True, "initialization_errors_compatible": True,
                   "results_equivalent": False}
        self.assertFalse(operation_gate(outcome))
        self.assertTrue(operation_gate(outcome, {"score": 1}))
        self.assertFalse(operation_gate(outcome, {"score": 0}))
        outcome["measurement"]["timed_out"] = True
        self.assertFalse(operation_gate(outcome, {"score": 1}))

    def test_refuse_double_rescore(self):
        with self.assertRaises(ValueError):
            rescore({"offline_re_evaluation": {"policy": POLICY}}, self.compute, "hash")

    def test_archive_traversal_rejected(self):
        for name in ("../x", "/tmp/x", "a/../../x", "./x"):
            with self.assertRaises(ValueError):
                safe(name)


if __name__ == "__main__":
    unittest.main()
