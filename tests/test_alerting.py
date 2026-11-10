import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow_dags"))

import alerting  # noqa: E402
from alerting import explain_failure  # noqa: E402


class FakeTI:
    """A task instance whose previous runs ended in `history` (most recent first)."""
    task_id, run_id, log_url = "run_x", "manual__x", ""

    def __init__(self, dag_id, history):
        self.dag_id, self.history = dag_id, list(history)
        self.state = None

    def get_previous_ti(self):
        if not self.history:
            return None
        previous = FakeTI(self.dag_id, self.history[1:])
        previous.state = self.history[0]
        return previous


class ThresholdTests(unittest.TestCase):
    def setUp(self):
        self.sent = []
        self._send, self._reason = alerting.send_ops_message, alerting._failure_reason
        alerting.send_ops_message = lambda message, embed=None: self.sent.append(message) or 1
        alerting._failure_reason = lambda context: "boom"

    def tearDown(self):
        alerting.send_ops_message, alerting._failure_reason = self._send, self._reason

    def _fail(self, dag_id, history):
        alerting.notify_failure({"ti": FakeTI(dag_id, history)})
        return len(self.sent)

    def _recover(self, dag_id, history):
        alerting.notify_recovery({"ti": FakeTI(dag_id, history)})
        return len(self.sent)

    def test_default_alerts_on_first_failure_only(self):
        self.assertEqual(self._fail("tfr", ["success"]), 1)
        self.assertEqual(self._fail("tfr", ["failed", "success"]), 1)

    def test_roadclosure_waits_for_three_failed_runs(self):
        self.assertEqual(self._fail("roadclosure", ["success"]), 0)
        self.assertEqual(self._fail("roadclosure", ["failed", "success"]), 0)
        self.assertEqual(self._fail("roadclosure", ["failed", "failed", "success"]), 1)
        self.assertEqual(self._fail("roadclosure", ["failed", "failed", "failed", "success"]), 1)

    def test_recovery_only_after_an_alerted_failure(self):
        self.assertEqual(self._recover("roadclosure", ["failed", "failed", "success"]), 0)
        self.assertEqual(self._recover("roadclosure", ["failed", "failed", "failed"]), 1)
        self.assertEqual(self._recover("tfr", ["success"]), 1)
        self.assertEqual(self._recover("tfr", ["failed"]), 2)


class ExplainFailureTests(unittest.TestCase):
    def test_traceback_gives_exception_and_our_frame(self):
        lines = [
            "INFO:root:[TFR] 3 active TFRs",
            "Traceback (most recent call last):",
            '  File "/opt/app/src/TFR.py", line 180, in run_TFR_checks',
            "    post = build_post(tfr)",
            '  File "/home/airflow/.local/lib/python3.11/site-packages/x/y.py", line 3, in z',
            "    return d[k]",
            "KeyError: 'notam_id'",
        ]
        self.assertEqual(explain_failure(lines), "KeyError: 'notam_id'\nat TFR.py:180 in run_TFR_checks()")

    def test_logged_errors_without_traceback(self):
        lines = ["INFO:root:start", "ERROR:root:[NAV][ERR] Failed to run nav warning checks: timeout"]
        self.assertEqual(explain_failure(lines), "[NAV][ERR] Failed to run nav warning checks: timeout")

    def test_falls_back_to_last_lines(self):
        self.assertEqual(explain_failure(["a", "b", "c", "d"]), "b\nc\nd")
        self.assertEqual(explain_failure([]), "")


if __name__ == "__main__":
    unittest.main()
