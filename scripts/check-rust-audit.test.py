"""The audit policy must not turn infrastructure failures into a green scan."""
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("rust_audit_policy", Path(__file__).with_name("check-rust-audit.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class AuditPolicy(unittest.TestCase):
    def report(self, vulnerable=False, warnings=False):
        row = {"package": {"name": "test-crate", "version": "1.0.0"},
               "advisory": {"id": "RUSTSEC-2099-0001", "title": "Test advisory"}}
        return {"database": {"last-commit": "1" * 40, "advisory-count": 1}, "lockfile": {"dependency-count": 1},
                "vulnerabilities": {"found": vulnerable, "count": int(vulnerable), "list": [row] if vulnerable else []},
                "warnings": {"unmaintained": [row]} if warnings else {}}

    def run_check(self, stdout, code=0, stderr=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = subprocess.CompletedProcess([], code, stdout, stderr)
            output = io.StringIO()
            with patch.object(audit.subprocess, "run", return_value=result) as run, \
                    patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(root / "summary.md")}), \
                    contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                status = audit.check(root / "reports" / "audit.json")
            run.assert_called_once_with(["cargo", "audit", "--json"], capture_output=True, text=True, check=False)
            return status, output.getvalue(), (root / "reports" / "audit.json").read_text(), (root / "summary.md").read_text() if (root / "summary.md").exists() else ""

    def test_clean_report_is_success_and_saved(self):
        raw = json.dumps(self.report())
        status, output, saved, summary = self.run_check(raw)
        self.assertEqual(status, 0)
        self.assertEqual(saved, raw)
        self.assertIn("Vulnerabilities: 0; warnings: 0", summary)
        self.assertNotIn("::warning::", output)

    def test_vulnerability_is_visible_warning_without_failing_ci(self):
        status, output, _, summary = self.run_check(json.dumps(self.report(vulnerable=True)), 1)
        self.assertEqual(status, 0)
        self.assertIn("::warning::vulnerability: test-crate", output)
        self.assertIn("RUSTSEC-2099-0001", summary)

    def test_existing_warnings_are_also_reported(self):
        status, output, _, summary = self.run_check(json.dumps(self.report(warnings=True)))
        self.assertEqual(status, 0)
        self.assertIn("::warning::unmaintained", output)
        self.assertIn("warnings: 1", summary)

    def test_failed_tool_or_database_is_not_a_successful_scan(self):
        for code, raw in [(1, ""), (1, '{"error":"database unavailable"}'), (101, ""), (1, json.dumps(self.report())), (2, json.dumps(self.report(True)))]:
            with self.subTest(code=code, raw=raw):
                status, output, _, summary = self.run_check(raw, code)
                self.assertEqual(status, 1)
                self.assertIn("::error::", output)
                self.assertNotIn("Vulnerabilities: 0", summary)

    def test_missing_cargo_fails(self):
        with tempfile.TemporaryDirectory() as root, patch.object(audit.subprocess, "run", side_effect=FileNotFoundError("cargo")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(audit.check(Path(root) / "report.json"), 1)

    def test_inconsistent_or_malformed_reports_fail(self):
        variants = [None, [], {}, self.report(True), self.report(True), self.report(True), self.report(True), self.report(True)]
        variants[3]["vulnerabilities"]["count"] = 0
        variants[4]["vulnerabilities"]["found"] = False
        variants[5]["warnings"] = {"unsound": None}
        variants[6]["vulnerabilities"]["list"] = ["not an advisory"]
        variants[7]["database"] = {}
        for report in variants:
            with self.subTest(report=report):
                self.assertEqual(self.run_check(json.dumps(report))[0], 1)

    def test_titles_cannot_inject_workflow_commands(self):
        report = copy.deepcopy(self.report(True))
        report["vulnerabilities"]["list"][0]["advisory"]["title"] = "Title%\n::error::injected\r"
        status, output, _, _ = self.run_check(json.dumps(report), 1)
        self.assertEqual(status, 0)
        self.assertIn("Title%25%0A::error::injected%0D", output)
        self.assertNotIn("\n::error::injected", output)


if __name__ == "__main__":
    unittest.main()
