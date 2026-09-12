"""Keep the CI diagnostic boundary from hiding project or compiler failures."""

import unittest

from scripts.check_quality import ROOT, actionable_diagnostics


class DiagnosticScopeTest(unittest.TestCase):
    def diagnostic(self, path, name="clang-analyzer-cplusplus.NewDelete"):
        return {"DiagnosticName": name, "DiagnosticMessage": {"FilePath": str(path)}}

    def test_project_and_integration_findings_fail(self):
        for path in (
            ROOT / "src/client/main.cpp",
            ROOT / "tests/integration/test_metrics.cpp",
        ):
            with self.subTest(path=path):
                diagnostic = self.diagnostic(path)
                self.assertEqual(actionable_diagnostics([diagnostic]), [diagnostic])

    def test_dependency_findings_are_retained_outside_the_project_gate(self):
        diagnostics = [
            self.diagnostic(ROOT / "build/third_party/src/library/header.hpp"),
            self.diagnostic(ROOT / "src-other/header.hpp"),
        ]
        self.assertEqual(actionable_diagnostics(diagnostics), [])

    def test_compiler_errors_in_dependencies_always_fail(self):
        diagnostic = self.diagnostic(
            ROOT / "build/third_party/src/library/header.hpp", "clang-diagnostic-error"
        )
        self.assertEqual(actionable_diagnostics([diagnostic]), [diagnostic])

    def test_diagnostics_without_a_location_fail_closed(self):
        diagnostic = self.diagnostic("")
        self.assertEqual(actionable_diagnostics([diagnostic]), [diagnostic])


if __name__ == "__main__":
    unittest.main()
