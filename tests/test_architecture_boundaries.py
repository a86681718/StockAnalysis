from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "src/stockanalysis"
FORBIDDEN_TOP_LEVEL_IMPORTS = {"apps", "deployment", "research"}


class PackageDependencyBoundaryTests(unittest.TestCase):
    def test_package_does_not_import_entrypoint_or_research_namespaces(self):
        violations: list[str] = []

        for path in sorted(PACKAGE_ROOT.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                imported_names: list[str] = []
                if isinstance(node, ast.Import):
                    imported_names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported_names = [node.module]

                for name in imported_names:
                    if name.split(".", 1)[0] in FORBIDDEN_TOP_LEVEL_IMPORTS:
                        relative = path.relative_to(ROOT)
                        violations.append(f"{relative}:{node.lineno}: {name}")

        self.assertEqual(violations, [], "Forbidden dependency direction:\n" + "\n".join(violations))


if __name__ == "__main__":
    unittest.main()
