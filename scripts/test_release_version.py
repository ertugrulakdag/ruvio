#!/usr/bin/env python3
"""Version bump and exact-set checks. Does not touch the repository manifests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import release_version


class ReleaseVersionTests(unittest.TestCase):
    def test_bump_patch(self) -> None:
        self.assertEqual(release_version.bump_patch("0.2.6"), "0.2.7")
        self.assertEqual(release_version.normalize("v1.4.0"), "1.4.0")

    def test_write_cargo_and_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / "Cargo.toml"
            lock = root / "Cargo.lock"
            cargo.write_text('[package]\nname = "demo"\nversion = "0.1.2"\n', encoding="utf-8")
            lock.write_text(
                'version = 4\n\n[[package]]\nname = "other"\nversion = "1.0.0"\n\n'
                '[[package]]\nname = "demo"\nversion = "0.1.2"\n',
                encoding="utf-8",
            )
            release_version.PACKAGES["demo"] = {
                "cargo": [cargo],
                "locks": [(lock, "demo")],
                "csproj": [],
            }

            try:
                chosen = release_version.apply_version("demo", "", True)
            finally:
                del release_version.PACKAGES["demo"]

            self.assertEqual(chosen, "0.1.3")
            self.assertIn('version = "0.1.3"', cargo.read_text(encoding="utf-8"))
            self.assertIn('name = "other"\nversion = "1.0.0"', lock.read_text(encoding="utf-8"))
            self.assertIn('name = "demo"\nversion = "0.1.3"', lock.read_text(encoding="utf-8"))

    def test_exact_csproj_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "Demo.csproj"
            project.write_text("<Project><Version>0.2.7</Version></Project>\n", encoding="utf-8")
            release_version.PACKAGES["demo-cs"] = {
                "cargo": [],
                "locks": [],
                "csproj": [project],
            }

            try:
                chosen = release_version.apply_version("demo-cs", "1.0.0", True)
            finally:
                del release_version.PACKAGES["demo-cs"]

            self.assertEqual(chosen, "1.0.0")
            self.assertIn("<Version>1.0.0</Version>", project.read_text(encoding="utf-8"))

    def test_matrix_pin_follows_the_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            matrix = Path(directory) / "matrix.json"
            matrix.write_text(
                '{"clients": [\n  {"id": "a", "version": "0.1.0"},\n'
                '  {"id": "b", "version": "0.1.0"}\n]}\n',
                encoding="utf-8",
            )
            release_version.write_matrix_pin(matrix, "b", "0.2.0")
            text = matrix.read_text(encoding="utf-8")
            self.assertIn('{"id": "a", "version": "0.1.0"}', text)
            self.assertIn('{"id": "b", "version": "0.2.0"}', text)
            with self.assertRaises(ValueError):
                release_version.write_matrix_pin(matrix, "missing", "0.2.0")

    def test_python_version_also_updates_dunder(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "pyproject.toml"
            project.write_text('[project]\nversion = "0.1.0"\n', encoding="utf-8")
            module = Path(directory) / "__init__.py"
            module.write_text('x = 1\n__version__ = "0.1.0"\n', encoding="utf-8")
            release_version.PACKAGES["demo-py"] = {
                "file": project,
                "kind": "python",
                "dunder": module,
            }

            try:
                chosen = release_version.apply_version("demo-py", "", True)
            finally:
                del release_version.PACKAGES["demo-py"]

            self.assertEqual(chosen, "0.1.1")
            self.assertIn('version = "0.1.1"', project.read_text(encoding="utf-8"))
            self.assertIn('__version__ = "0.1.1"', module.read_text(encoding="utf-8"))

    def test_release_moves_unreleased_notes_under_the_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / "Demo.csproj"
            changelog = root / "CHANGELOG.md"
            project.write_text("<Project><Version>0.1.0</Version></Project>\n", encoding="utf-8")
            changelog.write_text(
                "# Changelog\n\n## Unreleased\n\n- New thing.\n\n## 0.1.0 - 2026-01-01\n\n- First.\n",
                encoding="utf-8",
            )
            release_version.PACKAGES["demo-log"] = {
                "cargo": [],
                "locks": [],
                "csproj": [project],
                "changelog": changelog,
            }

            try:
                release_version.apply_version("demo-log", "", False)
                self.assertIn("## Unreleased\n\n- New thing.", changelog.read_text(encoding="utf-8"))
                release_version.apply_version("demo-log", "", True, date="2026-02-03")
                release_version.apply_version("demo-log", "", True, date="2026-02-04")
            finally:
                del release_version.PACKAGES["demo-log"]

            self.assertEqual(
                changelog.read_text(encoding="utf-8"),
                "# Changelog\n\n## Unreleased\n\n"
                "## 0.1.2 - 2026-02-04\n\nNo recorded changes.\n\n"
                "## 0.1.1 - 2026-02-03\n\n- New thing.\n\n"
                "## 0.1.0 - 2026-01-01\n\n- First.\n",
            )

    def test_install_pins_follow_the_server_even_when_the_version_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cargo = root / "Cargo.toml"
            readme = root / "README.md"
            cargo.write_text('[package]\nname = "demo"\nversion = "0.3.2"\n', encoding="utf-8")
            readme.write_text("```\nv=0.3.1\ncurl ...ruvio-$v\n```\n", encoding="utf-8")
            release_version.PACKAGES["demo-pins"] = {
                "cargo": [cargo],
                "locks": [],
                "csproj": [],
                "pins": [readme],
            }

            try:
                release_version.apply_version("demo-pins", "0.3.2", True, install_pins=True)
            finally:
                del release_version.PACKAGES["demo-pins"]

            self.assertIn("\nv=0.3.2\n", readme.read_text(encoding="utf-8"))

    def test_preview_versions_and_stamp(self) -> None:
        self.assertEqual(release_version.preview_version("csharp", "0.3.2", 7), "0.3.2-preview.7")
        self.assertEqual(release_version.preview_version("python", "0.3.2", 7), "0.3.2.dev7")
        with self.assertRaises(ValueError):
            release_version.preview_version("csharp", "0.3.2-preview.1", 2)
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "Demo.csproj"
            changelog = Path(directory) / "CHANGELOG.md"
            project.write_text("<Project><Version>0.2.7</Version></Project>\n", encoding="utf-8")
            changelog.write_text("# Changelog\n\n## Unreleased\n\n- note\n", encoding="utf-8")
            release_version.PACKAGES["demo-stamp"] = {
                "cargo": [],
                "locks": [],
                "csproj": [project],
                "changelog": changelog,
            }

            try:
                release_version.stamp_version("demo-stamp", "0.2.8-preview.3")
                with self.assertRaises(ValueError):
                    release_version.stamp_version("demo-stamp", "0.2.8-beta")
            finally:
                del release_version.PACKAGES["demo-stamp"]

            self.assertIn("<Version>0.2.8-preview.3</Version>", project.read_text(encoding="utf-8"))
            # A preview never moves the release notes.
            self.assertIn("## Unreleased\n\n- note", changelog.read_text(encoding="utf-8"))

    def test_every_package_file_exists(self) -> None:
        for name, spec in release_version.PACKAGES.items():
            paths = [*spec.get("cargo", []), *spec.get("csproj", []), *spec.get("pins", [])]
            paths += [lock for lock, _ in spec.get("locks", [])]
            paths += [spec[key] for key in ("file", "dunder", "changelog") if key in spec]

            for path in paths:
                self.assertTrue(path.exists(), f"{name}: {path}")

            self.assertIn("changelog", spec, name)
            self.assertIn(
                release_version.UNRELEASED + "\n",
                spec["changelog"].read_text(encoding="utf-8"),
                name,
            )
            release_version.current_version(name)

        for path in release_version.PACKAGES["server"]["pins"]:
            pins = release_version.PIN.findall(path.read_text(encoding="utf-8"))
            self.assertEqual(len(pins), 1, path)


if __name__ == "__main__":
    unittest.main()
