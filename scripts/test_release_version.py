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


if __name__ == "__main__":
    unittest.main()
