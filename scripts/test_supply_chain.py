#!/usr/bin/env python3

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import generate_sbom as sbom

ROOT = Path(__file__).resolve().parents[1]


class SupplyChainTests(unittest.TestCase):
    def test_deny_policy_covers_required_checks(self) -> None:
        text = (ROOT / "deny.toml").read_text()
        for section in ("[advisories]", "[licenses]", "[bans]", "[sources]"):
            self.assertIn(section, text)
        self.assertIn("unknown-git = \"deny\"", text)
        self.assertIn("wildcards = \"deny\"", text)

    def test_security_policy_requires_private_reporting(self) -> None:
        text = (ROOT / "SECURITY.md").read_text()
        self.assertIn("security/advisories/new", text)
        self.assertIn("Do not open a public GitHub issue", text)

    def test_generate_sbom_lists_workspace_and_known_crates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "sbom.cdx.json"
            subprocess.run(
                [
                    "python3",
                    str(ROOT / "scripts" / "generate_sbom.py"),
                    "--output",
                    str(output),
                    "--root",
                    str(ROOT),
                ],
                check=True,
                cwd=ROOT,
            )
            bom = json.loads(output.read_text())
        self.assertEqual(bom["bomFormat"], "CycloneDX")
        self.assertEqual(bom["specVersion"], "1.5")
        self.assertEqual(bom["metadata"]["component"]["name"], "ruvio")
        purls = {component["purl"] for component in bom["components"]}
        self.assertTrue(any(purl.startswith("pkg:cargo/rustls@") for purl in purls))
        self.assertTrue(any(purl.startswith("pkg:cargo/mlua@") for purl in purls))
        root_ref = bom["metadata"]["component"]["purl"]
        root_deps = next(item for item in bom["dependencies"] if item["ref"] == root_ref)
        self.assertTrue(root_deps["dependsOn"])

    def test_in_memory_graph_round_trips_direct_dependencies(self) -> None:
        metadata = {
            "packages": [
                {
                    "id": "ruvio 0.1.0",
                    "name": "ruvio",
                    "version": "0.1.0",
                    "source": None,
                    "license": None,
                },
                {
                    "id": "rustls 0.23.0",
                    "name": "rustls",
                    "version": "0.23.0",
                    "source": "registry+https://github.com/rust-lang/crates.io-index",
                    "license": "Apache-2.0 OR ISC OR MIT",
                },
            ],
            "resolve": {
                "root": "ruvio 0.1.0",
                "nodes": [
                    {
                        "id": "ruvio 0.1.0",
                        "deps": [{"name": "rustls", "pkg": "rustls 0.23.0"}],
                    },
                    {"id": "rustls 0.23.0", "deps": []},
                ],
            },
        }
        bom = sbom.generate(metadata)
        self.assertEqual(bom["components"][0]["purl"], "pkg:cargo/rustls@0.23.0")
        self.assertEqual(
            bom["dependencies"][1]["dependsOn"],
            ["pkg:cargo/rustls@0.23.0"],
        )


if __name__ == "__main__":
    unittest.main()
