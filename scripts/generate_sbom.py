#!/usr/bin/env python3
"""Emit a CycloneDX 1.5 SBOM from `cargo metadata`."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def cargo_metadata(root: Path) -> dict[str, Any]:
    completed = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--all-features"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def package_id(package: dict[str, Any]) -> str:
    return f"pkg:cargo/{package['name']}@{package['version']}"


def component(package: dict[str, Any], scope: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": "library" if package.get("source") else "application",
        "bom-ref": package_id(package),
        "name": package["name"],
        "version": package["version"],
        "purl": package_id(package),
        "scope": scope,
    }
    license_id = package.get("license")
    if license_id:
        entry["licenses"] = [{"license": {"id": license_id}}]
    return entry


def generate(metadata: dict[str, Any]) -> dict[str, Any]:
    packages = {package["id"]: package for package in metadata["packages"]}
    resolve = metadata["resolve"]
    root_id = resolve["root"]
    root = packages[root_id]

    nodes = {node["id"]: node for node in resolve["nodes"]}
    components = []
    dependencies = []
    seen: set[str] = set()

    for package_key, package in packages.items():
        if package_key == root_id:
            continue
        ref = package_id(package)
        if ref in seen:
            continue
        seen.add(ref)
        components.append(component(package, "required"))

    def depends_on(node_id: str) -> list[str]:
        node = nodes.get(node_id, {})
        refs = []
        for dep in node.get("deps", []):
            pkg = packages.get(dep["pkg"])
            if pkg is None:
                continue
            refs.append(package_id(pkg))
        return sorted(set(refs))

    dependencies.append(
        {"ref": package_id(root), "dependsOn": depends_on(root_id)}
    )
    for node_id in nodes:
        if node_id == root_id:
            continue
        pkg = packages[node_id]
        dependencies.append(
            {"ref": package_id(pkg), "dependsOn": depends_on(node_id)}
        )

    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "component": {
                "type": "application",
                "bom-ref": package_id(root),
                "name": root["name"],
                "version": root["version"],
                "purl": package_id(root),
            }
        },
        "components": sorted(components, key=lambda item: item["purl"]),
        "dependencies": sorted(dependencies, key=lambda item: item["ref"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON here instead of stdout",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Cargo workspace root",
    )
    args = parser.parse_args()
    bom = generate(cargo_metadata(args.root))
    payload = json.dumps(bom, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload)
    else:
        sys.stdout.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
