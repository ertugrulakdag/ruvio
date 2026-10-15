#!/usr/bin/env python3
"""Write licenses/THIRD_PARTY_NOTICES from the locked dependency graph.

The ruvio binary embeds that file. Normal and build dependencies of every
feature that can be linked into a release binary are included. Dev-dependencies
are not.
"""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "licenses" / "THIRD_PARTY_NOTICES"
LICENSE_PREFIXES = ("license", "licence", "copying", "notice", "authors")


def metadata() -> dict:
    completed = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    return json.loads(completed.stdout)


def runtime_package_ids(meta: dict) -> set[str]:
    packages = {package["id"]: package for package in meta["packages"]}
    root = next(package["id"] for package in meta["packages"] if package["name"] == "ruvio")
    nodes = {node["id"]: node for node in meta["resolve"]["nodes"]}
    seen = set()
    stack = [root]

    while stack:
        package_id = stack.pop()

        if package_id in seen:
            continue

        seen.add(package_id)

        for dep in nodes[package_id]["deps"]:
            kinds = {kind["kind"] for kind in dep["dep_kinds"]}

            if kinds <= {"dev"}:
                continue

            stack.append(dep["pkg"])

    seen.remove(root)

    return {package_id for package_id in seen if package_id in packages}


def license_files(manifest: Path) -> list[Path]:
    directory = manifest.parent
    found = []

    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue

        stem = path.name.lower().split(".", 1)[0]

        if stem.startswith(LICENSE_PREFIXES) or path.name.lower() in {"authors"}:
            found.append(path)

    return found


def declared_terms(spdx: str, authors: list[str]) -> str:
    holder = ", ".join(authors) if authors else "the copyright holders"
    apache = (ROOT / "src/hashbrown/LICENSE-APACHE").read_text().rstrip()
    mit = (
        f"Copyright (c) {holder}\n\n"
        + (ROOT / "src/hashbrown/LICENSE-MIT").read_text().split("\n", 2)[-1].rstrip()
    )
    normalized = spdx.replace("/", " OR ")
    parts = []

    if "MIT" in normalized:
        parts.append(mit)

    if "Apache-2.0" in normalized:
        parts.append(apache)

    if not parts:
        return (
            "This crate does not ship a license file. "
            f"Its package metadata declares {spdx}."
        )

    elected = normalized

    if "LGPL" in elected:
        elected = "MIT OR Apache-2.0 (the LGPL-2.1-or-later alternative is not used)"

    return f"Distributed under {elected}.\n\n" + "\n\n".join(parts)


def main() -> None:
    meta = metadata()
    packages = {package["id"]: package for package in meta["packages"]}
    selected = sorted(
        (packages[package_id] for package_id in runtime_package_ids(meta)),
        key=lambda package: (package["name"], package["version"]),
    )
    parts = [
        "Ruvio. Copyright (c) 2026 Salih Cantekin.",
        "Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0-only).",
        "",
        Path(ROOT / "LICENSE").read_text(),
        "",
        "The ruvio binary statically includes the third-party components below.",
        "Their license texts follow the Ruvio license above.",
        "",
        "---- vendored src/hashbrown ----",
        "Apache-2.0 OR MIT",
        "",
        (ROOT / "src/hashbrown/LICENSE-MIT").read_text().rstrip(),
        "",
        (ROOT / "src/hashbrown/LICENSE-APACHE").read_text().rstrip(),
        "",
    ]
    for package in selected:
        name = f'{package["name"]} {package["version"]}'
        spdx = package.get("license") or "NOASSERTION"
        files = license_files(Path(package["manifest_path"]))
        parts.append(f"---- {name} ----")
        parts.append(spdx)
        parts.append("")

        if not files:
            parts.append(declared_terms(spdx, package.get("authors") or []))
            parts.append("")
            continue

        for path in files:
            parts.append(f"[{path.name}]")
            parts.append(path.read_text(errors="replace").rstrip())
            parts.append("")

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text("\n".join(parts).rstrip() + "\n")
    print(f"wrote {OUT} ({len(selected)} packages)")


if __name__ == "__main__":
    main()
