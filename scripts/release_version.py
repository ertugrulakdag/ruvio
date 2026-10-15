#!/usr/bin/env python3
"""Choose a package version and write it into the manifest.

An empty --set bumps the patch number already in the file. A set value is
written as given. The chosen version is printed on stdout.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

PACKAGES = {
    "server": {
        "cargo": [ROOT / "Cargo.toml"],
        "locks": [(ROOT / "Cargo.lock", "ruvio")],
        "csproj": [],
    },
    "rust-client": {
        "cargo": [ROOT / "clients" / "ruvio-client" / "Cargo.toml"],
        "locks": [
            (ROOT / "clients" / "ruvio-client" / "Cargo.lock", "ruvio-client"),
            (ROOT / "clients" / "ruvio-ui" / "Cargo.lock", "ruvio-client"),
        ],
        "csproj": [],
        "matrix": "ruvio-client",
    },
    "csharp": {
        "cargo": [],
        "locks": [],
        "csproj": [
            ROOT / "clients" / "csharp" / "Ruvio.Client" / "Ruvio.Client.csproj",
            ROOT
            / "clients"
            / "csharp"
            / "Ruvio.Client.AspNetCore"
            / "Ruvio.Client.AspNetCore.csproj",
        ],
        "matrix": "ruvio-csharp",
    },
    "ui": {
        "cargo": [ROOT / "clients" / "ruvio-ui" / "Cargo.toml"],
        "locks": [(ROOT / "clients" / "ruvio-ui" / "Cargo.lock", "ruvio-ui")],
        "csproj": [],
    },
    "node": {"file": ROOT / "clients" / "node" / "package.json", "kind": "node"},
    "python": {
        "file": ROOT / "clients" / "python" / "pyproject.toml",
        "kind": "python",
        "dunder": ROOT / "clients" / "python" / "ruvio" / "__init__.py",
    },
    "java": {"file": ROOT / "clients" / "java" / "pom.xml", "kind": "java"},
}


MATRIX = ROOT / "docs" / "compatibility-matrix.json"


def write_matrix_pin(path: Path, client_id: str, version: str) -> None:
    """Keep the client version pinned in the compatibility matrix equal to the manifest."""
    text = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'("id": "' + re.escape(client_id) + r'"[^}]*?"version": ")([^"]+)(")', re.S
    )
    text, count = pattern.subn(lambda m: m.group(1) + version + m.group(3), text, count=1)

    if count != 1:
        raise ValueError(f"{path} has no client {client_id}")

    path.write_text(text, encoding="utf-8")


def bump_patch(version: str) -> str:
    match = SEMVER.fullmatch(version)

    if match is None:
        raise ValueError(f"not a major.minor.patch version: {version}")

    major, minor, patch = match.groups()

    return f"{major}.{minor}.{int(patch) + 1}"


def normalize(version: str) -> str:
    text = version.strip()

    if text.startswith("v"):
        text = text[1:]

    if SEMVER.fullmatch(text) is None:
        raise ValueError(f"not a major.minor.patch version: {version}")

    return text


def read_cargo_version(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith('version = "'):
            return line.split('"', 2)[1]

    raise ValueError(f"no package version in {path}")


def write_cargo_version(path: Path, version: str) -> None:
    current = read_cargo_version(path)
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace(f'version = "{current}"', f'version = "{version}"', 1), encoding="utf-8")


def write_lock_version(path: Path, name: str, version: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    replaced = False

    for index, line in enumerate(lines[:-1]):
        if line.strip() != "[[package]]":
            continue

        if lines[index + 1].strip() != f'name = "{name}"':
            continue

        lines[index + 2] = f'version = "{version}"\n'
        replaced = True
        break

    if not replaced:
        raise ValueError(f"{path} has no package {name}")

    path.write_text("".join(lines), encoding="utf-8")


def read_csproj_version(path: Path) -> str:
    match = re.search(r"<Version>([^<]+)</Version>", path.read_text(encoding="utf-8"))

    if match is None:
        raise ValueError(f"no Version in {path}")

    return match.group(1)


def write_csproj_version(path: Path, version: str) -> None:
    current = read_csproj_version(path)
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace(f"<Version>{current}</Version>", f"<Version>{version}</Version>", 1), encoding="utf-8")


def read_other_version(spec: dict) -> str:
    text = spec["file"].read_text(encoding="utf-8")
    kind = spec["kind"]

    if kind == "node":
        return json.loads(text)["version"]

    if kind == "python":
        match = re.search(r'^version = "([^"]+)"', text, re.M)
    else:
        match = re.search(r"<artifactId>ruvio-client</artifactId>\s*<version>([^<]+)</version>", text)

    if match is None:
        raise ValueError(f"no version in {spec['file']}")

    return match.group(1)


def write_other_version(spec: dict, version: str) -> None:
    path = spec["file"]
    current = read_other_version(spec)
    text = path.read_text(encoding="utf-8")
    kind = spec["kind"]

    if kind == "node":
        text = text.replace(f'"version": "{current}"', f'"version": "{version}"', 1)
        lock = path.with_name("package-lock.json")

        if lock.exists():
            lock_text = lock.read_text(encoding="utf-8")
            lock_text = lock_text.replace(f'"version": "{current}"', f'"version": "{version}"', 2)
            lock.write_text(lock_text, encoding="utf-8")
    elif kind == "python":
        text = text.replace(f'version = "{current}"', f'version = "{version}"', 1)
        dunder = spec.get("dunder")

        if dunder is not None and dunder.exists():
            module = dunder.read_text(encoding="utf-8")
            module, count = re.subn(
                r'(?m)^__version__ = "[^"]+"', f'__version__ = "{version}"', module, count=1
            )

            if count != 1:
                raise ValueError(f"no __version__ in {dunder}")

            dunder.write_text(module, encoding="utf-8")
    else:
        text = text.replace(
            f"<artifactId>ruvio-client</artifactId>\n  <version>{current}</version>",
            f"<artifactId>ruvio-client</artifactId>\n  <version>{version}</version>",
            1,
        )

    path.write_text(text, encoding="utf-8")


def current_version(package: str) -> str:
    spec = PACKAGES[package]

    if "kind" in spec:
        return read_other_version(spec)

    if spec["cargo"]:
        return read_cargo_version(spec["cargo"][0])

    return read_csproj_version(spec["csproj"][0])


def apply_version(package: str, requested: str, write: bool) -> str:
    current = current_version(package)

    if requested.strip():
        chosen = normalize(requested)
    elif write:
        chosen = bump_patch(current)
    else:
        chosen = current

    if not write or chosen == current:
        return chosen

    spec = PACKAGES[package]

    if "kind" in spec:
        write_other_version(spec, chosen)
        return chosen

    for path in spec["cargo"]:
        write_cargo_version(path, chosen)

    for path, name in spec["locks"]:
        write_lock_version(path, name, chosen)

    for path in spec["csproj"]:
        write_csproj_version(path, chosen)

    if "matrix" in spec:
        write_matrix_pin(MATRIX, spec["matrix"], chosen)

    return chosen


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("package", choices=sorted(PACKAGES))
    parser.add_argument("--set", default="", help="Exact version. Empty bumps the patch.")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    print(apply_version(args.package, args.set, args.write))


if __name__ == "__main__":
    main()
