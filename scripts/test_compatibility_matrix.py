#!/usr/bin/env python3

import json
import tempfile
import unittest
from pathlib import Path

import check_compatibility_matrix as matrix


SAMPLE_PARSER = """
pub(crate) fn parse_arguments(mut arguments: Vec<Bytes<'_>>) -> Result<Command<'_>, ProtocolError> {
    let name = arguments.remove(0);
    if name.eq_ignore_ascii_case(b"PING") {
        return Ok(Command::Ping(None));
    }
    if name.eq_ignore_ascii_case(b"GET") {
        return Ok(Command::Get(arguments.pop().unwrap()));
    }
    if name.eq_ignore_ascii_case(b"SCRIPT") {
        return parse_script(arguments);
    }
}

fn parse_eval() {}
"""


class CompatibilityMatrixTests(unittest.TestCase):
    def test_extracts_parser_commands_and_composites(self) -> None:
        self.assertEqual(
            matrix.extract_parser_commands(SAMPLE_PARSER),
            {
                "PING",
                "GET",
                "SCRIPT LOAD",
                "SCRIPT EXISTS",
                "SCRIPT FLUSH",
                "SCRIPT KILL",
            },
        )

    def test_extracts_supported_column_only(self) -> None:
        markdown = """
## Current Commands

| Family | Supported | Current limitations |
| --- | --- | --- |
| Strings | `GET`, `SET` | `SET GET` is not. `PUBLISH` later. |

## Data Semantics
"""
        self.assertEqual(
            matrix.extract_table_commands(
                markdown, "## Current Commands", "## Data Semantics"
            ),
            {"GET", "SET"},
        )

    def test_repo_matrix_is_valid(self) -> None:
        errors = matrix.validate(matrix.load_matrix())
        self.assertEqual(errors, [])

    def test_rejects_parser_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "docs").mkdir()
            (root / "src").mkdir()
            (root / "scripts").mkdir()
            payload = {
                "version": 1,
                "protocol": {"baseline": "RESP2", "resp3": False},
                "client_coverage": ["GET"],
                "commands": [
                    {
                        "name": "GET",
                        "family": "strings",
                        "status": "supported",
                        "same_slot": False,
                        "limitations": "",
                        "coverage": ["scripts/cover.py"],
                    }
                ],
                "clients": [
                    {
                        "id": language,
                        "protocol": "RESP2",
                        "version": "1",
                        "suite": "scripts/cover.py",
                    }
                    for language in ("python", "node", "rust", "go", "java")
                ],
            }
            (root / "docs" / "compatibility-matrix.json").write_text(
                json.dumps(payload), encoding="utf-8"
            )
            (root / "docs" / "COMPATIBILITY.md").write_text(
                "## Current Commands\n\n| Family | Supported | Current limitations |\n"
                "| --- | --- | --- |\n| Strings | `GET` | |\n\n## Data Semantics\n",
                encoding="utf-8",
            )
            (root / "docs" / "DATA_TYPES.md").write_text(
                "## Supported Commands\n\n| Type | Commands | Internal representation |\n"
                "| --- | --- | --- |\n| Bytes | `GET` | boxed |\n\n## Current Limits\n",
                encoding="utf-8",
            )
            (root / "src" / "protocol" / "parse").mkdir(parents=True)
            (root / "src" / "protocol" / "parse" / "mod.rs").write_text(
                SAMPLE_PARSER, encoding="utf-8"
            )
            (root / "src" / "protocol" / "table.rs").write_text("", encoding="utf-8")
            (root / "scripts" / "cover.py").write_text("GET\n", encoding="utf-8")

            errors = matrix.validate(payload, root=root)
            self.assertTrue(
                any("matrix missing parser commands" in error for error in errors)
            )


if __name__ == "__main__":
    unittest.main()
