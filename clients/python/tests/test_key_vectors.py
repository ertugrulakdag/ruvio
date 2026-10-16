# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""The command -> keys vectors in clients/testdata/key_vectors.json come from the server's parser;
the cluster key extraction must return exactly the keys the server routes each command by.

Sharded Pub/Sub (SPUBLISH, SSUBSCRIBE, SUNSUBSCRIBE) has no keys in the file, but the client routes
it by channel through the same function, so those three are skipped here."""

from __future__ import annotations

import json
import os

from ruvio import _cluster as cl

CHANNELS = {"SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE"}
VECTORS = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "key_vectors.json")


def test_command_keys_match_the_shared_vectors():
    with open(VECTORS, encoding="utf-8") as f:
        vectors = json.load(f)["vectors"]
    assert vectors
    wrong = []
    for v in vectors:
        if v["args"][0].upper() in CHANNELS:
            continue
        args = [a.encode("utf-8", "surrogateescape") for a in v["args"]]
        got = sorted(k.decode("utf-8", "surrogateescape") for k in cl.command_keys(args[0].upper(), args))
        if got != v["keys"]:
            wrong.append((v["args"], v["keys"], got))
    assert not wrong, "\n".join(f"{a}: want {w}, got {g}" for a, w, g in wrong[:50]) + f"\n({len(wrong)} wrong)"
