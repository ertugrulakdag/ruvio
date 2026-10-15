# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""The key to hash slot vectors in clients/testdata/slots.json are shared by the server and
every client."""

from __future__ import annotations

import json
import os

from ruvio import _cluster as cl

VECTORS = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "slots.json")


def test_key_slot_matches_the_shared_vectors():
    with open(VECTORS, encoding="utf-8") as f:
        keys = json.load(f)["keys"]
    assert keys
    for entry in keys:
        assert cl.key_slot(entry["key"]) == entry["slot"], entry["key"]
        assert cl.key_slot(entry["key"].encode()) == entry["slot"], entry["key"]
