# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import importlib.util
import pathlib

_spec = importlib.util.spec_from_file_location(
    "gen_raw_variants",
    pathlib.Path(__file__).parent / "gen-raw-variants.py",
)
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)

MD = (
    "## `mz_foo`\n\n"
    "<!-- RELATION_SPEC mz_introspection.mz_foo FROM_YAML -->\n"
    '{{< catalog-relation schema="mz_introspection" name="mz_foo" >}}\n\n'
    "<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_foo_raw -->\n"
    "<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_foo_extra_raw -->\n\n"
    "## `mz_bar`\n\n"
    "<!-- RELATION_SPEC mz_introspection.mz_bar FROM_YAML -->\n"
    '{{< catalog-relation schema="mz_introspection" name="mz_bar" >}}\n\n'
    "<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_bar_raw -->\n\n"
    "[`text`]: /sql/types/text\n\n"
    "<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_orphan_raw -->\n"
)


def test_raw_parents_attributes_and_resets_at_linkdefs():
    pairs = gen.raw_parents(MD)
    assert ("mz_foo_raw", "mz_foo") in pairs
    assert ("mz_foo_extra_raw", "mz_foo") in pairs
    assert ("mz_bar_raw", "mz_bar") in pairs
    # Orphan after the link-def block is skipped.
    assert all(name != "mz_orphan_raw" for name, _ in pairs)
    assert len(pairs) == 3


def test_add_raw_variants_injects_and_removes_markers():
    ydoc = {
        "relations": [
            {"name": "mz_foo", "columns": []},
            {"name": "mz_bar", "columns": []},
        ]
    }
    ydoc2, md2 = gen.add_raw_variants(ydoc, MD)
    foo = next(r for r in ydoc2["relations"] if r["name"] == "mz_foo")
    assert {v["name"] for v in foo["variants"]} == {"mz_foo_raw", "mz_foo_extra_raw"}
    assert all(v["kind"] == "raw" for v in foo["variants"])
    assert all("columns" not in v for v in foo["variants"])
    assert "RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_foo_raw" not in md2
    # Orphan marker left in place.
    assert "RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_orphan_raw" in md2
