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
    "gen_per_worker_variants",
    pathlib.Path(__file__).parent / "gen-per-worker-variants.py",
)
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)


def test_parent_of_direct_and_alias():
    docd = {"mz_dataflows", "mz_dataflow_global_ids", "mz_lir_mapping"}
    assert gen.parent_of("mz_dataflows_per_worker", docd) == "mz_dataflows"
    assert (
        gen.parent_of("mz_compute_dataflow_global_ids_per_worker", docd)
        == "mz_dataflow_global_ids"
    )
    assert gen.parent_of("mz_compute_lir_mapping_per_worker", docd) == "mz_lir_mapping"
    assert gen.parent_of("mz_orphan_per_worker", docd) is None


def test_add_variants_injects_and_removes_marker():
    ydoc = {
        "relations": [
            {
                "name": "mz_dataflows",
                "description": "d",
                "columns": [{"name": "id", "type": "uint8"}],
            }
        ]
    }
    md = (
        "## `mz_dataflows`\n\n"
        "<!-- RELATION_SPEC mz_introspection.mz_dataflows FROM_YAML -->\n"
        '{{< catalog-relation schema="mz_introspection" name="mz_dataflows" >}}\n\n'
        "<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflows_per_worker -->\n"
    )
    catalog = {
        "mz_dataflows_per_worker": [
            {"name": "id", "type": "uint8"},
            {"name": "worker_id", "type": "uint8"},
            {"name": "name", "type": "text"},
        ]
    }
    ydoc2, md2 = gen.add_variants(ydoc, md, catalog)
    rel = ydoc2["relations"][0]
    assert rel["variants"][0]["name"] == "mz_dataflows_per_worker"
    assert rel["variants"][0]["kind"] == "per_worker"
    assert rel["variants"][0]["columns"] == catalog["mz_dataflows_per_worker"]
    assert (
        "RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflows_per_worker" not in md2
    )
