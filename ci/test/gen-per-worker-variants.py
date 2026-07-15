#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Adds `per_worker` variants (Task 1's refined shape: a full catalog-sourced
column list, no meanings) to documented parent relations, sourced from the
live catalog rather than hand-invented.
"""

import re

PER_WORKER_SUFFIX = "_per_worker"
COMPUTE_PREFIX = "mz_compute_"
MZ_PREFIX = "mz_"


def parent_of(name: str, documented: set[str]) -> str | None:
    """Resolve a `_per_worker` relation name to its documented parent's name,
    or `None` if no documented parent exists (an orphan).

    The parent is usually the name with the `_per_worker` suffix stripped.
    A handful of per-worker relations additionally carry a `mz_compute_`
    prefix that their (older) global view lacks, e.g.
    `mz_compute_lir_mapping_per_worker` -> `mz_lir_mapping`; that alias is
    tried second.
    """
    if not name.endswith(PER_WORKER_SUFFIX):
        return None
    base = name[: -len(PER_WORKER_SUFFIX)]
    if base in documented:
        return base
    if base.startswith(COMPUTE_PREFIX):
        aliased = MZ_PREFIX + base[len(COMPUTE_PREFIX) :]
        if aliased in documented:
            return aliased
    return None


def add_variants(
    ydoc: dict, md: str, catalog_columns: dict[str, list[dict]]
) -> tuple[dict, str]:
    """Add a `per_worker` variant entry to each parent relation in `ydoc` for
    every `*_per_worker` relation in `catalog_columns`, and strip the
    corresponding `RELATION_SPEC_UNDOCUMENTED` marker from `md`.

    Per-worker relations with no documented parent (orphans) are left
    untouched: their marker stays in `md` for a later phase to pick up.
    Relations that already have a variant of that name (e.g.
    `mz_active_peeks_per_worker`, added by hand before this generator
    existed) are skipped so the variant is not duplicated.
    """
    relations_by_name = {r["name"]: r for r in ydoc["relations"]}
    documented = set(relations_by_name)

    for pw_name, columns in catalog_columns.items():
        if not pw_name.endswith(PER_WORKER_SUFFIX):
            continue
        parent = parent_of(pw_name, documented)
        if parent is None:
            continue
        relation = relations_by_name[parent]
        variants = relation.setdefault("variants", [])
        if any(v["name"] == pw_name for v in variants):
            continue
        variants.append(
            {
                "name": pw_name,
                "kind": "per_worker",
                "description": f"The per-worker data underlying `{parent}`.",
                "columns": columns,
            }
        )
        marker = f"<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.{pw_name} -->"
        marker_re = re.escape(marker)
        # A marker line sitting alone between two blank lines (the common
        # case) leaves a doubled blank line if we only drop the marker
        # itself, so consume one of the surrounding blank lines too. A
        # marker that instead sits next to sibling markers, with no blank
        # line between them, is removed on its own: its neighbors already
        # supply the correct blank-line spacing.
        md = re.sub(
            rf"(?<=\n)\n{marker_re}\n(?=\n)|{marker_re}\n?",
            "",
            md,
        )

    return ydoc, md


if __name__ == "__main__":
    import os
    import sys

    import yaml

    md_path = sys.argv[
        1
    ]  # e.g. doc/user/content/reference/system-catalog/mz_introspection.md
    tsv_path = sys.argv[2]  # relation<TAB>column<TAB>type, position order

    md_text = open(md_path, encoding="utf-8").read()

    schema_name = os.path.splitext(os.path.basename(md_path))[0]
    data_path = os.path.join("doc", "user", "data", f"{schema_name}.yml")
    ydoc = yaml.safe_load(open(data_path, encoding="utf-8"))

    catalog_columns: dict[str, list[dict]] = {}
    with open(tsv_path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            relation, column, type_ = line.split("\t")
            catalog_columns.setdefault(relation, []).append(
                {"name": column, "type": type_}
            )

    ydoc, md_text = add_variants(ydoc, md_text, catalog_columns)

    with open(data_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(ydoc, f, sort_keys=False, allow_unicode=True, width=10_000)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_text)

    print(f"updated {data_path} and {md_path}")
