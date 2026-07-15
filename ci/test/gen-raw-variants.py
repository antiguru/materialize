#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Adds `raw` variants (existence-only: `{name, kind: raw}`, no columns, no
description) to documented parent relations, one per `*_raw` differential
dataflow logging input found in the markdown's `RELATION_SPEC_UNDOCUMENTED`
markers.
"""

import re

RAW_MARKER_RE = re.compile(r"RELATION_SPEC_UNDOCUMENTED mz_introspection\.(\w+_raw)\b")
PARENT_RE = re.compile(r"<!-- RELATION_SPEC \w+\.(\w+) FROM_YAML -->")
LINKDEF_RE = re.compile(r"^\[[^\]]+\]:\s")


def raw_parents(md_text: str) -> list[tuple[str, str]]:
    """Pair each `*_raw` marker in `md_text` with its nearest preceding
    documented relation, by scanning line by line.

    A `RELATION_SPEC ... FROM_YAML` line sets the current parent. A
    reference-link-definition line (`[label]: url`) resets the parent to
    none: link definitions are conventionally collected in a trailing block
    at the end of the file, so any `_raw` marker after one belongs to no
    relation on this page (an orphan) rather than to the last documented one.
    """
    parent = None
    ended = False
    pairs = []
    for line in md_text.splitlines():
        if LINKDEF_RE.match(line):
            ended = True
            continue
        m = PARENT_RE.search(line)
        if m:
            parent = m.group(1)
            continue
        m = RAW_MARKER_RE.search(line)
        if m and not ended and parent is not None:
            pairs.append((m.group(1), parent))
    return pairs


def add_raw_variants(ydoc: dict, md_text: str) -> tuple[dict, str]:
    """Add a `{name, kind: raw}` variant to each parent relation in `ydoc`
    for every `*_raw` marker in `md_text`, and strip the corresponding
    `RELATION_SPEC_UNDOCUMENTED` marker line.

    Raw variants carry no columns and no description: the shortcode renders
    all of a relation's raw variants together under one grouped warning, so
    per-variant content would never be shown. Markers with no documented
    parent on the page (orphans, e.g. a trailing marker after the reference
    link-definition block) are left in place for a later phase.
    """
    relations_by_name = {r["name"]: r for r in ydoc["relations"]}

    for raw_name, parent in raw_parents(md_text):
        relation = relations_by_name[parent]
        variants = relation.setdefault("variants", [])
        if any(v["name"] == raw_name for v in variants):
            continue
        variants.append({"name": raw_name, "kind": "raw"})
        marker = f"<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.{raw_name} -->"
        marker_re = re.escape(marker)
        # A marker line sitting alone between two blank lines (the common
        # case) leaves a doubled blank line if we only drop the marker
        # itself, so consume one of the surrounding blank lines too. A
        # marker that instead sits next to sibling markers, with no blank
        # line between them, is removed on its own: its neighbors already
        # supply the correct blank-line spacing.
        md_text = re.sub(
            rf"(?<=\n)\n{marker_re}\n(?=\n)|{marker_re}\n?",
            "",
            md_text,
        )

    return ydoc, md_text


if __name__ == "__main__":
    import os
    import sys

    import yaml

    md_path = sys.argv[
        1
    ]  # e.g. doc/user/content/reference/system-catalog/mz_introspection.md

    md_text = open(md_path, encoding="utf-8").read()

    schema_name = os.path.splitext(os.path.basename(md_path))[0]
    data_path = os.path.join("doc", "user", "data", f"{schema_name}.yml")
    ydoc = yaml.safe_load(open(data_path, encoding="utf-8"))

    ydoc, md_text = add_raw_variants(ydoc, md_text)

    with open(data_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(ydoc, f, sort_keys=False, allow_unicode=True, width=10_000)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_text)

    print(f"updated {data_path} and {md_path}")
