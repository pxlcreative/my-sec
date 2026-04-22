#!/usr/bin/env python3
"""Enforce per-bucket coverage thresholds against a coverage.xml report.

Buckets (filename prefix → floor env var):

    api/services/       COV_SERVICES_MIN
    api/celery_tasks/   COV_CELERY_MIN
    api/routes/         COV_ROUTES_MIN
    scripts/            COV_SCRIPTS_MIN

Exits 1 if any bucket falls below its floor, 0 otherwise. Missing buckets
(no matching files in the report) are treated as "no data" and skipped —
they do not pass, but they also do not fail the build. A warning is
printed so it's visible.
"""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

BUCKETS: tuple[tuple[str, str], ...] = (
    ("api/services/", "COV_SERVICES_MIN"),
    ("api/celery_tasks/", "COV_CELERY_MIN"),
    ("api/routes/", "COV_ROUTES_MIN"),
    ("scripts/", "COV_SCRIPTS_MIN"),
)


def _iter_classes(root: ET.Element):
    for cls in root.iter("class"):
        filename = cls.attrib.get("filename", "")
        lines_el = cls.find("lines")
        if lines_el is None:
            continue
        total = 0
        covered = 0
        for line in lines_el.findall("line"):
            total += 1
            if int(line.attrib.get("hits", "0")) > 0:
                covered += 1
        yield filename, covered, total


def main(path: str) -> int:
    report = Path(path)
    if not report.exists():
        print(f"ERROR: coverage report not found: {path}", file=sys.stderr)
        return 1

    tree = ET.parse(report)
    root = tree.getroot()

    totals: dict[str, tuple[int, int]] = {prefix: (0, 0) for prefix, _ in BUCKETS}
    for filename, covered, total in _iter_classes(root):
        for prefix, _ in BUCKETS:
            if filename.startswith(prefix) or f"/{prefix}" in f"/{filename}":
                c, t = totals[prefix]
                totals[prefix] = (c + covered, t + total)
                break

    failures: list[str] = []
    print(f"{'Bucket':<24} {'Covered':>10} {'Total':>8} {'Pct':>8} {'Floor':>8}")
    for prefix, env_var in BUCKETS:
        covered, total = totals[prefix]
        floor = float(os.environ.get(env_var, "0"))
        if total == 0:
            print(f"{prefix:<24} {'—':>10} {'—':>8} {'—':>8} {floor:>8.1f}  (no data)")
            print(
                f"WARNING: no coverage data for {prefix} — is it excluded from --cov?",
                file=sys.stderr,
            )
            continue
        pct = (covered / total) * 100.0
        marker = "" if pct >= floor else "  FAIL"
        print(f"{prefix:<24} {covered:>10} {total:>8} {pct:>7.1f}% {floor:>7.1f}%{marker}")
        if pct < floor:
            failures.append(f"{prefix}: {pct:.1f}% < {floor:.1f}%")

    if failures:
        print("\nCoverage threshold(s) not met:", file=sys.stderr)
        for line in failures:
            print(f"  - {line}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: check_coverage.py <coverage.xml>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
