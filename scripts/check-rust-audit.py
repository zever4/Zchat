#!/usr/bin/env python3
"""Report dependency advisories without hiding a failed cargo-audit execution."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def findings(report):
    if not isinstance(report, dict) or not isinstance(report.get("database"), dict) or not isinstance(report.get("lockfile"), dict):
        raise ValueError("missing audit database/lockfile metadata")
    database, lockfile = report["database"], report["lockfile"]
    if (not isinstance(database.get("last-commit"), str)
            or not re.fullmatch(r"[0-9a-f]{40,64}", database["last-commit"])
            or type(database.get("advisory-count")) is not int or database["advisory-count"] < 0
            or type(lockfile.get("dependency-count")) is not int or lockfile["dependency-count"] < 0):
        raise ValueError("incomplete audit database/lockfile metadata")
    vulns = report.get("vulnerabilities", {})
    warnings = report.get("warnings")
    if not isinstance(vulns, dict) or not isinstance(warnings, dict):
        raise ValueError("invalid audit report")
    entries = vulns.get("list")
    if (not isinstance(entries, list) or type(vulns.get("count")) is not int
            or vulns["count"] != len(entries) or type(vulns.get("found")) is not bool
            or vulns["found"] != bool(entries)):
        raise ValueError("inconsistent vulnerability results")
    rows = [("vulnerability", row) for row in entries]
    for kind, group in warnings.items():
        if not isinstance(group, list):
            raise ValueError("invalid advisory warnings")
        rows.extend((kind, row) for row in group)
    for _, row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get("package"), dict)
                or not all(isinstance(row["package"].get(k), str) for k in ["name", "version"])
                or row.get("advisory") is not None and not isinstance(row["advisory"], dict)):
            raise ValueError("invalid advisory entry")
    return len(entries), rows


def annotation(level, text):
    # An advisory title must never inject another Actions workflow command.
    escaped = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::{level}::{escaped}")


def check(report_path):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(["cargo", "audit", "--json"], capture_output=True, text=True, check=False)
        report_path.write_text(result.stdout, encoding="utf-8")
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        count, rows = findings(json.loads(result.stdout))
        # Exit1 is advisory-only ONLY with a complete vulnerability report.
        # Fetch/tool/config/lockfile errors must still fail the check.
        if result.returncode not in (0, 1) or result.returncode == 1 and count == 0:
            raise ValueError(f"cargo-audit did not complete successfully (exit {result.returncode})")
        summary = ["## Rust dependency audit", "", f"Vulnerabilities: {count}; warnings: {len(rows) - count}.",
                   "Advisories are non-blocking in development CI. Run plain `cargo audit` before distribution.", ""]
        for kind, row in rows:
            package = row["package"]
            advisory = row.get("advisory") or {}
            description = f"{kind}: {package['name']} {package['version']}: {advisory.get('id', kind)} — {advisory.get('title', kind)}"
            annotation("warning", description)
            identifier = advisory.get("id", "")
            link = f" ([RustSec](https://rustsec.org/advisories/{identifier}.html))" if isinstance(identifier, str) and re.fullmatch(r"RUSTSEC-\d{4}-\d{4}", identifier) else ""
            summary.append("- " + description.replace("\r", " ").replace("\n", " ") + link)
        print(summary[2])
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as output:
                output.write("\n".join(summary) + "\n")
        return 0
    except (OSError, ValueError, TypeError) as error:
        annotation("error", f"Rust dependency audit did not complete: {error}")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("out/cargo-audit.json"))
    sys.exit(check(parser.parse_args().report))
