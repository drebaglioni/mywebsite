#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ.get("LIBRARY_SOURCE", "/Users/andrea/Obsidian/Aristotle/03_Reading/Library.md"))
SITE_JSON = REPO_ROOT / "data" / "library.json"
REPORT = REPO_ROOT / ".context" / "weekly_library_check.md"
LIVE_JSON_URL = "https://drebaglioni.com/data/library.json"


def run(cmd: list[str], *, cwd: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)


def split_table_row(row: str) -> list[str]:
    row = row.strip()
    if row.startswith("|"):
        row = row[1:]
    if row.endswith("|"):
        row = row[:-1]
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", row)]


def export_table_rows() -> list[tuple[int, str]]:
    lines = SOURCE.read_text().splitlines()
    export_start = next((idx + 1 for idx, line in enumerate(lines) if line.strip() == "## Export"), None)
    if export_start is None:
        raise RuntimeError("Could not find ## Export in Obsidian Library.md")

    rows: list[tuple[int, str]] = []
    for idx in range(export_start, len(lines)):
        line = lines[idx]
        if not line.strip():
            if rows:
                break
            continue
        if line.lstrip().startswith("|"):
            rows.append((idx + 1, line))
        elif rows:
            break
    if len(rows) < 3:
        raise RuntimeError("Export table is missing header, separator, or data rows")
    return rows


def check_table() -> tuple[bool, list[str]]:
    issues: list[str] = []
    rows = export_table_rows()
    header_width = len(split_table_row(rows[0][1]))

    for line_no, row in rows[1:]:
        width = len(split_table_row(row))
        if width != header_width:
            issues.append(f"Line {line_no}: expected {header_width} columns, found {width}")

    for line_no, row in rows[2:]:
        for match in re.finditer(r"\[\[([^\]]+)\]\]", row):
            if re.search(r"(?<!\\)\|", match.group(1)):
                issues.append(f"Line {line_no}: unescaped wikilink alias pipe")

    return len(issues) == 0, issues


def load_json(path: Path) -> object:
    return json.loads(path.read_text())


def normalized_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def check_sync_drift() -> tuple[bool, str, int | None]:
    with tempfile.TemporaryDirectory(prefix="library-sync-check-") as tmp_dir:
        tmp_out = Path(tmp_dir) / "library.json"
        result = run([
            sys.executable,
            str(REPO_ROOT / "scripts" / "sync_library.py"),
            "--source",
            str(SOURCE),
            "--out",
            str(tmp_out),
        ])
        if result.returncode != 0:
            return False, result.stderr.strip() or result.stdout.strip(), None

        generated = load_json(tmp_out)
        current = load_json(SITE_JSON)
        item_count = len(generated) if isinstance(generated, list) else None
        if normalized_json(generated) != normalized_json(current):
            return False, "Obsidian export differs from data/library.json; run sync_library.py.", item_count
        return True, "Obsidian export matches data/library.json.", item_count


def check_live_drift() -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(LIVE_JSON_URL, timeout=20) as response:
            live = json.loads(response.read().decode("utf-8"))
        local = load_json(SITE_JSON)
        if normalized_json(live) != normalized_json(local):
            return False, "Live data/library.json differs from local data/library.json."
        return True, "Live data/library.json matches local data/library.json."
    except Exception as error:  # noqa: BLE001 - report exact operational failure.
        return False, f"Could not check live JSON: {error}"


def check_git_status() -> tuple[bool, str]:
    result = run(["git", "status", "--short", "--branch"])
    text = result.stdout.strip()
    dirty_lines = [line for line in text.splitlines() if line and not line.startswith("## ")]
    return len(dirty_lines) == 0, text


def main() -> int:
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    sections: list[str] = [f"# Weekly Library Check\n\nGenerated: {now}\n"]
    failed = False

    table_ok, table_issues = check_table()
    failed = failed or not table_ok
    sections.append("## Obsidian Table\n")
    sections.append("- Status: " + ("OK" if table_ok else "Needs attention"))
    if table_issues:
        sections.extend(f"- {issue}" for issue in table_issues)

    sync_ok, sync_message, item_count = check_sync_drift()
    failed = failed or not sync_ok
    sections.append("\n## Sync Drift\n")
    sections.append("- Status: " + ("OK" if sync_ok else "Needs attention"))
    sections.append(f"- {sync_message}")
    if item_count is not None:
        sections.append(f"- Generated item count: {item_count}")

    git_ok, git_text = check_git_status()
    failed = failed or not git_ok
    sections.append("\n## Git State\n")
    sections.append("- Status: " + ("OK" if git_ok else "Needs attention"))
    sections.append("```text\n" + git_text + "\n```")

    live_ok, live_message = check_live_drift()
    failed = failed or not live_ok
    sections.append("\n## Live Data\n")
    sections.append("- Status: " + ("OK" if live_ok else "Needs attention"))
    sections.append(f"- {live_message}")

    sections.append("\n## Result\n")
    sections.append("Archive-ready." if not failed else "Review the sections marked Needs attention.")

    REPORT.write_text("\n".join(sections) + "\n")
    print(f"Wrote {REPORT}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
