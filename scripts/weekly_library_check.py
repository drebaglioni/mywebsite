#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
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


def current_branch() -> str:
    result = run(["git", "branch", "--show-current"])
    return result.stdout.strip()


def dirty_status_lines() -> list[str]:
    result = run(["git", "status", "--short"])
    return [line for line in result.stdout.splitlines() if line.strip()]


def pull_latest_main() -> tuple[bool, str]:
    fetch_result = run(["git", "fetch", "origin"])
    if fetch_result.returncode != 0:
        return False, fetch_result.stderr.strip() or fetch_result.stdout.strip()

    pull_result = run(["git", "pull", "--ff-only", "origin", "main"])
    if pull_result.returncode != 0:
        return False, pull_result.stderr.strip() or pull_result.stdout.strip()

    return True, pull_result.stdout.strip() or "Already up to date."


def sync_library_json() -> tuple[bool, str, int | None, bool]:
    before_path = SITE_JSON.with_suffix(".json.weekly-check-before")
    shutil.copy2(SITE_JSON, before_path)
    try:
        result = run([
            sys.executable,
            str(REPO_ROOT / "scripts" / "sync_library.py"),
            "--source",
            str(SOURCE),
            "--out",
            str(SITE_JSON),
        ])
        if result.returncode != 0:
            shutil.copy2(before_path, SITE_JSON)
            return False, result.stderr.strip() or result.stdout.strip(), None, False

        synced = load_json(SITE_JSON)
        previous = load_json(before_path)
        item_count = len(synced) if isinstance(synced, list) else None
        changed = normalized_json(synced) != normalized_json(previous)
        message = "Synced Obsidian export into data/library.json."
        if not changed:
            message = "Obsidian export already matched data/library.json."
        return True, message, item_count, changed
    finally:
        before_path.unlink(missing_ok=True)


def commit_and_push_sync() -> tuple[bool, str | None, str]:
    add_result = run(["git", "add", str(SITE_JSON.relative_to(REPO_ROOT))])
    if add_result.returncode != 0:
        return False, None, add_result.stderr.strip() or add_result.stdout.strip()

    commit_result = run(["git", "commit", "-m", "Weekly library sync"])
    if commit_result.returncode != 0:
        return False, None, commit_result.stderr.strip() or commit_result.stdout.strip()

    commit_sha = run(["git", "rev-parse", "--short", "HEAD"]).stdout.strip()

    push_result = run(["git", "push", "origin", "main"])
    if push_result.returncode != 0:
        return False, commit_sha, push_result.stderr.strip() or push_result.stdout.strip()

    return True, commit_sha, push_result.stdout.strip() or push_result.stderr.strip()


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

    branch = current_branch()
    pre_sync_dirty = dirty_status_lines()
    can_publish = table_ok and branch == "main" and not pre_sync_dirty

    sections.append("\n## Git State\n")
    sections.append("- Branch: " + (branch or "(unknown)"))
    if pre_sync_dirty:
        failed = True
        sections.append("- Status: Needs attention")
        sections.append("```text\n" + "\n".join(pre_sync_dirty) + "\n```")
    elif branch != "main":
        failed = True
        sections.append("- Status: Needs attention")
        sections.append("- Weekly auto-sync only publishes from the main branch.")
    else:
        sections.append("- Status: OK")

    sync_changed = False
    if can_publish:
        pull_ok, pull_message = pull_latest_main()
        failed = failed or not pull_ok
        sections.append("\n## Pull Latest\n")
        sections.append("- Status: " + ("OK" if pull_ok else "Needs attention"))
        sections.append(f"- {pull_message}")
        can_publish = pull_ok

    sections.append("\n## Sync\n")
    if can_publish:
        sync_ok, sync_message, item_count, sync_changed = sync_library_json()
        failed = failed or not sync_ok
        sections.append("- Status: " + ("OK" if sync_ok else "Needs attention"))
        sections.append(f"- {sync_message}")
        if item_count is not None:
            sections.append(f"- Synced item count: {item_count}")
    else:
        sections.append("- Status: Skipped")
        sections.append("- Resolve table or git state issues before auto-sync can run.")

    if can_publish and sync_changed:
        publish_ok, commit_sha, publish_message = commit_and_push_sync()
        failed = failed or not publish_ok
        sections.append("\n## Publish\n")
        sections.append("- Status: " + ("OK" if publish_ok else "Needs attention"))
        if commit_sha:
            sections.append(f"- Commit: {commit_sha}")
        sections.append(f"- {publish_message}")
    elif can_publish:
        sections.append("\n## Publish\n")
        sections.append("- Status: OK")
        sections.append("- No publish needed.")

    live_ok, live_message = check_live_drift()
    if not sync_changed:
        failed = failed or not live_ok
    sections.append("\n## Live Data\n")
    if live_ok:
        sections.append("- Status: OK")
    elif sync_changed:
        sections.append("- Status: Pending")
    else:
        sections.append("- Status: Needs attention")
    sections.append(f"- {live_message}")
    if sync_changed and not live_ok:
        sections.append("- A fresh publish can take a few minutes to appear through Pages/CDN.")

    sections.append("\n## Result\n")
    sections.append("Synced and archive-ready." if not failed else "Review the sections marked Needs attention.")

    REPORT.write_text("\n".join(sections) + "\n")
    print(f"Wrote {REPORT}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
