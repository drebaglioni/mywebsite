#!/usr/bin/env python3
"""Sync Obsidian markdown table export into data/library.json."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

EXPECTED_COLUMNS = [
    "title",
    "author",
    "format",
    "subjects",
    "year",
    "status",
    "finished",
    "rating",
    "url",
    "notes",
]

VALID_FORMATS = {"book", "article", "podcast", "audiobook"}
VALID_STATUSES = {"completed", "in-progress", "queued"}
STATUS_SORT_ORDER = {"in-progress": 0, "completed": 1, "queued": 2}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Obsidian export markdown to JSON")
    parser.add_argument("--source", required=True, help="Absolute path to Obsidian Library Export markdown note")
    parser.add_argument("--out", default="data/library.json", help="Output JSON path")
    return parser.parse_args()


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "entry"


def split_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError("Table row must start and end with '|'")

    content = stripped.strip("|")

    # Keep Obsidian wikilink aliases intact while splitting table columns.
    protected = re.sub(
        r"\[\[[^\]]+\]\]",
        lambda match: match.group(0).replace("|", "__PIPE__"),
        content,
    )

    cells = [cell.strip().replace("__PIPE__", "|") for cell in protected.split("|")]
    return cells


def is_separator_row(cells: list[str]) -> bool:
    if not cells:
        return False
    for cell in cells:
        normalized = cell.replace("-", "").replace(":", "").strip()
        if normalized:
            return False
    return True


def parse_year(value: str, row_no: int) -> int:
    if not re.fullmatch(r"\d{4}", value):
        raise ValueError(f"Row {row_no}: year must be a 4-digit number")
    return int(value)


def parse_finished(value: str, row_no: int) -> str | None:
    if value == "":
        return None
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Row {row_no}: finished must use YYYY-MM-DD") from exc
    return value


def parse_rating(value: str, row_no: int) -> int | None:
    if value == "":
        return None
    if not re.fullmatch(r"\d+", value):
        raise ValueError(f"Row {row_no}: rating must be an integer 1-5")
    rating = int(value)
    if rating < 1 or rating > 5:
        raise ValueError(f"Row {row_no}: rating must be between 1 and 5")
    return rating


def parse_url(value: str, row_no: int) -> str | None:
    if value == "":
        return None
    if not re.match(r"^https?://", value):
        raise ValueError(f"Row {row_no}: url must start with http:// or https://")
    return value


def parse_subjects(value: str) -> list[str]:
    if value == "":
        return []
    parts = [item.strip().lower() for item in value.split(";")]
    cleaned = [re.sub(r"\s+", "-", part) for part in parts if part]
    deduped = []
    for subject in cleaned:
        if subject not in deduped:
            deduped.append(subject)
    return deduped


def normalize_author(value: str) -> str | None:
    cleaned = value.strip()
    if cleaned == "":
        return None
    parts = [part.strip() for part in cleaned.split(";") if part.strip()]
    if not parts:
        return None
    return ", ".join(parts)


def normalize_obsidian_title(value: str) -> str:
    value = value.strip()

    alias_match = re.fullmatch(r"\[\[[^|\]]+\|([^\]]+)\]\]", value)
    if alias_match:
        return alias_match.group(1).strip()

    link_match = re.fullmatch(r"\[\[([^\]]+)\]\]", value)
    if link_match:
        return link_match.group(1).strip()

    markdown_link_match = re.fullmatch(r"\[([^\]]+)\]\([^)]+\)", value)
    if markdown_link_match:
        return markdown_link_match.group(1).strip()

    return value


def parse_table(source_path: Path) -> list[dict]:
    if not source_path.exists():
        raise ValueError(f"Source file not found: {source_path}")

    lines = source_path.read_text(encoding="utf-8").splitlines()

    export_index = -1
    for idx, line in enumerate(lines):
        if line.strip().lower() == "## export":
            export_index = idx
            break

    if export_index == -1:
        raise ValueError("Missing '## Export' section in source markdown")

    table_lines: list[tuple[int, str]] = []
    for offset in range(export_index + 1, len(lines)):
        line = lines[offset]
        if line.strip().startswith("#") and table_lines:
            break
        if "|" in line:
            table_lines.append((offset + 1, line))

    if len(table_lines) < 2:
        raise ValueError("Could not find markdown table rows under '## Export'")

    header_line_no, header_line = table_lines[0]
    header_cells = [cell.lower() for cell in split_row(header_line)]
    if header_cells != EXPECTED_COLUMNS:
        raise ValueError(
            "Header columns must exactly match: " + ", ".join(EXPECTED_COLUMNS)
            + f" (found at line {header_line_no})"
        )

    separator_cells = split_row(table_lines[1][1])
    if not is_separator_row(separator_cells):
        raise ValueError(f"Line {table_lines[1][0]} must be the markdown separator row")

    items: list[dict] = []
    seen_ids = set()

    for row_line_no, row_line in table_lines[2:]:
        if not row_line.strip() or row_line.strip().replace("|", "").strip() == "":
            continue

        cells = split_row(row_line)
        if len(cells) != len(EXPECTED_COLUMNS):
            raise ValueError(f"Row {row_line_no}: expected {len(EXPECTED_COLUMNS)} columns, found {len(cells)}")

        row = dict(zip(EXPECTED_COLUMNS, cells))

        title = normalize_obsidian_title(row["title"].strip())
        if not title:
            raise ValueError(f"Row {row_line_no}: title is required")

        item_format = row["format"].strip().lower()
        if item_format not in VALID_FORMATS:
            raise ValueError(f"Row {row_line_no}: format must be one of {sorted(VALID_FORMATS)}")

        status = row["status"].strip().lower()
        if status not in VALID_STATUSES:
            raise ValueError(f"Row {row_line_no}: status must be one of {sorted(VALID_STATUSES)}")

        year = parse_year(row["year"].strip(), row_line_no)
        finished = parse_finished(row["finished"].strip(), row_line_no)
        rating = parse_rating(row["rating"].strip(), row_line_no)
        url = parse_url(row["url"].strip(), row_line_no)
        subjects = parse_subjects(row["subjects"].strip())

        author = normalize_author(row["author"])
        notes = row["notes"].strip() or None

        base_id = slugify(f"{title}-{year}")
        item_id = base_id
        counter = 2
        while item_id in seen_ids:
            item_id = f"{base_id}-{counter}"
            counter += 1

        seen_ids.add(item_id)

        items.append(
            {
                "id": item_id,
                "title": title,
                "author": author,
                "format": item_format,
                "subjects": subjects,
                "year": year,
                "status": status,
                "finished": finished,
                "rating": rating,
                "url": url,
                "notes": notes,
            }
        )

    completed = [item for item in items if item["status"] == "completed"]
    in_progress = [item for item in items if item["status"] == "in-progress"]
    queued = [item for item in items if item["status"] == "queued"]

    completed.sort(key=lambda item: (item["finished"] is None, item["finished"] or "", item["title"].lower()), reverse=True)
    in_progress.sort(key=lambda item: item["title"].lower())
    queued.sort(key=lambda item: item["title"].lower())

    return in_progress + completed + queued


def write_output(items: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(items, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    source = Path(args.source).expanduser().resolve()
    output = Path(args.out).expanduser().resolve()

    try:
        items = parse_table(source)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    write_output(items, output)
    print(f"Synced {len(items)} entries to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
