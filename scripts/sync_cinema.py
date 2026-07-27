#!/usr/bin/env python3
"""Sync Obsidian Cinema notes into the website's JSON collection."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


DEFAULT_SOURCE = Path("/Users/andrea/Obsidian/Aristotle/personal/cinema")
DEFAULT_OUTPUT = Path("data/cinema.json")
DEFAULT_OVERRIDES = Path("data/cinema-overrides.json")


def parse_scalar(raw_value: str) -> Any:
    value = raw_value.strip()
    if not value:
        return ""
    if value.startswith("[") and value.endswith("]"):
        return [
            part.strip().strip("'\"")
            for part in value[1:-1].split(",")
            if part.strip()
        ]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    return value.strip("'\"")


def parse_frontmatter(markdown: str) -> dict[str, Any]:
    match = re.match(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", markdown, re.DOTALL)
    if not match:
        return {}

    values: dict[str, Any] = {}
    for line in match.group(1).splitlines():
        if not line.strip() or line.lstrip().startswith("#") or ":" not in line:
            continue
        key, raw_value = line.split(":", 1)
        values[key.strip()] = parse_scalar(raw_value)
    return values


def extract_summary(markdown: str) -> str:
    def replace_wikilink(match: re.Match[str]) -> str:
        target = match.group(1)
        alias = match.group(2)
        if alias:
            return alias
        if target.rstrip("/").endswith("the-cinema"):
            return "the cinema archive"
        return target.rsplit("/", 1)[-1].replace("-", " ")

    for line in markdown.splitlines():
        if line.startswith("> "):
            return re.sub(r"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]", replace_wikilink, line[2:]).strip()
    return ""


def extract_notes(markdown: str) -> str:
    match = re.search(r"^## Notes\s*\n(.*?)(?=^## |\Z)", markdown, re.MULTILINE | re.DOTALL)
    if not match:
        return ""

    notes = []
    for line in match.group(1).splitlines():
        cleaned = re.sub(r"^\s*[-*]\s*", "", line).strip()
        if cleaned:
            notes.append(cleaned)
    return " ".join(notes)


def load_overrides(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def note_to_item(path: Path, overrides: dict[str, dict[str, Any]]) -> dict[str, Any]:
    markdown = path.read_text(encoding="utf-8")
    frontmatter = parse_frontmatter(markdown)
    item_id = path.stem
    item = {
        "id": item_id,
        "title": frontmatter.get("title") or item_id.replace("-", " ").title(),
        "creator": frontmatter.get("creator", ""),
        "year": frontmatter.get("year"),
        "format": frontmatter.get("format", "video"),
        "status": frontmatter.get("status", ""),
        "rating": frontmatter.get("rating"),
        "created": frontmatter.get("created", ""),
        "last_reviewed": frontmatter.get("last_reviewed", ""),
        "tags": frontmatter.get("tags", []),
        "summary": extract_summary(markdown),
        "notes": extract_notes(markdown),
    }
    item.update(overrides.get(item_id, {}))
    return item


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--overrides", type=Path, default=DEFAULT_OVERRIDES)
    args = parser.parse_args()

    if not args.source.is_dir():
        raise SystemExit(f"Cinema source directory not found: {args.source}")

    overrides = load_overrides(args.overrides)
    items = [
        note_to_item(path, overrides)
        for path in sorted(args.source.glob("*.md"))
    ]
    items.sort(key=lambda item: (str(item.get("created") or ""), item["title"].lower()), reverse=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(items, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Synced {len(items)} Cinema entries to {args.output}")


if __name__ == "__main__":
    main()
