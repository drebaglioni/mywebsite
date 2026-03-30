#!/usr/bin/env python3
"""Enrich Obsidian library table with high-confidence ISBN matches."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


TABLE_SECTION_HEADER = "## export"
OPEN_LIBRARY_ENDPOINT = "https://openlibrary.org/search.json"
DEFAULT_REPORT_PATH = ".context/library_enrichment_review.md"


@dataclass
class Candidate:
    title: str
    author: str
    year: int | None
    isbn: str
    title_ratio: float
    author_ratio: float
    year_score: float
    score: float


@dataclass
class ReviewItem:
    title: str
    author: str
    year: str
    reason: str
    top_candidates: list[Candidate]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-fill high-confidence ISBNs for Obsidian library table rows.")
    parser.add_argument("--source", required=True, help="Absolute path to Obsidian Library.md")
    parser.add_argument("--report", default=DEFAULT_REPORT_PATH, help="Path for markdown review report")
    parser.add_argument("--min-score-author", type=float, default=0.90, help="Minimum score when author exists")
    parser.add_argument("--min-score-title-only", type=float, default=0.97, help="Minimum score when author missing")
    parser.add_argument("--min-margin-author", type=float, default=0.07, help="Minimum top-vs-next score margin with author")
    parser.add_argument("--min-margin-title-only", type=float, default=0.12, help="Minimum top-vs-next score margin without author")
    return parser.parse_args()


def split_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError("Table row must start and end with '|'")

    content = stripped.strip("|")
    protected = re.sub(
        r"\[\[[^\]]+\]\]",
        lambda match: match.group(0).replace("|", "__PIPE__"),
        content,
    )
    return [cell.strip().replace("__PIPE__", "|") for cell in protected.split("|")]


def normalize_obsidian_title(value: str) -> str:
    value = value.strip()
    alias_match = re.fullmatch(r"\[\[[^|\]]+\|([^\]]+)\]\]", value)
    if alias_match:
        return alias_match.group(1).strip()
    link_match = re.fullmatch(r"\[\[([^\]]+)\]\]", value)
    if link_match:
        return link_match.group(1).strip()
    markdown_match = re.fullmatch(r"\[([^\]]+)\]\([^)]+\)", value)
    if markdown_match:
        return markdown_match.group(1).strip()
    return value


def normalize_author(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    parts = [part.strip() for part in cleaned.split(";") if part.strip()]
    return ", ".join(parts)


def normalize_for_match(value: str) -> str:
    lowered = value.lower().strip()
    normalized = unicodedata.normalize("NFKD", lowered)
    ascii_only = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_only = ascii_only.replace("&", " and ")
    ascii_only = re.sub(r"[^a-z0-9\s]", " ", ascii_only)
    ascii_only = re.sub(r"\b(the|a|an)\b", " ", ascii_only)
    return re.sub(r"\s+", " ", ascii_only).strip()


def title_similarity(query_title: str, candidate_title: str) -> float:
    q = normalize_for_match(query_title)
    c = normalize_for_match(candidate_title)
    if not q or not c:
        return 0.0

    ratios = [SequenceMatcher(None, q, c).ratio()]
    if ":" in q:
        ratios.append(SequenceMatcher(None, q.split(":", 1)[0].strip(), c).ratio())
    if ":" in c:
        ratios.append(SequenceMatcher(None, q, c.split(":", 1)[0].strip()).ratio())
    if q in c or c in q:
        ratios.append(0.99)
    return max(ratios)


def author_similarity(query_author: str, candidate_authors: list[str]) -> float:
    qa = normalize_for_match(query_author)
    if not qa:
        return 1.0
    if not candidate_authors:
        return 0.0

    scores: list[float] = []
    for author in candidate_authors:
        ca = normalize_for_match(author)
        if not ca:
            continue
        direct = SequenceMatcher(None, qa, ca).ratio()
        if qa in ca or ca in qa:
            direct = max(direct, 0.97)
        scores.append(direct)

    return max(scores) if scores else 0.0


def year_similarity(query_year: int | None, candidate_year: int | None) -> float:
    if query_year is None:
        return 0.5
    if candidate_year is None:
        return 0.0
    delta = abs(query_year - candidate_year)
    if delta == 0:
        return 1.0
    if delta == 1:
        return 0.85
    if delta <= 3:
        return 0.55
    if delta <= 8:
        return 0.25
    return 0.0


def pick_isbn(isbns: list[str]) -> str:
    normalized: list[str] = []
    for raw in isbns:
        cleaned = re.sub(r"[\s-]+", "", str(raw)).upper()
        if re.fullmatch(r"(?:[0-9]{13}|[0-9]{9}[0-9X])", cleaned):
            normalized.append(cleaned)

    if not normalized:
        return ""

    isbn13 = sorted({isbn for isbn in normalized if len(isbn) == 13})
    if isbn13:
        return isbn13[0]
    return sorted(set(normalized))[0]


def query_open_library(title: str, author: str, limit: int = 8) -> list[dict[str, Any]]:
    params = {
        "title": title,
        "limit": str(limit),
        "fields": "title,author_name,first_publish_year,isbn",
    }
    if author:
        params["author"] = author

    url = OPEN_LIBRARY_ENDPOINT + "?" + urlencode(params)
    try:
        with urlopen(url, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return []

    docs = payload.get("docs")
    if not isinstance(docs, list):
        return []
    return [doc for doc in docs if isinstance(doc, dict)]


def score_candidates(title: str, author: str, year: int | None, docs: list[dict[str, Any]]) -> list[Candidate]:
    scored: list[Candidate] = []
    for doc in docs:
        candidate_title = str(doc.get("title") or "").strip()
        if not candidate_title:
            continue

        candidate_authors = doc.get("author_name")
        if not isinstance(candidate_authors, list):
            candidate_authors = []
        candidate_authors = [str(author_name).strip() for author_name in candidate_authors if str(author_name).strip()]

        candidate_year_raw = doc.get("first_publish_year")
        candidate_year = int(candidate_year_raw) if isinstance(candidate_year_raw, int) else None

        isbn_values = doc.get("isbn")
        if not isinstance(isbn_values, list):
            isbn_values = []
        isbn = pick_isbn(isbn_values)
        if not isbn:
            continue

        title_ratio = title_similarity(title, candidate_title)
        author_ratio = author_similarity(author, candidate_authors)
        year_score = year_similarity(year, candidate_year)

        has_author = bool(normalize_for_match(author))
        if has_author:
            score = (title_ratio * 0.78) + (author_ratio * 0.20) + (year_score * 0.02)
        else:
            score = (title_ratio * 0.95) + (year_score * 0.05)

        scored.append(Candidate(
            title=candidate_title,
            author=", ".join(candidate_authors),
            year=candidate_year,
            isbn=isbn,
            title_ratio=title_ratio,
            author_ratio=author_ratio,
            year_score=year_score,
            score=score,
        ))

    scored.sort(key=lambda candidate: candidate.score, reverse=True)
    return scored


def should_accept(best: Candidate, second_best: Candidate | None, has_author: bool, args: argparse.Namespace) -> tuple[bool, str]:
    margin = best.score - (second_best.score if second_best else 0.0)
    if has_author:
        if best.score < args.min_score_author:
            return False, f"score {best.score:.3f} below threshold"
        if best.title_ratio < 0.88:
            return False, f"title ratio {best.title_ratio:.3f} below threshold"
        if best.author_ratio < 0.82:
            return False, f"author ratio {best.author_ratio:.3f} below threshold"
        if margin < args.min_margin_author:
            return False, f"top margin {margin:.3f} below threshold"
        return True, "accepted with author"

    if best.score < args.min_score_title_only:
        return False, f"title-only score {best.score:.3f} below threshold"
    if best.title_ratio < 0.95:
        return False, f"title-only ratio {best.title_ratio:.3f} below threshold"
    if margin < args.min_margin_title_only:
        return False, f"title-only margin {margin:.3f} below threshold"
    return True, "accepted title-only"


def build_row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def run(args: argparse.Namespace) -> tuple[int, int, int, int]:
    source_path = Path(args.source).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    lines = source_path.read_text(encoding="utf-8").splitlines()

    export_index = -1
    for idx, line in enumerate(lines):
        if line.strip().lower() == TABLE_SECTION_HEADER:
            export_index = idx
            break
    if export_index == -1:
        raise ValueError("Missing '## Export' section")

    table_line_indexes: list[int] = []
    for idx in range(export_index + 1, len(lines)):
        current = lines[idx]
        if current.strip().startswith("#") and table_line_indexes:
            break
        if "|" in current:
            table_line_indexes.append(idx)

    if len(table_line_indexes) < 2:
        raise ValueError("Could not find export markdown table")

    header_cells = [cell.lower() for cell in split_row(lines[table_line_indexes[0]])]
    if "cover" not in header_cells or "isbn" not in header_cells:
        raise ValueError("Header must include both 'cover' and 'isbn' columns")

    cover_idx = header_cells.index("cover")
    isbn_idx = header_cells.index("isbn")
    title_idx = header_cells.index("title")
    author_idx = header_cells.index("author")
    format_idx = header_cells.index("format")
    year_idx = header_cells.index("year")

    cache: dict[tuple[str, str], list[Candidate]] = {}
    review_items: list[ReviewItem] = []

    total_candidates = 0
    filled_rows = 0
    already_filled = 0
    rows_considered = 0
    updated_line_indexes: list[int] = []

    for row_idx in table_line_indexes[2:]:
        row_line = lines[row_idx]
        if not row_line.strip():
            continue
        cells = split_row(row_line)
        if len(cells) != len(header_cells):
            continue

        item_format = cells[format_idx].strip().lower()
        if item_format not in {"book", "ebook"}:
            continue

        rows_considered += 1
        if cells[cover_idx].strip() or cells[isbn_idx].strip():
            already_filled += 1
            continue

        raw_title = cells[title_idx]
        title = normalize_obsidian_title(raw_title)
        author = normalize_author(cells[author_idx])
        year_text = cells[year_idx].strip()
        year = int(year_text) if re.fullmatch(r"\d{4}", year_text) else None

        cache_key = (title, author)
        if cache_key not in cache:
            docs = query_open_library(title, author)
            cache[cache_key] = score_candidates(title, author, year, docs)
        candidates = cache[cache_key]

        if not candidates:
            review_items.append(ReviewItem(
                title=title,
                author=author or "Unknown",
                year=year_text or "Unknown",
                reason="no candidates from OpenLibrary",
                top_candidates=[],
            ))
            continue

        total_candidates += len(candidates)
        best = candidates[0]
        second = candidates[1] if len(candidates) > 1 else None
        accepted, reason = should_accept(best, second, bool(author.strip()), args)
        if not accepted:
            review_items.append(ReviewItem(
                title=title,
                author=author or "Unknown",
                year=year_text or "Unknown",
                reason=reason,
                top_candidates=candidates[:3],
            ))
            continue

        cells[isbn_idx] = best.isbn
        lines[row_idx] = build_row(cells)
        updated_line_indexes.append(row_idx + 1)
        filled_rows += 1

    source_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    report_lines: list[str] = []
    report_lines.append("# Library Enrichment Review")
    report_lines.append("")
    report_lines.append(f"- Generated at: {datetime.now(timezone.utc).isoformat()}")
    report_lines.append(f"- Source: `{source_path}`")
    report_lines.append(f"- Book rows considered: **{rows_considered}**")
    report_lines.append(f"- Already had `cover`/`isbn`: **{already_filled}**")
    report_lines.append(f"- Auto-filled ISBN rows: **{filled_rows}**")
    report_lines.append(f"- Needs review: **{len(review_items)}**")
    report_lines.append("")
    if updated_line_indexes:
        report_lines.append("## Updated Row Line Numbers")
        report_lines.append("")
        report_lines.append(", ".join(str(line_no) for line_no in updated_line_indexes))
        report_lines.append("")

    report_lines.append("## Needs Review")
    report_lines.append("")
    if not review_items:
        report_lines.append("No unresolved rows.")
    else:
        for item in review_items:
            report_lines.append(f"### {item.title} ({item.year})")
            report_lines.append(f"- Author: {item.author}")
            report_lines.append(f"- Reason: {item.reason}")
            if item.top_candidates:
                report_lines.append("- Top candidates:")
                for candidate in item.top_candidates:
                    year_label = str(candidate.year) if candidate.year is not None else "?"
                    report_lines.append(
                        f"  - `{candidate.title}` by `{candidate.author or 'Unknown'}` ({year_label}) "
                        f"ISBN `{candidate.isbn}` score `{candidate.score:.3f}`"
                    )
            report_lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines).rstrip() + "\n", encoding="utf-8")

    return rows_considered, already_filled, filled_rows, len(review_items)


def main() -> int:
    args = parse_args()
    rows_considered, already_filled, filled_rows, review_count = run(args)
    print(
        "Rows considered: "
        f"{rows_considered} | Already filled: {already_filled} | "
        f"Auto-filled: {filled_rows} | Needs review: {review_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
