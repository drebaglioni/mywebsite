#!/usr/bin/env python3
"""Enrich Obsidian library table with conservative ISBN autofill + review flags."""

from __future__ import annotations

import argparse
import csv
import json
import re
import socket
import time
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
DEFAULT_APPLY_REPORT_PATH = ".context/library_enrichment_apply_report.md"
DEFAULT_QUEUE_CSV_PATH = ".context/library_enrichment_review_queue.csv"

AUTO_FILL_MIN_SCORE = 0.96
AUTO_FILL_MIN_MARGIN = 0.01
TOP_CANDIDATES_IN_REPORT = 3
DEFAULT_SCORE_MIN = 0.95
DEFAULT_MARGIN_MIN = 0.005

# Guardrails: these tokens often indicate bundles/audio junk results.
EXCLUDED_CANDIDATE_TOKENS = (
    "audiofy",
    "chips",
    "3 book set",
    "box set",
    "book set",
)


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
class RowDecision:
    title: str
    author: str
    year: str
    status: str  # auto_filled | needs_review | no_match
    reason: str
    chosen_isbn: str
    top_score: float
    margin: float
    top_candidates: list[Candidate]
    line_no: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-fill likely ISBNs and generate review flags for Obsidian library rows.")
    parser.add_argument("--source", required=True, help="Absolute path to Obsidian Library.md")
    parser.add_argument("--report", default=DEFAULT_APPLY_REPORT_PATH, help="Path for markdown apply report")
    parser.add_argument("--queue-csv", default=DEFAULT_QUEUE_CSV_PATH, help="Path for review queue CSV")
    parser.add_argument("--limit", type=int, default=8, help="OpenLibrary search result limit per row")
    parser.add_argument("--score-min", type=float, default=DEFAULT_SCORE_MIN, help="Minimum score to auto-fill")
    parser.add_argument("--margin-min", type=float, default=DEFAULT_MARGIN_MIN, help="Minimum top-vs-second margin to auto-fill")
    parser.add_argument("--http-timeout", type=float, default=5.0, help="HTTP timeout in seconds")
    parser.add_argument("--http-retries", type=int, default=1, help="HTTP retry attempts per lookup")
    return parser.parse_args()


def split_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError("Table row must start and end with '|'")

    content = stripped.strip("|")
    content = content.replace("\\|", "__ESCAPED_PIPE__")
    protected = re.sub(
        r"\[\[[^\]]+\]\]|\[[^\]]+\]\([^)]+\)",
        lambda match: match.group(0).replace("|", "__PIPE__"),
        content,
    )
    return [
        cell.strip().replace("__PIPE__", "|").replace("__ESCAPED_PIPE__", "\\|")
        for cell in protected.split("|")
    ]


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
    return value.replace("\\|", "|")


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


def query_open_library(
    title: str,
    author: str,
    limit: int = 8,
    timeout: float = 5.0,
    retries: int = 1,
) -> list[dict[str, Any]]:
    params = {
        "title": title,
        "limit": str(limit),
        "fields": "title,author_name,first_publish_year,isbn",
    }
    if author:
        params["author"] = author

    url = OPEN_LIBRARY_ENDPOINT + "?" + urlencode(params)
    payload = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            with urlopen(url, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
                break
        except (HTTPError, URLError, TimeoutError, socket.timeout, ConnectionResetError, OSError, json.JSONDecodeError) as exc:
            if isinstance(exc, HTTPError) and 400 <= exc.code < 500 and exc.code not in {408, 429}:
                return []
            if attempt >= max(1, retries):
                return []
            time.sleep(0.25 * attempt)

    docs = payload.get("docs")
    if not isinstance(docs, list):
        return []
    return [doc for doc in docs if isinstance(doc, dict)]


def candidate_is_excluded(query_title: str, candidate_title: str) -> tuple[bool, str]:
    query_norm = normalize_for_match(query_title)
    candidate_norm = normalize_for_match(candidate_title)
    for token in EXCLUDED_CANDIDATE_TOKENS:
        if token in candidate_norm and token not in query_norm:
            return True, f"excluded token '{token}'"
    return False, ""


def score_candidates(title: str, author: str, year: int | None, docs: list[dict[str, Any]]) -> list[Candidate]:
    scored: list[Candidate] = []
    for doc in docs:
        candidate_title = str(doc.get("title") or "").strip()
        if not candidate_title:
            continue

        excluded, _ = candidate_is_excluded(title, candidate_title)
        if excluded:
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


def primary_author_key(author: str) -> str:
    if not author:
        return ""
    first = re.split(r"[;,]", author)[0].strip()
    return normalize_for_match(first)


def same_work_key(title: str, author: str) -> tuple[str, str]:
    return normalize_for_match(title), primary_author_key(author)


def is_same_work(candidate_a: Candidate, candidate_b: Candidate) -> bool:
    a_title, a_author = same_work_key(candidate_a.title, candidate_a.author)
    b_title, b_author = same_work_key(candidate_b.title, candidate_b.author)

    if not a_title or not b_title:
        return False
    title_match = a_title == b_title or SequenceMatcher(None, a_title, b_title).ratio() >= 0.985
    if not title_match:
        return False

    if a_author and b_author:
        return a_author == b_author
    return True


def exact_title_match(query_title: str, candidate_title: str) -> bool:
    return normalize_for_match(query_title) == normalize_for_match(candidate_title)


def classify_decision(
    title: str,
    author: str,
    year_text: str,
    line_no: int,
    candidates: list[Candidate],
    score_min: float,
    margin_min: float,
) -> RowDecision:
    display_author = author or "Unknown"
    display_year = year_text or "Unknown"

    if not candidates:
        return RowDecision(
            title=title,
            author=display_author,
            year=display_year,
            status="no_match",
            reason="no candidates from OpenLibrary",
            chosen_isbn="",
            top_score=0.0,
            margin=0.0,
            top_candidates=[],
            line_no=line_no,
        )

    best = candidates[0]
    second = candidates[1] if len(candidates) > 1 else None
    margin = best.score - (second.score if second else 0.0)
    has_author = bool(author.strip())

    if best.score < score_min:
        return RowDecision(
            title=title,
            author=display_author,
            year=display_year,
            status="needs_review",
            reason=f"top score {best.score:.3f} below {score_min:.2f}",
            chosen_isbn="",
            top_score=best.score,
            margin=margin,
            top_candidates=candidates[:TOP_CANDIDATES_IN_REPORT],
            line_no=line_no,
        )

    if has_author and best.author_ratio < 0.82:
        return RowDecision(
            title=title,
            author=display_author,
            year=display_year,
            status="needs_review",
            reason=f"author ratio {best.author_ratio:.3f} below 0.82",
            chosen_isbn="",
            top_score=best.score,
            margin=margin,
            top_candidates=candidates[:TOP_CANDIDATES_IN_REPORT],
            line_no=line_no,
        )

    if not has_author and not exact_title_match(title, best.title):
        return RowDecision(
            title=title,
            author=display_author,
            year=display_year,
            status="needs_review",
            reason="title-only match not exact after normalization",
            chosen_isbn="",
            top_score=best.score,
            margin=margin,
            top_candidates=candidates[:TOP_CANDIDATES_IN_REPORT],
            line_no=line_no,
        )

    if margin >= margin_min:
        return RowDecision(
            title=title,
            author=display_author,
            year=display_year,
            status="auto_filled",
            reason=f"accepted: margin {margin:.3f} >= {margin_min:.2f}",
            chosen_isbn=best.isbn,
            top_score=best.score,
            margin=margin,
            top_candidates=candidates[:TOP_CANDIDATES_IN_REPORT],
            line_no=line_no,
        )

    if second and is_same_work(best, second):
        return RowDecision(
            title=title,
            author=display_author,
            year=display_year,
            status="auto_filled",
            reason="accepted: same-work edition tie",
            chosen_isbn=best.isbn,
            top_score=best.score,
            margin=margin,
            top_candidates=candidates[:TOP_CANDIDATES_IN_REPORT],
            line_no=line_no,
        )

    return RowDecision(
        title=title,
        author=display_author,
        year=display_year,
        status="needs_review",
        reason=f"margin {margin:.3f} below {margin_min:.2f}",
        chosen_isbn="",
        top_score=best.score,
        margin=margin,
        top_candidates=candidates[:TOP_CANDIDATES_IN_REPORT],
        line_no=line_no,
    )


def build_row(cells: list[str]) -> str:
    escaped_cells: list[str] = []
    for cell in cells:
        value = str(cell).replace("\\|", "__ESCAPED_PIPE__")
        value = re.sub(
            r"\[\[[^\]]+\]\]|\[[^\]]+\]\([^)]+\)",
            lambda match: match.group(0).replace("|", "__PIPE__"),
            value,
        )
        value = value.replace("|", "\\|")
        value = value.replace("__PIPE__", "|").replace("__ESCAPED_PIPE__", "\\|")
        escaped_cells.append(value)
    return "| " + " | ".join(escaped_cells) + " |"


def write_queue_csv(path: Path, decisions: list[RowDecision]) -> None:
    status_rank = {"needs_review": 0, "no_match": 1, "auto_filled": 2}
    ordered = sorted(
        decisions,
        key=lambda decision: (
            status_rank.get(decision.status, 9),
            -decision.top_score,
            -decision.margin,
            decision.title.lower(),
        ),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["title", "author", "year", "status", "chosen_isbn", "top_score", "margin", "reason"])
        for decision in ordered:
            writer.writerow([
                decision.title,
                decision.author,
                decision.year,
                decision.status,
                decision.chosen_isbn,
                f"{decision.top_score:.3f}" if decision.top_score else "",
                f"{decision.margin:.3f}" if decision.top_candidates else "",
                decision.reason,
            ])


def write_apply_report(
    path: Path,
    source_path: Path,
    rows_considered: int,
    already_has_isbn: int,
    decisions: list[RowDecision],
) -> None:
    status_counts = {
        "auto_filled": sum(1 for d in decisions if d.status == "auto_filled"),
        "needs_review": sum(1 for d in decisions if d.status == "needs_review"),
        "no_match": sum(1 for d in decisions if d.status == "no_match"),
    }
    auto_filled = [d for d in decisions if d.status == "auto_filled"]
    needs_review = [d for d in decisions if d.status == "needs_review"]
    no_match = [d for d in decisions if d.status == "no_match"]

    lines: list[str] = []
    lines.append("# Library Enrichment Apply Report")
    lines.append("")
    lines.append(f"- Generated at: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- Source: `{source_path}`")
    lines.append(f"- Book rows considered: **{rows_considered}**")
    lines.append(f"- Already had `isbn`: **{already_has_isbn}**")
    lines.append(f"- Auto-filled: **{status_counts['auto_filled']}**")
    lines.append(f"- Needs review: **{status_counts['needs_review']}**")
    lines.append(f"- No match: **{status_counts['no_match']}**")
    lines.append("")

    lines.append("## Auto Filled")
    lines.append("")
    if not auto_filled:
        lines.append("No auto-filled rows.")
    else:
        for item in auto_filled:
            lines.append(f"### {item.title} ({item.year})")
            lines.append(f"- Author: {item.author}")
            lines.append(f"- Chosen ISBN: `{item.chosen_isbn}`")
            lines.append(f"- Score: `{item.top_score:.3f}`")
            lines.append(f"- Margin: `{item.margin:.3f}`")
            lines.append(f"- Reason: {item.reason}")
            lines.append(f"- Line: `{item.line_no}`")
            if item.top_candidates:
                best = item.top_candidates[0]
                year_label = str(best.year) if best.year is not None else "?"
                lines.append(f"- Picked candidate: `{best.title}` by `{best.author or 'Unknown'}` ({year_label})")
            lines.append("")

    lines.append("## Needs Review")
    lines.append("")
    if not needs_review:
        lines.append("No rows need review.")
    else:
        for item in needs_review:
            lines.append(f"### {item.title} ({item.year})")
            lines.append(f"- Author: {item.author}")
            lines.append(f"- Reason: {item.reason}")
            lines.append(f"- Top score: `{item.top_score:.3f}`")
            lines.append(f"- Margin: `{item.margin:.3f}`")
            lines.append(f"- Line: `{item.line_no}`")
            if item.top_candidates:
                lines.append("- Top candidates:")
                for candidate in item.top_candidates:
                    year_label = str(candidate.year) if candidate.year is not None else "?"
                    lines.append(
                        f"  - `{candidate.title}` by `{candidate.author or 'Unknown'}` ({year_label}) "
                        f"ISBN `{candidate.isbn}` score `{candidate.score:.3f}`"
                    )
            lines.append("")

    lines.append("## No Match")
    lines.append("")
    if not no_match:
        lines.append("No no-match rows.")
    else:
        for item in no_match:
            lines.append(f"- {item.title} ({item.year}) — {item.author} [line {item.line_no}]")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> tuple[int, int, int, int, int]:
    source_path = Path(args.source).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    queue_csv_path = Path(args.queue_csv).expanduser().resolve()
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

    cache: dict[tuple[str, str, int | None], list[Candidate]] = {}
    decisions: list[RowDecision] = []

    rows_considered = 0
    already_has_isbn = 0
    auto_filled = 0

    for row_idx in table_line_indexes[2:]:
        row_line = lines[row_idx]
        if not row_line.strip():
            continue
        cells = split_row(row_line)
        if len(cells) != len(header_cells):
            continue

        item_format = cells[format_idx].strip().lower()
        if item_format not in {"book", "ebook", "audiobook"}:
            continue

        rows_considered += 1
        if cells[isbn_idx].strip():
            already_has_isbn += 1
            continue

        raw_title = cells[title_idx]
        title = normalize_obsidian_title(raw_title)
        author = normalize_author(cells[author_idx])
        year_text = cells[year_idx].strip()
        year = int(year_text) if re.fullmatch(r"\d{4}", year_text) else None

        cache_key = (title, author, year)
        if cache_key not in cache:
            docs = query_open_library(
                title,
                author,
                limit=args.limit,
                timeout=args.http_timeout,
                retries=args.http_retries,
            )
            cache[cache_key] = score_candidates(title, author, year, docs)
        candidates = cache[cache_key]

        decision = classify_decision(
            title,
            author,
            year_text,
            row_idx + 1,
            candidates,
            args.score_min,
            args.margin_min,
        )
        decisions.append(decision)

        if decision.status == "auto_filled" and decision.chosen_isbn:
            cells[isbn_idx] = decision.chosen_isbn
            lines[row_idx] = build_row(cells)
            auto_filled += 1

    source_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_apply_report(report_path, source_path, rows_considered, already_has_isbn, decisions)
    write_queue_csv(queue_csv_path, decisions)

    needs_review = sum(1 for d in decisions if d.status == "needs_review")
    no_match = sum(1 for d in decisions if d.status == "no_match")
    return rows_considered, already_has_isbn, auto_filled, needs_review, no_match


def main() -> int:
    args = parse_args()
    rows_considered, already_filled, auto_filled, needs_review, no_match = run(args)
    print(
        "Rows considered: "
        f"{rows_considered} | Already filled: {already_filled} | "
        f"Auto-filled: {auto_filled} | Needs review: {needs_review} | No match: {no_match}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
