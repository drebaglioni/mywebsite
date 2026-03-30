#!/usr/bin/env python3
"""Conservative URL + cover backfill for Obsidian library export table."""

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
from urllib.parse import quote, urlencode
from urllib.request import urlopen


TABLE_SECTION_HEADER = "## export"
DEFAULT_REPORT_PATH = ".context/link_cover_fill_report.md"
DEFAULT_QUEUE_CSV_PATH = ".context/link_cover_review_queue.csv"

WIKIPEDIA_SEARCH_API = "https://en.wikipedia.org/w/api.php"
OPEN_LIBRARY_SEARCH_API = "https://openlibrary.org/search.json"

TITLE_SIMILARITY_MIN = 0.95
AUTHOR_SIMILARITY_MIN = 0.80
TOP_CANDIDATES = 3

DERIVATIVE_TOKENS = (
    "summary",
    "workbook",
    "collection",
    "set",
    "booknation",
    "whizbooks",
    "guide",
    "key takeaways",
)


@dataclass
class UrlCandidate:
    source: str
    title: str
    url: str
    author_hint: str
    title_similarity: float
    author_similarity: float
    confidence: float
    reason: str


@dataclass
class ActionRow:
    title: str
    author: str
    year: str
    field: str  # url | cover
    status: str  # filled | flagged | skipped
    proposed_value: str
    source: str
    confidence: float
    reason: str
    line_no: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Conservative URL + cover backfill for Obsidian library export.")
    parser.add_argument("--source", required=True, help="Absolute path to Obsidian Library.md")
    parser.add_argument("--report", default=DEFAULT_REPORT_PATH, help="Path to markdown report")
    parser.add_argument("--queue-csv", default=DEFAULT_QUEUE_CSV_PATH, help="Path to review queue CSV")
    parser.add_argument("--limit", type=int, default=8, help="Result limit per upstream lookup")
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
    query = normalize_for_match(query_title)
    candidate = normalize_for_match(candidate_title)
    if not query or not candidate:
        return 0.0

    scores = [SequenceMatcher(None, query, candidate).ratio()]
    if query in candidate or candidate in query:
        scores.append(0.99)
    if ":" in query:
        scores.append(SequenceMatcher(None, query.split(":", 1)[0].strip(), candidate).ratio())
    if ":" in candidate:
        scores.append(SequenceMatcher(None, query, candidate.split(":", 1)[0].strip()).ratio())
    return max(scores)


def primary_author(author: str) -> str:
    if not author:
        return ""
    primary = re.split(r"[;,]", author)[0].strip()
    return primary


def author_similarity(query_author: str, candidate_author: str) -> float:
    query = normalize_for_match(primary_author(query_author))
    candidate = normalize_for_match(primary_author(candidate_author))
    if not query:
        return 1.0
    if not candidate:
        return 0.0

    score = SequenceMatcher(None, query, candidate).ratio()
    if query in candidate or candidate in query:
        score = max(score, 0.97)
    return score


def year_score(query_year: int | None, candidate_year: int | None) -> float:
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


def contains_derivative_noise(query_title: str, candidate_title: str) -> bool:
    query_norm = normalize_for_match(query_title)
    candidate_norm = normalize_for_match(candidate_title)
    for token in DERIVATIVE_TOKENS:
        if token in candidate_norm and token not in query_norm:
            return True
    return False


def fetch_json(url: str, retries: int = 3) -> dict[str, Any] | list[Any] | None:
    for attempt in range(1, retries + 1):
        try:
            with urlopen(url, timeout=8) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, socket.timeout, ConnectionResetError, OSError, json.JSONDecodeError) as exc:
            if attempt >= retries:
                return None
            if isinstance(exc, HTTPError) and 400 <= exc.code < 500 and exc.code not in {408, 429}:
                return None
            time.sleep(0.5 * attempt)
    return None


def build_wikipedia_candidates(title: str, author: str, year: int | None, limit: int) -> list[UrlCandidate]:
    query_bits = [title]
    primary = primary_author(author)
    if primary:
        query_bits.append(primary)
    query = " ".join(query_bits)

    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": str(limit),
        "format": "json",
        "utf8": "1",
    }
    payload = fetch_json(WIKIPEDIA_SEARCH_API + "?" + urlencode(params))
    if not isinstance(payload, dict):
        return []

    query_block = payload.get("query")
    if not isinstance(query_block, dict):
        return []
    search_results = query_block.get("search")
    if not isinstance(search_results, list):
        return []

    candidates: list[UrlCandidate] = []
    for raw in search_results:
        if not isinstance(raw, dict):
            continue
        candidate_title = str(raw.get("title") or "").strip()
        if not candidate_title:
            continue
        if contains_derivative_noise(title, candidate_title):
            continue

        title_sim = title_similarity(title, candidate_title)
        if title_sim < TITLE_SIMILARITY_MIN:
            continue

        snippet = str(raw.get("snippet") or "")
        snippet_clean = re.sub(r"<[^>]+>", " ", snippet)
        snippet_author_hint = primary if primary and normalize_for_match(primary) in normalize_for_match(snippet_clean) else ""
        author_sim = author_similarity(author, snippet_author_hint) if primary else 1.0
        if primary and author_sim < AUTHOR_SIMILARITY_MIN:
            continue

        page_title_encoded = quote(candidate_title.replace(" ", "_"), safe=":/()_-%")
        page_url = "https://en.wikipedia.org/wiki/" + page_title_encoded

        confidence = (title_sim * 0.82) + (author_sim * 0.15) + (year_score(year, None) * 0.03)
        candidates.append(UrlCandidate(
            source="wikipedia",
            title=candidate_title,
            url=page_url,
            author_hint=snippet_author_hint,
            title_similarity=title_sim,
            author_similarity=author_sim,
            confidence=confidence,
            reason="wikipedia search match",
        ))

    candidates.sort(key=lambda candidate: candidate.confidence, reverse=True)
    return candidates


def build_openlibrary_candidates(title: str, author: str, year: int | None, limit: int) -> list[UrlCandidate]:
    params = {
        "title": title,
        "limit": str(limit),
        "fields": "title,author_name,first_publish_year,key",
    }
    if author:
        params["author"] = primary_author(author)

    payload = fetch_json(OPEN_LIBRARY_SEARCH_API + "?" + urlencode(params))
    if not isinstance(payload, dict):
        return []
    docs = payload.get("docs")
    if not isinstance(docs, list):
        return []

    candidates: list[UrlCandidate] = []
    for raw in docs:
        if not isinstance(raw, dict):
            continue
        candidate_title = str(raw.get("title") or "").strip()
        if not candidate_title:
            continue
        if contains_derivative_noise(title, candidate_title):
            continue

        key = str(raw.get("key") or "").strip()
        if not key:
            continue
        if not key.startswith("/works/") and not key.startswith("/books/"):
            continue

        candidate_authors_raw = raw.get("author_name")
        candidate_authors = candidate_authors_raw if isinstance(candidate_authors_raw, list) else []
        candidate_author = str(candidate_authors[0]).strip() if candidate_authors else ""
        candidate_year_raw = raw.get("first_publish_year")
        candidate_year = candidate_year_raw if isinstance(candidate_year_raw, int) else None

        title_sim = title_similarity(title, candidate_title)
        if title_sim < TITLE_SIMILARITY_MIN:
            continue
        author_sim = author_similarity(author, candidate_author)
        if primary_author(author) and author_sim < AUTHOR_SIMILARITY_MIN:
            continue

        confidence = (title_sim * 0.70) + (author_sim * 0.22) + (year_score(year, candidate_year) * 0.08)
        candidates.append(UrlCandidate(
            source="openlibrary",
            title=candidate_title,
            url="https://openlibrary.org" + key,
            author_hint=candidate_author,
            title_similarity=title_sim,
            author_similarity=author_sim,
            confidence=confidence,
            reason="openlibrary search match",
        ))

    candidates.sort(key=lambda candidate: candidate.confidence, reverse=True)
    return candidates


def choose_url_candidate(wiki_candidates: list[UrlCandidate], openlibrary_candidates: list[UrlCandidate]) -> tuple[UrlCandidate | None, str]:
    if wiki_candidates:
        return wiki_candidates[0], "selected wikipedia candidate"
    if openlibrary_candidates:
        return openlibrary_candidates[0], "selected openlibrary candidate"
    return None, "no candidate passed conservative gates"


def build_row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def write_csv(queue_path: Path, actions: list[ActionRow]) -> None:
    status_rank = {"flagged": 0, "skipped": 1, "filled": 2}
    ordered = sorted(
        actions,
        key=lambda action: (
            status_rank.get(action.status, 9),
            -action.confidence,
            action.field,
            action.title.lower(),
        ),
    )

    queue_path.parent.mkdir(parents=True, exist_ok=True)
    with queue_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["title", "author", "year", "field", "status", "proposed_value", "source", "confidence", "reason"])
        for action in ordered:
            writer.writerow([
                action.title,
                action.author,
                action.year,
                action.field,
                action.status,
                action.proposed_value,
                action.source,
                f"{action.confidence:.3f}",
                action.reason,
            ])


def write_report(report_path: Path, source_path: Path, rows_scanned: int, actions: list[ActionRow]) -> None:
    url_filled = sum(1 for action in actions if action.field == "url" and action.status == "filled")
    cover_filled = sum(1 for action in actions if action.field == "cover" and action.status == "filled")
    flagged = sum(1 for action in actions if action.status == "flagged")
    skipped = sum(1 for action in actions if action.status == "skipped")

    lines: list[str] = []
    lines.append("# Link + Cover Fill Report")
    lines.append("")
    lines.append(f"- Generated at: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- Source: `{source_path}`")
    lines.append(f"- Rows scanned: **{rows_scanned}**")
    lines.append(f"- url_filled: **{url_filled}**")
    lines.append(f"- cover_filled: **{cover_filled}**")
    lines.append(f"- flagged: **{flagged}**")
    lines.append(f"- skipped: **{skipped}**")
    lines.append("")

    lines.append("## Actions")
    lines.append("")
    if not actions:
        lines.append("No actions.")
    else:
        for action in actions:
            lines.append(f"### line {action.line_no} — {action.title} [{action.field}]")
            lines.append(f"- Status: {action.status}")
            lines.append(f"- Source: {action.source or 'n/a'}")
            lines.append(f"- Confidence: {action.confidence:.3f}")
            lines.append(f"- Value: `{action.proposed_value}`" if action.proposed_value else "- Value: (none)")
            lines.append(f"- Reason: {action.reason}")
            lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> tuple[int, int, int, int, int]:
    source_path = Path(args.source).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    queue_path = Path(args.queue_csv).expanduser().resolve()

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
    required = {"title", "author", "format", "year", "url", "cover", "isbn"}
    missing = [column for column in required if column not in header_cells]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(sorted(missing)))

    title_idx = header_cells.index("title")
    author_idx = header_cells.index("author")
    format_idx = header_cells.index("format")
    year_idx = header_cells.index("year")
    url_idx = header_cells.index("url")
    cover_idx = header_cells.index("cover")
    isbn_idx = header_cells.index("isbn")

    actions: list[ActionRow] = []
    rows_scanned = 0

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
        rows_scanned += 1

        title = normalize_obsidian_title(cells[title_idx])
        author = normalize_author(cells[author_idx])
        year_text = cells[year_idx].strip()
        year_value = int(year_text) if re.fullmatch(r"\d{4}", year_text) else None

        # Fill cover deterministically from ISBN.
        cover_value = cells[cover_idx].strip()
        isbn_value = re.sub(r"[\s-]+", "", cells[isbn_idx].strip()).upper()
        if cover_value:
            actions.append(ActionRow(
                title=title,
                author=author,
                year=year_text,
                field="cover",
                status="skipped",
                proposed_value="",
                source="existing",
                confidence=1.0,
                reason="cover already present",
                line_no=row_idx + 1,
            ))
        elif isbn_value and re.fullmatch(r"(?:[0-9]{13}|[0-9]{9}[0-9X])", isbn_value):
            cover_url = "https://covers.openlibrary.org/b/isbn/" + quote(isbn_value) + "-M.jpg?default=false"
            cells[cover_idx] = cover_url
            actions.append(ActionRow(
                title=title,
                author=author,
                year=year_text,
                field="cover",
                status="filled",
                proposed_value=cover_url,
                source="isbn-openlibrary",
                confidence=1.0,
                reason="filled from isbn",
                line_no=row_idx + 1,
            ))
        else:
            actions.append(ActionRow(
                title=title,
                author=author,
                year=year_text,
                field="cover",
                status="skipped",
                proposed_value="",
                source="none",
                confidence=0.0,
                reason="missing or invalid isbn",
                line_no=row_idx + 1,
            ))

        # Fill URL conservatively from canonical sources only.
        existing_url = cells[url_idx].strip()
        if existing_url:
            actions.append(ActionRow(
                title=title,
                author=author,
                year=year_text,
                field="url",
                status="skipped",
                proposed_value="",
                source="existing",
                confidence=1.0,
                reason="url already present",
                line_no=row_idx + 1,
            ))
            lines[row_idx] = build_row(cells)
            continue

        wiki_candidates = build_wikipedia_candidates(title, author, year_value, args.limit)
        openlibrary_candidates = build_openlibrary_candidates(title, author, year_value, args.limit)
        selected, decision_reason = choose_url_candidate(wiki_candidates, openlibrary_candidates)
        if selected:
            cells[url_idx] = selected.url
            actions.append(ActionRow(
                title=title,
                author=author,
                year=year_text,
                field="url",
                status="filled",
                proposed_value=selected.url,
                source=selected.source,
                confidence=selected.confidence,
                reason=decision_reason + f" ({selected.reason})",
                line_no=row_idx + 1,
            ))
        else:
            candidates = wiki_candidates[:TOP_CANDIDATES] + openlibrary_candidates[:TOP_CANDIDATES]
            if candidates:
                best = candidates[0]
                actions.append(ActionRow(
                    title=title,
                    author=author,
                    year=year_text,
                    field="url",
                    status="flagged",
                    proposed_value=best.url,
                    source=best.source,
                    confidence=best.confidence,
                    reason="candidate exists but failed conservative source preference or gates",
                    line_no=row_idx + 1,
                ))
            else:
                actions.append(ActionRow(
                    title=title,
                    author=author,
                    year=year_text,
                    field="url",
                    status="flagged",
                    proposed_value="",
                    source="none",
                    confidence=0.0,
                    reason=decision_reason,
                    line_no=row_idx + 1,
                ))

        lines[row_idx] = build_row(cells)

    source_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_csv(queue_path, actions)
    write_report(report_path, source_path, rows_scanned, actions)

    url_filled = sum(1 for action in actions if action.field == "url" and action.status == "filled")
    cover_filled = sum(1 for action in actions if action.field == "cover" and action.status == "filled")
    flagged = sum(1 for action in actions if action.status == "flagged")
    skipped = sum(1 for action in actions if action.status == "skipped")
    return rows_scanned, url_filled, cover_filled, flagged, skipped


def main() -> int:
    args = parse_args()
    rows_scanned, url_filled, cover_filled, flagged, skipped = run(args)
    print(
        "Rows scanned: "
        f"{rows_scanned} | url_filled: {url_filled} | cover_filled: {cover_filled} | "
        f"flagged: {flagged} | skipped: {skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
