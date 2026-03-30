#!/usr/bin/env python3
"""Conservative cover repair for Obsidian library export table.

This pass repairs only missing/invalid cover values for book-like entries:
- keeps valid existing covers unchanged
- tries OpenLibrary ISBN cover first
- falls back to Amazon search image URL if matching gates pass
"""

from __future__ import annotations

import argparse
import csv
import html
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
from urllib.request import Request, urlopen

TABLE_SECTION_HEADER = "## export"
DEFAULT_REPORT_PATH = ".context/cover_backfill_report.md"
DEFAULT_QUEUE_CSV_PATH = ".context/cover_backfill_review_queue.csv"
AMAZON_SEARCH_URL = "https://www.amazon.com/s"
OPENLIB_COVER_TEMPLATE = "https://covers.openlibrary.org/b/isbn/{isbn}-M.jpg?default=false"
VALID_BOOK_FORMATS = {"book", "ebook", "audiobook"}


@dataclass
class UrlCheck:
    ok: bool
    status: int | None
    content_type: str
    final_url: str
    reason: str


@dataclass
class CoverCandidate:
    url: str
    source: str
    confidence: float
    title_similarity: float
    author_overlap: float
    reason: str


@dataclass
class CoverAction:
    item_id: str
    title: str
    author: str
    isbn: str
    old_cover: str
    new_cover: str
    status: str
    source: str
    confidence: float
    reason: str
    line_no: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair broken/missing covers in Obsidian Library export table")
    parser.add_argument("--source", required=True, help="Absolute path to Obsidian Library.md")
    parser.add_argument("--report", default=DEFAULT_REPORT_PATH, help="Markdown report output path")
    parser.add_argument("--queue-csv", default=DEFAULT_QUEUE_CSV_PATH, help="CSV output path")
    parser.add_argument("--http-timeout", type=float, default=8.0, help="HTTP timeout in seconds")
    parser.add_argument("--http-retries", type=int, default=2, help="HTTP retries per request")
    parser.add_argument("--amazon-limit", type=int, default=8, help="Max valid Amazon image candidates to evaluate")
    parser.add_argument("--title-similarity-min", type=float, default=0.95, help="Min normalized title similarity")
    parser.add_argument("--author-overlap-min", type=float, default=0.80, help="Min author token overlap")
    return parser.parse_args()


def split_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError("Table row must start and end with '|' ")

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


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "entry"


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
    return re.split(r"[;,]", author)[0].strip()


def author_token_overlap(author: str, candidate_text: str) -> float:
    base = normalize_for_match(primary_author(author))
    if not base:
        return 1.0
    candidate_norm = normalize_for_match(candidate_text)
    if not candidate_norm:
        return 0.0

    base_tokens = [token for token in base.split() if token]
    if not base_tokens:
        return 1.0
    candidate_tokens = set(candidate_norm.split())
    matched = sum(1 for token in base_tokens if token in candidate_tokens)
    return matched / len(base_tokens)


def normalize_isbn(value: str) -> str:
    return re.sub(r"[\s-]+", "", (value or "").strip()).upper()


def fetch_url(url: str, timeout: float, retries: int, max_bytes: int = 1200000) -> tuple[int | None, str, str, bytes]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for attempt in range(1, retries + 1):
        try:
            request = Request(url, headers=headers, method="GET")
            with urlopen(request, timeout=timeout) as response:
                status = getattr(response, "status", None) or response.getcode()
                content_type = (response.headers.get("Content-Type") or "").lower()
                final_url = response.geturl()
                body = response.read(max_bytes)
                return status, content_type, final_url, body
        except HTTPError as exc:
            status = exc.code
            content_type = (exc.headers.get("Content-Type") or "").lower()
            final_url = exc.geturl() if hasattr(exc, "geturl") else url
            if 400 <= status < 500 and status not in {408, 429}:
                return status, content_type, final_url, b""
            if attempt >= retries:
                return status, content_type, final_url, b""
            time.sleep(0.35 * attempt)
        except (URLError, TimeoutError, socket.timeout, ConnectionResetError, OSError):
            if attempt >= retries:
                return None, "", url, b""
            time.sleep(0.35 * attempt)

    return None, "", url, b""


def validate_cover_image(url: str, timeout: float, retries: int) -> UrlCheck:
    raw = (url or "").strip()
    if not raw:
        return UrlCheck(False, None, "", "", "missing_cover")

    status, content_type, final_url, _ = fetch_url(raw, timeout=timeout, retries=retries, max_bytes=262144)
    if status != 200:
        if status is None:
            return UrlCheck(False, status, content_type, final_url, "network_error")
        return UrlCheck(False, status, content_type, final_url, f"http_{status}")

    if not content_type.startswith("image/"):
        return UrlCheck(False, status, content_type, final_url, "non_image_content_type")

    return UrlCheck(True, status, content_type, final_url, "valid")


def strip_tags(value: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def build_openlibrary_cover_candidate(isbn: str, timeout: float, retries: int) -> CoverCandidate | None:
    normalized_isbn = normalize_isbn(isbn)
    if not re.fullmatch(r"(?:[0-9]{13}|[0-9]{9}[0-9X])", normalized_isbn):
        return None

    cover_url = OPENLIB_COVER_TEMPLATE.format(isbn=quote(normalized_isbn))
    check = validate_cover_image(cover_url, timeout=timeout, retries=retries)
    if not check.ok:
        return None

    return CoverCandidate(
        url=cover_url,
        source="openlibrary",
        confidence=1.0,
        title_similarity=1.0,
        author_overlap=1.0,
        reason="openlibrary_isbn_image_ok",
    )


def parse_amazon_image_candidates(search_html: str) -> list[tuple[str, str, str]]:
    candidates: list[tuple[str, str, str]] = []
    seen_urls: set[str] = set()

    img_pattern = re.compile(
        r'<img[^>]+src="(https://m\.media-amazon\.com/images/I/[^"]+)"[^>]*>',
        flags=re.IGNORECASE,
    )

    for match in img_pattern.finditer(search_html):
        src = html.unescape(match.group(1)).replace("&amp;", "&").strip()
        tag = match.group(0)
        alt_match = re.search(r'alt="([^"]*)"', tag, flags=re.IGNORECASE)
        alt = html.unescape(alt_match.group(1)).strip() if alt_match else ""

        if not src or src in seen_urls:
            continue
        seen_urls.add(src)

        start = max(0, match.start() - 1200)
        end = min(len(search_html), match.end() + 1200)
        context_text = strip_tags(search_html[start:end])
        candidates.append((src, alt, context_text))

    return candidates


def build_amazon_cover_candidate(
    title: str,
    author: str,
    timeout: float,
    retries: int,
    limit: int,
    title_similarity_min: float,
    author_overlap_min: float,
) -> CoverCandidate | None:
    query = " ".join(part for part in [title, primary_author(author)] if part).strip()
    if not query:
        return None

    params = {
        "i": "stripbooks",
        "k": query,
    }
    search_url = AMAZON_SEARCH_URL + "?" + urlencode(params)
    status, content_type, _, body = fetch_url(search_url, timeout=timeout, retries=retries, max_bytes=2_500_000)
    if status != 200 or "html" not in content_type:
        return None

    search_html = body.decode("utf-8", "ignore")
    parsed = parse_amazon_image_candidates(search_html)
    if not parsed:
        return None

    ranked: list[CoverCandidate] = []
    for raw_url, alt_text, context_text in parsed:
        candidate_title_source = alt_text or context_text
        sim = title_similarity(title, candidate_title_source)
        if sim < title_similarity_min:
            continue

        overlap = author_token_overlap(author, (alt_text + " " + context_text).strip())
        if primary_author(author) and overlap < author_overlap_min:
            continue

        check = validate_cover_image(raw_url, timeout=timeout, retries=retries)
        if not check.ok:
            continue

        confidence = (sim * 0.78) + (overlap * 0.22)
        ranked.append(CoverCandidate(
            url=raw_url,
            source="amazon",
            confidence=confidence,
            title_similarity=sim,
            author_overlap=overlap,
            reason="amazon_search_image_ok",
        ))

        if len(ranked) >= limit:
            break

    if not ranked:
        return None

    ranked.sort(key=lambda item: item.confidence, reverse=True)
    return ranked[0]


def write_review_csv(path: Path, actions: list[CoverAction]) -> None:
    status_rank = {
        "flagged": 0,
        "filled_amazon": 1,
        "filled_openlibrary": 2,
        "valid_skip": 3,
    }
    ordered = sorted(
        actions,
        key=lambda action: (
            status_rank.get(action.status, 9),
            -action.confidence,
            action.title.lower(),
            action.item_id,
        ),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "title", "author", "isbn", "old_cover", "new_cover", "status", "source", "confidence", "reason"])
        for action in ordered:
            writer.writerow([
                action.item_id,
                action.title,
                action.author,
                action.isbn,
                action.old_cover,
                action.new_cover,
                action.status,
                action.source,
                f"{action.confidence:.3f}",
                action.reason,
            ])


def write_report(path: Path, source_path: Path, rows_scanned: int, targeted: int, actions: list[CoverAction]) -> None:
    valid_skip = sum(1 for action in actions if action.status == "valid_skip")
    filled_amazon = sum(1 for action in actions if action.status == "filled_amazon")
    filled_openlibrary = sum(1 for action in actions if action.status == "filled_openlibrary")
    flagged = sum(1 for action in actions if action.status == "flagged")

    lines: list[str] = []
    lines.append("# Cover Backfill Report")
    lines.append("")
    lines.append(f"- Generated at: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- Source: `{source_path}`")
    lines.append(f"- Rows scanned (book-like): **{rows_scanned}**")
    lines.append(f"- Rows targeted (missing/invalid): **{targeted}**")
    lines.append(f"- valid_skip: **{valid_skip}**")
    lines.append(f"- filled_openlibrary: **{filled_openlibrary}**")
    lines.append(f"- filled_amazon: **{filled_amazon}**")
    lines.append(f"- flagged: **{flagged}**")
    lines.append("")

    lines.append("## Actions")
    lines.append("")
    if not actions:
        lines.append("No actions.")
    else:
        for action in actions:
            lines.append(f"### line {action.line_no} — {action.item_id}")
            lines.append(f"- Title: {action.title}")
            lines.append(f"- Status: {action.status}")
            lines.append(f"- Source: {action.source}")
            lines.append(f"- Confidence: {action.confidence:.3f}")
            lines.append(f"- Old cover: `{action.old_cover}`" if action.old_cover else "- Old cover: (empty)")
            lines.append(f"- New cover: `{action.new_cover}`" if action.new_cover else "- New cover: (none)")
            lines.append(f"- Reason: {action.reason}")
            lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> tuple[int, int, int, int, int]:
    source_path = Path(args.source).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve()
    csv_path = Path(args.queue_csv).expanduser().resolve()

    lines = source_path.read_text(encoding="utf-8").splitlines()

    export_index = -1
    for idx, line in enumerate(lines):
        if line.strip().lower() == TABLE_SECTION_HEADER:
            export_index = idx
            break
    if export_index == -1:
        raise ValueError("Missing '## Export' section")

    table_indexes: list[int] = []
    for idx in range(export_index + 1, len(lines)):
        current = lines[idx]
        if current.strip().startswith("#") and table_indexes:
            break
        if "|" in current:
            table_indexes.append(idx)

    if len(table_indexes) < 2:
        raise ValueError("Could not find export table")

    header_cells = [cell.lower() for cell in split_row(lines[table_indexes[0]])]
    required = {"title", "author", "format", "year", "cover", "isbn"}
    missing = [column for column in required if column not in header_cells]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(sorted(missing)))

    title_idx = header_cells.index("title")
    author_idx = header_cells.index("author")
    format_idx = header_cells.index("format")
    year_idx = header_cells.index("year")
    cover_idx = header_cells.index("cover")
    isbn_idx = header_cells.index("isbn")

    actions: list[CoverAction] = []
    rows_scanned = 0
    targeted = 0

    for row_idx in table_indexes[2:]:
        raw_line = lines[row_idx]
        if not raw_line.strip():
            continue

        cells = split_row(raw_line)
        if len(cells) != len(header_cells):
            continue

        item_format = cells[format_idx].strip().lower()
        if item_format not in VALID_BOOK_FORMATS:
            continue

        rows_scanned += 1
        title = normalize_obsidian_title(cells[title_idx])
        author = normalize_author(cells[author_idx])
        year_text = cells[year_idx].strip()
        item_id = f"{slugify(title)}-{year_text}" if year_text else slugify(title)

        old_cover = cells[cover_idx].strip()
        isbn = normalize_isbn(cells[isbn_idx])

        health = validate_cover_image(old_cover, timeout=args.http_timeout, retries=args.http_retries)
        if health.ok:
            actions.append(CoverAction(
                item_id=item_id,
                title=title,
                author=author,
                isbn=isbn,
                old_cover=old_cover,
                new_cover=old_cover,
                status="valid_skip",
                source="existing",
                confidence=1.0,
                reason="existing_cover_valid_image",
                line_no=row_idx + 1,
            ))
            continue

        targeted += 1
        reason_prefix = health.reason

        replacement: CoverCandidate | None = build_openlibrary_cover_candidate(
            isbn,
            timeout=args.http_timeout,
            retries=args.http_retries,
        )

        if replacement:
            cells[cover_idx] = replacement.url
            lines[row_idx] = build_row(cells)
            actions.append(CoverAction(
                item_id=item_id,
                title=title,
                author=author,
                isbn=isbn,
                old_cover=old_cover,
                new_cover=replacement.url,
                status="filled_openlibrary",
                source=replacement.source,
                confidence=replacement.confidence,
                reason=f"{reason_prefix} -> {replacement.reason}",
                line_no=row_idx + 1,
            ))
            continue

        amazon_candidate = build_amazon_cover_candidate(
            title=title,
            author=author,
            timeout=args.http_timeout,
            retries=args.http_retries,
            limit=args.amazon_limit,
            title_similarity_min=args.title_similarity_min,
            author_overlap_min=args.author_overlap_min,
        )

        if amazon_candidate:
            cells[cover_idx] = amazon_candidate.url
            lines[row_idx] = build_row(cells)
            actions.append(CoverAction(
                item_id=item_id,
                title=title,
                author=author,
                isbn=isbn,
                old_cover=old_cover,
                new_cover=amazon_candidate.url,
                status="filled_amazon",
                source=amazon_candidate.source,
                confidence=amazon_candidate.confidence,
                reason=f"{reason_prefix} -> {amazon_candidate.reason}",
                line_no=row_idx + 1,
            ))
            continue

        actions.append(CoverAction(
            item_id=item_id,
            title=title,
            author=author,
            isbn=isbn,
            old_cover=old_cover,
            new_cover="",
            status="flagged",
            source="none",
            confidence=0.0,
            reason=f"{reason_prefix}; no candidate passed gates (or low_confidence_match)",
            line_no=row_idx + 1,
        ))

    source_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    write_review_csv(csv_path, actions)
    write_report(report_path, source_path, rows_scanned, targeted, actions)

    valid_skip = sum(1 for action in actions if action.status == "valid_skip")
    filled_amazon = sum(1 for action in actions if action.status == "filled_amazon")
    filled_openlibrary = sum(1 for action in actions if action.status == "filled_openlibrary")
    flagged = sum(1 for action in actions if action.status == "flagged")
    return rows_scanned, valid_skip, filled_openlibrary, filled_amazon, flagged


def main() -> int:
    args = parse_args()
    rows_scanned, valid_skip, filled_openlibrary, filled_amazon, flagged = run(args)
    print(
        "Rows scanned: "
        f"{rows_scanned} | valid_skip: {valid_skip} | filled_openlibrary: {filled_openlibrary} | "
        f"filled_amazon: {filled_amazon} | flagged: {flagged}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
