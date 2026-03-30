# Library Sync From Obsidian

Use this workflow to update `other.html` library content once a week.

## 1) Prepare your Obsidian export note
Create or update one note (for example: `Library Export.md`) with this structure:

```md
# Library

## Export

| title | author | format | subjects | year | status | finished | rating | url | cover | isbn | notes |
|---|---|---|---|---:|---|---|---:|---|---|---|---|
| Designing Programs | Karl Gerstner | book | design;systems | 2025 | completed | 2025-11-18 | 5 | https://example.com |  | 9780262046305 | Modular systems reference |
| As We May Think | Vannevar Bush | article | computing;history | 2024 | completed | 2024-09-03 | 4 | https://www.theatlantic.com/magazine/archive/1945/07/as-we-may-think/303881/ |  |  | Foundational essay |
| Se una notte d'inverno un viaggiatore | Italo Calvino | book | literature;italian | 1979 | completed | 2026-03-27 |  | https://it.wikipedia.org/wiki/Se_una_notte_d%27inverno_un_viaggiatore | https://example.com/calvino-italian-cover.jpg |  | Manual Italian cover override |
```

## 2) Field requirements
- `title`: required text.
- `author`: optional text.
- `format`: required, must be `book`, `ebook`, `article`, `podcast`, or `audiobook`.
- `subjects`: optional, semicolon-separated values (`design;systems`).
- `year`: required, 4-digit year (`2026`).
- `status`: required, one of `completed`, `in-progress`, `queued`.
- `finished`: optional, `YYYY-MM-DD`.
- `rating`: optional integer `1-5`.
- `url`: optional `http://` or `https://` link.
- `cover`: optional manual override.
  - `https://...` remote image URL, or
  - relative local path (for example `assets/images/library/covers/calvino.jpg`).
- `isbn`: optional ISBN-10 or ISBN-13 (`9780143120537` or `0143120530`).
- `notes`: optional text.

Important:
- Required columns must be present exactly as named.
- Optional columns `cover` and `isbn` are supported.
- `ebook` is accepted and normalized to `book` in `data/library.json`.
- Do not put `|` characters inside cell text.

Cover precedence:
1. `cover` (manual override)
2. `isbn` (lookup path)
3. automatic source resolution

## 3) Run sync command
From repo root:

```bash
python3 scripts/sync_library.py --source "/absolute/path/to/Library Export.md" --out data/library.json
```

If the source is valid, the script prints how many entries were synced.
If not, it shows an exact row-level error.

## 4) Publish
After sync:

```bash
git add data/library.json
git commit -m "Update library from Obsidian"
git push origin main
```

GitHub Pages will rebuild and publish automatically.
