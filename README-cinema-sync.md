# Cinema Sync From Obsidian

The Cinema page reads `data/cinema.json`. Generate it from the Markdown notes in the Obsidian Cinema folder:

```bash
python3 scripts/sync_cinema.py
```

The default source is:

```text
/Users/andrea/Obsidian/Aristotle/personal/cinema
```

Use `--source` to sync another folder:

```bash
python3 scripts/sync_cinema.py --source "/absolute/path/to/cinema"
```

Each note contributes its frontmatter, summary blockquote, and `## Notes` section. Artwork, trailer IDs, release-year corrections, and accent colors live in `data/cinema-overrides.json`, keyed by the note filename.

After adding a new Cinema note:

1. Add its visual metadata to `data/cinema-overrides.json`.
2. Run `python3 scripts/sync_cinema.py`.
3. Preview `cinema.html`.
4. Commit the updated JSON and any new artwork.
