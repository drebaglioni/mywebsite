import json
import tempfile
import unittest
from pathlib import Path

from scripts.sync_cinema import extract_notes, extract_summary, note_to_item, parse_frontmatter


SAMPLE_NOTE = """---
title: Sample Film
creator: Example Director
format: movie
year: 1999
rating: 4
tags: [cinema, favorite]
---

# Sample Film

> Logged in [[personal/the-cinema]] as a favorite.

## Notes
- First note.
- Second note.
"""


class CinemaSyncTests(unittest.TestCase):
    def test_parses_supported_frontmatter_types(self):
        values = parse_frontmatter(SAMPLE_NOTE)

        self.assertEqual(values["title"], "Sample Film")
        self.assertEqual(values["year"], 1999)
        self.assertEqual(values["rating"], 4)
        self.assertEqual(values["tags"], ["cinema", "favorite"])

    def test_extracts_public_summary_and_notes(self):
        self.assertEqual(
            extract_summary(SAMPLE_NOTE),
            "Logged in the cinema archive as a favorite.",
        )
        self.assertEqual(extract_notes(SAMPLE_NOTE), "First note. Second note.")

    def test_merges_visual_overrides_by_note_slug(self):
        with tempfile.TemporaryDirectory() as directory:
            note_path = Path(directory) / "sample-film.md"
            note_path.write_text(SAMPLE_NOTE, encoding="utf-8")

            item = note_to_item(
                note_path,
                {
                    "sample-film": {
                        "poster": "assets/sample-poster.jpg",
                        "trailer": "abc123",
                    }
                },
            )

        self.assertEqual(item["id"], "sample-film")
        self.assertEqual(item["poster"], "assets/sample-poster.jpg")
        self.assertEqual(item["trailer"], "abc123")
        self.assertEqual(item["notes"], "First note. Second note.")

    def test_published_collection_references_existing_local_assets(self):
        repository = Path(__file__).resolve().parents[1]
        items = json.loads((repository / "data/cinema.json").read_text(encoding="utf-8"))

        self.assertEqual({item["id"] for item in items}, {"the-odyssey", "avatar"})
        for item in items:
            for field in ("poster", "still"):
                self.assertTrue(
                    (repository / item[field]).is_file(),
                    f"{item['id']} is missing {field}: {item[field]}",
                )
            self.assertTrue(item["trailer"])


if __name__ == "__main__":
    unittest.main()
