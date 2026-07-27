import re
import unittest
from pathlib import Path


class ProjectsPageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repository = Path(__file__).resolve().parents[1]
        cls.html = (cls.repository / "projects.html").read_text(encoding="utf-8")

    def test_every_project_node_has_preview_css_and_animation_state(self):
        nodes = set(re.findall(r'data-project="([^"]+)"', self.html))
        preview_styles = set(
            re.findall(r'\.figure-field\[data-active="([^"]+)"\]', self.html)
        )
        animation_states = set(
            re.findall(r"^\s{12}([a-z]+): \{ x:", self.html, flags=re.MULTILINE)
        )

        self.assertEqual(nodes, preview_styles)
        self.assertEqual(nodes, animation_states)

    def test_cinema_project_links_to_page_and_uses_local_preview(self):
        self.assertRegex(
            self.html,
            r'class="project-node cinema"[^>]+href="cinema\.html"',
        )
        self.assertIn(
            'url("assets/images/projects/preview-cinema.jpg")',
            self.html,
        )
        self.assertTrue(
            (self.repository / "assets/images/projects/preview-cinema.jpg").is_file()
        )


if __name__ == "__main__":
    unittest.main()
