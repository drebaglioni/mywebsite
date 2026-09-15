import json
import unittest
from pathlib import Path


class FlyGambitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repository = Path(__file__).resolve().parents[1]
        cls.page = (cls.repository / "fly-gambit.html").read_text(encoding="utf-8")
        cls.projects = (cls.repository / "projects.html").read_text(encoding="utf-8")
        cls.preview = cls.repository.joinpath(
            "assets", "images", "projects", "preview-fly-gambit.svg"
        ).read_text(encoding="utf-8")
        cls.policy = json.loads(
            (cls.repository / "data" / "fly-gambit-policy.json").read_text(
                encoding="utf-8"
            )
        )
        cls.bridge_proof = json.loads(
            (cls.repository / "data" / "fly-gambit-bridge-proof.json").read_text(
                encoding="utf-8"
            )
        )

    def test_saved_policy_has_consistent_dimensions(self):
        architecture = self.policy["architecture"]
        weights = self.policy["weights"]
        input_size = architecture["inputSize"]
        hidden_size = architecture["hiddenLayers"][0]
        output_size = architecture["outputSize"]

        self.assertEqual(len(weights["w1"]), input_size * hidden_size)
        self.assertEqual(len(weights["b1"]), hidden_size)
        self.assertEqual(len(weights["w2"]), hidden_size * output_size)
        self.assertEqual(len(weights["b2"]), output_size)
        self.assertEqual(len(self.policy["directions"]), output_size)

    def test_saved_policy_meets_quality_floor(self):
        metrics = self.policy["metrics"]
        self.assertGreaterEqual(metrics["testAccuracy"], 0.80)
        self.assertGreaterEqual(metrics["rolloutSuccessRate"], 0.95)

    def test_page_states_the_flybody_boundary(self):
        self.assertIn("receives a structured board layout", self.page)
        self.assertIn("pretrained FlyBody controller", self.page)
        self.assertIn("It does not play chess, learn live", self.page)
        self.assertIn("View experiment evidence", self.page)
        self.assertIn("data/fly-gambit-policy.json", self.repository.joinpath(
            "scripts", "fly-gambit.js"
        ).read_text(encoding="utf-8"))

    def test_page_keeps_the_primary_interface_focused(self):
        self.assertIn("The chessboard is an obstacle course—not a chess game.", self.page)
        self.assertIn("route completion on unseen boards", self.page)
        self.assertEqual(self.page.count('<button class="control'), 2)
        self.assertNotIn("Local sensor crop", self.page)
        self.assertNotIn("Eight motor intentions", self.page)
        self.assertNotIn("Reset trail", self.page)

    def test_flybody_bridge_has_reproducible_verified_proof(self):
        proof = self.bridge_proof
        result = proof["result"]
        self.assertTrue(proof["verified"])
        self.assertTrue(result["terminated"])
        self.assertEqual(result["discount"], 1.0)
        self.assertLessEqual(
            result["finalTargetDistanceCm"], result["successThresholdCm"]
        )
        self.assertEqual(result["distinctPhysicalPoses"], result["environmentSteps"])
        self.assertGreater(result["netDisplacementCm"], 3.5)
        self.assertEqual(proof["perceptionPolicy"]["testAccuracy"], 1.0)
        self.assertTrue(proof["perceptionPolicy"]["exactDefaultBoardMatch"])
        self.assertTrue(proof["arena"]["collisionEnabled"])
        self.assertEqual(proof["arena"]["obstacleContactSteps"], 0)

    def test_bridge_runner_pins_runtime_and_uses_final_distance(self):
        runner = self.repository.joinpath("scripts", "run_flybody_bridge.py").read_text(
            encoding="utf-8"
        )
        dockerfile = self.repository.joinpath(
            "experiments", "flybody-bridge", "Dockerfile"
        ).read_text(encoding="utf-8")
        self.assertIn("distances[-1] <= threshold", runner)
        self.assertIn("copy=True", runner)
        self.assertIn("mujoco.mjtObj.mjOBJ_GEOM", runner)
        self.assertIn("train_camera_perception", runner)
        self.assertIn('"tensorflow==2.15.1"', dockerfile)
        self.assertIn('"mujoco==3.3.3"', dockerfile)

    def test_project_index_links_to_local_experiment(self):
        self.assertIn('data-project="gambit" href="fly-gambit.html"', self.projects)
        self.assertIn('url("assets/images/projects/preview-fly-gambit.svg")', self.projects)
        self.assertTrue(
            self.repository.joinpath(
                "assets", "images", "projects", "preview-fly-gambit.svg"
            ).is_file()
        )

    def test_fly_uses_anatomical_specimen_details(self):
        self.assertIn('class="fly-legs"', self.page)
        self.assertIn('class="fly-wing-vein"', self.page)
        self.assertIn('class="fly-abdomen-band"', self.page)
        self.assertIn('class="fly-bristle"', self.page)
        self.assertEqual(self.page.count('class="fly-leg-joint"'), 6)
        self.assertNotIn('<circle class="fly-eye"', self.page)
        self.assertIn('id="preview-wing"', self.preview)
        self.assertIn("M67 70 48 58 28 45", self.preview)


if __name__ == "__main__":
    unittest.main()
